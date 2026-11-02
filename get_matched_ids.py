import os
import gc
import re
import time
import ctypes
from typing import List

import pandas as pd
import psutil
from src.db_connexion.data_extraction import connect_db, load_query
from src.code.name_normalisation import exact_normalization_series, PUNCT_ALNUM_RE
from utils.config import BATCH_SIZE


# ---------- Utilities ----------

def debug_mem(msg: str = "") -> None:
    """
    Print current process memory usage (Resident Set Size) in MB.
    Useful for debugging memory leaks or tracking memory growth.
    """
    rss_mb = psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)
    print(f"[MEM] {msg} {rss_mb:.1f} MB")


def hard_gc() -> None:
    """
    Force an explicit garbage collection cycle.
    Useful in long loops (streaming, big DataFrames) to reclaim memory earlier.
    """
    gc.collect()
    ctypes.pythonapi.PyGC_Collect()


# ---------- Extraction of companies ----------

@connect_db
def stream_companies_by_country(conn, country: str, batch_size: int = BATCH_SIZE):
    """
    Streams companies for a given country using queries/fetch_companies_for_country.sql.
    - Iterates in batches (default BATCH_SIZE : see utils/config.py).
    - Keeps columns : Id, Name, LegalStatus, HistoricSinceDate, NrLinks, Branch, Person, Foreign
    - Adds:
        • Name_NO_LEGAL : company name without legal form,
        • Name_KEY      : compact key for exact matching.
    - Normalization is applied once per unique name per batch.
    """
    print(f"[FETCH] {country} | batch={batch_size}")
    sql_tpl = load_query("queries/fetch_companies_for_country_fake.sql")

    last_id = ""
    total_loaded = 0

    while True:
        sql = sql_tpl.format(country=country, last_id=last_id, batch_size=batch_size)
        t0 = time.time()

        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()

        if not rows:
            break

        df = pd.DataFrame.from_records(rows, columns=cols)

        # Normalisation
        names = df["Name"].fillna("").astype(str)
        uniq = names.unique()
        norm = exact_normalization_series(pd.Series(uniq), already_clean=False, country_code=country)
        map_no_legal = pd.Series(norm["no_legal"].values, index=uniq)
        map_key = pd.Series(norm["compact"].values, index=uniq)

        df["Name_NO_LEGAL"] = names.map(map_no_legal)
        df["Name_KEY"] = names.map(map_key)

        total_loaded += len(df)

        dt = time.time() - t0
        print(f"[FETCH] {country} loaded_total={total_loaded} rows_in_batch={len(df)} in {dt:.2f}s")

        drop_cols = [c for c in ("Branch", "Person", "Foreign") if c in df.columns]
        yield df.drop(columns=drop_cols).reset_index(drop=True)

        last_id = str(df["Id"].iloc[-1])

        hard_gc()


# ---------- Preparation of cases (key + useful columns) ----------

def build_cases_table(df_cases_country: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Builds the cases table for a given country.
    Produces:
        • Key          : compact normalized key,
        • OriginalId
        • Name
        • CleanedName  : fallback = no_legal,
        • CaseNoLegal  : no_legal côté "case".
    """
    names = df_cases_country["Name"].fillna("").astype(str)
    uniq = names.unique()

    norm = exact_normalization_series(pd.Series(uniq), already_clean=False, country_code=country)
    map_no_legal = pd.Series(norm["no_legal"].values, index=uniq)
    map_key = pd.Series(norm["compact"].values, index=uniq)

    no_legal = names.map(map_no_legal)
    key = names.map(map_key)

    cleaned = (
        df_cases_country["CleanedName"]
        if "CleanedName" in df_cases_country.columns
        else no_legal
    )

    out = pd.DataFrame({
        "Key": key,
        "OriginalId": df_cases_country["OriginalId"].astype(str),
        "Name": names,
        "CleanedName": cleaned,
        "CaseNoLegal": no_legal,
    })
    return out[out["Key"] != ""].reset_index(drop=True)


# ---------- OPTIMIZED: Link-based propagation ----------

@connect_db
def propagate_via_links_and_crossref(conn, df_cases: pd.DataFrame, country: str,
                                     already_matched_ids: set) -> pd.DataFrame:
    """
    Logique bidirectionnelle optimisée :
    - Si notre ID est ParentId : cherche autres ParentIds des mêmes ChildIds avec mêmes %
    - Si notre ID est ChildId : cherche autres ChildIds des mêmes ParentIds avec mêmes %
    - Filtre EXISTS appliqué directement dans les CTEs pour meilleures performances
    """
    df_cases_clean = df_cases.copy()
    df_cases_clean["OriginalId_Clean"] = df_cases_clean["OriginalId"].astype(str).str.rstrip("!")

    already_matched_ids_clean = {str(id_val).rstrip("!") for id_val in already_matched_ids}

    df_cases_unmatched = df_cases_clean[
        ~df_cases_clean["OriginalId_Clean"].isin(already_matched_ids_clean)
    ].copy()

    if df_cases_unmatched.empty:
        return pd.DataFrame(columns=[
            "OriginalId", "Name", "CleanedName", "CaseNoLegal",
            "MatchedId", "MatchedName", "MatchedNoLegal", "Rule", "isFromCrossRef"
        ])

    t_start = time.time()

    case_ids = df_cases_unmatched["OriginalId"].unique().tolist()
    df_cases_unmatched["Name"] = df_cases_unmatched["Name"].fillna("UNKNOWN")
    case_names_map = df_cases_unmatched.set_index("OriginalId")["Name"].to_dict()

    temp_table_ids = f"#TempCaseIds_{int(time.time())}"
    temp_table_names = f"#TempCaseNames_{int(time.time())}"

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE {temp_table_ids} (
                    CaseId VARCHAR(50) NOT NULL PRIMARY KEY CLUSTERED
                )
            """)

            cur.execute(f"""
                CREATE TABLE {temp_table_names} (
                    CaseId VARCHAR(50) NOT NULL,
                    CaseName NVARCHAR(500) NOT NULL
                )
            """)
            cur.execute(f"CREATE CLUSTERED INDEX IX_Names ON {temp_table_names}(CaseId, CaseName)")
            cur.execute(f"CREATE NONCLUSTERED INDEX IX_Name ON {temp_table_names}(CaseName) INCLUDE (CaseId)")

            batch_size = 1000
            for i in range(0, len(case_ids), batch_size):
                batch = case_ids[i:i + batch_size]
                values = ",".join([f"('{cid}')" for cid in batch])
                cur.execute(f"INSERT INTO {temp_table_ids} VALUES {values}")

            for i in range(0, len(case_ids), batch_size):
                batch = case_ids[i:i + batch_size]
                values_list = []
                for cid in batch:
                    name = case_names_map.get(cid, "UNKNOWN")
                    if pd.notna(name) and str(name).strip():
                        name_clean = str(name).replace("'", "''")
                    else:
                        name_clean = "UNKNOWN"
                    values_list.append(f"('{cid}', N'{name_clean}')")

                if values_list:
                    values = ",".join(values_list)
                    cur.execute(f"INSERT INTO {temp_table_names} VALUES {values}")

            conn.commit()
            cur.execute(f"UPDATE STATISTICS {temp_table_ids}")
            cur.execute(f"UPDATE STATISTICS {temp_table_names}")

            t_sql = time.time()

            sql_combined = f"""
            WITH CaseAsChild AS (
                SELECT DISTINCT
                    t.CaseId AS OriginalId,
                    tn.CaseName AS CaseName,
                    l2.ChildId AS CandidateId,
                    'AsChild' AS Direction
                FROM {temp_table_ids} t
                JOIN {temp_table_names} tn ON tn.CaseId = t.CaseId
                JOIN bvdaffils_yesterday.dbo.Links l1 WITH (NOLOCK) 
                    ON l1.ChildId = t.CaseId 
                    AND l1.Type = 1
                JOIN bvdaffils_yesterday.dbo.Links l2 WITH (NOLOCK)
                    ON l2.ParentId = l1.ParentId 
                    AND l2.Type = 1
                    AND l2.ChildId != t.CaseId
                    AND l2.ChildId != l1.ChildId
                    AND LEFT(l2.ChildId, 2) = '{country}'
                    AND ISNULL(l2.DirectPercent, -999.0) = ISNULL(l1.DirectPercent, -999.0)
                    AND ISNULL(l2.TotalPercent, -999.0) = ISNULL(l1.TotalPercent, -999.0)
                    AND ISNULL(l2.DirectPercentIndic, 'NULL') = ISNULL(l1.DirectPercentIndic, 'NULL')
                    AND ISNULL(l2.TotalPercentIndic, 'NULL') = ISNULL(l1.TotalPercentIndic, 'NULL')
                    AND l2.ChildId NOT LIKE '%*%'
                    AND l2.ChildId NOT LIKE '%PE%'
                    AND l2.ChildId NOT LIKE '%SCRL%'
                    AND l2.ChildId NOT LIKE '%CEBI%'
                    AND l2.ChildId NOT LIKE '%CBNB%'
                    AND l2.ChildId NOT LIKE '%CRL[ABC]%'
                    AND l2.ChildId NOT LIKE '%CSTAT%'
                    AND l2.ChildId NOT LIKE '%CFACT%'
                    AND l2.ChildId NOT LIKE '%CWOR%'
                    AND l2.ChildId NOT LIKE '%CSINO%'
                    AND l2.ChildId NOT LIKE '%CPEI%'
                    AND l2.ChildId NOT LIKE '%COFWI%'
                    AND l2.ChildId NOT LIKE '%CWIN%'
                    AND l2.ChildId NOT LIKE '%CVEDA%'
                    AND l2.ChildId NOT LIKE '%CZEF%'
                    AND l2.ChildId NOT LIKE '%CSIREN%'
                    AND l2.ChildId NOT LIKE '%CNIF%'
                    AND l2.ChildId NOT LIKE '%CFS%'
                    AND l2.ChildId NOT LIKE '%CLEI%'
                    AND l2.ChildId NOT LIKE '%CBOE%'
                WHERE (
                    SELECT COUNT(*)
                    FROM bvdaffils_yesterday.dbo.CrossRef cr WITH (NOLOCK)
                    WHERE cr.NewId = l2.ChildId
                      AND cr.Name = tn.CaseName
                      AND cr.UserID NOT IN ('SASH', 'PSIN')
                ) >= 2
            ),
            CaseAsParent AS (
                SELECT DISTINCT
                    t.CaseId AS OriginalId,
                    tn.CaseName AS CaseName,
                    l2.ParentId AS CandidateId,
                    'AsParent' AS Direction
                FROM {temp_table_ids} t
                JOIN {temp_table_names} tn ON tn.CaseId = t.CaseId
                JOIN bvdaffils_yesterday.dbo.Links l1 WITH (NOLOCK)
                    ON l1.ParentId = t.CaseId 
                    AND l1.Type = 1
                JOIN bvdaffils_yesterday.dbo.Links l2 WITH (NOLOCK)
                    ON l2.ChildId = l1.ChildId 
                    AND l2.Type = 1
                    AND l2.ParentId != t.CaseId
                    AND LEFT(l2.ParentId, 2) = '{country}'
                    AND ISNULL(l2.DirectPercent, -999.0) = ISNULL(l1.DirectPercent, -999.0)
                    AND ISNULL(l2.TotalPercent, -999.0) = ISNULL(l1.TotalPercent, -999.0)
                    AND ISNULL(l2.DirectPercentIndic, 'NULL') = ISNULL(l1.DirectPercentIndic, 'NULL')
                    AND ISNULL(l2.TotalPercentIndic, 'NULL') = ISNULL(l1.TotalPercentIndic, 'NULL')
                    AND l2.ParentId NOT LIKE '%*%'
                    AND l2.ParentId NOT LIKE '%PE%'
                    AND l2.ParentId NOT LIKE '%SCRL%'
                    AND l2.ParentId NOT LIKE '%CEBI%'
                    AND l2.ParentId NOT LIKE '%CBNB%'
                    AND l2.ParentId NOT LIKE '%CRL[ABC]%'
                    AND l2.ParentId NOT LIKE '%CSTAT%'
                    AND l2.ParentId NOT LIKE '%CFACT%'
                    AND l2.ParentId NOT LIKE '%CWOR%'
                    AND l2.ParentId NOT LIKE '%CSINO%'
                    AND l2.ParentId NOT LIKE '%CPEI%'
                    AND l2.ParentId NOT LIKE '%COFWI%'
                    AND l2.ParentId NOT LIKE '%CWIN%'
                    AND l2.ParentId NOT LIKE '%CVEDA%'
                    AND l2.ParentId NOT LIKE '%CZEF%'
                    AND l2.ParentId NOT LIKE '%CSIREN%'
                    AND l2.ParentId NOT LIKE '%CNIF%'
                    AND l2.ParentId NOT LIKE '%CFS%'
                    AND l2.ParentId NOT LIKE '%CLEI%'
                    AND l2.ParentId NOT LIKE '%CBOE%'
                WHERE (
                    SELECT COUNT(*)
                    FROM bvdaffils_yesterday.dbo.CrossRef cr WITH (NOLOCK)
                    WHERE cr.NewId = l2.ChildId
                      AND cr.Name = tn.CaseName
                      AND cr.UserID NOT IN ('SASH', 'PSIN')
                ) >= 2
            )
            SELECT 
                OriginalId,
                CandidateId,
                Direction,
                CaseName
            FROM CaseAsChild
            UNION ALL
            SELECT 
                OriginalId,
                CandidateId,
                Direction,
                CaseName
            FROM CaseAsParent
            OPTION (RECOMPILE, MAXDOP 4);
            """

            cur.execute(sql_combined)
            rows = cur.fetchall()

            cur.execute(f"DROP TABLE {temp_table_ids}")
            cur.execute(f"DROP TABLE {temp_table_names}")
            conn.commit()

            print(f"[LINK_PROPAGATION] SQL executed in {time.time() - t_sql:.2f}s, {len(rows)} raw candidates")

    except Exception as e:
        print(f"[LINK_PROPAGATION] ERROR: {e}")
        import traceback
        traceback.print_exc()

        try:
            with conn.cursor() as cur:
                cur.execute(f"IF OBJECT_ID('tempdb..{temp_table_ids}') IS NOT NULL DROP TABLE {temp_table_ids}")
                cur.execute(f"IF OBJECT_ID('tempdb..{temp_table_names}') IS NOT NULL DROP TABLE {temp_table_names}")
                conn.commit()
        except:
            pass

        return pd.DataFrame(columns=[
            "OriginalId", "Name", "CleanedName", "CaseNoLegal",
            "MatchedId", "MatchedName", "MatchedNoLegal", "Rule", "isFromCrossRef"
        ])

    if not rows:
        return pd.DataFrame(columns=[
            "OriginalId", "Name", "CleanedName", "CaseNoLegal",
            "MatchedId", "MatchedName", "MatchedNoLegal", "Rule", "isFromCrossRef"
        ])

    df_raw = pd.DataFrame.from_records(
        rows,
        columns=["OriginalId", "CandidateId", "Direction", "CaseName"]
    )

    df_raw = df_raw.rename(columns={"CandidateId": "MatchedId"})

    # ⭐ FILTRER LES AUTO-MATCHES IMMÉDIATEMENT
    initial_count = len(df_raw)
    df_raw = df_raw[df_raw["OriginalId"] != df_raw["MatchedId"]].copy()
    auto_removed = initial_count - len(df_raw)
    if auto_removed > 0:
        print(f"[LINK_PROPAGATION] ⚠️ Removed {auto_removed} auto-matches at SQL result level")

    if df_raw.empty:
        return pd.DataFrame(columns=[
            "OriginalId", "Name", "CleanedName", "CaseNoLegal",
            "MatchedId", "MatchedName", "MatchedNoLegal", "Rule", "isFromCrossRef"
        ])

    df_raw = df_raw.drop_duplicates(subset=["OriginalId", "MatchedId"], keep="first")

    print(f"[LINK_PROPAGATION] {len(df_raw)} matches after CrossRef filter")

    df_cases_filtered = df_cases_unmatched[["OriginalId", "Name", "CleanedName", "CaseNoLegal"]]
    matches = df_raw.merge(df_cases_filtered, on="OriginalId", how="inner")

    if matches.empty:
        return pd.DataFrame(columns=[
            "OriginalId", "Name", "CleanedName", "CaseNoLegal",
            "MatchedId", "MatchedName", "MatchedNoLegal", "Rule", "isFromCrossRef"
        ])

    matched_ids = matches["MatchedId"].unique().tolist()
    company_map = {}

    with conn.cursor() as cur:
        for i in range(0, len(matched_ids), 1000):
            batch = matched_ids[i:i + 1000]
            ids_list = ",".join([f"'{mid}'" for mid in batch])
            cur.execute(f"""
                SELECT Id, Name 
                FROM bvdaffils_yesterday.dbo.Companies WITH (NOLOCK)
                WHERE Id IN ({ids_list})
            """)
            company_rows = cur.fetchall()
            company_map.update({row[0]: row[1] for row in company_rows})

    matches["MatchedName"] = matches["MatchedId"].map(company_map)

    comp_names = matches["MatchedName"].fillna("").astype(str).unique()
    comp_norm = exact_normalization_series(pd.Series(comp_names), already_clean=False, country_code=country)
    comp_map_no_legal = pd.Series(comp_norm["no_legal"].values, index=comp_names)
    matches["MatchedNoLegal"] = matches["MatchedName"].map(comp_map_no_legal)

    result = matches[[
        "OriginalId", "Name", "CleanedName", "CaseNoLegal",
        "MatchedId", "MatchedName", "MatchedNoLegal", "Direction"
    ]].copy()

    result["Rule"] = "link_propagation_" + result["Direction"].str.lower()
    result["isFromCrossRef"] = 1
    result.drop(columns=["Direction"], inplace=True)
    result.drop_duplicates(subset=["OriginalId", "MatchedId"], inplace=True)
    result.reset_index(drop=True, inplace=True)

    print(f"[LINK_PROPAGATION] Final: {len(result)} matches (total time: {time.time() - t_start:.2f}s)")

    return result




# ---------- Orchestrateur ----------

@connect_db
def get_matched_ids(conn, df_cases: pd.DataFrame, country: str):
    """
    Exact matching (clé compacte) + propagation des no_legal + link propagation.
    Link propagation s'exécute pour TOUS les IDs (pas seulement non-matchés).
    """
    t0 = time.time()

    if "CountryCode" not in df_cases.columns:
        df_cases = df_cases.assign(CountryCode=df_cases["OriginalId"].astype(str).str[:2])

    df_cases_country = df_cases[df_cases["CountryCode"] == country].copy()
    if df_cases_country.empty:
        return pd.DataFrame(
            columns=[
                "OriginalId", "Name", "CleanedName", "CaseNoLegal",
                "MatchedId", "MatchedName", "MatchedNoLegal", "Rule", "isFromCrossRef"
            ]
        ), 0.0

    cases = build_cases_table(df_cases_country, country)

    exact_matched_original_ids: set[str] = set()
    results: List[pd.DataFrame] = []

    LAST_DASH_TAIL_RE = re.compile(r"\s*-\s*[^-]*$")
    MIN_ROOT_LEN = 3

    for companies in stream_companies_by_country(country):
        if companies.empty:
            continue

        for col in ("Id", "Name", "Name_KEY", "Name_NO_LEGAL"):
            assert col in companies.columns, f"Missing column in companies batch: {col}"

        comp = companies[["Id", "Name", "Name_KEY", "Name_NO_LEGAL"]].rename(
            columns={
                "Name": "MatchedName",
                "Name_KEY": "DBKey",
                "Name_NO_LEGAL": "MatchedNoLegal",
            }
        )

        # A. Exact matching
        joined_exact = cases.merge(comp, left_on="Key", right_on="DBKey", how="inner", copy=False)
        if not joined_exact.empty:
            out_exact = joined_exact[
                ["OriginalId", "Name", "CleanedName", "CaseNoLegal",
                 "Id", "MatchedName", "MatchedNoLegal"]
            ].rename(columns={"Id": "MatchedId"})

            out_exact = out_exact[out_exact["OriginalId"] != out_exact["MatchedId"]].copy()

            if not out_exact.empty:
                out_exact["Rule"] = "exact_normalization"
                out_exact["isFromCrossRef"] = 0
                results.append(out_exact)
                exact_matched_original_ids.update(out_exact["OriginalId"].unique().tolist())

        # B. Fallback PT/BR
        if country == "PT" or country == "BR":
            cases_remaining = cases[~cases["OriginalId"].isin(exact_matched_original_ids)].copy()
            if cases_remaining.empty:
                hard_gc()
                continue

            cases_remaining["CaseKey"] = cases_remaining["Key"].fillna("").astype(str)
            cases_remaining = cases_remaining[cases_remaining["CaseKey"].str.len() >= MIN_ROOT_LEN]
            if cases_remaining.empty:
                hard_gc()
                continue

            comp["MatchedNoLegal"] = comp["MatchedNoLegal"].fillna("").astype(str)

            root_no_legal = comp["MatchedNoLegal"].where(
                ~comp["MatchedNoLegal"].str.contains("-", regex=False),
                comp["MatchedNoLegal"].str.replace(LAST_DASH_TAIL_RE, "", regex=True)
            ).str.strip()

            root_db_key = root_no_legal.str.replace(PUNCT_ALNUM_RE, "", regex=True)

            comp_root = pd.DataFrame({
                "MatchedId": comp["Id"].values,
                "MatchedName": comp["MatchedName"].values,
                "MatchedNoLegal": comp["MatchedNoLegal"].values,
                "RootDBKey": root_db_key.values,
            })

            comp_root = comp_root[comp_root["RootDBKey"].str.len() >= MIN_ROOT_LEN]
            if comp_root.empty:
                hard_gc()
                continue

            joined_root = cases_remaining.merge(
                comp_root, left_on="CaseKey", right_on="RootDBKey", how="inner", copy=False
            )
            if not joined_root.empty:
                out_root = joined_root[
                    ["OriginalId", "Name", "CleanedName", "CaseNoLegal",
                     "MatchedId", "MatchedName", "MatchedNoLegal"]
                ].copy()

                out_root = out_root[out_root["OriginalId"] != out_root["MatchedId"]].copy()

                if not out_root.empty:
                    out_root["Rule"] = "exact_no_legal_root_hyphen_PT"
                    out_root["isFromCrossRef"] = 0
                    results.append(out_root)
                    exact_matched_original_ids.update(out_root["OriginalId"].unique().tolist())

        hard_gc()

    # ⭐ MODIFICATION PRINCIPALE : Link propagation pour TOUS les IDs
    print(f"[LINK_PROPAGATION] Launching for ALL {len(cases)} cases (not just {len(exact_matched_original_ids)} already matched)")

    link_matches = propagate_via_links_and_crossref(
        df_cases=cases,  # ⭐ TOUS les cases
        country=country,
        already_matched_ids=set()  # ⭐ SET VIDE : on ne filtre rien
    )

    if not link_matches.empty:
        print(f"[LINK_PROPAGATION] Found {len(link_matches)} additional matches via links")
        results.append(link_matches)

    # Final concat
    final = pd.concat(results, ignore_index=True) if results else pd.DataFrame(
        columns=[
            "OriginalId", "Name", "CleanedName", "CaseNoLegal",
            "MatchedId", "MatchedName", "MatchedNoLegal", "Rule", "isFromCrossRef"
        ]
    )

    if not final.empty:
        if country == "PT" or country == "BR":
            exact_flag = final.loc[final["Rule"] == "exact_normalization", "OriginalId"].unique()
            is_fb = final["Rule"].astype(str).str.startswith("exact_no_legal_root_hyphen_PT")

            problem_ids = set(final.loc[is_fb, "OriginalId"]).intersection(set(exact_flag))
            if problem_ids:
                print("OriginalIds with both exact and fallback matches:", problem_ids)
                print(final[final["OriginalId"].isin(problem_ids)].sort_values(["OriginalId", "Rule"]))

            final = final[(~final["OriginalId"].isin(exact_flag)) | (~is_fb)]

        # ⭐ DÉDUPLICATION : garder tous les matches uniques par (OriginalId, MatchedId)
        final = final.drop_duplicates(subset=["OriginalId", "MatchedId"], keep="first").reset_index(drop=True)

        # Sécurité finale
        initial_count = len(final)
        final = final[final["OriginalId"] != final["MatchedId"]].copy()
        auto_removed = initial_count - len(final)
        if auto_removed > 0:
            print(f"[FINAL] ⚠️ Removed {auto_removed} auto-matches in final results for {country}")

    return final, time.time() - t0


