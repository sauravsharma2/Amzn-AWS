import sys
import time
import warnings
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List


import os


import numpy as np
import pandas as pd

from run_finalize import finalize_run
from src.code.filter import apply_filters_pipeline, solo_match_uo_then_links_check, solo_shortname_guard, \
     solo_validation
from src.code.get_cases import get_cases, get_cases_from_file
from src.code.get_matched_id_Aida import get_matched_ids_aida
from src.code.get_matched_ids import get_matched_ids
from src.code.name_normalisation import exact_normalization_series
from src.db_connexion.data_extraction import load_query, connect_db


from pathlib import Path

script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir))




OUTPUT_DIR = Path("output")

### Helpers ###

def _insert_blank_rows_between_groups(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    """
    Insert a blank row between groups (no trailing blank row).
    """
    if df.empty:
        return df
    parts = []
    for _, g in df.groupby(group_col, sort=False):
        parts.append(g)
        parts.append(pd.DataFrame({c: [None] for c in df.columns}))
    parts.pop()
    return pd.concat(parts, ignore_index=True)




@connect_db
def _fetch_oldest_link(conn, orig_ids: List[str]) -> pd.DataFrame:
    """
    Returns the oldest link date per OriginalId (YYYYMMDD), considering BOTH:
    - OriginalId as ParentId (subsidiaries / beneficial subsidiaries)
    - OriginalId as ChildId  (shareholders / beneficial owner)
    Uses queries/get_oldest_link.sql.
    """
    ids = [str(x) for x in orig_ids if pd.notna(x)]
    if not ids:
        return pd.DataFrame(columns=["OriginalId", "OldestSourceDate"])

    sql_tpl = load_query("queries/get_oldest_link_date.sql")
    ids_values = ",".join(f"('{x}')" for x in ids)  # VALUES list
    sql = sql_tpl.format(orig_values=ids_values)

    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame(columns=["OriginalId", "OldestSourceDate"])

    return (pd.DataFrame.from_records(rows, columns=cols)
            .astype({"OriginalId": "string", "OldestSourceDate": "string"}))



def run() -> tuple[int, dict]:
    """
    Orchestrates the full matching pipeline and produces:
      - matches.xlsx (sheets: Matching, UniqueMatch, NoMatch)
      - output.txt   (only UniqueMatch as OldID/NewID/UserID)

    High-level steps:
      1) Load cases (SQL) and prepare base columns.
      2) Parallel per-country exact matching (normalized keys).
      3) Deduplicate (prefer current ID over 'ID!').
      4) Apply isolation filters + solo fallback rules.
      5) Normalize names for export.
      6) Export Excel and TXT.
    """

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 0)
    pd.set_option('display.max_colwidth', None)
    """
    s = pd.Series("E-CAPITAL I")
    print(exact_normalization_series(s))
    return
    """
    warnings.filterwarnings("ignore")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- 1) Load + base prep -------------------------------------------------
    print("[1] Fetching cases...")
    df_cases = get_cases_from_file()
    print(f"→ {len(df_cases)} cases retrieved.")

    df_cases = (
        df_cases
        .rename(columns={"Id": "OriginalId"})
        .assign(CleanedName=lambda df: df["Name"])
    )

    df_cases["CountryCode"] = df_cases["OriginalId"].astype(str).str[:2]
    df_cases = df_cases.sort_values(by=["CleanedName", "CountryCode"])

    print(f"[2] {len(df_cases)} cases to process.")
    print("Countries to process:", df_cases["CountryCode"].unique())


    # Country scheduling (priorities first, then the rest)
    priority_countries = []
    all_countries = list(df_cases["CountryCode"].unique())
    sorted_countries = priority_countries + [c for c in all_countries if c not in priority_countries]


    # Exclude specific countries
    excluded_countries = {"AU", "AR", "ZZ", "YY", "WW", "IN", "RU", "US", "CN", "BR", "GB", "IT", "DE", "MX", "PT", "TH", "FR", "ES", "PL", "BE", "CH", "LU", "HK", "NL", "PE", "CO", "RS", "HU", "AL", "AT"}
    #excluded_countries = {}
    sorted_countries = [c for c in sorted_countries if c not in excluded_countries]
    df_cases = df_cases[~df_cases["CountryCode"].isin(excluded_countries)]
    print(f"[INFO] Excluded countries: {', '.join(excluded_countries)}")
    print(f"[INFO] Remaining cases: {len(df_cases)} across {len(sorted_countries)} countries : {sorted_countries}" )


    """
    # Test subset
    test_countries = {"ES", "IT"}
    sorted_countries = [c for c in sorted_countries if c in test_countries]
    print(f"[INFO] Selected countries for test run: {', '.join(sorted_countries) if sorted_countries else '(none)'}")
    """

    # --- 2) Matching per-country -----------------------------
    print("[MATCH] Launching country matching...")
    unique_countries = list(dict.fromkeys(sorted_countries))

    matched_df = pd.DataFrame()

    tCOuntries = time.perf_counter()

    if unique_countries:
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_country = {
                executor.submit(
                    get_matched_ids_aida,
                    df_cases[df_cases["CountryCode"] == country][["OriginalId", "Name", "CleanedName", "CountryCode"]],
                    country
                ): country
                for country in unique_countries
                if not df_cases[df_cases["CountryCode"] == country].empty
            }
            for fut in as_completed(future_to_country):
                country = future_to_country[fut]
                try:
                    df_matched, elapsed = fut.result()
                    print(f"[MATCH] {country} done: {len(df_matched)} matches in {elapsed:.2f}s")
                    if not df_matched.empty:
                        matched_df = pd.concat([matched_df, df_matched], ignore_index=True)
                except Exception as e:
                    print(f"[ERROR] Matching failed for {country}: {e}")
    else:
        print("[MATCH] No countries selected, skipping matching.")

    elapsed_countries = time.perf_counter() - tCOuntries
    hours, rem = divmod(elapsed_countries, 3600)
    minutes, seconds = divmod(rem, 60)

    matched_df_raw = matched_df.copy()

    if "isFromCrossRef" not in matched_df_raw.columns:
        matched_df_raw["isFromCrossRef"] = 0


    # # Chemin relatif vers le fichier blacklist (un dossier avant le script)
    # filepath = os.path.join("..", "BlockedMatchings.txt")
    # df_blacklist = pd.read_csv(filepath, sep="\t", dtype=str, header=0)
    #
    # # Création de colonnes temporaires pour la comparaison (MatchedId sans '!' final)
    # matched_df_raw['MatchedId_clean'] = matched_df_raw['MatchedId'].str.rstrip('!')
    #
    # # Merge avec la colonne nettoyée pour la comparaison
    # merged = matched_df_raw.merge(
    #     df_blacklist,
    #     left_on=['OriginalId', 'MatchedId_clean'],
    #     right_on=['OldID', 'NewID'],
    #     how='left',
    #     indicator=True
    # )
    #
    # # Filtrer les lignes qui ne sont pas dans la blacklist
    # matched_df_raw = merged[merged['_merge'] == 'left_only'][matched_df_raw.columns]
    #
    # # Nettoyer la colonne temporaire si tu veux
    # matched_df_raw.drop(columns=['MatchedId_clean'], inplace=True)
    #
    # print(f"Lignes avant filtrage : {len(matched_df)}")
    # print(f"Lignes après filtrage : {len(matched_df_raw)}")





    print(f"[COUNTRIES AND TIMER] Total: {len(matched_df)} matches in {hours:.0f}h {minutes:.0f}m {seconds:.1f}s "
          f"({elapsed_countries / 60:.2f} min, {elapsed_countries:.1f} s)")


    # --- 3) Deduplicate and filtering pipeline -------------------------------

    if matched_df_raw.empty:
        export_df = pd.DataFrame(columns=[
            "OriginalId", "Name", "NameNormalized", "MatchedId", "MatchedName",
            "IsolationFilter", "isFromCrossRef"
        ])
    else:
        export_df = matched_df_raw.copy()

        # De-dupe rule: prefer current ID over ex-ID (ID!) for the same OriginalId/MatchedId
        export_df["OriginalId"] = export_df["OriginalId"].astype(str)
        export_df["MatchedId"] = export_df["MatchedId"].astype(str)

        s = export_df["MatchedId"]
        export_df["_base"] = s.str.rstrip("!")
        export_df["_is_current"] = ~s.str.endswith("!")

        dup = export_df.duplicated(subset=["OriginalId", "_base"], keep=False)
        if dup.any():
            idx_pref = export_df.loc[dup].groupby(["OriginalId", "_base"])["_is_current"].idxmax()
            keep_idx = pd.Index(idx_pref).union(export_df.index[~dup])
            export_df = export_df.loc[keep_idx]

        export_df = export_df.drop(columns=["_base", "_is_current"])

        # Sets IsolationFilter when a step makes the group unique
        export_df = apply_filters_pipeline(export_df)

        # Solo fallback 1: UO check, then links check (subsidiaries/shareholders)
        t_solo = time.perf_counter()
        export_df = solo_match_uo_then_links_check(export_df)
        print(f"[FILTER] SOLO_UO {time.perf_counter() - t_solo:.3f}s")

        # Ensure CountryCode exists for normalization
        if "CountryCode" not in export_df.columns:
            export_df["CountryCode"] = export_df["OriginalId"].astype(str).str[:2]


        # Normalize names after filtering
        export_df["NameNormalized"] = export_df.apply(
            lambda row: exact_normalization_series(
                pd.Series([row["Name"]]),
                country_code=row["CountryCode"]
            )["compact"].iloc[0],
            axis=1
        )
        # Solo fallback 2: guard for very short normalized names (<=3)
        export_df = solo_shortname_guard(export_df)



        # Solo fallback 3: Solo Validation
        t_solo = time.perf_counter()
        export_df = solo_validation(export_df)
        print(f"[FILTER] SOLO_VALIDATION {time.perf_counter() - t_solo:.3f}s")

        cols = ["OriginalId", "Name", "NameNormalized", "MatchedId", "MatchedName"]
        if "IsolationFilter" in export_df.columns:
            cols.append("IsolationFilter")
        else:
            export_df["IsolationFilter"] = pd.NA
            cols.append("IsolationFilter")

        # ← NOUVEAU : Ajouter isFromCrossRef
        if "isFromCrossRef" in export_df.columns:
            cols.append("isFromCrossRef")
        else:
            export_df["isFromCrossRef"] = 0
            cols.append("isFromCrossRef")

        export_df = export_df[cols].sort_values(["OriginalId", "MatchedId"], kind="stable", ignore_index=True)





    # --- 4) UniqueMatch view --------------------------------------------------
    if export_df.empty:
        unique_df = pd.DataFrame(columns=[
            "OriginalId", "Name", "NameNormalized", "MatchedId", "MatchedName",
            "IsolationFilter", "isFromCrossRef"  # ← AJOUT
        ])
    else:
        counts = export_df["OriginalId"].value_counts()
        single_ids = counts[counts == 1].index
        unique_df = (
            export_df[export_df["OriginalId"].isin(single_ids)]
            .sort_values(["OriginalId", "MatchedId"], kind="stable", ignore_index=True)
        )

        # --- Add the oldest OriginalId link date in UniqueMatch -----------------
    if not unique_df.empty:
        unique_ids = unique_df["OriginalId"].astype(str).unique().tolist()
        oldest = _fetch_oldest_link(unique_ids)

        unique_df = (
            unique_df.merge(oldest, on="OriginalId", how="left", validate="one_to_one")
            .assign(OldestSourceDate=lambda d: d["OldestSourceDate"].astype("string"))
        )

        desired_cols = [
            "OriginalId", "Name", "NameNormalized", "MatchedId", "MatchedName",
            "IsolationFilter", "isFromCrossRef", "OldestSourceDate",  # ← AJOUT isFromCrossRef
        ]

        existing_cols = [c for c in desired_cols if c in unique_df.columns]
        unique_df = unique_df[existing_cols + [c for c in unique_df.columns if c not in existing_cols]]

    # --- 5) NoMatch view ------------------------------
    base_cases = (
        df_cases[["OriginalId", "Name", "CountryCode"]]
        .assign(OriginalId=lambda d: d["OriginalId"].astype(str))
        .drop_duplicates(subset=["OriginalId"], keep="first")
    )
    matched_ids = set(export_df["OriginalId"].unique()) if not export_df.empty else set()
    df_nomatch = base_cases[~base_cases["OriginalId"].isin(matched_ids)].copy()

    if not df_nomatch.empty:
        # Normalize for display consistency (uses country code per row)
        df_nomatch["NameNormalized"] = df_nomatch.apply(
            lambda row: exact_normalization_series(
                pd.Series([row["Name"]]),
                country_code=row["CountryCode"]
            )["compact"].iloc[0],
            axis=1
        )
        df_nomatch = df_nomatch[["OriginalId", "Name", "NameNormalized"]].sort_values(
            ["OriginalId"], kind="stable", ignore_index=True
        )
    else:
        df_nomatch = pd.DataFrame(columns=["OriginalId", "Name", "NameNormalized"])

    # --- 6) Display/Export artifacts -----------------------------------------


    # Excel (5 sheets)
    xlsx_path = os.path.join(OUTPUT_DIR, "matches.xlsx")
    try:
        with pd.ExcelWriter(xlsx_path) as writer:
            # 1. "Entries": all initial cases from get_cases(), only two columns
            df_entries = df_cases[["OriginalId", "Name"]].copy()
            df_entries.to_excel(writer, index=False, sheet_name="Entries")

            # 2. "Matching_Propositions": all raw matches grouped by id (separated by blank rows)
            if matched_df_raw.empty:
                matching_props = pd.DataFrame(columns=[
                    "OriginalId", "Name", "NameNormalized", "MatchedId", "MatchedName", "isFromCrossRef"
                ])
            else:
                # Forcer le type string pour un merge correct
                matched_df_raw["OriginalId"] = matched_df_raw["OriginalId"].astype(str)
                df_cases["OriginalId"] = df_cases["OriginalId"].astype(str)

                cases_base = df_cases[["OriginalId", "Name"]].copy()

                # Fusionner matched_df_raw avec les noms de base des cas
                df_merge = matched_df_raw.merge(cases_base, on="OriginalId", how="left")

                # Renommer Name_y en Name, supprimer Name_x
                df_merge = df_merge.rename(columns={"Name_y": "Name"})
                if "Name_x" in df_merge.columns:
                    df_merge = df_merge.drop(columns=["Name_x"])

                # Ajouter CountryCode si absent
                if "CountryCode" not in df_merge.columns:
                    df_merge["CountryCode"] = df_merge["OriginalId"].str[:2]

                # Calcul sécurisé de NameNormalized
                def safe_normalize(name, cc):
                    try:
                        return exact_normalization_series(pd.Series([name]), country_code=cc)["compact"].iloc[0]
                    except Exception as e:
                        print(f"[WARN] Normalization failed for '{name}' ({cc}): {e}")
                        return pd.NA

                df_merge["NameNormalized"] = [
                    safe_normalize(n, c) for n, c in zip(df_merge["Name"], df_merge["CountryCode"])
                ]

                # Sélection des colonnes finales et insertion de lignes blanches entre groupes
                matching_props = df_merge[[
                    "OriginalId", "Name", "NameNormalized", "MatchedId", "MatchedName", "isFromCrossRef"
                ]]
                matching_props = _insert_blank_rows_between_groups(matching_props, "OriginalId")

            matching_props.to_excel(writer, index=False, sheet_name="Matching_Propositions")


            # 3. Unchanged: UniqueMatch
            unique_df.to_excel(writer, index=False, sheet_name="UniqueMatch")
            # 4. Unchanged: NoMatch
            df_nomatch.to_excel(writer, index=False, sheet_name="NoMatch")
            # 5. "Completed" sheet: number of records processed in OBO: total number of records minus the matches blocked
            df_completed = pd.DataFrame([["Matchings applied in OBO minus the ones that were blocked"]])
            df_completed.to_excel(writer, index=False, header=False, sheet_name="Completed")

            # 6. "MatchingOK": synced export from UniqueMatch with post-processed IDs
            nrows = len(unique_df)
            if nrows == 0:
                matchingok_formula = pd.DataFrame(columns=["OldID", "NewID", "UserID"])
            else:
                # Indices Excel = lignes + numéro de la première ligne (ici on commence à la ligne 2 car header)
                base_row = 2
                rows = np.arange(0, nrows)
                matchingok_formula = pd.DataFrame({
                    "OldID": [f"=IF(ISBLANK(UniqueMatch!A{base_row + i}), \"\", UniqueMatch!A{base_row + i})" for i in
                              rows],
                    "NewID": [
                        f"=IF(ISBLANK(UniqueMatch!D{base_row + i}), \"\", SUBSTITUTE(UniqueMatch!D{base_row + i}, \"!\", \"\"))"
                        for i in rows],
                    "UserID": ["AISA"] * nrows
                })

            matchingok_formula.to_excel(writer, index=False, sheet_name="MatchingOK", engine="openpyxl")
        print(
            f"[EXPORT] Excel: {xlsx_path} | Matching_Propositions={len(matching_props)} | UniqueMatch={len(unique_df)} | NoMatch={len(df_nomatch)}")
    except Exception as e:
        print(f"[EXPORT][ERROR] Impossible d'écrire l'Excel: {e}")

    # TXT: only UniqueMatch (OldID, NewID, UserID)
    txt_path = os.path.join(OUTPUT_DIR, "output.txt")
    try:
        if unique_df.empty:
            out_txt = pd.DataFrame(columns=["OldID", "NewID", "UserID"])
        else:
            out_txt = (
                unique_df[["OriginalId", "MatchedId"]]
                .dropna()
                .astype({"OriginalId": str, "MatchedId": str})
                .drop_duplicates()
                .rename(columns={"OriginalId": "OldID", "MatchedId": "NewID"})
                .assign(UserID="AISA")[["OldID", "NewID", "UserID"]]
            )
        out_txt.to_csv(txt_path, sep="\t", index=False, lineterminator="\n")
        print(f"[EXPORT] TXT: {txt_path} ({len(out_txt)} lignes)")
    except Exception as e:
        print(f"[EXPORT][ERROR] Impossible d'écrire le TXT: {e}")



if __name__ == "__main__":
    t0 = time.perf_counter()

    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    exit_code = 0
    metrics = {}
    extra=[]
    try:
        run()

    except Exception as e:
        exit_code = 1
        metrics = {"error": 1}
        traceback.print_exc()


    finally:
        file1 = os.path.join(OUTPUT_DIR, "matches.xlsx")
        # Construis la liste d'artefacts additionnels
        extra = []
        if file1 is not None:
            p = Path(file1)
            if p.exists():
                extra.append((str(p), "file", "xlsx", False))


        finalize_run(
            project_name="Switzerland_direct_semi_direct_entries",
            started_at=started_at,
            exit_code=exit_code,
            metrics=metrics,
            require_primary_txt=True,  # TXT ici, donc True pour lever une erreur si on ne le trouve pas
            extra_artifacts=extra,
        )

        elapsed_s = time.perf_counter() - t0
        print(f"[TIMER] Total: {elapsed_s / 60:.2f} min ({elapsed_s:.1f} s)")

        sys.exit(exit_code)