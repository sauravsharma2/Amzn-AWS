from __future__ import annotations

import re
from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import datetime
from typing import List, Iterable, Optional, Set, Tuple
import pandas as pd
import time
from typing import Set

from src.db_connexion.data_extraction import connect_db, load_query
from utils.config import PROBLEMATIC_STATUSES


# --- Constantes ---
_CHUNK = 1000


# --- Utilities ---

def to_base_ids(ids: Iterable[str]) -> List[str]:
    """
    Removes trailing "!" from IDs to query the SQL database.
    """
    if ids is None:
        return []
    s = pd.Series(list(ids), dtype="string").dropna()
    s = s.str.rstrip("!")
    return pd.unique(s).tolist()


def _keep_ok_or_all(df: pd.DataFrame, ok: pd.Series, group_col: str = "OriginalId") -> pd.DataFrame:
    """
    Keep only 'ok' rows within each group if present,
    otherwise keep the full group.
    """
    if df.empty:
        return df
    ok = ok.fillna(False)
    has_ok = ok.groupby(df[group_col]).transform("any")
    return df[~has_ok | ok]



def _parse_yyyymmdd(val):
    """
    Fast parser for YYYYMMDD without creating Pandas Series overhead.
    """
    s = str(val) if pd.notna(val) else ""
    # On cherche juste 8 chiffres, simple et rapide
    match = re.search(r"(\d{8})", s)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y%m%d")
        except ValueError:
            return pd.NaT
    return pd.NaT

def _date_pair_key(row: pd.Series) -> tuple:
    """
    Optimized comparison key.
    """
    o_sd = _parse_yyyymmdd(row.get("O_SourceDate"))
    m_sd = _parse_yyyymmdd(row.get("M_SourceDate"))

    # Si l'une des dates manque, on met une différence énorme
    if pd.isna(o_sd) or pd.isna(m_sd):
        diff_days = 100_000
    else:
        # Calcul direct en jours (sans passer par Timedelta lourd si possible)
        diff_days = abs((o_sd - m_sd).days)

    # Récence : on veut le plus récent (max). On inverse pour le tri (min).
    valid_dates = [d for d in (o_sd, m_sd) if pd.notna(d)]
    if valid_dates:
        # On utilise timestamp() pour avoir un float comparable
        recency = -max(valid_dates).timestamp()
    else:
        recency = 0 # ou une valeur par défaut neutre

    return (diff_days, recency)



def _percent_sets_equal_any(df: pd.DataFrame) -> pd.Series:
    o = df[["O_DirectPercent", "O_TotalPercent", "O_TotalPercentIndic"]].apply(
        pd.to_numeric, errors="coerce"
    )
    m = df[["M_DirectPercent", "M_TotalPercent", "M_TotalPercentIndic"]].apply(
        pd.to_numeric, errors="coerce"
    )
    o_empty = o.isna().all(axis=1)
    m_empty = m.isna().all(axis=1)

    both_empty = o_empty & m_empty
    one_empty  = o_empty ^ m_empty

    # Vérifie si une valeur O apparaît dans M (cross-compare chaque colonne)
    has_intersection = pd.Series(False, index=df.index)
    check_mask = ~both_empty & ~one_empty
    if check_mask.any():
        o_cols = ["O_DirectPercent", "O_TotalPercent", "O_TotalPercentIndic"]
        m_cols = ["M_DirectPercent", "M_TotalPercent", "M_TotalPercentIndic"]
        for oc in o_cols:
            for mc in m_cols:
                match = df[oc].eq(df[mc]) & df[oc].notna()
                has_intersection |= match

    return both_empty | (check_mask & has_intersection)


def _any_meta_diff_vec(df: pd.DataFrame) -> pd.Series:
    return (
        df["O_SourceDate"].astype(str).ne(df["M_SourceDate"].astype(str)) |
        df["O_SourceNr"].astype(str).ne(df["M_SourceNr"].astype(str)) |
        df["O_LastModifiedDate"].astype(str).ne(df["M_LastModifiedDate"].astype(str))
    )





def _any_meta_diff(row: pd.Series) -> bool:
    """
    Checks if O and M differ in any metadata field:
    SourceDate, SourceNr, or LastModifiedDate.
    Returns True if at least one mismatch is found.
    """
    return (
        str(row.get("O_SourceDate", ""))       != str(row.get("M_SourceDate", "")) or
        str(row.get("O_SourceNr", ""))         != str(row.get("M_SourceNr", "")) or
        str(row.get("O_LastModifiedDate", "")) != str(row.get("M_LastModifiedDate", ""))
    )


# --- SQL access ---
@connect_db
def _fetch_uo(conn, ids: List[str]) -> pd.DataFrame:
    ids = [str(x) for x in ids if pd.notna(x)]
    if not ids:
        return pd.DataFrame(columns=["ChildId", "ParentId"])

    sql_tpl = load_query("queries/get_uo.sql")
    out = []

    with conn.cursor() as cur:
        for i in range(0, len(ids), _CHUNK):
            chunk    = ids[i:i + _CHUNK]
            ids_list = ", ".join(f"'{x.replace(chr(39), chr(39)+chr(39))}'" for x in chunk)
            cur.execute(sql_tpl.format(ids_list=ids_list))
            if cur.description:
                cols = [c[0] for c in cur.description]
                rows = cur.fetchall()
                if rows:
                    out.append(pd.DataFrame.from_records(rows, columns=cols))

    if not out:
        return pd.DataFrame(columns=["ChildId", "ParentId"])
    return (
        pd.concat(out, ignore_index=True)
          .drop_duplicates()
          .astype({"ChildId": "string", "ParentId": "string"})
    )


_uo_cache: dict = {}

def _fetch_uo_cached(ids: List[str]) -> pd.DataFrame:
    key = tuple(sorted(set(str(x) for x in ids if pd.notna(x))))
    if not key:
        return pd.DataFrame(columns=["ChildId", "ParentId"])
    if key not in _uo_cache:
        _uo_cache[key] = _fetch_uo(list(key))
    return _uo_cache[key]



@connect_db
def _fetch_links_for_pairs(conn, pairs: List[Tuple[str, str]], sql_file_path: str,
                           batch_size: int = 2000) -> pd.DataFrame:
    if not pairs:
        return pd.DataFrame()

    data_to_insert = [
        (str(o), str(m))
        for o, m in pairs
        if pd.notna(o) and pd.notna(m)
    ]
    if not data_to_insert:
        return pd.DataFrame()

    sql_tpl = load_query(sql_file_path)
    all_results = []

    with conn.cursor() as cur:
        try:
            cur.fast_executemany = True
        except Exception:
            pass

        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]

            # Tous les IDs uniques du batch → seek direct sur Links
            all_ids  = list({id_ for pair in batch for id_ in pair})
            ids_list = ", ".join(f"'{x.replace(chr(39), chr(39)+chr(39))}'" for x in all_ids)

            try:
                # #PairsToProcess conservé uniquement pour la logique de JOIN O↔M dans le SQL
                cur.execute("""
                    CREATE TABLE #PairsToProcess (
                        O_Id VARCHAR(50) COLLATE Database_Default,
                        M_Id VARCHAR(50) COLLATE Database_Default
                    )
                """)
                cur.executemany(
                    "INSERT INTO #PairsToProcess (O_Id, M_Id) VALUES (?, ?)",
                    batch
                )
                cur.execute("CREATE CLUSTERED INDEX IX_Pairs ON #PairsToProcess(O_Id, M_Id)")

                # Injecte ids_list dans le SQL pour remplacer le JOIN scan
                sql = sql_tpl.format(ids_list=ids_list)
                cur.execute(sql)

                while cur.description is None:
                    if not cur.nextset():
                        break
                if cur.description:
                    cols = [c[0] for c in cur.description]
                    rows = cur.fetchall()
                    if rows:
                        all_results.append(pd.DataFrame.from_records(rows, columns=cols))
            finally:
                cur.execute("DROP TABLE IF EXISTS #PairsToProcess")
                cur.execute("DROP TABLE IF EXISTS #RawLinks")

    if not all_results:
        return pd.DataFrame()
    return pd.concat(all_results, ignore_index=True).drop_duplicates()




@connect_db
def _fetch_info_without_exclam(conn, base_ids: List[str]) -> pd.DataFrame:
    ids = [str(x) for x in base_ids if pd.notna(x)]
    if not ids:
        return pd.DataFrame(columns=["Id", "HistoricSinceDate", "LegalStatus"])

    sql_tpl = load_query("queries/get_info_without_exclam.sql")
    out = []

    with conn.cursor() as cur:
        for i in range(0, len(ids), _CHUNK):
            chunk    = ids[i:i + _CHUNK]
            ids_list = ", ".join(f"'{x.replace(chr(39), chr(39)+chr(39))}'" for x in chunk)
            cur.execute(sql_tpl.format(ids_list=ids_list))
            if cur.description:
                cols = [c[0] for c in cur.description]
                rows = cur.fetchall()
                if rows:
                    out.append(pd.DataFrame.from_records(rows, columns=cols))

    if not out:
        return pd.DataFrame(columns=["Id", "HistoricSinceDate", "LegalStatus"])

    df = (
        pd.concat(out, ignore_index=True)
          .drop_duplicates(subset=["Id"])
          .astype({"Id": "string"})
    )
    df["HistoricSinceDate"] = df["HistoricSinceDate"].astype("string")
    df["LegalStatus"]       = pd.to_numeric(df["LegalStatus"], errors="coerce").astype("Int64")
    return df

# --- Filtres ---

C_EXCLUDE = r"(?:LEI|FR|FDIC|FEO|FS|0C|IA|STAT|FACT|ECB|FFH|EBI|BNB|RLA|RLB|RLC|WOR|SINO|PEI|OFWI|WIN|VEDA|ZEF|SIREN|NIF|\*|D\d|[1-4]-)"

PRIORITY_PATTERNS = [
    ("Scope National ID",            re.compile(r"^[A-Z]{2}B[A-Z0-9\-!]+$")),
    ("ISIS National ID",             re.compile(r"^[A-Z]{2}I[A-Z0-9\-!]+$")),
    ("National ID",                  re.compile(fr"^[A-Z]{{2}}C(?!{C_EXCLUDE})[A-Z0-9\-!]+$")),

    ("Scope IP ID",                  re.compile(r"^[A-Z]{2}BFEB\d{5,}!?$")),
    ("ISIS IP ID",                   re.compile(r"^[A-Z]{2}IFEI\d{6,}!?$")),
    ("RSSD ID",                      re.compile(r"^[A-Z]{2}CFR\d+!?$")),
    ("Osiris Multex",                re.compile(r"^[A-Z]{2}C[A-Z0-9]{6}$")),
    ("Osiris WVB",                   re.compile(r"^[A-Z]{2}C(\d{5}[A-Z]{2}|\d{9}[A-Z]{3})$")),
    ("FDIC",                         re.compile(r"^[A-Z]{2}CFDIC\d+$")),

    # ⬇️ IP ID générique, idem: n'attrape pas les sous-préfixes réservés
    ("IP ID",                        re.compile(fr"^[A-Z]{{2}}C(?!{C_EXCLUDE})[A-Z0-9\-!]+$")),

    ("Funds Morningstar",            re.compile(r"^[A-Z]{2}CFS[A-Z0-9]+$")),
    ("Osiris FEO",                   re.compile(r"^[A-Z]{2}CFEO[A-Z0-9]+$")),
    ("LEI",                          re.compile(r"^[A-Z]{2}CLEI[A-Z0-9]+$")),
    ("ECB CECB",                     re.compile(r"^[A-Z]{2}CECB[A-Z0-9]+$")),
    ("ECB ESCBMFI",                  re.compile(r"^[A-Z]{2}ESCBMFI[A-Z0-9]+$")),
    ("ECB ESCBFVC",                  re.compile(r"^[A-Z]{2}ESCBFVC[A-Z0-9]+$")),
    ("Funds Roles",                  re.compile(r"^[A-Z]{2}C0C[A-Z0-9]+$")),
    ("IAPD",                         re.compile(r"^[A-Z]{2}CIA[A-Z0-9]+$")),
    ("US scraping",                  re.compile(r"^[A-Z]{2}[A-Z]{2}-[A-Z0-9]+$")),
    ("FHQ",                          re.compile(r"^[A-Z]{2}[A-Z0-9\-]*-FHQ$")),
    # --- Direct entries 2 (même rang, tous exclus du générique via C_EXCLUDE) ---
    ("Direct2 PE/PEM/PEB",           re.compile(r"^[A-Z]{2}C[0-9A-Z]+PE(M|B)?$")),
    ("Direct2 SCRL",                 re.compile(r"^[A-Z]{2}SCRL[A-Z0-9]+$")),
    ("Direct2 CFFH",                 re.compile(r"^[A-Z]{2}CFFH[A-Z0-9]+$")),
    ("Direct2 CEBI",                 re.compile(r"^[A-Z]{2}CEBI[A-Z0-9]+$")),
    ("Direct2 CBNB",                 re.compile(r"^[A-Z]{2}CBNB[A-Z0-9]+$")),
    ("Direct2 CRLA",                 re.compile(r"^[A-Z]{2}CRLA[A-Z0-9]+$")),
    ("Direct2 CRLB",                 re.compile(r"^[A-Z]{2}CRLB[A-Z0-9]+$")),
    ("Direct2 CRLC",                 re.compile(r"^[A-Z]{2}CRLC[A-Z0-9]+$")),
    ("Direct2 CSTAT",                re.compile(r"^[A-Z]{2}CSTAT[A-Z0-9]+$")),
    ("Direct2 CFACT",                re.compile(r"^[A-Z]{2}CFACT[A-Z0-9]+$")),
    ("Direct2 CWOR",                 re.compile(r"^[A-Z]{2}CWOR[A-Z0-9]+$")),
    ("Direct2 CSINO",                re.compile(r"^[A-Z]{2}CSINO[A-Z0-9]+$")),
    ("Direct2 CPEI",                 re.compile(r"^[A-Z]{2}CPEI[A-Z0-9\-]+$")),
    ("Direct2 COFWI",                re.compile(r"^[A-Z]{2}COFWI[A-Z0-9\-]+$")),
    ("Direct2 CWIN",                 re.compile(r"^[A-Z]{2}CWIN[A-Z0-9]+$")),
    ("Direct2 CVEDA",                re.compile(r"^[A-Z]{2}CVEDA[A-Z0-9]+$")),
    ("Direct2 CZEF",                 re.compile(r"^[A-Z]{2}CZEF[A-Z0-9]+$")),
    ("Direct2 CSIREN",               re.compile(r"^[A-Z]{2}CSIREN[A-Z0-9]+$")),
    ("Direct2 CNIF",                 re.compile(r"^[A-Z]{2}CNIF[A-Z0-9]+$")),
    ("Direct2 ...+L (non US/CA)",    re.compile(r"^(?!US|CA)[A-Z]{2}[A-Z0-9\-]+L$")),
    ("Direct2 C[1-4]-",              re.compile(r"^[A-Z]{2}C[1-4]-\d+$")),
    # --- Fin des Direct2 ---
    ("DUNS double D",                re.compile(r"^[A-Z]{2}CD\d{9}D?$")),
    ("Direct1 C*digits",             re.compile(r"^[A-Z]{2}C\*\d+$")),
    ("Generic ZZ CBVD",              re.compile(r"^ZZCBVD[A-Z0-9\*]+$")),
    ("Jointly owned CJNZ",           re.compile(r"^(NZ|YY)CJNZ[A-Z0-9\-]+$")),
]

def _rank_to_name(rank: int) -> str:
    return PRIORITY_PATTERNS[rank][0] if 0 <= rank < len(PRIORITY_PATTERNS) else "Unknown"

def _classify_id(x: str) -> int:
    s = "" if x is None else str(x).strip().upper()
    for idx, (_name, rgx) in enumerate(PRIORITY_PATTERNS):
        if rgx.match(s):
            return idx
    # Unknown → assign the lowest possible rank
    return len(PRIORITY_PATTERNS)

def hierarchy_of_id(matches: pd.DataFrame) -> pd.DataFrame:
    """
    For each OriginalId:
      1) assign a rank to each MatchedId based on PRIORITY_PATTERNS (0 = best),
      2) keep only rows with the best rank present,
      3) no internal tie-break (Direct entries 2 included: considered equivalent).
    The '!' character is neutral (handled by the regex patterns).
    """
    if (
        matches is None
        or matches.empty
        or "MatchedId" not in matches.columns
        or "OriginalId" not in matches.columns
    ):
        return matches

    df = matches.copy()

    df["OriginalId"] = df["OriginalId"].astype(str).str.strip()
    df["MatchedId"]  = df["MatchedId"].astype(str).str.strip()


    df["__rank"] = df["MatchedId"].apply(_classify_id)


    best_rank = df.groupby("OriginalId")["__rank"].transform("min")
    out = df.loc[df["__rank"] == best_rank].drop(columns=["__rank"])
    return out


# --- Filtres Optimisés (Remplace tes anciennes fonctions filter_by_...) ---

def filter_by_subsidiaries(matches: pd.DataFrame) -> pd.DataFrame:
    """
    Optimized filter using pair-based fetching (SQL 'A' files).
    SECURITY: Skips groups with > 10 candidates to avoid massive timeouts on huge holdings.
    """
    return _generic_filter_links(
        matches,
        sql_file="queries/get_links_information_subsidiaries.sql",
        key_cols=["OriginalId", "MatchedId_base", "ChildId"],
        max_candidates=None
    )


def filter_by_shareholders(matches: pd.DataFrame) -> pd.DataFrame:
    """
    Optimized filter using pair-based fetching.
    """
    return _generic_filter_links(
        matches,
        sql_file="queries/get_links_information_shareholders.sql",
        key_cols=["OriginalId", "MatchedId_base", "ParentId"]
    )


def filter_by_beneficial_owner(matches: pd.DataFrame) -> pd.DataFrame:
    """
    Optimized filter using pair-based fetching.
    """
    return _generic_filter_links(
        matches,
        sql_file="queries/get_links_information_beneficial_owner.sql",
        key_cols=["OriginalId", "MatchedId_base", "ParentId"]
    )


def filter_by_beneficial_subsidiaries(matches: pd.DataFrame) -> pd.DataFrame:
    """
    Optimized filter using pair-based fetching.
    """
    return _generic_filter_links(
        matches,
        sql_file="queries/get_links_information_beneficial_subsidiaries.sql",
        key_cols=["OriginalId", "MatchedId_base", "ChildId"]
    )


# --- Fonction Générique (Le moteur caché) ---

def _generic_filter_links(matches: pd.DataFrame, sql_file: str, key_cols: List[str],
                          max_candidates: int = None) -> pd.DataFrame:
    """
    Generic engine for all link-based filters.
    Args:
        max_candidates: If set, skips filtering for groups larger than this size (returns them as-is).
    """
    if matches is None or matches.empty or "MatchedId" not in matches.columns:
        return matches

    df = matches.copy()
    df["OriginalId"] = df["OriginalId"].astype(str)
    df["MatchedId"] = df["MatchedId"].astype(str)
    # On s'assure d'avoir la base sans '!'
    df = df.assign(MatchedId_base=df["MatchedId"].str.rstrip("!"))

    # 1. Identifier les cibles (Groupes avec > 1 match)
    multi = df["OriginalId"].value_counts()

    if max_candidates is not None:
        target_ids = set(multi.index[(multi.gt(1)) & (multi.le(max_candidates))])

        skipped = multi.gt(max_candidates).sum()
        if skipped > 0:
            print(f"[FILTER] {sql_file}: Skipped {skipped} groups (> {max_candidates} candidates)")
    else:
        target_ids = set(multi.index[multi.gt(1)])

    if not target_ids:
        return df.drop(columns=["MatchedId_base"])

    cand = df[df["OriginalId"].isin(target_ids)].copy()

    # 2. Préparer les paires exactes à vérifier
    pairs_to_check = list(zip(
        cand["OriginalId"].tolist(),
        cand["MatchedId_base"].tolist()
    ))

    # 3. Fetch Optimisé (Utilise tes nouveaux SQL 'A')
    links = _fetch_links_for_pairs(pairs_to_check, sql_file)

    if links.empty:
        return df.drop(columns=["MatchedId_base"])

    best_links = links
    # ----------------------

    # 5. Validation
    ok_pct = _percent_sets_equal_any(best_links)
    ok_meta = _any_meta_diff_vec(best_links)
    valid_links = best_links[ok_pct & ok_meta]

    if valid_links.empty:
        return df.drop(columns=["MatchedId_base"])

    # 6. Application du filtre
    # On récupère l'ensemble des paires valides
    ok_pairs = set(map(tuple, valid_links[["OriginalId", "MatchedId_base"]].drop_duplicates().to_numpy()))

    index_cand = pd.MultiIndex.from_frame(df[["OriginalId", "MatchedId_base"]])
    index_ok = pd.MultiIndex.from_tuples(ok_pairs, names=["OriginalId", "MatchedId_base"])

    ok_series = pd.Series(index_cand.isin(index_ok), index=df.index)

    # Appel de ta fonction utilitaire existante pour filtrer
    # Les groupes ignorés (trop gros) auront ok_series=False partout, donc _keep_ok_or_all les gardera entiers (fallback)
    kept = _keep_ok_or_all(df, ok_series, group_col="OriginalId")

    return kept.drop(columns=["MatchedId_base"])


def filter_by_uo(matches: pd.DataFrame) -> pd.DataFrame:
    """
    Filters by Ultimate Owner (UO).
    - If an OriginalId has no UO → keep the whole group.
    - If it has a UO:
        • keep only rows where the MatchedId's UO (base id, without '!') equals the OriginalId's UO.
        • If no candidate shares the UO → fallback: keep the whole group.
    """

    if matches is None or matches.empty:
        return matches

    df = matches.copy()
    df["OriginalId"] = df["OriginalId"].astype(str)
    df["MatchedId"]  = df["MatchedId"].astype(str)

    uo_orig = _fetch_uo_cached(df["OriginalId"].unique().tolist())
    if uo_orig.empty:
        return df

    orig_with_uo = set(uo_orig["ChildId"].unique())
    keep_all   = df[~df["OriginalId"].isin(orig_with_uo)]
    candidates = df[df["OriginalId"].isin(orig_with_uo)]
    if candidates.empty:
        return df

    candidates = candidates.assign(MatchedId_base=candidates["MatchedId"].str.rstrip("!"))
    base_ids = to_base_ids(candidates["MatchedId"].unique().tolist())
    uo_match = _fetch_uo_cached(base_ids)
    if uo_match.empty:
        return pd.concat([keep_all, candidates.drop(columns=["MatchedId_base"])], ignore_index=True).drop_duplicates()

    o = uo_orig.rename(columns={"ChildId": "OriginalId",     "ParentId": "UO"})
    m = uo_match.rename(columns={"ChildId": "MatchedId_base","ParentId": "UO"})
    merged = (candidates.merge(o, on="OriginalId", how="left", validate="many_to_many")
                        .merge(m, on=["MatchedId_base", "UO"], how="inner", validate="many_to_many"))

    with_match = set(merged["OriginalId"].unique())
    fallback_ids = set(candidates["OriginalId"].unique()) - with_match
    fallback = candidates[candidates["OriginalId"].isin(fallback_ids)]

    merged = merged.drop(columns=["MatchedId_base", "UO"])
    return pd.concat([keep_all, merged[df.columns], fallback[df.columns]], ignore_index=True).drop_duplicates()





def filter_by_legal_status(matches: pd.DataFrame,
                           problematic_statuses: Set[int] = PROBLEMATIC_STATUSES) -> pd.DataFrame:
    """
    Filters by LegalStatus with enrichment for '!' MatchedIds.
    - For MatchedIds ending with '!' → fetch base id LegalStatus from SQL and overwrite.
    - Keep only rows whose LegalStatus is not in problematic_statuses.
    - Fallback: if no valid row remains for an OriginalId, keep the full group.
    """

    if matches is None or matches.empty or "LegalStatus" not in matches.columns:
        return matches

    df = matches.copy()

    if "MatchedId" in df.columns:
        needs_lookup = df["MatchedId"].astype("string").str.endswith("!")
        if needs_lookup.any():
            df = df.assign(MatchedId_base=df["MatchedId"].astype("string").str.rstrip("!"))
            base_ids = df.loc[needs_lookup, "MatchedId_base"].dropna().unique().tolist()

            enrich = _fetch_info_without_exclam(base_ids)
            if not enrich.empty:
                df = df.merge(
                    enrich.rename(columns={"Id": "MatchedId_base",
                                           "LegalStatus": "LegalStatus_SQL"}),
                    on="MatchedId_base",
                    how="left",
                    validate="many_to_one"
                )
                mask_use_sql = needs_lookup & df["LegalStatus_SQL"].notna()
                df.loc[mask_use_sql, "LegalStatus"] = pd.to_numeric(
                    df.loc[mask_use_sql, "LegalStatus_SQL"], errors="coerce"
                ).astype("Int64")

            drop_cols = [c for c in ["MatchedId_base", "LegalStatus_SQL"] if c in df.columns]
            if drop_cols:
                df = df.drop(columns=drop_cols)

    ok = ~pd.to_numeric(df["LegalStatus"], errors="coerce").astype("Int64").isin(
        pd.Series(list(problematic_statuses), dtype="Int64")
    )

    if not ok.any():
        return df
    return _keep_ok_or_all(df, ok, group_col="OriginalId")




def filter_by_historic_since_date(matches: pd.DataFrame,
                                  col: str = "HistoricSinceDate") -> pd.DataFrame:
    """
    Filters by HistoricSinceDate with enrichment for '!' MatchedIds.
    - For MatchedIds ending with '!' → fetch base id HistoricSinceDate from SQL and overwrite.
    - Keep rows where HistoricSinceDate is:
        • -1,
        • NULL/NaN,
        • or equal to the current year.
    - Fallback: if no valid row remains for an OriginalId, keep the full group.
    """

    if matches is None or matches.empty or col not in matches.columns:
        return matches

    df = matches.copy()

    if "MatchedId" in df.columns:
        needs_lookup = df["MatchedId"].astype("string").str.endswith("!")
        if needs_lookup.any():
            df = df.assign(MatchedId_base=df["MatchedId"].astype("string").str.rstrip("!"))
            base_ids = df.loc[needs_lookup, "MatchedId_base"].dropna().unique().tolist()

            enrich = _fetch_info_without_exclam(base_ids)
            if not enrich.empty:
                df = df.merge(
                    enrich.rename(columns={"Id": "MatchedId_base",
                                           "HistoricSinceDate": f"{col}_SQL"}),
                    on="MatchedId_base",
                    how="left",
                    validate="many_to_one"
                )
                mask_use_sql = needs_lookup & df[f"{col}_SQL"].notna()
                df.loc[mask_use_sql, col] = df.loc[mask_use_sql, f"{col}_SQL"].astype("string")

            drop_cols = [c for c in ["MatchedId_base", f"{col}_SQL"] if c in df.columns]
            if drop_cols:
                df = df.drop(columns=drop_cols)

    year_str = str(pd.Timestamp.now().year)

    s = df[col].astype("string").str.strip()

    ok_unknown = s.eq("-1") | s.eq("-1.0") | s.isna()

    year_extracted = s.str.extract(r"(\d{4})", expand=False)
    ok_year = year_extracted.eq(year_str)

    ok = ok_unknown | ok_year

    if not ok.any():
        return df
    return _keep_ok_or_all(df, ok, group_col="OriginalId")



# --- Solo matches filter ---


# --- Optimisation de solo_match_uo_then_links_check ---


def _apply_date_keys_vectorized(links: pd.DataFrame) -> pd.DataFrame:
    o_dates = pd.to_datetime(
        links["O_SourceDate"].astype(str).str.extract(r"(\d{8})", expand=False),
        format="%Y%m%d", errors="coerce"
    )
    m_dates = pd.to_datetime(
        links["M_SourceDate"].astype(str).str.extract(r"(\d{8})", expand=False),
        format="%Y%m%d", errors="coerce"
    )
    links["_diff_days"] = (o_dates - m_dates).abs().dt.days.fillna(100_000).astype(int)
    max_dates           = pd.concat([o_dates, m_dates], axis=1).max(axis=1)
    links["_recency"]   = max_dates.apply(lambda d: -d.timestamp() if pd.notna(d) else 0.0)
    return links


def solo_match_uo_then_links_check(matches: pd.DataFrame) -> pd.DataFrame:
    """
    Handles solo cases where an OriginalId has exactly one candidate MatchedId.
    Optimized using pair-based SQL fetching (SQL 'A' files).
    """
    if (matches is None or matches.empty or
            "OriginalId" not in matches.columns or "MatchedId" not in matches.columns):
        return matches

    df = matches.copy()
    df["OriginalId"] = df["OriginalId"].astype(str)
    df["MatchedId"] = df["MatchedId"].astype("string")

    # Identification des cas uniques
    single_ids = set(df["OriginalId"].value_counts().pipe(lambda s: s.index[s.eq(1)]))
    if not single_ids:
        return df

    # Séparation Uniques / Reste
    uniques = df[df["OriginalId"].isin(single_ids)].copy()
    rest = df[~df["OriginalId"].isin(single_ids)].copy()

    uniques = uniques.assign(MatchedId_base=uniques["MatchedId"].str.rstrip("!"))

    # 1. Vérification UO (inchangée car rapide via lookup direct)
    orig_ids = pd.unique(uniques["OriginalId"]).tolist()
    match_ids_base = pd.unique(uniques["MatchedId_base"].dropna()).tolist()

    uo_orig = _fetch_uo_cached(orig_ids)
    uo_match = _fetch_uo_cached(match_ids_base)

    from collections import defaultdict
    uo_map_orig = defaultdict(set)
    uo_map_match = defaultdict(set)

    if not uo_orig.empty:
        for child, parent in uo_orig[["ChildId", "ParentId"]].itertuples(index=False):
            uo_map_orig[str(child)].add(str(parent))
    if not uo_match.empty:
        for child, parent in uo_match[["ChildId", "ParentId"]].itertuples(index=False):
            uo_map_match[str(child)].add(str(parent))

    def _needs_link_salvage(row) -> bool:
        oid = str(row["OriginalId"])
        midb = str(row["MatchedId_base"])
        orig_uos = uo_map_orig.get(oid, set())
        # Si pas d'UO original => on garde (pas besoin de salvage)
        if len(orig_uos) == 0:
            return False
        match_uos = uo_map_match.get(midb, set())
        # Si UO commun => on garde
        if len(orig_uos & match_uos) > 0:
            return False
        # Sinon (UO différents), on doit vérifier les liens
        return True

    salvage_mask = uniques.apply(_needs_link_salvage, axis=1)
    keep_mask = ~salvage_mask  # Par défaut on garde ceux qui ont passé le test UO

    salvage_df = uniques[salvage_mask].copy()

    # 2. Vérification Liens (OPTIMISÉE)
    if not salvage_df.empty:
        # Préparation des paires exactes
        pairs_to_check = list(zip(
            salvage_df["OriginalId"].tolist(),
            salvage_df["MatchedId_base"].tolist()
        ))

        ok_pairs = set()

        # Helper pour traiter un type de lien
        def _check_link_type(sql_file, key_cols):
            links = _fetch_links_for_pairs(pairs_to_check, sql_file)
            if links.empty:
                return set()

            links = _apply_date_keys_vectorized(links)
            best = (
                links
                .sort_values(["_diff_days", "_recency"])
                .drop_duplicates(subset=key_cols, keep="first")
                .drop(columns=["_diff_days", "_recency"])
            )

            ok_pct = _percent_sets_equal_any(best)
            ok_meta = _any_meta_diff_vec(best)
            valid = best[ok_pct & ok_meta]
            if valid.empty:
                return set()
            return set(map(tuple, valid[["OriginalId", "MatchedId_base"]].drop_duplicates().to_numpy()))

        # Check Shareholders
        ok_pairs |= _check_link_type(
            "queries/get_links_information_shareholders.sql",
            ["OriginalId", "MatchedId_base", "ParentId"]
        )

        # Check Subsidiaries
        ok_pairs |= _check_link_type(
            "queries/get_links_information_subsidiaries.sql",
            ["OriginalId", "MatchedId_base", "ChildId"]
        )

        # Check Beneficial Owners (optionnel, selon ta logique métier initiale)
        # ok_pairs |= _check_link_type("queries/get_links_information_beneficial_owner.sql", ...)

        def _salvage_keep(row) -> bool:
            key = (str(row["OriginalId"]), str(row["MatchedId_base"]))
            return key in ok_pairs

        salvage_keep = salvage_df.apply(_salvage_keep, axis=1)
        keep_mask.loc[salvage_df.index] = salvage_keep

    kept_uniques = uniques[keep_mask].drop(columns=["MatchedId_base"])
    removed = len(uniques) - len(kept_uniques)
    if removed > 0:
        print(f"[SOLO UO+LINKS] removed {removed} solo matches (UO mismatch w/o valid common links)")

    return pd.concat([rest, kept_uniques], ignore_index=True)


# --- Optimisation de solo_shortname_guard ---

def solo_shortname_guard(matches: pd.DataFrame, name_col: str = "NameNormalized") -> pd.DataFrame:
    """
    Handles solo cases with short names (<=3 chars).
    Optimized using pair-based SQL fetching (SQL 'A' files).
    """
    if (matches is None or matches.empty or
            "OriginalId" not in matches.columns or
            "MatchedId" not in matches.columns or
            name_col not in matches.columns):
        return matches

    df = matches.copy()
    df["OriginalId"] = df["OriginalId"].astype(str)
    df["MatchedId"] = df["MatchedId"].astype("string")

    # Identification des cas uniques
    single_ids = set(df["OriginalId"].value_counts().pipe(lambda s: s.index[s.eq(1)]))
    if not single_ids:
        return df

    is_short = df[name_col].astype("string").str.len().le(3)
    target_mask = df["OriginalId"].isin(single_ids) & is_short

    shorts = df[target_mask].copy()
    rest = df[~target_mask].copy()

    if shorts.empty:
        return df

    # Si déjà validé par un filtre fort, on laisse passer (logique conservée)
    if "IsolationFilter" in shorts.columns:
        pre_valid = shorts["IsolationFilter"].isin(
            ["filter_by_shareholders", "filter_by_uo", "filter_by_subsidiaries",
             "same_shareholder", "same_subsidiary", "same_uo"]  # Ajout des labels 'solo' au cas où
        )
        already_valid = shorts[pre_valid].copy()
        shorts = shorts[~pre_valid].copy()
    else:
        already_valid = pd.DataFrame(columns=shorts.columns)

    if shorts.empty:
        return pd.concat([rest, already_valid], ignore_index=True)

    shorts = shorts.assign(MatchedId_base=shorts["MatchedId"].str.rstrip("!"))

    # 1. Vérification UO (inchangée)
    orig_ids = pd.unique(shorts["OriginalId"]).tolist()
    match_ids_base = pd.unique(shorts["MatchedId_base"].dropna()).tolist()

    uo_orig = _fetch_uo_cached(orig_ids)
    uo_match = _fetch_uo_cached(match_ids_base)

    from collections import defaultdict
    uo_map_orig = defaultdict(set)
    uo_map_match = defaultdict(set)

    if not uo_orig.empty:
        for child, parent in uo_orig[["ChildId", "ParentId"]].itertuples(index=False):
            uo_map_orig[str(child)].add(str(parent))
    if not uo_match.empty:
        for child, parent in uo_match[["ChildId", "ParentId"]].itertuples(index=False):
            uo_map_match[str(child)].add(str(parent))

    def _uo_ok(row) -> bool:
        oid = str(row["OriginalId"])
        midb = str(row["MatchedId_base"])
        oset = uo_map_orig.get(oid, set())
        mset = uo_map_match.get(midb, set()) \
            # Il faut un UO commun pour sauver un shortname sans liens
        return bool(oset) and bool(mset) and len(oset & mset) > 0

    uo_ok_mask = shorts.apply(_uo_ok, axis=1)

    # Ceux qui n'ont pas l'UO commun doivent être sauvés par les liens
    salvage_df = shorts[~uo_ok_mask].copy()
    ok_pairs = set()

    if not salvage_df.empty:
        pairs_to_check = list(zip(
            salvage_df["OriginalId"].tolist(),
            salvage_df["MatchedId_base"].tolist()
        ))

        # Helper pour traiter un type de lien (réutilisé)
        def _check_link_type(sql_file, key_cols):
            links = _fetch_links_for_pairs(pairs_to_check, sql_file)
            if links.empty:
                return set()

            links = _apply_date_keys_vectorized(links)
            best = (
                links
                .sort_values(["_diff_days", "_recency"])
                .drop_duplicates(subset=key_cols, keep="first")
                .drop(columns=["_diff_days", "_recency"])
            )

            ok_pct = _percent_sets_equal_any(best)
            ok_meta = _any_meta_diff_vec(best)
            valid = best[ok_pct & ok_meta]
            if valid.empty:
                return set()
            return set(map(tuple, valid[["OriginalId", "MatchedId_base"]].drop_duplicates().to_numpy()))

        # Fetch Optimisé
        ok_pairs |= _check_link_type(
            "queries/get_links_information_subsidiaries.sql",
            ["OriginalId", "MatchedId_base", "ChildId"]
        )
        ok_pairs |= _check_link_type(
            "queries/get_links_information_shareholders.sql",
            ["OriginalId", "MatchedId_base", "ParentId"]
        )

    def _keep_short(row) -> bool:
        if _uo_ok(row): return True
        key = (str(row["OriginalId"]), str(row["MatchedId_base"]))
        return key in ok_pairs

    keep_mask = shorts.apply(_keep_short, axis=1)
    kept_short = shorts[keep_mask].drop(columns=["MatchedId_base"])

    removed = len(shorts) - len(kept_short)
    if removed > 0:
        print(f"[SOLO SHORTNAME] removed {removed} solo(s) with short name (<=3)")

    return pd.concat([rest, already_valid, kept_short], ignore_index=True)


# --- Solo Validation Optimisée ---
def _validate_worker(cand_df, sql_file, label, key_cols):
    """Worker function for parallel validation. Pure calculation, no DF modification."""
    # On prépare la liste des paires à vérifier pour ce batch
    pairs_to_check = list(zip(cand_df["OriginalId"].tolist(), cand_df["MatchedId_base"].tolist()))

    # 1. Fetch SQL (via la nouvelle méthode optimisée Temp Table)
    links_df = _fetch_links_for_pairs(pairs_to_check, sql_file)

    if links_df.empty:
        return label, set(), set()  # label, ok_pairs, neg_pairs

    # 2. Sélection du meilleur lien (Logique identique à l'originale)
    links_df = _apply_date_keys_vectorized(links_df)

    best = (
        links_df
        .sort_values(["_diff_days", "_recency"])  # tri : min diff_days, puis plus récent
        .drop_duplicates(subset=key_cols, keep="first")  # garde la meilleure ligne par groupe
        .drop(columns=["_diff_days", "_recency"])
    )

    # 3. Vérification des règles métier (Pourcentages et Métadonnées)
    ok_pct = _percent_sets_equal_any(best)
    ok_meta = _any_meta_diff_vec(best)

    valid = best[ok_pct & ok_meta]
    same_info_candidates = best[ok_pct & (~ok_meta)]

    # 4. Identification des "Same Archive Negative" (pour le fallback)
    neg_pairs = set()
    if not same_info_candidates.empty:
        o_arch = pd.to_numeric(same_info_candidates["O_Archive"], errors='coerce').fillna(0)
        m_arch = pd.to_numeric(same_info_candidates["M_Archive"], errors='coerce').fillna(0)
        xor_mask = (o_arch <= 0) ^ (m_arch <= 0)
        neg_pairs_df = same_info_candidates[xor_mask][["OriginalId", "MatchedId_base"]].drop_duplicates()
        neg_pairs = set(map(tuple, neg_pairs_df.to_numpy()))

    # 5. Extraction des paires valides
    ok_pairs = set()
    if not valid.empty:
        ok_pairs = set(map(tuple, valid[["OriginalId", "MatchedId_base"]].drop_duplicates().to_numpy()))

    return label, ok_pairs, neg_pairs


def solo_validation(matches: pd.DataFrame) -> pd.DataFrame:
    """
    Optimized solo_validation with CASCADING PARALLEL execution.
    Phase 1: Check Shareholders & Subsidiaries (Most likely matches).
    Phase 2: Check Beneficial Owners only for remaining candidates.
    """
    if (matches is None or matches.empty or "OriginalId" not in matches.columns):
        return matches

    df = matches.copy()
    df["OriginalId"] = df["OriginalId"].astype("string")
    df["MatchedId"] = df["MatchedId"].astype("string")
    df["MatchedId_base"] = df["MatchedId"].str.rstrip("!")

    if "IsolationFilter" not in df.columns: df["IsolationFilter"] = pd.NA
    if "Same_Archive_Negative" not in df.columns: df["Same_Archive_Negative"] = False

    # Identification des cibles (OriginalId unique)
    counts = df["OriginalId"].value_counts()
    target_ids = set(counts.index[counts.eq(1)])
    if not target_ids: return df.drop(columns=["MatchedId_base"])

    # On ne traite que ceux qui n'ont pas encore de filtre
    is_empty_if = df["IsolationFilter"].isna() | (df["IsolationFilter"].astype("string").str.strip() == "")
    target_mask = df["OriginalId"].isin(target_ids) & is_empty_if
    cand = df[target_mask].copy()

    if cand.empty: return df.drop(columns=["MatchedId_base"])

    print(f"[SOLO] Starting Cascade Validation on {len(cand)} candidates...")

    results_ok = {}  # label -> set(ok_pairs)
    all_neg_pairs = set()

    # --- PHASE 1 : Primary Checks (Shareholders & Subsidiaries) ---
    # Lancés en parallèle sur TOUS les candidats car très fréquents.
    tasks_phase1 = [
        ("queries/get_links_information_shareholders.sql", "same_shareholder",
         ["OriginalId", "MatchedId_base", "ParentId"]),
        ("queries/get_links_information_subsidiaries.sql", "same_subsidiary",
         ["OriginalId", "MatchedId_base", "ChildId"]),
    ]

    for sql, lbl, keys in tasks_phase1:
        try:
            lbl, ok_p, neg_p = _validate_worker(cand, sql, lbl, keys)
            if ok_p: results_ok[lbl] = ok_p
            if neg_p: all_neg_pairs.update(neg_p)
        except Exception as e:
            print(f"[ERROR] Phase 1 worker failed: {e}")

    # Application des résultats Phase 1
    current_cand_idx = cand.index
    priority_order_p1 = ["same_shareholder", "same_subsidiary"]

    for lbl in priority_order_p1:
        ok_pairs = results_ok.get(lbl, set())
        if not ok_pairs: continue

        mask_cand = df.index.isin(current_cand_idx)
        subset = df[mask_cand]
        if subset.empty: break


        idx_ok = pd.MultiIndex.from_tuples(ok_pairs, names=["OriginalId", "MatchedId_base"])
        idx_subset = pd.MultiIndex.from_frame(subset[["OriginalId", "MatchedId_base"]])
        valid_indices = subset.index[idx_subset.isin(idx_ok)]

        if not valid_indices.empty:
            df.loc[valid_indices, "IsolationFilter"] = lbl
            current_cand_idx = current_cand_idx.difference(valid_indices)

    remaining_count = len(current_cand_idx)

    if remaining_count > 0:
        print(f"[SOLO] Phase 1 complete. {remaining_count} candidates remaining. Launching Phase 2 (BO)...")

        cand_remaining = df.loc[current_cand_idx].copy()
        cand_remaining["MatchedId_base"] = cand_remaining["MatchedId"].str.rstrip("!")

        # --- PHASE 2 : Secondary Checks (BO & BO Subs) ---
        tasks_phase2 = [
            ("queries/get_links_information_beneficial_owner.sql", "same_beneficial_owner",
             ["OriginalId", "MatchedId_base", "ParentId"]),
            ("queries/get_links_information_beneficial_subsidiaries.sql", "same_beneficial_subsidiary",
             ["OriginalId", "MatchedId_base", "ChildId"]),
        ]

        for sql, lbl, keys in tasks_phase2:
            try:
                lbl, ok_p, neg_p = _validate_worker(cand_remaining, sql, lbl, keys)
                if ok_p: results_ok[lbl] = ok_p
                if neg_p: all_neg_pairs.update(neg_p)
            except Exception as e:
                print(f"[ERROR] Phase 1 worker failed: {e}")

        # Application des résultats Phase 2
        priority_order_p2 = ["same_beneficial_owner", "same_beneficial_subsidiary"]
        for lbl in priority_order_p2:
            ok_pairs = results_ok.get(lbl, set())
            if not ok_pairs: continue

            mask_cand = df.index.isin(current_cand_idx)  # Toujours sur ceux qui restent
            subset = df[mask_cand]
            if subset.empty: break

            idx_ok = pd.MultiIndex.from_tuples(ok_pairs, names=["OriginalId", "MatchedId_base"])
            idx_subset = pd.MultiIndex.from_frame(subset[["OriginalId", "MatchedId_base"]])
            valid_indices = subset.index[idx_subset.isin(idx_ok)]

            if not valid_indices.empty:
                df.loc[valid_indices, "IsolationFilter"] = lbl
                current_cand_idx = current_cand_idx.difference(valid_indices)

    else:
        print("[SOLO] Phase 1 resolved 100% of cases. Skipping Phase 2.")

    # --- Final Steps (Globales) ---

    # 1. Archive Negative (On applique sur tout le monde car on a collecté les infos des 2 phases)
    if all_neg_pairs:
        idx_neg = pd.MultiIndex.from_tuples(all_neg_pairs, names=["OriginalId", "MatchedId_base"])
        current_idx = pd.MultiIndex.from_frame(df[["OriginalId", "MatchedId_base"]])
        df.loc[current_idx.isin(idx_neg), "Same_Archive_Negative"] = True

    # 2. Ultimate Owner (UO) - Sequential fallback (Last step)
    # Sur ce qu'il reste vraiment à la fin
    remaining_final = df.loc[current_cand_idx]

    if not remaining_final.empty:
        rem_orig = pd.unique(remaining_final["OriginalId"]).tolist()
        rem_match = pd.unique(remaining_final["MatchedId_base"].dropna()).tolist()

        uo_o = _fetch_uo_cached(rem_orig)
        uo_m = _fetch_uo_cached(rem_match)

        from collections import defaultdict
        uo_map_o, uo_map_m = defaultdict(set), defaultdict(set)

        if not uo_o.empty:
            for c, p in uo_o[["ChildId", "ParentId"]].itertuples(index=False):
                uo_map_o[str(c)].add(str(p))
        if not uo_m.empty:
            for c, p in uo_m[["ChildId", "ParentId"]].itertuples(index=False):
                uo_map_m[str(c)].add(str(p))

        def _same_uo_strict(row) -> bool:
            oset = uo_map_o.get(str(row["OriginalId"]), set())
            if not oset: return False
            mset = uo_map_m.get(str(row["MatchedId_base"]), set())
            if not mset: return False
            return oset == mset

        mask_uo = remaining_final.apply(_same_uo_strict, axis=1)
        valid_uo_indices = remaining_final[mask_uo].index
        if not valid_uo_indices.empty:
            df.loc[valid_uo_indices, "IsolationFilter"] = "same_uo"

    # Finalisation (Archive Fallback)
    mask_unfiltered = df["IsolationFilter"].isna() | (df["IsolationFilter"].astype("string").str.strip() == "")
    mask_archive_neg = df["Same_Archive_Negative"] == True

    df.loc[mask_unfiltered & mask_archive_neg, "IsolationFilter"] = "same_information"

    return df.drop(columns=["MatchedId_base"])


# --- Pipeline ---


def run_filter_step(df_in: pd.DataFrame, fn, label: str, **kwargs) -> pd.DataFrame:
    """
    Runs a filter function, logs elapsed time, and tags OriginalIds that become unique at this step.
    - Computes candidate counts before/after.
    - If an OriginalId transitions from >1 to exactly 1 row, sets IsolationFilter to the step label.
    """

    t = time.perf_counter()

    before_counts = (
        df_in["OriginalId"].value_counts() if not df_in.empty
        else pd.Series(dtype="int64")
    )

    out = fn(df_in, **kwargs) if kwargs else fn(df_in)

    after_counts = (
        out["OriginalId"].value_counts() if not out.empty
        else pd.Series(dtype="int64")
    )

    if not out.empty and "IsolationFilter" in out.columns and not after_counts.empty:
        before_on_after = before_counts.reindex(after_counts.index).fillna(0).astype(int)
        newly_unique = after_counts.index[(after_counts == 1) & (before_on_after > 1)]
        if len(newly_unique) > 0:
            mask = out["OriginalId"].isin(newly_unique) & out["IsolationFilter"].isna()
            out.loc[mask, "IsolationFilter"] = label

    print(f"[FILTER] {label.upper()}: {time.perf_counter()-t:.3f}s -> {len(out)} rows")
    return out


def apply_filters_pipeline(matches: pd.DataFrame) -> pd.DataFrame:
    """
    Isolation pipeline (order):
      1) hierarchy_of_id
      2) filter_by_subsidiaries
      3) filter_by_shareholders
      4) filter_by_uo
      5) filter_by_historic_since_date

    Sets IsolationFilter to the first step that makes an OriginalId unique and logs timings.
    """
    t0 = time.perf_counter()
    df = matches.copy() if matches is not None else pd.DataFrame()

    _uo_cache.clear()

    if df.empty:
        print("[FILTER] start: 0 rows")
        print(f"[FILTER] total: {time.perf_counter()-t0:.3f}s | final rows=0")
        return df

    if "IsolationFilter" not in df.columns:
        df["IsolationFilter"] = pd.NA

    print(f"[FILTER] start: {len(df)} rows")

    df = run_filter_step(df, filter_by_subsidiaries, "filter_by_subsidiaries")
    df = run_filter_step(df, filter_by_shareholders, "filter_by_shareholders")
    df = run_filter_step(df, filter_by_beneficial_owner, "filter_by_beneficial_owner")
    df = run_filter_step(df, filter_by_beneficial_subsidiaries, "filter_by_beneficial_subsidiaries")
    df = run_filter_step(df, filter_by_uo,           "filter_by_uo")
    df = run_filter_step(df, hierarchy_of_id,        "hierarchy_of_id")
    df = run_filter_step(df, filter_by_historic_since_date, "filter_by_historic_since")

    print(f"[FILTER] total filter : {time.perf_counter()-t0:.3f}s | final rows={len(df)}")
    return df

