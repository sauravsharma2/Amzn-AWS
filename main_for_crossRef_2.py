# main.py — Links + CrossRef matching with InactiveSinceDate filtering + Debug Sheet
# UPDATED: exact name-equality join replaced with fuzzy (RapidFuzz) similarity scoring

import sys
import time
import traceback
import warnings
from pathlib import Path
from typing import List
import os

import numpy as np
import pandas as pd
from rapidfuzz import fuzz

from run_finalize import finalize_run
from src.code.get_cases import get_cases_from_file
from src.code.name_normalisation import exact_normalization_series
from src.db_connexion.data_extraction import connect_db, load_query

script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir))

OUTPUT_DIR = Path("output")

# Fuzzy match threshold — tune here
SIMILARITY_THRESHOLD = 96


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_name(name: str, cc: str) -> str:
    try:
        return exact_normalization_series(pd.Series([name]), country_code=cc)["compact"].iloc[0]
    except Exception:
        return name


import re

_CROSSREF_NORM_RE = re.compile(r'[^A-Z0-9]')


def _norm_for_crossref(name: str) -> str:
    """Keep only uppercase alphanumeric characters."""
    return _CROSSREF_NORM_RE.sub('', str(name).upper())


def _insert_blank_rows_between_groups(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
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
    ids = [str(x) for x in orig_ids if pd.notna(x)]
    if not ids:
        return pd.DataFrame(columns=["OriginalId", "OldestSourceDate"])
    sql_tpl = load_query("queries/get_oldest_link_date.sql")
    ids_values = ",".join(f"('{x}')" for x in ids)
    sql = sql_tpl.format(orig_values=ids_values)
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    if not rows:
        return pd.DataFrame(columns=["OriginalId", "OldestSourceDate"])
    return (
        pd.DataFrame.from_records(rows, columns=cols)
        .astype({"OriginalId": "string", "OldestSourceDate": "string"})
    )


def _apply_inactive_filter(batch_result: pd.DataFrame) -> pd.DataFrame:
    """
    Filter based on InactiveSinceDate:
    - If OriginalId has 2+ matches:
        - Remove matches with InactiveSinceDate NOT NULL (if any have NULL)
        - If ALL have InactiveSinceDate NOT NULL, keep all
    - If OriginalId has 1 match: keep regardless of InactiveSinceDate
    """
    if batch_result.empty:
        return batch_result

    result_list = []
    for orig_id, group in batch_result.groupby("OriginalId", sort=False):
        group = group.reset_index(drop=True)

        # Count matches for this OriginalId
        match_count = len(group)

        if match_count == 1:
            # Single match → keep regardless of InactiveSinceDate
            result_list.append(group)
        else:
            # Multiple matches → check InactiveSinceDate
            has_null = group["InactiveSinceDate"].isna().any()

            if has_null:
                # Some have NULL → remove the NOT NULL ones, keep only NULL
                filtered = group[group["InactiveSinceDate"].isna()].copy()
            else:
                # All have NOT NULL → keep all
                filtered = group.copy()

            if not filtered.empty:
                result_list.append(filtered)

    if result_list:
        return pd.concat(result_list, ignore_index=True)
    else:
        return pd.DataFrame(columns=batch_result.columns)


# ─────────────────────────────────────────────────────────────────────────────
#  Core: Links → CrossRef validation
# ─────────────────────────────────────────────────────────────────────────────


@connect_db
def find_matches_via_links_crossref(conn, df_cases: pd.DataFrame, batch_size: int = 1000) -> tuple:
    """
    Returns: (result_df, debug_df)
    """
    if df_cases.empty:
        return (
            pd.DataFrame(
                columns=["OriginalId", "Name", "MatchedId", "MatchedName", "InactiveSinceDate",
                         "isFromCrossRef", "MatchScore"]),
            pd.DataFrame(
                columns=["OriginalId", "Name", "MatchedId", "InactiveSinceDate", "MatchScore",
                         "MatchCount", "FilterDecision"])
        )

    t_start = time.time()
    case_ids = df_cases["OriginalId"].astype(str).tolist()
    name_map = df_cases.set_index("OriginalId")["Name"].to_dict()
    norm_map = {cid: _norm_for_crossref(name) for cid, name in name_map.items()}

    t_cases = "#TCases"
    t_links1 = "#TLinks1"
    t_cands = "#TCandidates"

    EXCLUDED_PATTERNS = [
        "%PE%", "%SCRL%", "%CEBI%", "%CBNB%", "%CRL[ABC]%",
        "%CSTAT%", "%CFACT%", "%CWOR%", "%CSINO%", "%CPEI%",
        "%COFWI%", "%CWIN%", "%CVEDA%", "%CZEF%", "%CSIREN%",
        "%CNIF%", "%CFS%", "%CLEI%", "%CBOE%",
    ]

    def _excl(col):
        return "\n    AND ".join(f"{col} NOT LIKE '{p}'" for p in EXCLUDED_PATTERNS)

    excl_child = _excl("l2.ChildId")
    excl_parent = _excl("l2.ParentId")

    all_results: list[pd.DataFrame] = []
    all_results_debug: list[pd.DataFrame] = []
    n_batches = (len(case_ids) + batch_size - 1) // batch_size

    print(f"[MATCH] {len(case_ids)} IDs to process in {n_batches} batches of {batch_size}")

    for batch_idx, i in enumerate(range(0, len(case_ids), batch_size)):
        batch_ids = case_ids[i:i + batch_size]
        t_batch = time.time()
        stage_drops = []  # cases dropped before reaching CrossRef/fuzzy step, with reason
        print(f"[BATCH {batch_idx + 1}/{n_batches}] Processing {len(batch_ids)} IDs...")

        try:
            with conn.cursor() as cur:

                # Cleanup in case a previous batch crashed
                for t in (t_cases, t_links1, t_cands):
                    cur.execute(f"IF OBJECT_ID('tempdb..{t}') IS NOT NULL DROP TABLE {t}")
                conn.commit()

                # ── STEP 1: Insert batch into #TCases ───────────────────
                cur.execute(f"""
                    CREATE TABLE {t_cases} (
                        CaseId        VARCHAR(50)   NOT NULL PRIMARY KEY CLUSTERED,
                        CaseName      NVARCHAR(500) NOT NULL,
                        CaseNameNorm  VARCHAR(500)  NOT NULL,
                        CountryPrefix CHAR(2)       NOT NULL
                    )
                """)
                values = ",".join(
                    "('{cid}',N'{name}','{norm}','{pfx}')".format(
                        cid=cid,
                        name=str(name_map.get(cid, "")).replace("'", "''"),
                        norm=norm_map[cid],
                        pfx=cid[:2],
                    )
                    for cid in batch_ids
                )
                cur.execute(f"INSERT INTO {t_cases} VALUES {values}")
                conn.commit()

                # ── STEP 2a: Materialize direct links ─────────────────
                cur.execute(f"""
                    SELECT
                        t.CaseId            AS OriginalId,
                        t.CaseNameNorm      AS CaseNameNorm,
                        t.CountryPrefix     AS CountryPrefix,
                        l1.ParentId         AS SharedParent,
                        l1.ChildId          AS SharedChild,
                        l1.DirectPercent,
                        l1.TotalPercent,
                        l1.DirectPercentIndic,
                        l1.TotalPercentIndic,
                        'shareholder'       AS Direction
                    INTO {t_links1}
                    FROM {t_cases} t
                    JOIN bvdaffils.dbo.Links l1 WITH (NOLOCK)
                        ON l1.ChildId = t.CaseId AND l1.Type = 1

                    UNION ALL

                    SELECT
                        t.CaseId, t.CaseNameNorm, t.CountryPrefix,
                        l1.ParentId, l1.ChildId,
                        l1.DirectPercent, l1.TotalPercent,
                        l1.DirectPercentIndic, l1.TotalPercentIndic,
                        'subsidiary'
                    FROM {t_cases} t
                    JOIN bvdaffils.dbo.Links l1 WITH (NOLOCK)
                        ON l1.ParentId = t.CaseId AND l1.Type = 1

                    UNION ALL

                    -- Beneficial Owner (CaseId is the child in BO_links)
                    SELECT
                        t.CaseId, t.CaseNameNorm, t.CountryPrefix,
                        l1.ParentId, l1.ChildId,
                        l1.DirectPercent, l1.TotalPercent,
                        NULL AS DirectPercentIndic,
                        l1.TotalPercentIndic,
                        'beneficial_owner'
                    FROM {t_cases} t
                    JOIN bvdaffils.dbo.BO_links l1 WITH (NOLOCK)
                        ON l1.ChildId = t.CaseId

                    UNION ALL

                    -- Beneficial Subsidiary (CaseId is the parent in BO_links)
                    SELECT
                        t.CaseId, t.CaseNameNorm, t.CountryPrefix,
                        l1.ParentId, l1.ChildId,
                        l1.DirectPercent, l1.TotalPercent,
                        NULL AS DirectPercentIndic,
                        l1.TotalPercentIndic,
                        'beneficial_subsidiary'
                    FROM {t_cases} t
                    JOIN bvdaffils.dbo.BO_links l1 WITH (NOLOCK)
                        ON l1.ParentId = t.CaseId
                """)
                cur.execute(f"CREATE CLUSTERED INDEX IX_L1 ON {t_links1}(SharedParent, SharedChild, Direction)")
                conn.commit()
                cnt1 = cur.execute(f"SELECT COUNT(*) FROM {t_links1}").fetchone()[0]

                # Track cases with zero rows in t_links1 (dropped before candidate step)
                ids_with_links = {
                    r[0] for r in cur.execute(f"SELECT DISTINCT OriginalId FROM {t_links1}").fetchall()
                }
                ids_no_links = [cid for cid in batch_ids if cid not in ids_with_links]
                for cid in ids_no_links:
                    stage_drops.append({
                        "OriginalId": cid, "Name": name_map.get(cid), "MatchedId": pd.NA,
                        "InactiveSinceDate": pd.NA, "MatchScore": pd.NA, "MatchCount": 0,
                        "FilterDecision": "DROPPED (no Links/BO_links row found for this CaseId)"
                    })

                if cnt1 == 0:
                    print(f"[BATCH {batch_idx + 1}/{n_batches}] No links found, skipping.")
                    for t in (t_cases, t_links1):
                        cur.execute(f"DROP TABLE {t}")
                    conn.commit()
                    if stage_drops:
                        all_results_debug.append(pd.DataFrame(stage_drops))
                    continue

                # ── STEP 2b: Candidates (sibling entities) ───────────────────
                cur.execute(f"""
                    SELECT DISTINCT
                        b.OriginalId, b.CaseNameNorm, l2.ChildId AS CandidateId
                    INTO {t_cands}
                    FROM {t_links1} b
                    JOIN bvdaffils.dbo.Links l2 WITH (NOLOCK)
                        ON  l2.ParentId = b.SharedParent AND l2.Type = 1
                        AND l2.ChildId <> b.OriginalId
                        AND LEFT(l2.ChildId, 2) = b.CountryPrefix
                        AND l2.ChildId NOT LIKE '%*%'
                        AND ISNULL(l2.DirectPercent,      -999.0) = ISNULL(b.DirectPercent,      -999.0)
                        AND ISNULL(l2.TotalPercent,       -999.0) = ISNULL(b.TotalPercent,       -999.0)
                        AND ISNULL(l2.DirectPercentIndic, 'NULL') = ISNULL(b.DirectPercentIndic, 'NULL')
                        AND ISNULL(l2.TotalPercentIndic,  'NULL') = ISNULL(b.TotalPercentIndic,  'NULL')
                        AND {excl_child}
                    WHERE b.Direction = 'shareholder'

                    UNION

                    SELECT DISTINCT
                        b.OriginalId, b.CaseNameNorm, l2.ParentId AS CandidateId
                    FROM {t_links1} b
                    JOIN bvdaffils.dbo.Links l2 WITH (NOLOCK)
                        ON  l2.ChildId = b.SharedChild AND l2.Type = 1
                        AND l2.ParentId <> b.OriginalId
                        AND LEFT(l2.ParentId, 2) = b.CountryPrefix
                        AND l2.ParentId NOT LIKE '%*%'
                        AND ISNULL(l2.DirectPercent,      -999.0) = ISNULL(b.DirectPercent,      -999.0)
                        AND ISNULL(l2.TotalPercent,       -999.0) = ISNULL(b.TotalPercent,       -999.0)
                        AND ISNULL(l2.DirectPercentIndic, 'NULL') = ISNULL(b.DirectPercentIndic, 'NULL')
                        AND ISNULL(l2.TotalPercentIndic,  'NULL') = ISNULL(b.TotalPercentIndic,  'NULL')
                        AND {excl_parent}
                    WHERE b.Direction = 'subsidiary'

                    UNION

                    -- Beneficial Owner — candidates sharing same ParentId in BO_links
                    SELECT DISTINCT
                        b.OriginalId, b.CaseNameNorm, l2.ChildId AS CandidateId
                    FROM {t_links1} b
                    JOIN bvdaffils.dbo.BO_links l2 WITH (NOLOCK)
                        ON  l2.ParentId = b.SharedParent
                        AND l2.ChildId <> b.OriginalId
                        AND LEFT(l2.ChildId, 2) = b.CountryPrefix
                        AND l2.ChildId NOT LIKE '%*%'
                        AND ISNULL(l2.DirectPercent,      -999.0) = ISNULL(b.DirectPercent,      -999.0)
                        AND ISNULL(l2.TotalPercent,       -999.0) = ISNULL(b.TotalPercent,       -999.0)
                        AND ISNULL(l2.TotalPercentIndic,  'NULL') = ISNULL(b.TotalPercentIndic,  'NULL')
                        AND {excl_child}
                    WHERE b.Direction = 'beneficial_owner'

                    UNION

                    -- Beneficial Subsidiary — candidates sharing same ChildId in BO_links
                    SELECT DISTINCT
                        b.OriginalId, b.CaseNameNorm, l2.ParentId AS CandidateId
                    FROM {t_links1} b
                    JOIN bvdaffils.dbo.BO_links l2 WITH (NOLOCK)
                        ON  l2.ChildId = b.SharedChild
                        AND l2.ParentId <> b.OriginalId
                        AND LEFT(l2.ParentId, 2) = b.CountryPrefix
                        AND l2.ParentId NOT LIKE '%*%'
                        AND ISNULL(l2.DirectPercent,      -999.0) = ISNULL(b.DirectPercent,      -999.0)
                        AND ISNULL(l2.TotalPercent,       -999.0) = ISNULL(b.TotalPercent,       -999.0)
                        AND ISNULL(l2.TotalPercentIndic,  'NULL') = ISNULL(b.TotalPercentIndic,  'NULL')
                        AND {excl_parent}
                    WHERE b.Direction = 'beneficial_subsidiary'
                    OPTION (RECOMPILE, MAXDOP 4)
                """)
                cur.execute(f"DROP TABLE {t_links1}")
                conn.commit()

                total, distinct = cur.execute(
                    f"SELECT COUNT(*), COUNT(DISTINCT CandidateId) FROM {t_cands}"
                ).fetchone()

                # Track cases that had links but zero surviving sibling candidates
                # (dropped by percent/indicator/country-prefix/exclusion-pattern mismatch)
                ids_with_cands = {
                    r[0] for r in cur.execute(f"SELECT DISTINCT OriginalId FROM {t_cands}").fetchall()
                } if total > 0 else set()
                ids_no_cands = [cid for cid in ids_with_links if cid not in ids_with_cands]
                for cid in ids_no_cands:
                    stage_drops.append({
                        "OriginalId": cid, "Name": name_map.get(cid), "MatchedId": pd.NA,
                        "InactiveSinceDate": pd.NA, "MatchScore": pd.NA, "MatchCount": 0,
                        "FilterDecision": "DROPPED (link found, but no sibling passed percent/indicator/country/exclusion checks)"
                    })

                if total == 0:
                    print(f"[BATCH {batch_idx + 1}/{n_batches}] No candidates found.")
                    for t in (t_cases, t_cands):
                        cur.execute(f"DROP TABLE {t}")
                    conn.commit()
                    if stage_drops:
                        all_results_debug.append(pd.DataFrame(stage_drops))
                    continue

                # ── STEP 3: Read candidates into Python ──────────────────
                cands_rows = cur.execute(
                    f"SELECT OriginalId, CaseNameNorm, CandidateId FROM {t_cands}"
                ).fetchall()
                cands_df = pd.DataFrame.from_records(
                    cands_rows, columns=["OriginalId", "CaseNameNorm", "CandidateId"]
                )

                # ── STEP 4: CrossRef fetch ───────────────────────────────
                candidate_ids = cands_df["CandidateId"].unique().tolist()

                # Create temp table and bulk-insert
                cur.execute("CREATE TABLE #CandIds (CandidateId VARCHAR(MAX))")
                for chunk in [candidate_ids[i:i + 500] for i in range(0, len(candidate_ids), 500)]:
                    placeholders = ",".join(f"('{cid}')" for cid in chunk)
                    cur.execute(f"INSERT INTO #CandIds VALUES {placeholders}")

                # Fetch InactiveSinceDate from Companies table
                cr_rows = cur.execute("""
                    SELECT c.NewId, c.Name, comp.InactiveSinceDate
                    FROM bvdaffils.dbo.CrossRef c WITH (NOLOCK)
                    INNER JOIN #CandIds cand ON c.NewId = cand.CandidateId
                    LEFT JOIN bvdaffils.dbo.Companies comp WITH (NOLOCK) ON c.NewId = comp.Id
                    WHERE c.Name IS NOT NULL
                      --AND c.UserID NOT IN ('SASH', 'PSIN')
                """).fetchall()

                cur.execute("DROP TABLE #CandIds")
                for t in (t_cases, t_cands):
                    cur.execute(f"DROP TABLE {t}")
                conn.commit()

        except Exception as e:
            print(f"[BATCH {batch_idx + 1}/{n_batches}][ERROR] {e}")
            traceback.print_exc()
            try:
                with conn.cursor() as cur:
                    for t in (t_cases, t_links1, t_cands):
                        cur.execute(f"IF OBJECT_ID('tempdb..{t}') IS NOT NULL DROP TABLE {t}")
                    conn.commit()
            except Exception:
                pass
            if stage_drops:
                all_results_debug.append(pd.DataFrame(stage_drops))
            continue

        # ── STEP 5: Normalization + validation in Python ────────────────────
        if not cr_rows:
            print(f"[BATCH {batch_idx + 1}/{n_batches}] No CrossRef rows, skipping.")
            if stage_drops:
                all_results_debug.append(pd.DataFrame(stage_drops))
            continue

        cr_df = pd.DataFrame.from_records(cr_rows, columns=["NewId", "CrName", "InactiveSinceDate"])
        cr_df["CrNameNorm"] = cr_df["CrName"].apply(_norm_for_crossref)
        cr_valid = (
            cr_df.groupby(["NewId", "CrNameNorm"], as_index=False)
            .aggregate(
                InactiveSinceDate=("InactiveSinceDate", "first"),
                cnt=("NewId", "size")
            )
            .query("cnt >= 1")[["NewId", "CrNameNorm", "InactiveSinceDate"]]
        )

        # ── STEP 6: Final join — fuzzy name match instead of exact ──
        # (in-place replacement: merge on CandidateId only, then score name
        #  similarity instead of requiring exact CaseNameNorm == CrNameNorm)
        if cr_valid.empty:
            batch_result = pd.DataFrame(columns=["OriginalId", "MatchedId", "InactiveSinceDate", "MatchScore"])
        else:
            merged = cands_df.merge(
                cr_valid,
                left_on="CandidateId",
                right_on="NewId",
                how="inner"
            )

            if merged.empty:
                batch_result = pd.DataFrame(columns=["OriginalId", "MatchedId", "InactiveSinceDate", "MatchScore"])
            else:
                merged["MatchScore"] = merged.apply(
                    lambda r: fuzz.ratio(r["CaseNameNorm"], r["CrNameNorm"]), axis=1
                )

                batch_result = (
                    merged[merged["MatchScore"] >= SIMILARITY_THRESHOLD]
                    [["OriginalId", "CandidateId", "InactiveSinceDate", "MatchScore"]]
                    .sort_values("MatchScore", ascending=False)
                    .drop_duplicates(subset=["OriginalId", "CandidateId"], keep="first")
                )

        batch_result = (
            batch_result[batch_result["OriginalId"] != batch_result["CandidateId"]]
            .rename(columns={"CandidateId": "MatchedId"})
        ) if not batch_result.empty else batch_result.rename(columns={"CandidateId": "MatchedId"})

        batch_result["Name"] = batch_result["OriginalId"].map(name_map)

        # Prepare debug data before filtering
        if not batch_result.empty:
            debug_batch = batch_result.copy()
            debug_batch["MatchCount"] = debug_batch.groupby("OriginalId")["MatchedId"].transform("count")

            # Determine filter decision for each row
            def _get_filter_decision(row):
                match_count = row["MatchCount"]
                inactive_date = row["InactiveSinceDate"]

                if match_count == 1:
                    return "KEEP (single match)"
                elif pd.isna(inactive_date):
                    return "KEEP (active)"
                else:
                    # Multiple matches and this one is inactive
                    # Check if group has any active ones
                    return "REMOVE (inactive, multiple matches)"

            debug_batch["FilterDecision"] = debug_batch.apply(_get_filter_decision, axis=1)
            all_results_debug.append(
                debug_batch[["OriginalId", "Name", "MatchedId", "InactiveSinceDate", "MatchScore",
                              "MatchCount", "FilterDecision"]]
            )

        # Flush cases dropped earlier in this batch (no links / no candidates)
        # so their reason shows up in Debug_PreFilter too, not just silently missing
        if stage_drops:
            all_results_debug.append(pd.DataFrame(stage_drops))

        # Apply InactiveSinceDate filter
        batch_result = _apply_inactive_filter(batch_result)

        print(
            f"[BATCH {batch_idx + 1}/{n_batches}] {len(batch_result)} matches (after InactiveSinceDate filter) — {time.time() - t_batch:.2f}s")

        if not batch_result.empty:
            all_results.append(batch_result)

    # ── Concat all batches ─────────────────────────────────────────────
    if not all_results:
        print(f"[MATCH] No matches found across all batches — {time.time() - t_start:.2f}s")
        debug_df = pd.concat(all_results_debug, ignore_index=True) if all_results_debug else pd.DataFrame()
        return (
            pd.DataFrame(
                columns=["OriginalId", "Name", "MatchedId", "MatchedName", "InactiveSinceDate",
                         "isFromCrossRef", "MatchScore"]),
            debug_df
        )

    result = (
        pd.concat(all_results, ignore_index=True)
        .drop_duplicates(subset=["OriginalId", "MatchedId"])
        .reset_index(drop=True)
    )

    # ── STEP 7: Fetch MatchedName (once for all) ────────────
    t7 = time.time()
    company_map: dict = {}
    with conn.cursor() as cur:
        for i in range(0, len(result["MatchedId"].unique()), 1000):
            batch = result["MatchedId"].unique().tolist()[i:i + 1000]
            ids_str = ",".join(f"'{mid}'" for mid in batch)
            # Also fetch InactiveSinceDate here if not already available
            cur.execute(
                f"SELECT Id, Name, InactiveSinceDate FROM bvdaffils.dbo.Companies WITH (NOLOCK) WHERE Id IN ({ids_str})"
            )
            for row in cur.fetchall():
                company_map[row[0]] = (row[1], row[2])  # (Name, InactiveSinceDate)

    result["MatchedName"] = result["MatchedId"].map(lambda x: company_map.get(x, (None, None))[0])
    result["InactiveSinceDate"] = result["InactiveSinceDate"].fillna(
        result["MatchedId"].map(lambda x: company_map.get(x, (None, None))[1]))
    result["isFromCrossRef"] = 1
    print(f"[STEP 7] MatchedName fetched — {time.time() - t7:.2f}s")

    print(f"[MATCH] Total: {len(result)} matches across {n_batches} batches in {time.time() - t_start:.2f}s")

    # Prepare final debug df
    debug_df = pd.concat(all_results_debug, ignore_index=True) if all_results_debug else pd.DataFrame()

    return (
        result[["OriginalId", "Name", "MatchedId", "MatchedName", "InactiveSinceDate",
                 "isFromCrossRef", "MatchScore"]].reset_index(drop=True),
        debug_df
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def run() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    warnings.filterwarnings("ignore")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 0)
    pd.set_option("display.max_colwidth", None)

    # ── 1. Load cases ─────────────────────────────────────────────────────────
    print("[1] Loading cases from file...")
    df_cases = (
        get_cases_from_file()
        .rename(columns={"Id": "OriginalId"})
        .assign(CountryCode=lambda d: d["OriginalId"].astype(str).str[:2])
        .sort_values(["Name", "CountryCode"])
        .reset_index(drop=True)
    )
    print(f"    → {len(df_cases)} cases | countries: {sorted(df_cases['CountryCode'].unique())}")

    # ── 1b. Country filtering ──────────────────────────────────────────────────────

    # Countries to exclude entirely
    excluded_countries = {"US", "CN", "BR"}
    df_cases = df_cases[~df_cases["CountryCode"].isin(excluded_countries)]

    # Test subset (empty = all non-excluded countries)
    # test_countries = {"GB", "IT", "DE", "MX", "PT", "TH", "FR", "ES",
    #                    "PL", "BE", "CH", "LU", "HK", "NL", "PE", "CO", "RS",
    #                    "HU", "AL", "AT"}
    test_countries = {}

    print(f"    → After exclusions ({', '.join(sorted(excluded_countries))}): {len(df_cases)} cases")

    if test_countries:
        df_cases = df_cases[df_cases["CountryCode"].isin(test_countries)]
        print(f"    → Test subset ({', '.join(sorted(test_countries))}): {len(df_cases)} cases")

    if df_cases.empty:
        print("    [WARN] No cases remaining after country filtering, stopping.")
        return

    print(f"    → Final countries to process: {sorted(df_cases['CountryCode'].unique())}")
    # ── 2. Matching ────────────────────────────────────────────────────────────
    print("[2] Matching via Links + CrossRef (fuzzy name match, threshold="
          f"{SIMILARITY_THRESHOLD})...")
    t_match = time.perf_counter()

    matched_df_raw, debug_df = find_matches_via_links_crossref(
        df_cases[["OriginalId", "Name"]].drop_duplicates("OriginalId").reset_index(drop=True)
    )
    print(f"    → {len(matched_df_raw)} matches in {time.perf_counter() - t_match:.1f}s")

    # ── 3. Blacklist filtering ─────────────────────────────────────────────────
    # blacklist_path = os.path.join("..", "BlockedMatchings.txt")
    # if not matched_df_raw.empty and os.path.exists(blacklist_path):
    #     df_blacklist = pd.read_csv(blacklist_path, sep="\t", dtype=str, header=0)
    #     orig_cols = matched_df_raw.columns.tolist()
    #     matched_df_raw["_MatchedId_clean"] = matched_df_raw["MatchedId"].str.rstrip("!")
    #     merged = matched_df_raw.merge(
    #         df_blacklist,
    #         left_on  = ["OriginalId", "_MatchedId_clean"],
    #         right_on = ["OldID", "NewID"],
    #         how      = "left",
    #         indicator = True,
    #     )
    #     matched_df_raw = (
    #         merged[merged["_merge"] == "left_only"][orig_cols]
    #         .reset_index(drop=True)
    #     )
    #     print(f"    → {len(matched_df_raw)} matches after blacklist filtering")
    # elif not os.path.exists(blacklist_path):
    #     print(f"    [WARN] Blacklist not found at {blacklist_path}, skipping.")

    # ── 4. Add NameNormalized ──────────────────────────────────────────────────
    if not matched_df_raw.empty:
        matched_df_raw["CountryCode"] = matched_df_raw["OriginalId"].astype(str).str[:2]
        matched_df_raw["NameNormalized"] = [
            _normalize_name(n, c)
            for n, c in zip(matched_df_raw["Name"], matched_df_raw["CountryCode"])
        ]
    else:
        matched_df_raw["CountryCode"] = pd.NA
        matched_df_raw["NameNormalized"] = pd.NA
        matched_df_raw["InactiveSinceDate"] = pd.NA
        matched_df_raw["MatchScore"] = pd.NA

    # ── 5. Build export_df ─────────────────────────────────────────────────────
    EXPORT_COLS = ["OriginalId", "Name", "NameNormalized", "MatchedId", "MatchedName",
                   "InactiveSinceDate", "isFromCrossRef", "MatchScore"]

    if matched_df_raw.empty:
        export_df = pd.DataFrame(columns=EXPORT_COLS)
    else:
        export_df = (
            matched_df_raw[EXPORT_COLS]
            .sort_values(["OriginalId", "MatchedId"], kind="stable")
            .reset_index(drop=True)
        )

    # ── 6. UniqueMatch — exactly 1 validated candidate ─────────────────────────
    if export_df.empty:
        unique_df = pd.DataFrame(columns=EXPORT_COLS)
    else:
        counts = export_df["OriginalId"].value_counts()
        unique_df = (
            export_df[export_df["OriginalId"].isin(counts[counts == 1].index)]
            .sort_values(["OriginalId", "MatchedId"], kind="stable")
            .reset_index(drop=True)
        )

    # Add OldestSourceDate
    if not unique_df.empty:
        oldest = _fetch_oldest_link(unique_df["OriginalId"].astype(str).tolist())
        unique_df = (
            unique_df
            .merge(oldest, on="OriginalId", how="left", validate="one_to_one")
            .assign(OldestSourceDate=lambda d: d["OldestSourceDate"].astype("string"))
        )

    # ── 7. NoMatch ─────────────────────────────────────────────────────────────
    matched_orig_ids = set(export_df["OriginalId"].unique()) if not export_df.empty else set()
    df_nomatch = (
        df_cases[~df_cases["OriginalId"].isin(matched_orig_ids)]
        [["OriginalId", "Name", "CountryCode"]]
        .drop_duplicates("OriginalId")
        .reset_index(drop=True)
    )
    if not df_nomatch.empty:
        df_nomatch["NameNormalized"] = [
            _normalize_name(n, c)
            for n, c in zip(df_nomatch["Name"], df_nomatch["CountryCode"])
        ]
        df_nomatch = df_nomatch[["OriginalId", "Name", "NameNormalized"]]
    else:
        df_nomatch = pd.DataFrame(columns=["OriginalId", "Name", "NameNormalized"])

    # ── 8. Export Excel ────────────────────────────────────────────────────────
    xlsx_path = os.path.join(OUTPUT_DIR, "matches.xlsx")
    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:

            # Entries
            df_cases[["OriginalId", "Name"]].to_excel(writer, index=False, sheet_name="Entries")

            # Matching_Propositions (all validated candidates, blank rows between groups)
            if export_df.empty:
                matching_props = pd.DataFrame(columns=EXPORT_COLS)
            else:
                matching_props = _insert_blank_rows_between_groups(export_df.copy(), "OriginalId")
            matching_props.to_excel(writer, index=False, sheet_name="Matching_Propositions")

            # Debug_PreFilter sheet
            if debug_df.empty:
                debug_check = pd.DataFrame(
                    columns=["OriginalId", "Name", "MatchedId", "InactiveSinceDate", "MatchScore",
                             "MatchCount", "FilterDecision"])
            else:
                debug_check = _insert_blank_rows_between_groups(debug_df.copy(), "OriginalId")
            debug_check.to_excel(writer, index=False, sheet_name="Debug_PreFilter")

            # UniqueMatch
            unique_df.to_excel(writer, index=False, sheet_name="UniqueMatch")

            # NoMatch
            df_nomatch.to_excel(writer, index=False, sheet_name="NoMatch")

            # Completed
            pd.DataFrame([["Matchings applied in OBO minus the ones that were blocked"]]).to_excel(
                writer, index=False, header=False, sheet_name="Completed"
            )

            # MatchingOK — formulas referencing UniqueMatch
            # Column layout in UniqueMatch: A=OriginalId, B=Name, C=NameNormalized,
            #                               D=MatchedId,  E=MatchedName, F=InactiveSinceDate,
            #                               G=isFromCrossRef, H=MatchScore, ...
            nrows = len(unique_df)
            if nrows == 0:
                matchingok = pd.DataFrame(columns=["OldID", "NewID", "UserID"])
            else:
                base_row = 2
                rows_idx = np.arange(nrows)
                matchingok = pd.DataFrame({
                    "OldID": [
                        f'=IF(ISBLANK(UniqueMatch!A{base_row + i}),"",UniqueMatch!A{base_row + i})'
                        for i in rows_idx
                    ],
                    "NewID": [
                        f'=IF(ISBLANK(UniqueMatch!D{base_row + i}),"",SUBSTITUTE(UniqueMatch!D{base_row + i},"!",""))'
                        for i in rows_idx
                    ],
                    "UserID": ["SASH"] * nrows,
                })
            matchingok.to_excel(writer, index=False, sheet_name="MatchingOK")

        print(
            f"[EXPORT] {xlsx_path} | "
            f"Propositions={len(matching_props)} | "
            f"Debug={len(debug_check)} | "
            f"UniqueMatch={len(unique_df)} | "
            f"NoMatch={len(df_nomatch)}"
        )
    except Exception as e:
        print(f"[EXPORT][ERROR] Excel: {e}")
        traceback.print_exc()

    # ── 9. Export TXT (UniqueMatch only) ───────────────────────────────────────
    txt_path = os.path.join(OUTPUT_DIR, "output.txt")
    try:
        if unique_df.empty:
            out_txt = pd.DataFrame(columns=["OldID", "NewID", "UserID"])
        else:
            out_txt = (
                unique_df[["OriginalId", "MatchedId"]]
                .dropna()
                .astype(str)
                .drop_duplicates()
                .rename(columns={"OriginalId": "OldID", "MatchedId": "NewID"})
                .assign(UserID="SASH")[["OldID", "NewID", "UserID"]]
            )
        out_txt.to_csv(txt_path, sep="\t", index=False, lineterminator="\n")
        print(f"[EXPORT] {txt_path} — {len(out_txt)} lines")
    except Exception as e:
        print(f"[EXPORT][ERROR] TXT: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    t0 = time.perf_counter()
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    exit_code = 0
    metrics = {}

    try:
        run()
    except Exception:
        exit_code = 1
        metrics = {"error": 1}
        traceback.print_exc()
    finally:
        file1 = os.path.join(OUTPUT_DIR, "matches.xlsx")
        extra = []
        p = Path(file1)
        if p.exists():
            extra.append((str(p), "file", "xlsx", False))

        finalize_run(
            project_name="MSOF_direct_semi_direct_entries",
            started_at=started_at,
            exit_code=exit_code,
            metrics=metrics,
            require_primary_txt=True,
            extra_artifacts=extra,
        )

        elapsed_s = time.perf_counter() - t0
        print(f"[TIMER] Total: {elapsed_s / 60:.2f} min ({elapsed_s:.1f} s)")
        sys.exit(exit_code)