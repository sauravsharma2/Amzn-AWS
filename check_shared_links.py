# check_shared_links.py — adds SharedOwnershipLink (info only) to a matches Excel.
#
# Standalone:
#   python check_shared_links.py                                   # output/matches.xlsx -> output/matches_with_links.xlsx
#   python check_shared_links.py path/to/matches.xlsx              # -> path/to/matches_with_links.xlsx
#   python check_shared_links.py in.xlsx -o out.xlsx
#
# Input : Excel with a 'Matched' sheet containing OriginalId and MatchedId (other sheets are copied as-is).
# Output: same workbook + column SharedOwnershipLink on 'Matched'. No rows removed.
#
# Also imported by main_exact_match.py (add_shared_links).

import argparse
import sys
import time
import traceback
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from src.db_connexion.data_extraction import connect_db, load_query

SHARED_LINKS_SQL = "queries/get_shared_ownership_links.sql"
PAIR_BATCH_SIZE = 2000            # same as filter._fetch_links_for_pairs
SHARED_LINKS_MAX_IDS = 5          # max shared IDs listed per link type in the cell (count always shown)

MATCHED_SHEET = "Matched"
SHARED_LINK_COL = "SharedOwnershipLink"
LINK_TYPE_ORDER = ["Shareholder", "Subsidiary", "BO", "BO Subsidiary", "UO"]
NO_LINK_TEXT = "No shared link"
FAILED_TEXT = "N/A (lookup failed)"


# ─────────────────────────────────────────────────────────────────────────────
#  SQL (same pattern as filter._fetch_links_for_pairs)
# ─────────────────────────────────────────────────────────────────────────────

@connect_db
def fetch_shared_links(conn, pairs: List[Tuple[str, str]], batch_size: int = PAIR_BATCH_SIZE) -> pd.DataFrame:
    """
    pairs = [(OriginalId_base, MatchedId_base)].
    Returns OriginalId, MatchedId, LinkType, SharedId (one row per shared entity).
    """
    cols = ["OriginalId", "MatchedId", "LinkType", "SharedId"]
    data = [(str(o), str(m)) for o, m in pairs if pd.notna(o) and pd.notna(m)]
    if not data:
        return pd.DataFrame(columns=cols)

    sql_tpl = load_query(SHARED_LINKS_SQL)
    results = []
    n_batches = (len(data) + batch_size - 1) // batch_size

    with conn.cursor() as cur:
        try:
            cur.fast_executemany = True
        except Exception:
            pass

        for b, i in enumerate(range(0, len(data), batch_size), start=1):
            batch = data[i:i + batch_size]
            t0 = time.time()

            all_ids = list({x for pair in batch for x in pair})
            ids_list = ", ".join("'{}'".format(x.replace("'", "''")) for x in all_ids)

            try:
                cur.execute("""
                    CREATE TABLE #PairsToProcess (
                        O_Id VARCHAR(50) COLLATE Database_Default,
                        M_Id VARCHAR(50) COLLATE Database_Default
                    )
                """)
                cur.executemany("INSERT INTO #PairsToProcess (O_Id, M_Id) VALUES (?, ?)", batch)
                cur.execute("CREATE CLUSTERED INDEX IX_Pairs ON #PairsToProcess(O_Id, M_Id)")

                cur.execute(sql_tpl.replace("{ids_list}", ids_list))

                while cur.description is None:
                    if not cur.nextset():
                        break
                rows = cur.fetchall() if cur.description else []
                if rows:
                    results.append(pd.DataFrame.from_records([tuple(r) for r in rows], columns=cols))
            finally:
                cur.execute("DROP TABLE IF EXISTS #PairsToProcess")
                cur.execute("DROP TABLE IF EXISTS #RawShared")

            print(f"[LINKS] batch {b}/{n_batches} | pairs={len(batch)} | shared rows={len(rows)} "
                  f"| {time.time() - t0:.1f}s", flush=True)

    return pd.concat(results, ignore_index=True).drop_duplicates() if results else pd.DataFrame(columns=cols)


# ─────────────────────────────────────────────────────────────────────────────
#  Formatting
# ─────────────────────────────────────────────────────────────────────────────

def format_shared_links(links: pd.DataFrame) -> dict:
    """
    {(OriginalId, MatchedId): 'Shareholder(2): DE123, DE456 | UO(1): US999'}.
    Max SHARED_LINKS_MAX_IDS IDs listed per type, count always shown.
    """
    out = {}
    if links.empty:
        return out
    for (o, m), g in links.groupby(["OriginalId", "MatchedId"], sort=False):
        parts = []
        for lt in LINK_TYPE_ORDER:
            ids = sorted(g.loc[g["LinkType"] == lt, "SharedId"].astype(str).unique())
            if not ids:
                continue
            shown = ", ".join(ids[:SHARED_LINKS_MAX_IDS])
            more = f" +{len(ids) - SHARED_LINKS_MAX_IDS} more" if len(ids) > SHARED_LINKS_MAX_IDS else ""
            parts.append(f"{lt}({len(ids)}): {shown}{more}")
        out[(str(o), str(m))] = " | ".join(parts)
    return out


def add_shared_links(matched: pd.DataFrame) -> pd.DataFrame:
    """Adds/overwrites SharedOwnershipLink. Never removes rows."""
    df = matched.copy()
    if df.empty:
        df[SHARED_LINK_COL] = pd.Series(dtype="object")
        return df

    o_base = df["OriginalId"].astype(str).str.strip().str.rstrip("!")
    m_base = df["MatchedId"].astype(str).str.strip().str.rstrip("!")
    pairs = list(dict.fromkeys(zip(o_base, m_base)))
    print(f"[LINKS] {len(pairs)} unique pairs to check")

    try:
        links = fetch_shared_links(pairs)
    except Exception as e:
        print(f"[LINKS][ERROR] shared link lookup failed -> '{FAILED_TEXT}': {e}")
        traceback.print_exc()
        df[SHARED_LINK_COL] = FAILED_TEXT
        return df

    lookup = format_shared_links(links)
    df[SHARED_LINK_COL] = [lookup.get((o, m)) or NO_LINK_TEXT for o, m in zip(o_base, m_base)]
    n_with = int((df[SHARED_LINK_COL] != NO_LINK_TEXT).sum())
    print(f"[LINKS] {n_with}/{len(df)} propositions have at least one shared ownership link")
    return df


# ─────────────────────────────────────────────────────────────────────────────
#  Standalone run
# ─────────────────────────────────────────────────────────────────────────────

def run_on_excel(in_path: Path, out_path: Path) -> None:
    sheets = pd.read_excel(in_path, sheet_name=None, dtype=str, keep_default_na=False)
    if MATCHED_SHEET not in sheets:
        raise ValueError(f"Sheet '{MATCHED_SHEET}' not found in {in_path}. Sheets: {list(sheets)}")

    matched = sheets[MATCHED_SHEET]
    for c in ("OriginalId", "MatchedId"):
        if c not in matched.columns:
            raise ValueError(f"Column '{c}' missing in sheet '{MATCHED_SHEET}'")

    matched = matched.drop(columns=[SHARED_LINK_COL], errors="ignore")
    matched = matched[(matched["OriginalId"] != "") & (matched["MatchedId"] != "")]
    print(f"[INPUT] {in_path} | {len(matched)} propositions")

    t = time.perf_counter()
    sheets[MATCHED_SHEET] = add_shared_links(matched)
    print(f"[LINKS] done in {time.perf_counter() - t:.1f}s")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        for name, df in sheets.items():
            df.to_excel(w, index=False, sheet_name=name)
    print(f"[EXPORT] {out_path}")


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Add SharedOwnershipLink column to a matches Excel.")
    ap.add_argument("input", nargs="?", default="output/matches.xlsx", help="matches Excel (default: output/matches.xlsx)")
    ap.add_argument("-o", "--output", help="output Excel (default: <input>_with_links.xlsx)")
    args = ap.parse_args()

    in_path = Path(args.input)
    out_path = Path(args.output) if args.output else in_path.with_name(f"{in_path.stem}_with_links.xlsx")
    try:
        run_on_excel(in_path, out_path)
        return 0
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
