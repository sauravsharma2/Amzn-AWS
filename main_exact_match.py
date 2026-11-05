# main_exact_match.py — exact name matching (normalised name + legal form). Links used only for an info column.
#
# Flow:
#   1) Load original IDs from cases.txt (+ Name/Address/City/Postcode/Country from Companies).
#   2) Stream the population from queries/fetch_population.sql in batches.
#   3) Normalise both sides (name_normalisation, country hook = first 2 chars of the Id).
#   4) Keep pairs where:
#        - normalised compact name is 100% equal,
#        - legal forms identical (no legal form only matches no legal form),
#        - country rule passes (same prefix / matched YY / allowed pair / original YY),
#        - OriginalId != MatchedId.
#   5) Export output/matches.xlsx with 2 sheets: Matched, NoMatch (all propositions kept).
#   6) Add SharedOwnershipLink (info only, check_shared_links.py) and export again.
#      If step 6 fails/hangs, the file from step 5 is already saved.
#      Re-run step 6 alone: python check_shared_links.py output/matches.xlsx

import os
import re
import sys
import time
import traceback
import warnings
from pathlib import Path
from typing import Iterator, List
from unicodedata import category, normalize

import pandas as pd

from check_shared_links import add_shared_links
from run_finalize import finalize_run
import src.code.name_normalisation as nn
from src.code.name_normalisation import _build_legal_union, exact_normalization_series
from src.db_connexion.data_extraction import connect_db, load_query
from utils.config import LEGAL_FORM_DICT, WORDS_TO_DELETE

script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir))

# ─────────────────────────────────────────────────────────────────────────────
#  Settings
# ─────────────────────────────────────────────────────────────────────────────

PROJECT_NAME = "Exact_name_legal_form_matching"
CASES_FILE = "cases.txt"
CASES_SQL = "queries/get_cases_with_address.sql"
POPULATION_SQL = "queries/fetch_population.sql"   # your population query
POPULATION_BATCH_SIZE = 500_000

OUTPUT_DIR = Path("output")
XLSX_PATH = OUTPUT_DIR / "matches.xlsx"

# Wildcard country: matched side may always be YY; original YY may match any country.
WILDCARD_PREFIX = "YY"
# Extra allowed country pairs (both directions).
ALLOWED_COUNTRY_PAIRS = {("CN", "HK")}

# Words from utils/config.py WORDS_TO_DELETE that should NOT be removed in this script.
# Only affects main_exact_match.py (other scripts keep the full list). Lowercase.
# Example: WORDS_TO_KEEP = {"in", "en", "cooperativa"}
WORDS_TO_KEEP: set = set()

DETAIL_COLS = ["Name", "Address", "City", "Country", "Postcode"]

MATCHED_SHEET_COLS = [
    "OriginalId", "OriginalName", "OriginalNormalised", "OriginalAddress",
    "OriginalCity", "OriginalCountry", "OriginalPostcode",
    "MatchedId", "MatchedName", "MatchedNormalised", "MatchedAddress",
    "MatchedCity", "MatchedCountry", "MatchedPostcode",
]
NOMATCH_SHEET_COLS = MATCHED_SHEET_COLS[:7]


# ─────────────────────────────────────────────────────────────────────────────
#  Legal form extraction (same preprocessing + same regexes as name_normalisation)
# ─────────────────────────────────────────────────────────────────────────────

_LEGAL_CODES: List[str] = list(LEGAL_FORM_DICT.values())
_LEGAL_NAMED_RE = re.compile(
    "|".join(
        f"(?P<lf{i}>{_build_legal_union([pattern])})"
        for i, pattern in enumerate(LEGAL_FORM_DICT.keys())
    ),
    re.IGNORECASE,
)
_SPACES_RE = re.compile(r"\s+")


def _prep_for_legal(name: str) -> str:
    """Same steps BaseNormalizer runs before removing legal forms: lower, no accents, spaces, no dots."""
    t = "".join(c for c in normalize("NFD", str(name).lower()) if category(c) != "Mn")
    t = _SPACES_RE.sub(" ", t).strip()
    return t.replace(".", "")


def extract_legal_forms(name) -> str:
    """
    Returns the normalised legal form code(s) found in a name, as a sorted '|' string.
    '' = no legal form. Example: 'Siemens A.G.' -> 'AG', 'X GmbH & Co KG' -> 'GMBH|KG'.
    Codes come from the LegalForm column of legal_form_mapping.csv.
    """
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    codes = {
        _LEGAL_CODES[int(m.lastgroup[2:])]
        for m in _LEGAL_NAMED_RE.finditer(_prep_for_legal(name))
        if m.lastgroup
    }
    return "|".join(sorted(codes))


def legal_forms_compatible(orig_lf: pd.Series, match_lf: pd.Series) -> pd.Series:
    """
    Strict: legal form code(s) must be identical on both sides.
    - Original has legal form X  -> matched must have X.
    - Original has no legal form -> matched must have no legal form.
    """
    return orig_lf.fillna("") == match_lf.fillna("")


# ─────────────────────────────────────────────────────────────────────────────
#  Country rule
# ─────────────────────────────────────────────────────────────────────────────

def country_rule_ok(orig_ids: pd.Series, match_ids: pd.Series) -> pd.Series:
    """
    - Original YY        -> any matched country.
    - Same 2-char prefix -> ok.
    - Matched YY         -> ok.
    - Allowed pair (CN<->HK, both directions) -> ok.
    """
    o = orig_ids.astype(str).str[:2].str.upper()
    m = match_ids.astype(str).str[:2].str.upper()

    ok = (o == WILDCARD_PREFIX) | (o == m) | (m == WILDCARD_PREFIX)
    pairs = ALLOWED_COUNTRY_PAIRS | {(b, a) for a, b in ALLOWED_COUNTRY_PAIRS}
    if pairs:
        ok |= pd.Series([(a, b) in pairs for a, b in zip(o, m)], index=orig_ids.index)
    return ok


# ─────────────────────────────────────────────────────────────────────────────
#  Normalisation (per country prefix, once per unique name)
# ─────────────────────────────────────────────────────────────────────────────

def normalise_names(df: pd.DataFrame, id_col: str = "Id", name_col: str = "Name") -> pd.Series:
    """Returns the 'compact' normalised name for each row, using the country hook of the Id prefix."""
    out = pd.Series("", index=df.index, dtype="object")
    if df.empty:
        return out
    names = df[name_col].fillna("").astype(str)
    prefixes = df[id_col].astype(str).str[:2].str.upper()
    for cc, idx in prefixes.groupby(prefixes).groups.items():
        sub = names.loc[idx]
        uniq = sub.unique()
        norm = exact_normalization_series(pd.Series(uniq), already_clean=False, country_code=cc)
        out.loc[idx] = sub.map(pd.Series(norm["compact"].values, index=uniq)).values
    return out


# ─────────────────────────────────────────────────────────────────────────────
#  DB access
# ─────────────────────────────────────────────────────────────────────────────

def read_case_ids(file_path: str = CASES_FILE) -> List[str]:
    with open(file_path, "r", encoding="utf-8") as f:
        ids = [line.strip() for line in f if line.strip()]
    return list(dict.fromkeys(ids))  # dedupe, keep order


@connect_db
def fetch_cases(conn, ids: List[str]) -> pd.DataFrame:
    cols = ["Id"] + DETAIL_COLS
    if not ids:
        return pd.DataFrame(columns=cols)

    cur = conn.cursor()
    try:
        cur.execute("""
            IF OBJECT_ID('tempdb..#TempIds') IS NOT NULL DROP TABLE #TempIds;
            CREATE TABLE #TempIds (Id VARCHAR(50) NOT NULL PRIMARY KEY);
        """)
        for i in range(0, len(ids), 1000):
            values = ",".join("('{}')".format(x.replace("'", "''")) for x in ids[i:i + 1000])
            cur.execute(f"INSERT INTO #TempIds (Id) VALUES {values}")
        conn.commit()

        cur.execute(load_query(CASES_SQL))
        rows = cur.fetchall()
        df = pd.DataFrame.from_records(rows, columns=[c[0] for c in cur.description])
    finally:
        try:
            cur.execute("IF OBJECT_ID('tempdb..#TempIds') IS NOT NULL DROP TABLE #TempIds")
            conn.commit()
        except Exception as e:
            print(f"[WARN] cleanup #TempIds: {e}")

    print(f"[CASES] found {len(df)}/{len(ids)} IDs in Companies")
    return df


@connect_db
def _run_query(conn, sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame.from_records(rows, columns=cols)


def stream_population(batch_size: int = POPULATION_BATCH_SIZE) -> Iterator[pd.DataFrame]:
    """Pages through POPULATION_SQL using {last_id}/{batch_size}. Runs once if placeholders are absent."""
    tpl = load_query(POPULATION_SQL)
    paged = "{last_id}" in tpl
    last_id, total, n = "", 0, 0

    while True:
        n += 1
        sql = tpl.replace("{batch_size}", str(batch_size)).replace("{last_id}", last_id.replace("'", "''"))
        t0 = time.time()
        df = _run_query(sql)
        if df.empty:
            break
        total += len(df)
        print(f"[POPULATION] batch #{n} | rows={len(df):,} | total={total:,} | {time.time() - t0:.1f}s", flush=True)
        yield df
        if not paged:
            break
        new_last = str(df["Id"].iloc[-1])
        if new_last == last_id:  # safety: SQL not ordered / not filtered by last_id
            raise RuntimeError("Population paging stuck: check ORDER BY Id and Id > '{last_id}' in the SQL.")
        last_id = new_last
    print(f"[POPULATION] done | {total:,} rows")


# ─────────────────────────────────────────────────────────────────────────────
#  Matching
# ─────────────────────────────────────────────────────────────────────────────

def apply_words_to_keep() -> None:
    """Rebuilds name_normalisation.WORDS_RE without WORDS_TO_KEEP (local to this script's run)."""
    keep = {w.lower() for w in WORDS_TO_KEEP}
    if not keep:
        return
    words = sorted({w.lower() for w in WORDS_TO_DELETE} - keep)
    nn.WORDS_RE = (
        re.compile(r"\b(?:%s)\b" % "|".join(map(re.escape, words)), re.IGNORECASE)
        if words else re.compile(r"$(?!)")
    )
    print(f"[NORMALISATION] Not removing: {sorted(keep)}")


def prepare_cases(df_cases: pd.DataFrame) -> pd.DataFrame:
    df = df_cases.copy()
    df["Id"] = df["Id"].astype(str)
    df["Normalised"] = normalise_names(df)
    df["LegalForm"] = df["Name"].map(extract_legal_forms)
    return df


def match_batch(cases: pd.DataFrame, population: pd.DataFrame) -> pd.DataFrame:
    """Exact compact-name join, then country rule, legal form rule, no self-match."""
    keyed = cases[cases["Normalised"] != ""]
    if keyed.empty or population.empty:
        return pd.DataFrame(columns=MATCHED_SHEET_COLS)

    pop = population.copy()
    pop["Id"] = pop["Id"].astype(str)
    for c in DETAIL_COLS:
        if c not in pop.columns:
            pop[c] = pd.NA
    pop["Normalised"] = normalise_names(pop)
    pop = pop[pop["Normalised"] != ""]

    orig = keyed[["Id", "Normalised", "LegalForm"] + DETAIL_COLS].add_prefix("Original")
    mat = pop[["Id", "Normalised"] + DETAIL_COLS].add_prefix("Matched")

    m = orig.merge(mat, left_on="OriginalNormalised", right_on="MatchedNormalised", how="inner")
    if m.empty:
        return pd.DataFrame(columns=MATCHED_SHEET_COLS)

    m = m[m["OriginalId"] != m["MatchedId"]]
    m = m[country_rule_ok(m["OriginalId"], m["MatchedId"])]
    if m.empty:
        return pd.DataFrame(columns=MATCHED_SHEET_COLS)

    # Legal forms only computed for surviving candidates (cheap)
    uniq_names = m["MatchedName"].dropna().unique()
    lf_map = {n: extract_legal_forms(n) for n in uniq_names}
    m_lf = m["MatchedName"].map(lf_map).fillna("")
    m = m[legal_forms_compatible(m["OriginalLegalForm"], m_lf)]

    return m[MATCHED_SHEET_COLS]


def run_matching(cases: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for batch in stream_population():
        res = match_batch(cases, batch)
        if not res.empty:
            parts.append(res)
            print(f"[MATCH] +{len(res)} pairs", flush=True)
    if not parts:
        return pd.DataFrame(columns=MATCHED_SHEET_COLS)
    return (
        pd.concat(parts, ignore_index=True)
        .drop_duplicates(subset=["OriginalId", "MatchedId"])
        .sort_values(["OriginalId", "MatchedId"], kind="stable")
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Export
# ─────────────────────────────────────────────────────────────────────────────

def build_nomatch(case_ids: List[str], cases: pd.DataFrame, matched: pd.DataFrame) -> pd.DataFrame:
    """All cases.txt IDs without any proposition (IDs not found in Companies included, details empty)."""
    base = pd.DataFrame({"Id": case_ids}).merge(
        cases[["Id", "Normalised"] + DETAIL_COLS], on="Id", how="left"
    )
    base = base.add_prefix("Original")
    done = set(matched["OriginalId"]) if not matched.empty else set()
    return base[~base["OriginalId"].isin(done)][NOMATCH_SHEET_COLS].reset_index(drop=True)


def export(matched: pd.DataFrame, nomatch: pd.DataFrame, path: Path = XLSX_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        matched.to_excel(w, index=False, sheet_name="Matched")
        nomatch.to_excel(w, index=False, sheet_name="NoMatch")
    n_orig = matched["OriginalId"].nunique() if not matched.empty else 0
    print(f"[EXPORT] {path} | Matched rows={len(matched)} ({n_orig} original IDs) | NoMatch={len(nomatch)}")


# ─────────────────────────────────────────────────────────────────────────────
#  Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def run() -> dict:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    warnings.filterwarnings("ignore")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    apply_words_to_keep()

    print("[1] Loading cases...")
    case_ids = read_case_ids()
    cases = prepare_cases(fetch_cases(case_ids))
    empty_key = (cases["Normalised"] == "").sum()
    if empty_key:
        print(f"[WARN] {empty_key} cases have an empty normalised name -> cannot match (go to NoMatch)")

    print("[2] Matching against population...")
    t = time.perf_counter()
    matched = run_matching(cases)
    print(f"    -> {len(matched)} propositions in {time.perf_counter() - t:.1f}s")

    print("[3] Export (without links, saved first)...")
    nomatch = build_nomatch(case_ids, cases, matched)
    export(matched, nomatch)

    if not matched.empty:
        print("[4] Shared ownership links (info only)...")
        t = time.perf_counter()
        matched = add_shared_links(matched)
        print(f"    -> done in {time.perf_counter() - t:.1f}s")
        export(matched, nomatch)

    return {
        "cases": len(case_ids),
        "matched_original_ids": int(matched["OriginalId"].nunique()) if not matched.empty else 0,
        "propositions": len(matched),
        "nomatch": len(nomatch),
    }


if __name__ == "__main__":
    t0 = time.perf_counter()
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    exit_code, metrics = 0, {}
    try:
        metrics = run()
    except Exception:
        exit_code, metrics = 1, {"error": 1}
        traceback.print_exc()
    finally:
        extra = [(str(XLSX_PATH), "file", "xlsx", True)] if XLSX_PATH.exists() else []
        finalize_run(
            project_name=PROJECT_NAME,
            started_at=started_at,
            exit_code=exit_code,
            metrics=metrics,
            require_primary_txt=False,
            extra_artifacts=extra,
        )
        print(f"[TIMER] Total: {(time.perf_counter() - t0) / 60:.2f} min")
        sys.exit(exit_code)
