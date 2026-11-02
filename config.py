import pandas as pd

BATCH_SIZE = 2_000_000

# === File paths ===
LEGAL_FORM_MAPPING_FILE = "utils/legal_form_mapping.csv"
COUNTRY_FILE = "utils/country_to_delete.xlsx"
COUNTRY_MAPPING = "utils/country_mapping.csv"

PROJECT_NAME = 'Switzerland_direct_semi_direct_entries'

# === Column names ===
LEGAL_FORM_COL = "LegalForm"
REGEX_COL = "Regex"
RELATED_LEGAL_FORMS_COL = "RelatedLegalForms"

# === Load legal form mappings ===
legal_form_mapping = pd.read_csv(LEGAL_FORM_MAPPING_FILE, dtype=str, keep_default_na=False)
LEGAL_FORM_DICT = {row[REGEX_COL]: row[LEGAL_FORM_COL] for _, row in legal_form_mapping.iterrows()}

# === Load countries ===
COUNTRY_MAPPING = pd.read_csv(COUNTRY_MAPPING, dtype=str)

# List of country names (lowercased)
COUNTRY_NAMES = COUNTRY_MAPPING['CountryName'].dropna().str.lower().tolist()


# === Words to replace with '%' in normalize_name ===
WORDS_TO_DELETE = [
    "the", "as", "of", "for", "an", "and", "&", "et", "y", "this", "or", "le", "la",
    "de", "du", "co", "company", "compnay", "in", "en", "liquidation", "liquidazione",
    "liquiazione", "di", "del", "?", "unipessoal", "-", "anonyme",
    "societe", "unipersonal", "sociedad", "de'", "par action", "simplifiee", "societa",
    "responsabilita", "limitata", "societa\'", "responsabilita\'", "consortile",
    "semplificata", "responsabilit", "soccoop", "cooperativa",
    "v likvidaci", "extinguida", "(extinguida)", "liquidacao",
    "associe unique", 'responsabilite', 'limitee', 'limitiee', 'civile', "simplifie", "cokg", "GBMH",
   "haftungsbeschrankt", 'ldta',
]


PROBLEMATIC_STATUSES = {4, 11, 12, 13, 14, 17, 18, 19}