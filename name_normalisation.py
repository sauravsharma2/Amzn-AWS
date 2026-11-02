import pandas as pd
import itertools
import re
import unicodedata
from typing import Optional
from unicodedata import normalize, category


from utils.config import (
    LEGAL_FORM_DICT,
    COUNTRY_NAMES,
    WORDS_TO_DELETE
)

# === Utils ===

def like_to_regex(pattern: str) -> str:
    """
    _ -> [^A-Za-z]?      (zéro ou un non-lettre)
    % -> .*              (suite arbitraire)
    espace final optionnel si pattern termine par '_ %'
    """
    # Cas spécial « _ % » en fin
    if pattern.endswith('_ %'):
        prefix = pattern[:-3]            # enlève le suffixe "_ %"
        # construit la regex sur le préfixe
        regex = '^'
        for c in prefix:
            if c == '%':
                regex += '.*'
            elif c == '_':
                regex += '[^A-Za-z]?'
            else:
                regex += re.escape(c)
        # ajoute espace optionnel + reste ou fin de chaîne
        regex += r'(?:\s.*|$)$'
        return regex

    # Sinon, traitement normal
    regex = '^'
    for c in pattern:
        if c == '%':
            regex += '.*'
        elif c == '_':
            regex += '[^A-Za-z]?'
        else:
            regex += re.escape(c)
    regex += '$'
    return regex



def _build_legal_union(patterns):
    """
    Builds a regex from multiple patterns.
        - Remove ^ and $ so patterns can also match inside a company name.
          Example: '\bO\sY$\b|\bOY$\b would not match 'Google OY GROUP', without anchors it will.
        - Wrap each pattern in (?:...) to group it without creating groups.
    """

    alts = []
    for raw in patterns:
        pat = re.sub(r'(?<!\\)\^|(?<!\\)\$', '', raw)
        alts.append(f"(?:{pat})")
    return "|".join(alts) if alts else r"(?!)"

"""
Precompiled regexes for normalization:
- LEGAL_RE       : union of legal-form patterns (case-insensitive).
                   Note: in the normalization pipeline, dots are removed before LEGAL_RE is applied
                   (e.g., "A.G." → "AG"), so dotted variants do not need any handling in legal_form_mapping.csv.
- WORDS_RE       : words to remove (utils.config.WORDS_TO_DELETE)
- AMP_RE         : replaces '&' surrounded by spaces with a single space.
- EMPTY_PARENS_RE: removes empty parentheses '()'.
- SPACES_RE      : collapses multiple spaces into one.
- PUNCT_ALNUM_RE : removes all non-alphanumeric characters.
"""

LEGAL_RE = re.compile(_build_legal_union(LEGAL_FORM_DICT), re.IGNORECASE)
WORDS_LOWER = {w.lower() for w in WORDS_TO_DELETE}
WORDS_RE = re.compile(r"\b(?:%s)\b" % "|".join(map(re.escape, sorted(WORDS_LOWER))), re.IGNORECASE) if WORDS_LOWER else re.compile(r"$(?!)")
AMP_RE   = re.compile(r"\s*&\s*")
EMPTY_PARENS_RE = re.compile(r"\(\s*\)")
SPACES_RE       = re.compile(r"\s+")
PUNCT_ALNUM_RE  = re.compile(r"[\W_]+", re.UNICODE)




def exact_normalization_series(
    s: pd.Series,
    already_clean: bool = False,
    country_code: Optional[str] = None
) -> pd.DataFrame:

    """
    Country-aware normalization.
    - Selects a normalizer by country_code (defaults to BaseNormalizer).
    - Returns a DataFrame with:
        • 'no_legal' : text after lowercasing/diacritics removal + legal/stops/amp cleanup,
        • 'compact'  : alphanumeric-only version of 'no_legal'.
    """


    normalizer = NORMALIZERS.get(country_code, BaseNormalizer())
    return normalizer.normalize_series(s, already_clean=already_clean)




class BaseNormalizer:
    """
    Standard normalization pipeline with a country hook.
    - If not already_clean:
        • lowercase + strip,
        • normalize spaces.
    - Remove legal forms (LEGAL_RE), stopwords (WORDS_RE), '&', empty parentheses.
    - Normalize spaces again.
    - hook_precompact(df): country-specific adjustments on 'no_legal' BEFORE compaction.
    - 'compact' = remove all non-alphanumerics from 'no_legal'.
    """

    def hook_precompact(self, df: pd.DataFrame) -> pd.DataFrame:
        # by default
        return df

    def normalize_series(self, s: pd.Series, already_clean: bool = False) -> pd.DataFrame:
        x = s.fillna("").astype(str)
        if not already_clean:
            def _rm_acc(t):
                return ''.join(c for c in normalize('NFD', t.lower()) if category(c) != 'Mn')
            x = x.map(_rm_acc).str.replace(SPACES_RE, " ", regex=True).str.strip()

        x_no_dots = x.str.replace(".", "", regex=False)
        x = x_no_dots.str.replace(LEGAL_RE, " ", regex=True)
        x = x.str.replace(WORDS_RE, " ", regex=True)
        x = x.str.replace(AMP_RE, " ", regex=True)
        x = x.str.replace(EMPTY_PARENS_RE, " ", regex=True)
        x = x.str.replace(SPACES_RE, " ", regex=True).str.strip()

        df = pd.DataFrame({"no_legal": x}) # Exemple : Google Group OY -> google group

        # --- specific hook ---
        df = self.hook_precompact(df)

        compact = df["no_legal"].str.replace(PUNCT_ALNUM_RE, "", regex=True)  # Exemple : google group -> googlegroup
        df["compact"] = compact

        return df

# --- Countries Normalizer ---

class MYNormalizer(BaseNormalizer):
    def hook_precompact(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            no_legal=df["no_legal"]
            .str.replace(r"\(?m\)?\b", "", regex=True, flags=re.IGNORECASE)
            .str.replace(r"\b(?:malaisie|malaysia)\b", "", regex=True, flags=re.IGNORECASE)
            .str.replace(SPACES_RE, " ", regex=True).str.strip()
        )

class SGNormalizer(BaseNormalizer):
    def hook_precompact(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            no_legal=df["no_legal"]
            .str.replace(r"\(?s\)?\b", "", regex=True, flags=re.IGNORECASE)
            .str.replace(r"\bsingapour\b", "", regex=True, flags=re.IGNORECASE)
            .str.replace(SPACES_RE, " ", regex=True).str.strip()
        )

class RUNormalizer(BaseNormalizer):
    def hook_precompact(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            no_legal=df["no_legal"]
            .str.replace(r"\brus\b", "", regex=True, flags=re.IGNORECASE)
            .str.replace(r"\brussia\b", "", regex=True, flags=re.IGNORECASE)
            .str.replace(SPACES_RE, " ", regex=True).str.strip()
        )

class AUNormalizer(BaseNormalizer):
    def hook_precompact(self, df: pd.DataFrame) -> pd.DataFrame:
        # gère "Australia" et versions parenthésées; nettoie les espaces derrière
        return df.assign(
            no_legal=df["no_legal"]
            .str.replace(r"\baustralia\b", "", regex=True, case=False)
            .str.replace(SPACES_RE, " ", regex=True).str.strip()
        )

BR_STOPWORDS_RE = re.compile(
    r"\b(?:me|do|da|dos|das|brasil|brasile|empresario individual|"
    r"comercio|servicos|associacao|representacoes|comercial|industria|informatica|"
    r"consultoria|transportes|produtos|construcao|distribuidora|importacao|assistiva|"
    r"exportacao|participacoes|empreendimentos|producoes|alimenticios|tecnologia|"
    r"associados|cooperativa|comunicacao|instituto|empresarial|acessorios|sociedade|"
    r"equipamentos|comerciais|construtora|mercearia|produtores|representacao|alimentos|"
    r"construcoes|assessoria)\b",
    re.IGNORECASE
)

class BRNormalizer(BaseNormalizer):
    def hook_precompact(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            no_legal=df["no_legal"]
            .str.replace(BR_STOPWORDS_RE, "", regex=True)
            .str.replace(SPACES_RE, " ", regex=True)
            .str.strip()
        )


class FRNormalizer(BaseNormalizer):
    def hook_precompact(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(
            no_legal=df["no_legal"]
            .str.replace(r"\b(?:st|saint)\b", "", regex=True, flags=re.IGNORECASE)
            .str.replace(SPACES_RE, " ", regex=True).str.strip()
        )


# Registry mapping country codes to their specific normalizers
NORMALIZERS = {
    "MY": MYNormalizer(),
    "SG": SGNormalizer(),
    "RU": RUNormalizer(),
    "AU": AUNormalizer(),
    "BR":BRNormalizer(),
    "FR":FRNormalizer(),
}


