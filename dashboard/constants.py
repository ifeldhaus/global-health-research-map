"""
dashboard/constants.py

Shared constants: color palettes, WHO region mapping, taxonomy loaders.
"""

import csv
import hashlib
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent
TAXONOMY_DIR = PROJECT_ROOT / 'data' / 'taxonomy'

# ---------------------------------------------------------------------------
# Color palettes (Plotly hex, matching visualization_agent.py tab20)
# ---------------------------------------------------------------------------

# Colorblind-safe, vibrant base palettes. Okabe-Ito is the standard CVD-safe
# qualitative set; Paul Tol's "muted" and "vibrant" extend it for larger
# categorical needs. Every categorical color on the dashboard draws from these.
_OKABE_ITO = [
    '#0072B2',  # blue
    '#E69F00',  # orange
    '#009E73',  # bluish green
    '#D55E00',  # vermillion
    '#CC79A7',  # reddish purple
    '#56B4E9',  # sky blue
    '#F0E442',  # yellow
    '#999999',  # grey
]
_TOL_VIBRANT = [
    '#EE7733', '#0077BB', '#33BBEE', '#EE3377', '#CC3311',
    '#009988', '#BBBBBB',
]

# 15 saturated, CVD-safe hues for the topic taxonomy (A-O), drawn from
# Okabe-Ito and Paul Tol's "vibrant"/"bright" sets (not the muted set). Topic
# bars are directly labeled, so the few within-family hues are disambiguated
# by their labels.
_TOPIC15 = [
    '#0072B2', '#E69F00', '#009E73', '#D55E00', '#CC79A7',
    '#56B4E9', '#EE3377', '#33BBEE', '#CC3311', '#009988',
    '#EE7733', '#AA3377', '#332288', '#CCBB44', '#117733',
]
TOPIC_COLORS = {chr(65 + i): _TOPIC15[i] for i in range(15)}  # A-O
TOPIC_COLORS['Z'] = '#BBBBBB'  # uncategorized

FUNDER_CATEGORY_COLORS = {
    'Government': '#0072B2',       # blue
    'Philanthropic': '#CC79A7',    # reddish purple
    'Multilateral': '#009E73',     # green
    'Pharmaceutical': '#D55E00',   # vermillion
    'NGO': '#E69F00',              # orange
    'Academic': '#56B4E9',         # sky blue
    'Other': '#999999',            # grey
}

GENDER_COLORS = {
    'female': '#CC79A7',   # reddish purple
    'male': '#0072B2',     # blue
    'unknown': '#999999',  # grey
}

# Qualitative palette for general use (Okabe-Ito + Tol vibrant, CVD-safe).
QUAL_PALETTE = [
    '#0072B2', '#E69F00', '#009E73', '#D55E00', '#CC79A7',
    '#56B4E9', '#EE3377', '#33BBEE', '#CC3311', '#009988',
]

# Diverging palette for z-scores and heatmaps
DIVERGING_COLORSCALE = 'RdBu_r'  # red = over, blue = under

# Non-empirical method types to exclude from analytical lenses
# (Geographic Power, Topic Trends, Methods Gaps) but kept in Overview,
# and shown in supplementary sections for Funder Power & Institutions.
# Excludes opinion/discourse and indeterminate records; keeps empirical
# primary research AND rigorous synthesis (systematic M05 / scoping M13
# reviews), which are treated as research. Matches EVIDENCE_TYPE_MAP's
# `non_empirical` group in pipeline/utils.py.
NON_EMPIRICAL_METHODS = (
    'M15',  # Commentary / Editorial / Perspective
    'M14',  # Narrative review (no protocol)
    'M18',  # Other / Unclear (errata, indeterminate)
)

# Uncategorized topic categories to exclude from analytical charts.
# These are kept in the detailed data table but removed from visualizations
# because they don't represent a meaningful research area.
UNCATEGORIZED_TOPICS = ('Z',)  # Other / Uncategorized

# ---------------------------------------------------------------------------
# WHO regions (ISO-2 → WHO region code)
# ---------------------------------------------------------------------------

WHO_REGIONS = {
    # AFRO: African Region
    'DZ': 'AFRO', 'AO': 'AFRO', 'BJ': 'AFRO', 'BW': 'AFRO', 'BF': 'AFRO',
    'BI': 'AFRO', 'CV': 'AFRO', 'CM': 'AFRO', 'CF': 'AFRO', 'TD': 'AFRO',
    'KM': 'AFRO', 'CG': 'AFRO', 'CD': 'AFRO', 'CI': 'AFRO', 'GQ': 'AFRO',
    'ER': 'AFRO', 'SZ': 'AFRO', 'ET': 'AFRO', 'GA': 'AFRO', 'GM': 'AFRO',
    'GH': 'AFRO', 'GN': 'AFRO', 'GW': 'AFRO', 'KE': 'AFRO', 'LS': 'AFRO',
    'LR': 'AFRO', 'MG': 'AFRO', 'MW': 'AFRO', 'ML': 'AFRO', 'MR': 'AFRO',
    'MU': 'AFRO', 'MZ': 'AFRO', 'NA': 'AFRO', 'NE': 'AFRO', 'NG': 'AFRO',
    'RW': 'AFRO', 'ST': 'AFRO', 'SN': 'AFRO', 'SC': 'AFRO', 'SL': 'AFRO',
    'ZA': 'AFRO', 'SS': 'AFRO', 'TG': 'AFRO', 'UG': 'AFRO', 'TZ': 'AFRO',
    'ZM': 'AFRO', 'ZW': 'AFRO',
    # AMRO: Americas
    'AG': 'AMRO', 'AR': 'AMRO', 'BS': 'AMRO', 'BB': 'AMRO', 'BZ': 'AMRO',
    'BO': 'AMRO', 'BR': 'AMRO', 'CA': 'AMRO', 'CL': 'AMRO', 'CO': 'AMRO',
    'CR': 'AMRO', 'CU': 'AMRO', 'DM': 'AMRO', 'DO': 'AMRO', 'EC': 'AMRO',
    'SV': 'AMRO', 'GD': 'AMRO', 'GT': 'AMRO', 'GY': 'AMRO', 'HT': 'AMRO',
    'HN': 'AMRO', 'JM': 'AMRO', 'MX': 'AMRO', 'NI': 'AMRO', 'PA': 'AMRO',
    'PY': 'AMRO', 'PE': 'AMRO', 'KN': 'AMRO', 'LC': 'AMRO', 'VC': 'AMRO',
    'SR': 'AMRO', 'TT': 'AMRO', 'US': 'AMRO', 'UY': 'AMRO', 'VE': 'AMRO',
    # SEARO: South-East Asia
    'BD': 'SEARO', 'BT': 'SEARO', 'KP': 'SEARO', 'IN': 'SEARO', 'ID': 'SEARO',
    'MV': 'SEARO', 'MM': 'SEARO', 'NP': 'SEARO', 'LK': 'SEARO', 'TH': 'SEARO',
    'TL': 'SEARO',
    # EURO: Europe
    'AL': 'EURO', 'AD': 'EURO', 'AM': 'EURO', 'AT': 'EURO', 'AZ': 'EURO',
    'BY': 'EURO', 'BE': 'EURO', 'BA': 'EURO', 'BG': 'EURO', 'HR': 'EURO',
    'CY': 'EURO', 'CZ': 'EURO', 'DK': 'EURO', 'EE': 'EURO', 'FI': 'EURO',
    'FR': 'EURO', 'GE': 'EURO', 'DE': 'EURO', 'GR': 'EURO', 'HU': 'EURO',
    'IS': 'EURO', 'IE': 'EURO', 'IL': 'EURO', 'IT': 'EURO', 'KZ': 'EURO',
    'KG': 'EURO', 'LV': 'EURO', 'LT': 'EURO', 'LU': 'EURO', 'MT': 'EURO',
    'MC': 'EURO', 'ME': 'EURO', 'NL': 'EURO', 'MK': 'EURO', 'NO': 'EURO',
    'PL': 'EURO', 'PT': 'EURO', 'MD': 'EURO', 'RO': 'EURO', 'RU': 'EURO',
    'SM': 'EURO', 'RS': 'EURO', 'SK': 'EURO', 'SI': 'EURO', 'ES': 'EURO',
    'SE': 'EURO', 'CH': 'EURO', 'TJ': 'EURO', 'TR': 'EURO', 'TM': 'EURO',
    'UA': 'EURO', 'GB': 'EURO', 'UZ': 'EURO',
    # EMRO: Eastern Mediterranean
    'AF': 'EMRO', 'BH': 'EMRO', 'DJ': 'EMRO', 'EG': 'EMRO', 'IR': 'EMRO',
    'IQ': 'EMRO', 'JO': 'EMRO', 'KW': 'EMRO', 'LB': 'EMRO', 'LY': 'EMRO',
    'MA': 'EMRO', 'OM': 'EMRO', 'PK': 'EMRO', 'PS': 'EMRO', 'QA': 'EMRO',
    'SA': 'EMRO', 'SO': 'EMRO', 'SD': 'EMRO', 'SY': 'EMRO', 'TN': 'EMRO',
    'AE': 'EMRO', 'YE': 'EMRO',
    # WPRO: Western Pacific
    'AU': 'WPRO', 'BN': 'WPRO', 'KH': 'WPRO', 'CN': 'WPRO', 'CK': 'WPRO',
    'FJ': 'WPRO', 'JP': 'WPRO', 'KI': 'WPRO', 'LA': 'WPRO', 'MY': 'WPRO',
    'MH': 'WPRO', 'FM': 'WPRO', 'MN': 'WPRO', 'NR': 'WPRO', 'NZ': 'WPRO',
    'NU': 'WPRO', 'PW': 'WPRO', 'PG': 'WPRO', 'PH': 'WPRO', 'KR': 'WPRO',
    'WS': 'WPRO', 'SG': 'WPRO', 'SB': 'WPRO', 'TO': 'WPRO', 'TV': 'WPRO',
    'VU': 'WPRO', 'VN': 'WPRO',
}


_COUNTRY_NAME_OVERRIDES = {
    'CD': 'DR Congo',
    'CF': 'Central African Rep.',
    'TZ': 'Tanzania',
    'VE': 'Venezuela',
    'BO': 'Bolivia',
    'IR': 'Iran',
    'KR': 'South Korea',
    'KP': 'North Korea',
    'LA': 'Laos',
    'SY': 'Syria',
    'TW': 'Taiwan',
    'RU': 'Russia',
    'MD': 'Moldova',
    'PS': 'Palestine',
    'MK': 'North Macedonia',
}


def iso2_to_country_name(code: str) -> str:
    """Convert ISO-2 code to readable country name.

    Uses short overrides for countries whose official pycountry names
    are unwieldy (e.g. "Congo, The Democratic Republic of the" → "DR Congo").
    Handles multi-country pipe-separated codes.
    """
    import pycountry
    if not code or code in ('GLOBAL', 'UNKNOWN'):
        return code
    if '|' in code:
        parts = [iso2_to_country_name(c.strip()) for c in code.split('|')]
        return ' / '.join(parts)
    if code in _COUNTRY_NAME_OVERRIDES:
        return _COUNTRY_NAME_OVERRIDES[code]
    country = pycountry.countries.get(alpha_2=code)
    return country.name if country else code


WHO_REGION_NAMES = {
    'AFRO': 'African Region',
    'AMRO': 'Region of the Americas',
    'SEARO': 'South-East Asia Region',
    'EURO': 'European Region',
    'EMRO': 'Eastern Mediterranean Region',
    'WPRO': 'Western Pacific Region',
}

# Colorblind-friendly categorical palette, keyed by WHO region display name.
WHO_REGION_COLORS = {
    'African Region': '#009E73',                # green
    'Region of the Americas': '#D55E00',        # vermillion
    'South-East Asia Region': '#0072B2',        # blue
    'European Region': '#CC79A7',               # reddish purple
    'Eastern Mediterranean Region': '#E69F00',  # orange
    'Western Pacific Region': '#56B4E9',        # sky blue
    'Other': '#999999',                         # grey
}

# ---------------------------------------------------------------------------
# GBD burden reference regions
# ---------------------------------------------------------------------------
# The corpus is global-health research, which overwhelmingly concerns low- and
# middle-income settings.  Comparing its topic mix against *global* disease
# burden (which folds in the high-income NCD burden) understates NCDs and
# overstates infectious disease.  The research-intensity comparison therefore
# benchmarks against the pooled World Bank low-/lower-middle-/upper-middle-
# income burden.  These are the World Bank income aggregates as stored in
# gbd_burden.region (loaded from IHME GBD 2023, year 2023).
LMIC_REGIONS = (
    'World Bank Low Income',
    'World Bank Lower Middle Income',
    'World Bank Upper Middle Income',
)
LMIC_BURDEN_LABEL = 'low- and middle-income (World Bank) disease burden'

# ---------------------------------------------------------------------------
# Institution income group (Global North vs South)
# ---------------------------------------------------------------------------
# ISO2 codes of World Bank high-income economies (FY2024 classification), used
# to split producing institutions into Global North (high-income) vs Global
# South (low- and middle-income). Countries not listed are treated as low- or
# middle-income; unknown/blank codes are excluded from the split.
HIGH_INCOME_ISO2 = frozenset({
    # North America
    'US', 'CA', 'BM', 'GL',
    # Western / Northern / Southern / Central Europe + micro-states
    'AT', 'BE', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR', 'GB',
    'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'MC', 'MT',
    'NL', 'NO', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK', 'AD', 'SM', 'FO', 'GI',
    'BG',
    # Asia-Pacific high-income
    'AU', 'NZ', 'JP', 'KR', 'SG', 'HK', 'MO', 'TW', 'BN',
    # Gulf / Middle East high-income
    'IL', 'AE', 'SA', 'QA', 'KW', 'BH', 'OM',
    # Latin America / Caribbean high-income
    'CL', 'UY', 'PA', 'TT', 'BS', 'BB', 'AG', 'KN', 'AW', 'PR', 'KY', 'VG',
    # Other high-income
    'RU', 'SC', 'GU', 'NC', 'PF',
})


def income_group(iso2: str | None) -> str | None:
    """Global North (high-income) vs South (low/middle-income) for an ISO2."""
    if not iso2:
        return None
    return ('High-income' if iso2.strip().upper() in HIGH_INCOME_ISO2
            else 'Low- & middle-income')


# ---------------------------------------------------------------------------
# Country colors (shared across every chart that colors by country)
# ---------------------------------------------------------------------------
# A given country keeps the same color in every figure on the dashboard.
_COUNTRY_PALETTE = [
    '#0072B2', '#E69F00', '#009E73', '#D55E00', '#CC79A7', '#56B4E9',
    '#EE3377', '#33BBEE', '#CC3311', '#009988', '#EE7733', '#AA3377',
    '#332288', '#CCBB44', '#117733', '#0077BB', '#EE6677', '#228833',
    '#66CCEE', '#AA4499', '#994455', '#DDCC77', '#4477AA', '#882255',
]
# Anchor the highest-volume producing countries to fixed, well-separated slots
# so the busiest charts stay maximally distinct; everything else is assigned a
# stable color by hash.
_ANCHOR_COUNTRIES = [
    'United States', 'United Kingdom', 'Canada', 'Australia', 'Switzerland',
    'South Africa', 'China', 'India', 'Brazil', 'Netherlands', 'Kenya',
    'Uganda', 'Nigeria', 'Germany', 'Belgium', 'Ethiopia', 'Pakistan',
    'France', 'Ghana', 'Tanzania', 'Sweden', 'Bangladesh', 'Thailand',
    'Spain',
]
COUNTRY_COLORS = {c: _COUNTRY_PALETTE[i % len(_COUNTRY_PALETTE)]
                  for i, c in enumerate(_ANCHOR_COUNTRIES)}


def country_color(name: str) -> str:
    """Stable color for a country name, identical across every chart."""
    if name in COUNTRY_COLORS:
        return COUNTRY_COLORS[name]
    h = int(hashlib.md5(str(name).encode()).hexdigest(), 16)
    return _COUNTRY_PALETTE[h % len(_COUNTRY_PALETTE)]


def country_color_map(names) -> dict:
    """Build a {country_name: color} map covering the given names."""
    return {n: country_color(n) for n in names}


def institution_label(name, country, shared_names) -> str:
    """Disambiguate umbrella institution names by appending their country.

    Names that OpenAlex shares across multiple institution IDs (e.g. the many
    national "Ministry of Health" entities) get a ", <country>" suffix so each
    reads as its own institution; unambiguous names are returned unchanged.
    """
    if name in shared_names:
        return f"{name}, {iso2_to_country_name(country)}"
    return name

# ---------------------------------------------------------------------------
# Taxonomy label loaders
# ---------------------------------------------------------------------------

def load_topic_labels() -> dict[str, str]:
    """Map category letter -> readable name from taxonomy CSV."""
    labels = {}
    path = TAXONOMY_DIR / 'topic_taxonomy.csv'
    if path.exists():
        with open(path) as f:
            for row in csv.DictReader(f):
                labels[row['category_letter']] = row['category_name']
    return labels


def load_method_labels() -> dict[str, str]:
    """Map method ID -> readable name from taxonomy CSV."""
    labels = {}
    path = TAXONOMY_DIR / 'methods_taxonomy.csv'
    if path.exists():
        with open(path) as f:
            for row in csv.DictReader(f):
                labels[row['method_id']] = row['method_name']
    return labels


# Pre-load at import time (small files, safe to cache)
TOPIC_LABELS = load_topic_labels()
TOPIC_LABELS.setdefault('Z', 'Uncategorized / Other')
METHOD_LABELS = load_method_labels()

# Topic colors keyed by the readable label, for charts that color by topic
# NAME rather than by category letter. Keeps every topic-category chart on the
# same fixed color dashboard-wide (a category is always the same color).
TOPIC_COLORS_BY_LABEL = {
    TOPIC_LABELS.get(k, k): v for k, v in TOPIC_COLORS.items()
}

# ---------------------------------------------------------------------------
# Journal ISSN → name mapping (loaded from journal_list.csv)
# ---------------------------------------------------------------------------

def load_journal_names() -> dict[str, str]:
    """Map ISSN → journal name from data/journal_list.csv."""
    names = {}
    path = PROJECT_ROOT / 'data' / 'journal_list.csv'
    if path.exists():
        with open(path) as f:
            for row in csv.DictReader(f):
                names[row['issn']] = row['journal_name']
    return names


JOURNAL_NAMES = load_journal_names()


# ---------------------------------------------------------------------------
# Data completeness colors
# ---------------------------------------------------------------------------

COMPLETENESS_COLORS = {
    'classifiable': '#009E73',            # green: has usable abstract
    'no_abstract': '#D55E00',             # vermillion: no abstract in OpenAlex
    'insufficient_abstract': '#E69F00',   # orange: junk/short abstract
    'boilerplate_abstract': '#CC79A7',    # purple: journal boilerplate
}

# Plotly chart defaults
CHART_TEMPLATE = 'plotly_white'
CHART_HEIGHT = 500
CHART_HEIGHT_TALL = 700
CHART_MARGIN = dict(l=20, r=20, t=50, b=20)


# ---------------------------------------------------------------------------
# Funder display labels: append the base country in parentheses ONLY for
# generically-named funders. Skip multilateral/UN bodies and any funder whose
# name already references its country (e.g. "Australian ...", "... of China",
# "MRC UK", "USAID").
# ---------------------------------------------------------------------------

# Per-country aliases to detect an existing country reference in a funder name,
# plus the display token to append when none is found.
_FUNDER_COUNTRY = {
    'US': ('USA', ('united states', 'usa', 'u.s.', 'america', 'american')),
    'UK': ('UK', ('united kingdom', ' uk', 'uk ', 'u.k.', 'british', 'britain')),
    'Canada': ('Canada', ('canada', 'canadian')),
    'China': ('China', ('china', 'chinese')),
    'Australia': ('Australia', ('australia', 'australian')),
    'South Africa': ('South Africa', ('south africa', 'south african')),
    'India': ('India', ('india', 'indian')),
    'Brazil': ('Brazil', ('brazil', 'brazilian')),
    'Germany': ('Germany', ('germany', 'german')),
    'France': ('France', ('france', 'french')),
    'Japan': ('Japan', ('japan', 'japanese')),
    'South Korea': ('South Korea', ('korea', 'korean')),
    'Netherlands': ('Netherlands', ('netherlands', 'dutch')),
    'Sweden': ('Sweden', ('sweden', 'swedish')),
    'Norway': ('Norway', ('norway', 'norwegian')),
    'Spain': ('Spain', ('spain', 'spanish')),
    'Switzerland': ('Switzerland', ('switzerland', 'swiss')),
    'Ireland': ('Ireland', ('ireland', 'irish')),
    'Iran': ('Iran', ('iran', 'iranian')),
    'Mexico': ('Mexico', ('mexico', 'mexican')),
    'Thailand': ('Thailand', ('thailand', 'thai')),
    'Kenya': ('Kenya', ('kenya', 'kenyan')),
    'Uganda': ('Uganda', ('uganda', 'ugandan')),
}

# Countries/bodies that never get a suffix.
_FUNDER_NO_SUFFIX = {'Multilateral', '', None}


def funder_display_name(name: str, country) -> str:
    """Return the funder name with '(Country)' appended only when the name is
    generic (no existing country reference) and the funder is not multilateral."""
    if country in _FUNDER_NO_SUFFIX:
        return name
    display, aliases = _FUNDER_COUNTRY.get(country, (country, (str(country).lower(),)))
    low = f' {name.lower()} '
    if any(a in low for a in aliases):
        return name
    return f'{name} ({display})'


# ---------------------------------------------------------------------------
# World Bank income tiers (FY2024) for finer North/South breakdowns.
# ---------------------------------------------------------------------------
LOW_INCOME_ISO2 = frozenset({
    'AF', 'BF', 'BI', 'CF', 'TD', 'CD', 'ER', 'ET', 'GM', 'GW', 'KP', 'LR',
    'MG', 'MW', 'ML', 'MZ', 'NE', 'RW', 'SL', 'SO', 'SS', 'SD', 'SY', 'TG',
    'UG', 'YE',
})
LOWER_MIDDLE_INCOME_ISO2 = frozenset({
    'AO', 'DZ', 'BD', 'BJ', 'BT', 'BO', 'CV', 'KH', 'CM', 'KM', 'CG', 'CI',
    'DJ', 'EG', 'SZ', 'GH', 'GN', 'HT', 'HN', 'IN', 'IR', 'KE', 'KI', 'KG',
    'LA', 'LB', 'LS', 'MR', 'FM', 'MA', 'MN', 'MM', 'NP', 'NI', 'NG', 'PK',
    'PG', 'PH', 'WS', 'ST', 'SN', 'SB', 'LK', 'TZ', 'TJ', 'TL', 'TN', 'UA',
    'UZ', 'VU', 'VN', 'ZM', 'ZW',
})


def income_tier(iso2):
    """World Bank income tier for an ISO2 country (FY2024)."""
    if not iso2:
        return None
    c = iso2.strip().upper()
    if c in HIGH_INCOME_ISO2:
        return 'High income'
    if c in LOW_INCOME_ISO2:
        return 'Low income'
    if c in LOWER_MIDDLE_INCOME_ISO2:
        return 'Lower-middle income'
    return 'Upper-middle income'


INCOME_TIER_ORDER = [
    'Low income', 'Lower-middle income', 'Upper-middle income', 'High income',
]
INCOME_TIER_COLORS = {
    'Low income': '#CC3311',
    'Lower-middle income': '#EE7733',
    'Upper-middle income': '#33BBEE',
    'High income': '#0072B2',
}
