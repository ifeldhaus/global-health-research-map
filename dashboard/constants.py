"""
dashboard/constants.py

Shared constants: color palettes, WHO region mapping, taxonomy loaders.
"""

import csv
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.parent
TAXONOMY_DIR = PROJECT_ROOT / 'data' / 'taxonomy'

# ---------------------------------------------------------------------------
# Color palettes (Plotly hex, matching visualization_agent.py tab20)
# ---------------------------------------------------------------------------

# Matplotlib tab20 palette in hex
_TAB20 = [
    '#1f77b4', '#aec7e8', '#ff7f0e', '#ffbb78', '#2ca02c',
    '#98df8a', '#d62728', '#ff9896', '#9467bd', '#c5b0d5',
    '#8c564b', '#c49c94', '#e377c2', '#f7b6d2', '#7f7f7f',
    '#c7c7c7', '#bcbd22', '#dbdb8d', '#17becf', '#9edae5',
]

TOPIC_COLORS = {chr(65 + i): _TAB20[i] for i in range(15)}  # A-O
TOPIC_COLORS['Z'] = '#b0b0b0'  # uncategorized

FUNDER_CATEGORY_COLORS = {
    'Government': '#2171b5',
    'Philanthropic': '#6a3d9a',
    'Multilateral': '#33a02c',
    'Pharmaceutical': '#e31a1c',
    'NGO': '#ff7f00',
    'Academic': '#a6cee3',
    'Other': '#999999',
}

GENDER_COLORS = {
    'female': '#e377c2',
    'male': '#1f77b4',
    'unknown': '#999999',
}

# Qualitative palette for general use (Plotly safe)
QUAL_PALETTE = [
    '#636efa', '#ef553b', '#00cc96', '#ab63fa', '#ffa15a',
    '#19d3f3', '#ff6692', '#b6e880', '#ff97ff', '#fecb52',
]

# Diverging palette for z-scores and heatmaps
DIVERGING_COLORSCALE = 'RdBu_r'  # red = over, blue = under

# Non-empirical method types to exclude from analytical lenses
# (Geographic Power, Topic Trends, Methods Gaps) but kept in Overview,
# and shown in supplementary sections for Funder Power & Institutions.
NON_EMPIRICAL_METHODS = ('M15',)  # Commentary / Editorial / Perspective

# ---------------------------------------------------------------------------
# WHO regions (ISO-2 → WHO region code)
# ---------------------------------------------------------------------------

WHO_REGIONS = {
    # AFRO — African Region
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
    # AMRO — Americas
    'AG': 'AMRO', 'AR': 'AMRO', 'BS': 'AMRO', 'BB': 'AMRO', 'BZ': 'AMRO',
    'BO': 'AMRO', 'BR': 'AMRO', 'CA': 'AMRO', 'CL': 'AMRO', 'CO': 'AMRO',
    'CR': 'AMRO', 'CU': 'AMRO', 'DM': 'AMRO', 'DO': 'AMRO', 'EC': 'AMRO',
    'SV': 'AMRO', 'GD': 'AMRO', 'GT': 'AMRO', 'GY': 'AMRO', 'HT': 'AMRO',
    'HN': 'AMRO', 'JM': 'AMRO', 'MX': 'AMRO', 'NI': 'AMRO', 'PA': 'AMRO',
    'PY': 'AMRO', 'PE': 'AMRO', 'KN': 'AMRO', 'LC': 'AMRO', 'VC': 'AMRO',
    'SR': 'AMRO', 'TT': 'AMRO', 'US': 'AMRO', 'UY': 'AMRO', 'VE': 'AMRO',
    # SEARO — South-East Asia
    'BD': 'SEARO', 'BT': 'SEARO', 'KP': 'SEARO', 'IN': 'SEARO', 'ID': 'SEARO',
    'MV': 'SEARO', 'MM': 'SEARO', 'NP': 'SEARO', 'LK': 'SEARO', 'TH': 'SEARO',
    'TL': 'SEARO',
    # EURO — Europe
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
    # EMRO — Eastern Mediterranean
    'AF': 'EMRO', 'BH': 'EMRO', 'DJ': 'EMRO', 'EG': 'EMRO', 'IR': 'EMRO',
    'IQ': 'EMRO', 'JO': 'EMRO', 'KW': 'EMRO', 'LB': 'EMRO', 'LY': 'EMRO',
    'MA': 'EMRO', 'OM': 'EMRO', 'PK': 'EMRO', 'PS': 'EMRO', 'QA': 'EMRO',
    'SA': 'EMRO', 'SO': 'EMRO', 'SD': 'EMRO', 'SY': 'EMRO', 'TN': 'EMRO',
    'AE': 'EMRO', 'YE': 'EMRO',
    # WPRO — Western Pacific
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

# Plotly chart defaults
CHART_TEMPLATE = 'plotly_white'
CHART_HEIGHT = 500
CHART_HEIGHT_TALL = 700
CHART_MARGIN = dict(l=20, r=20, t=50, b=20)
