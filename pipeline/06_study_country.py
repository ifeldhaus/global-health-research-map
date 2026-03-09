"""
pipeline/06_study_country.py

Extracts the study country (where the research was conducted or focuses on)
from each paper's title and abstract using claude-haiku-4-5.
Async, resumable, writes to DuckDB after every chunk.

Usage:
    uv run python pipeline/06_study_country.py          # full run
    uv run python pipeline/06_study_country.py --test   # first 100 records only
    uv run python pipeline/06_study_country.py --mock   # keyword-based mock (no API)
    uv run python pipeline/06_study_country.py --test --mock

Run overnight:
    caffeinate -i uv run python pipeline/06_study_country.py
"""

import argparse
import asyncio
import os
import re
import sys

import anthropic
import duckdb
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import pipeline_complete, truncate_abstract  # noqa: E402

load_dotenv(override=True)

DB         = 'data/global_health.duckdb'
MODEL      = 'claude-haiku-4-5'
CHUNK_SIZE = 10   # concurrent requests; conservative to avoid rate limits
MAX_TOKENS = 40   # e.g. "KE,TZ,UG,MW,ZM|high" is ~25 chars
MOCK       = False


# ---------------------------------------------------------------------------
# Country name → ISO 3166-1 alpha-2 code lookup
# ---------------------------------------------------------------------------

COUNTRY_NAME_TO_CODE: dict[str, str] = {
    # Africa
    'kenya': 'KE', 'tanzania': 'TZ', 'uganda': 'UG', 'nigeria': 'NG',
    'south africa': 'ZA', 'ethiopia': 'ET', 'ghana': 'GH', 'cameroon': 'CM',
    'senegal': 'SN', 'mozambique': 'MZ', 'malawi': 'MW', 'zambia': 'ZM',
    'zimbabwe': 'ZW', 'rwanda': 'RW', 'burundi': 'BI', 'madagascar': 'MG',
    'sierra leone': 'SL', 'liberia': 'LR', 'guinea': 'GN',
    'burkina faso': 'BF', 'mali': 'ML', 'niger': 'NE', 'chad': 'TD',
    'democratic republic of the congo': 'CD', 'drc': 'CD', 'congo': 'CG',
    'eswatini': 'SZ', 'swaziland': 'SZ', 'lesotho': 'LS',
    'namibia': 'NA', 'botswana': 'BW', 'angola': 'AO',
    'ivory coast': 'CI', "cote d'ivoire": 'CI', 'togo': 'TG', 'benin': 'BJ',
    'gambia': 'GM', 'mauritania': 'MR', 'somalia': 'SO', 'eritrea': 'ER',
    'south sudan': 'SS', 'sudan': 'SD', 'egypt': 'EG', 'morocco': 'MA',
    'tunisia': 'TN', 'algeria': 'DZ', 'libya': 'LY',
    # South/Southeast Asia
    'india': 'IN', 'bangladesh': 'BD', 'pakistan': 'PK', 'nepal': 'NP',
    'sri lanka': 'LK', 'myanmar': 'MM', 'thailand': 'TH', 'vietnam': 'VN',
    'cambodia': 'KH', 'laos': 'LA', 'indonesia': 'ID', 'philippines': 'PH',
    'malaysia': 'MY', 'timor-leste': 'TL', 'afghanistan': 'AF',
    # East Asia
    'china': 'CN', 'japan': 'JP', 'south korea': 'KR', 'taiwan': 'TW',
    'mongolia': 'MN',
    # Latin America & Caribbean
    'brazil': 'BR', 'mexico': 'MX', 'colombia': 'CO', 'peru': 'PE',
    'argentina': 'AR', 'chile': 'CL', 'bolivia': 'BO', 'ecuador': 'EC',
    'venezuela': 'VE', 'paraguay': 'PY', 'uruguay': 'UY',
    'guatemala': 'GT', 'honduras': 'HN', 'el salvador': 'SV',
    'nicaragua': 'NI', 'costa rica': 'CR', 'panama': 'PA',
    'haiti': 'HT', 'dominican republic': 'DO', 'jamaica': 'JM',
    'cuba': 'CU', 'trinidad and tobago': 'TT', 'guyana': 'GY',
    # Middle East
    'iran': 'IR', 'iraq': 'IQ', 'jordan': 'JO', 'lebanon': 'LB',
    'syria': 'SY', 'yemen': 'YE', 'saudi arabia': 'SA',
    'palestine': 'PS', 'turkey': 'TR',
    # Pacific
    'papua new guinea': 'PG', 'fiji': 'FJ',
    # High-income
    'united states': 'US', 'usa': 'US', 'united kingdom': 'GB', 'uk': 'GB',
    'australia': 'AU', 'canada': 'CA', 'france': 'FR', 'germany': 'DE',
    'netherlands': 'NL', 'switzerland': 'CH', 'sweden': 'SE',
    'norway': 'NO', 'denmark': 'DK', 'finland': 'FI', 'ireland': 'IE',
    'italy': 'IT', 'spain': 'ES', 'portugal': 'PT', 'belgium': 'BE',
    'new zealand': 'NZ', 'singapore': 'SG',
    # Special values
    'global': 'GLOBAL', 'worldwide': 'GLOBAL', 'multi-country': 'GLOBAL',
    'multiple countries': 'GLOBAL', 'unknown': 'UNKNOWN',
}

# Regional labels → GLOBAL
_REGIONAL_LABELS = {
    'sub-saharan africa', 'southeast asia', 'south asia', 'east asia',
    'latin america', 'east africa', 'west africa', 'southern africa',
    'central africa', 'central america', 'caribbean', 'middle east',
    'north africa', 'asia pacific', 'western pacific',
}
for label in _REGIONAL_LABELS:
    COUNTRY_NAME_TO_CODE[label] = 'GLOBAL'


def normalize_country_code(code: str) -> str:
    """Normalize a country code or name to ISO 3166-1 alpha-2."""
    stripped = code.strip()
    if len(stripped) == 2 and stripped.isalpha():
        return stripped.upper()
    if stripped.upper() in ('GLOBAL', 'UNKNOWN'):
        return stripped.upper()
    lower = stripped.lower()
    if lower in COUNTRY_NAME_TO_CODE:
        return COUNTRY_NAME_TO_CODE[lower]
    return stripped.upper()  # return as-is if unrecognised


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Extract the country or countries where the research described in this \
global health paper was conducted or focuses on. This is the STUDY COUNTRY — where the \
study population lives or where data was collected — NOT the country where the researchers \
are based.

Return ONLY this format (no explanation, no preamble):
CODES|CONFIDENCE

Rules:
- Use ISO 3166-1 alpha-2 country codes (e.g., KE for Kenya, IN for India, BR for Brazil)
- For multi-country studies, separate codes with commas: KE,TZ,UG|high
- For studies spanning an entire WHO region or continent without naming specific countries: GLOBAL|med
- For systematic reviews or meta-analyses covering many countries worldwide: GLOBAL|high
- If no country can be determined from the title and abstract: UNKNOWN|low

Where confidence is: high, med, or low
- high: country explicitly named or clearly identifiable from the context
- med: country strongly implied but not explicitly named
- low: country only weakly inferred or ambiguous

Examples:
KE|high
KE,TZ,UG|high
IN|med
GLOBAL|high
UNKNOWN|low"""


# ---------------------------------------------------------------------------
# Mock classifier — keyword matching, no API needed
# ---------------------------------------------------------------------------

_MOCK_COUNTRIES: list[dict] = []


def _load_country_keywords() -> list[dict]:
    """Build keyword → code mappings for mock country extraction."""
    entries = []
    seen_codes: set[str] = set()
    for name, code in COUNTRY_NAME_TO_CODE.items():
        if code in ('GLOBAL', 'UNKNOWN'):
            continue
        if len(name) < 4:          # skip 'uk', 'us', 'drc' etc.
            continue
        if code in seen_codes:     # one entry per country is enough
            continue
        seen_codes.add(code)
        entries.append({'keyword': name, 'code': code})
    return entries


def mock_classify(title: str, abstract: str) -> str:
    """Extract countries by keyword search in title + abstract."""
    global _MOCK_COUNTRIES
    if not _MOCK_COUNTRIES:
        _MOCK_COUNTRIES = _load_country_keywords()

    text = f'{title} {abstract}'.lower()
    found: list[str] = []

    for entry in _MOCK_COUNTRIES:
        if entry['keyword'] in text and entry['code'] not in found:
            found.append(entry['code'])

    if not found:
        return 'UNKNOWN|low'
    if len(found) > 5:
        return 'GLOBAL|med'

    conf = 'high' if len(found) <= 2 else 'med'
    return f"{','.join(found)}|{conf}"


# ---------------------------------------------------------------------------
# Async classification
# ---------------------------------------------------------------------------

client = anthropic.AsyncAnthropic()


def _is_retryable(exc: BaseException) -> bool:
    """Only retry on transient errors (rate-limit, server errors), not 400s."""
    if isinstance(exc, anthropic.RateLimitError):
        return True
    if isinstance(exc, anthropic.APIStatusError) and exc.status_code >= 500:
        return True
    return False


@retry(
    wait=wait_exponential(min=1, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception(_is_retryable),
)
async def classify_one(
    openalex_id: str, title: str, abstract: str, system: str,
) -> tuple[str, str]:
    """Returns (openalex_id, raw_label_string)."""
    user_content = f'Title: {title}\n\nAbstract: {truncate_abstract(abstract)}'
    msg = await client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        messages=[{'role': 'user', 'content': user_content}],
    )
    return openalex_id, msg.content[0].text.strip()


class BillingError(Exception):
    """Raised when the API returns a billing/credits error."""
    pass


async def classify_batch(
    batch: list[tuple[str, str, str]], system: str,
) -> list[tuple[str, str]]:
    if MOCK:
        return [
            (oid, mock_classify(title, abstract))
            for oid, title, abstract in batch
        ]

    tasks = [
        classify_one(oid, title, abstract, system)
        for oid, title, abstract in batch
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            err_msg = str(r).lower()
            if 'credit balance' in err_msg or 'billing' in err_msg:
                raise BillingError(
                    'API credit balance too low. The script will stop now.\n'
                    'Top up credits at https://console.anthropic.com/settings/billing\n'
                    'then re-run this script — it will resume where it left off.'
                )
            print(f'  WARNING: classify_one failed for {batch[i][0]}: {r}')
            out.append((batch[i][0], 'UNKNOWN|low'))
        else:
            out.append(r)
    return out


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def load_unclassified(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str]]:
    rows = con.execute("""
        SELECT openalex_id, title, abstract
        FROM works
        WHERE classified_country = FALSE
          AND abstract IS NOT NULL
          AND LENGTH(abstract) > 50
        ORDER BY publication_year DESC
    """).fetchall()
    return rows


def parse_label(raw: str) -> tuple[str, str]:
    """Parse 'KE,TZ,UG|high' → ('KE|TZ|UG', 'high').

    Handles common model response variants:
    - 'KE|high'              → single country
    - 'KE,TZ,UG|high'        → multi-country (comma-separated)
    - 'KE|TZ|UG|high'        → multi-country (pipe-separated)
    - 'GLOBAL|high'           → global study
    - 'UNKNOWN|low'           → unidentifiable
    - 'KE|high\\n...'         → extra text after label
    - 'KE'                    → missing confidence
    - 'Kenya|high'            → full name instead of code
    """
    first_line = raw.split('\n')[0].strip()
    parts = [p.strip() for p in first_line.split('|')]

    valid_conf = {'high', 'med', 'low'}

    # Determine which parts are confidence vs country codes
    if len(parts) >= 2 and parts[-1] in valid_conf:
        conf = parts[-1]
        country_parts = parts[:-1]
    else:
        conf = 'med'
        country_parts = parts

    # Split on commas too (model may use commas within pipe segments)
    codes: list[str] = []
    for part in country_parts:
        for token in part.split(','):
            token = token.strip()
            if not token:
                continue
            # Skip XML-style placeholders the model may echo from prompt
            if token.startswith('<') and token.endswith('>'):
                continue
            codes.append(normalize_country_code(token))

    if not codes:
        return 'UNKNOWN', 'low'

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            unique.append(c)

    # If GLOBAL is one of many, just use GLOBAL
    if 'GLOBAL' in unique and len(unique) > 1:
        return 'GLOBAL', conf

    return '|'.join(unique), conf


def write_results(
    con: duckdb.DuckDBPyConnection,
    results: list[tuple[str, str]],
):
    rows = []
    for openalex_id, raw in results:
        country, confidence = parse_label(raw)
        rows.append((country, confidence, openalex_id))

    con.executemany(
        """
        UPDATE works
        SET study_country      = ?,
            country_confidence = ?,
            classified_country = TRUE
        WHERE openalex_id = ?
        """,
        rows,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global MOCK
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true',
                        help='Classify only the first 100 unclassified records')
    parser.add_argument('--mock', action='store_true',
                        help='Use keyword-based mock classifier (no API calls)')
    args = parser.parse_args()
    MOCK = args.mock

    con = duckdb.connect(DB)

    # Ensure country_confidence column exists (may be missing from older DB)
    try:
        con.execute('ALTER TABLE works ADD COLUMN country_confidence VARCHAR')
        print('  Added country_confidence column to works table.')
    except duckdb.CatalogException:
        pass

    rows = load_unclassified(con)

    if args.test:
        rows = rows[:100]

    mode_parts = []
    if args.test:
        mode_parts.append('TEST')
    if MOCK:
        mode_parts.append('MOCK')
    mode_label = f' [{" + ".join(mode_parts)}]' if mode_parts else ''
    print(f'Classifying {len(rows):,} works (study country)...{mode_label}')

    if not rows:
        print('Nothing to classify. All works already have study country labels.')
        con.close()
        return

    system = SYSTEM_PROMPT
    total = 0

    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        try:
            results = asyncio.run(classify_batch(chunk, system))
        except BillingError as e:
            print(f'\n✗ {e}')
            print(f'  Progress saved: {total:,}/{len(rows):,} classified so far.')
            con.close()
            return
        write_results(con, results)
        total += len(chunk)
        pct = total / len(rows) * 100
        print(f'  {total:,}/{len(rows):,} ({pct:.1f}%) classified')

    # --- Verification summary -----------------------------------------------
    total_done = con.execute(
        'SELECT COUNT(*) FROM works WHERE classified_country = TRUE'
    ).fetchone()[0]
    print(f'\n  Total classified: {total_done:,}')

    top = con.execute("""
        SELECT study_country, COUNT(*) AS n
        FROM works
        WHERE classified_country = TRUE
          AND study_country NOT IN ('UNKNOWN', 'GLOBAL')
        GROUP BY study_country
        ORDER BY n DESC
        LIMIT 15
    """).fetchall()
    if top:
        print('  Top study countries:')
        for country, n in top:
            print(f'    {country:>10}  {n:,}')

    conf_dist = con.execute("""
        SELECT country_confidence, COUNT(*) AS n
        FROM works
        WHERE classified_country = TRUE
        GROUP BY country_confidence
        ORDER BY n DESC
    """).fetchall()
    print('  Confidence distribution:')
    for conf, n in conf_dist:
        print(f'    {conf:>6}  {n:,}')

    unknowns = con.execute(
        "SELECT COUNT(*) FROM works WHERE study_country = 'UNKNOWN'"
    ).fetchone()[0]
    globals_ = con.execute(
        "SELECT COUNT(*) FROM works WHERE study_country = 'GLOBAL'"
    ).fetchone()[0]
    print(f'  UNKNOWN: {unknowns:,}  |  GLOBAL: {globals_:,}')

    con.close()
    pipeline_complete('06_study_country')


if __name__ == '__main__':
    main()
