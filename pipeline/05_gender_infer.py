"""
pipeline/05_gender_infer.py

Infers gender for first and last authors of each work using the
genderize.io API. Deduplicates names before querying to minimise
API calls. Stores results in gender_first / gender_last on the works table.

Usage:
    uv run python pipeline/05_gender_infer.py          # full run
    uv run python pipeline/05_gender_infer.py --test   # first 100 works only
    uv run python pipeline/05_gender_infer.py --mock   # heuristic mock (no API)
    uv run python pipeline/05_gender_infer.py --test --mock

Run overnight:
    caffeinate -i uv run python pipeline/05_gender_infer.py
"""

import argparse
import os
import re
import sys
import time
import unicodedata

import duckdb
import requests
from dotenv import load_dotenv
from tenacity import (
    retry,
    retry_if_not_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ---------------------------------------------------------------------------
# Make `from pipeline.utils import ...` work when run directly
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import pipeline_complete  # noqa: E402

load_dotenv(override=True)

DB          = 'data/global_health.duckdb'
API_KEY     = os.getenv('GENDERIZE_API_KEY', '')
BATCH_SIZE  = 10    # max names per genderize.io request
CHUNK_SIZE  = 200   # works per DB write cycle
CONFIDENCE_THRESHOLD = 0.6
RATE_SLEEP  = 0.12  # pause between API calls (free tier ≈ 10 req/sec)
MOCK        = False


# ---------------------------------------------------------------------------
# Name extraction
# ---------------------------------------------------------------------------

def strip_diacritics(s: str) -> str:
    """Remove diacritical marks: 'José' → 'Jose', 'René' → 'Rene'.

    Uses Unicode NFD decomposition to separate base characters from
    combining marks, then discards the marks.
    Reference: Santamaría & Mihaljević (2018), doi:10.7717/peerj-cs.156
    """
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in nfkd if unicodedata.category(c) != 'Mn')


def extract_first_name(full_name: str | None) -> str | None:
    """Extract given name from a full author name.

    Applies two corrections from Santamaría & Mihaljević (2018) to
    improve genderize.io accuracy:
    1. Strip diacritics (José → Jose)
    2. Take first component of compound names (Jean-Pierre → Jean)

    Returns None for names that can't be meaningfully gendered:
    - None / empty input
    - Single initials: "J. Smith", "J Smith"
    - Hyphenated initials: "J.-P. Dupont"
    """
    if not full_name or not full_name.strip():
        return None

    first = full_name.strip().split()[0]

    # Strip trailing dots: "Jean-Pierre." → "Jean-Pierre"
    cleaned = first.rstrip('.')

    # Skip single characters / pure initials: "J", "J."
    if len(cleaned) <= 1:
        return None

    # Skip initial-style tokens: "J-P", "J.-P"
    if re.match(r'^[A-Z]\W', first):
        return None

    # Take first component of compound/hyphenated names:
    # "Jean-Pierre" → "Jean", "Ana-María" → "Ana"
    cleaned = cleaned.split('-')[0]

    # Skip if the component is a single initial: "J" from "J-Pierre"
    if len(cleaned) <= 1:
        return None

    # Strip diacritics: "José" → "Jose", "René" → "Rene"
    cleaned = strip_diacritics(cleaned)

    return cleaned


# ---------------------------------------------------------------------------
# Genderize.io API
# ---------------------------------------------------------------------------

class QuotaExhaustedError(Exception):
    """Raised when the genderize.io daily quota is fully exhausted."""
    pass


@retry(
    wait=wait_exponential(min=2, max=120),
    stop=stop_after_attempt(5),
    retry=retry_if_not_exception_type(QuotaExhaustedError),
)
def genderize_batch(names: list[str]) -> dict[str, tuple[str, float]]:
    """Query genderize.io for a batch of up to 10 names.

    Returns dict of lowercase_name → (gender, probability).
    """
    params: list[tuple[str, str]] = [('name[]', n) for n in names]
    if API_KEY and API_KEY != 'your-key-here':
        params.append(('apikey', API_KEY))

    r = requests.get(
        'https://api.genderize.io',
        params=params,
        timeout=30,
    )

    if r.status_code == 429:
        remaining = int(r.headers.get('x-rate-limit-remaining', -1))
        reset_secs = int(r.headers.get('x-rate-limit-reset', 60))

        if remaining == 0:
            # Daily quota exhausted — abort immediately
            hours = reset_secs / 3600
            raise QuotaExhaustedError(
                f'Daily API quota exhausted. Resets in {hours:.1f}h. '
                f'Set GENDERIZE_API_KEY in .env for higher limits, '
                f'or re-run after the reset.'
            )

        # Transient rate limit — wait and retry
        wait = min(reset_secs, 120)
        print(f'  Rate limited. Waiting {wait}s...')
        time.sleep(wait)
        raise requests.exceptions.HTTPError('429 rate limited')

    r.raise_for_status()
    data = r.json()

    results = {}
    for item in data:
        name = item['name'].lower()
        gender = item.get('gender')
        prob = item.get('probability', 0.0)

        if gender and prob >= CONFIDENCE_THRESHOLD:
            results[name] = (gender, prob)
        else:
            results[name] = ('unknown', prob or 0.0)

    return results


def genderize_all(unique_names: list[str]) -> dict[str, tuple[str, float]]:
    """Genderize all unique names, batching into groups of 10.

    Returns dict of lowercase_name → (gender, probability).
    Stops early if daily quota is exhausted.
    """
    cache: dict[str, tuple[str, float]] = {}
    total = len(unique_names)

    for i in range(0, total, BATCH_SIZE):
        batch = unique_names[i:i + BATCH_SIZE]
        try:
            results = genderize_batch(batch)
            cache.update(results)
        except QuotaExhaustedError as e:
            print(f'\n  ERROR: {e}')
            # Mark all remaining names as unknown
            for j in range(i, total):
                name = unique_names[j].lower()
                if name not in cache:
                    cache[name] = ('unknown', 0.0)
            remaining = total - len(cache) + sum(
                1 for v in cache.values() if v == ('unknown', 0.0)
            )
            print(f'  Resolved {i:,}/{total:,} names before quota hit. '
                  f'Re-run after reset to continue.')
            break
        except Exception as e:
            print(f'  WARNING: genderize batch failed after retries: {e}')
            for name in batch:
                cache[name.lower()] = ('unknown', 0.0)

        time.sleep(RATE_SLEEP)

        done = min(i + BATCH_SIZE, total)
        if done % 200 == 0 or done == total:
            print(f'    genderized {done:,}/{total:,} unique names')

    return cache


# ---------------------------------------------------------------------------
# Mock genderizer — no API calls
# ---------------------------------------------------------------------------

def mock_genderize(names: list[str]) -> dict[str, tuple[str, float]]:
    """Simple heuristic: names ending in a, e, i, y → female; others → male."""
    results = {}
    for name in names:
        lower = name.lower().rstrip('.')
        if lower.endswith(('a', 'e', 'i', 'y')):
            results[lower] = ('female', 0.85)
        else:
            results[lower] = ('male', 0.85)
    return results


# ---------------------------------------------------------------------------
# Format result
# ---------------------------------------------------------------------------

def format_gender(gender: str, probability: float) -> str:
    """Format as 'male|0.95' or 'unknown|0.00'."""
    return f'{gender}|{probability:.2f}'


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def load_unclassified(con: duckdb.DuckDBPyConnection,
                      limit: int | None = None) -> list[dict]:
    """Load works that haven't been gender-classified yet."""
    query = """
        SELECT w.openalex_id,
               (SELECT a.author_name FROM authorships a
                WHERE a.openalex_id = w.openalex_id AND a.position = 'first'
                LIMIT 1) AS first_author,
               (SELECT a.author_name FROM authorships a
                WHERE a.openalex_id = w.openalex_id AND a.position = 'last'
                LIMIT 1) AS last_author
        FROM works w
        WHERE w.gender_first IS NULL
        ORDER BY w.publication_year DESC
    """
    if limit:
        query += f'\nLIMIT {limit}'

    rows = con.execute(query).fetchall()
    return [
        {'openalex_id': r[0], 'first_author': r[1], 'last_author': r[2]}
        for r in rows
    ]


def write_results(con: duckdb.DuckDBPyConnection,
                  results: list[tuple[str, str, str]]):
    """Write gender results to DB.

    results: list of (openalex_id, gender_first, gender_last)
    """
    con.executemany(
        """
        UPDATE works
        SET gender_first = ?,
            gender_last  = ?
        WHERE openalex_id = ?
        """,
        [(gf, gl, oid) for oid, gf, gl in results],
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global MOCK
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true',
                        help='Process only the first 100 works')
    parser.add_argument('--mock', action='store_true',
                        help='Use heuristic mock (no API calls)')
    args = parser.parse_args()
    MOCK = args.mock

    if not MOCK:
        has_key = API_KEY and API_KEY != 'your-key-here'
        if not has_key:
            print('NOTE: No GENDERIZE_API_KEY set — using free tier (100 req/day).')
            print('      Set GENDERIZE_API_KEY in .env for higher limits.')
        else:
            print(f'Using genderize.io API key (first 8 chars: {API_KEY[:8]}...)')

    con = duckdb.connect(DB)

    limit = 100 if args.test else None
    works = load_unclassified(con, limit=limit)

    mode_parts = []
    if args.test:
        mode_parts.append('TEST')
    if MOCK:
        mode_parts.append('MOCK')
    mode_label = f' [{" + ".join(mode_parts)}]' if mode_parts else ''
    print(f'Gender inference for {len(works):,} unclassified works...{mode_label}')

    if not works:
        print('Nothing to classify. All works already have gender labels.')
        con.close()
        return

    # --- Step 1: Extract unique first names from all authors ----------------
    name_set: set[str] = set()
    for w in works:
        fn = extract_first_name(w['first_author'])
        if fn:
            name_set.add(fn.lower())
        fn = extract_first_name(w['last_author'])
        if fn:
            name_set.add(fn.lower())

    unique_names = sorted(name_set)
    print(f'  {len(unique_names):,} unique first names to genderize')

    # --- Step 2: Genderize all unique names ---------------------------------
    if MOCK:
        gender_cache = mock_genderize(unique_names)
        print(f'  Mock genderization complete. {len(gender_cache):,} names resolved.')
    else:
        gender_cache = genderize_all(unique_names)
        print(f'  Genderization complete. {len(gender_cache):,} names resolved.')

    # --- Step 3: Apply results in chunks and write to DB --------------------
    total = 0
    for i in range(0, len(works), CHUNK_SIZE):
        chunk = works[i:i + CHUNK_SIZE]
        results = []

        for w in chunk:
            # First author gender
            fn_first = extract_first_name(w['first_author'])
            if fn_first and fn_first.lower() in gender_cache:
                g, p = gender_cache[fn_first.lower()]
                gf = format_gender(g, p)
            else:
                gf = format_gender('unknown', 0.0)

            # Last author gender
            fn_last = extract_first_name(w['last_author'])
            if fn_last and fn_last.lower() in gender_cache:
                g, p = gender_cache[fn_last.lower()]
                gl = format_gender(g, p)
            else:
                gl = format_gender('unknown', 0.0)

            results.append((w['openalex_id'], gf, gl))

        write_results(con, results)
        total += len(chunk)
        pct = total / len(works) * 100
        print(f'  {total:,}/{len(works):,} ({pct:.1f}%) written to DB')

    con.close()
    pipeline_complete('05_gender_infer')


if __name__ == '__main__':
    main()
