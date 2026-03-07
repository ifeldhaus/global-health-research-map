"""
pipeline/04_funder_normalize.py

Seeds the funders lookup table from funders_canonical.csv, then backfills
any grants rows that have empty funder data by re-fetching from OpenAlex.
Finally, reports how many grants match a canonical funder.

No LLM calls — this is pure ID matching + optional alias fallback.

Usage:
    uv run python pipeline/04_funder_normalize.py          # full run
    uv run python pipeline/04_funder_normalize.py --test   # first 100 works only
"""

import argparse
import csv
import os
import sys
import time

import duckdb
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import pipeline_complete  # noqa: E402

load_dotenv(override=True)

DB           = 'data/global_health.duckdb'
FUNDER_CSV   = 'data/funders_canonical.csv'
EMAIL        = os.getenv('OPENALEX_EMAIL', 'researcher@example.com')
RATE_SLEEP   = 0.12   # OpenAlex polite pool rate limit
PER_PAGE     = 200    # max per API page


# ---------------------------------------------------------------------------
# Step 1: Seed the funders lookup table
# ---------------------------------------------------------------------------

def seed_funders(con: duckdb.DuckDBPyConnection):
    """Load canonical funders from CSV into the funders table."""
    with open(FUNDER_CSV, encoding='utf-8', errors='replace') as f:
        rows = list(csv.DictReader(f))

    con.execute("DELETE FROM funders")  # refresh each run
    con.executemany(
        """
        INSERT INTO funders (canonical_name, funder_category, funder_country,
                             openalex_id, aliases)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                r['canonical_name'],
                r['funder_category'],
                r['funder_country'],
                r['openalex_id'],
                r['aliases'],
            )
            for r in rows
        ],
    )
    print(f'  Seeded {len(rows)} canonical funders.')


# ---------------------------------------------------------------------------
# Step 2: Backfill empty grants from OpenAlex (batch API)
# ---------------------------------------------------------------------------

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def fetch_works_page(openalex_filter: str, cursor: str = '*') -> dict:
    """Fetch a page of works from OpenAlex with funders field."""
    r = requests.get(
        'https://api.openalex.org/works',
        params={
            'filter': openalex_filter,
            'per-page': PER_PAGE,
            'cursor': cursor,
            'select': 'id,funders',
        },
        headers={'User-Agent': f'mailto:{EMAIL}'},
        timeout=30,
    )
    r.raise_for_status()
    time.sleep(RATE_SLEEP)
    return r.json()


def backfill_grants(con: duckdb.DuckDBPyConnection, test: bool = False):
    """Re-fetch funder data for works whose grants rows have empty funder_id.

    Uses the OpenAlex batch API (filter by openalex IDs) rather than
    individual work fetches for speed.
    """
    # Find works with empty grants
    empty_works = con.execute("""
        SELECT DISTINCT g.openalex_id
        FROM grants g
        WHERE g.funder_id = '' OR g.funder_id IS NULL
    """).fetchall()

    if not empty_works:
        print('  No empty grants to backfill.')
        return

    work_ids = [r[0] for r in empty_works]
    if test:
        work_ids = work_ids[:100]

    print(f'  Backfilling funder data for {len(work_ids):,} works...')

    # Delete the empty placeholder rows
    for work_id in work_ids:
        con.execute("DELETE FROM grants WHERE openalex_id = ?", [work_id])

    # Batch-fetch in chunks of 50 IDs via OpenAlex filter pipe syntax
    BATCH = 50
    total_inserted = 0
    total_fetched = 0

    for i in range(0, len(work_ids), BATCH):
        chunk = work_ids[i:i + BATCH]
        # OpenAlex expects bare IDs separated by |
        bare_ids = '|'.join(wid.replace('https://openalex.org/', '') for wid in chunk)
        oa_filter = f'openalex:{bare_ids}'

        cursor = '*'
        while True:
            try:
                data = fetch_works_page(oa_filter, cursor)
            except Exception as e:
                print(f'    WARNING: batch fetch failed: {e}')
                break

            results = data.get('results', [])
            if not results:
                break

            for work in results:
                work_id = work.get('id', '')
                for f in work.get('funders', []):
                    funder_id = f.get('id', '')
                    funder_name = f.get('display_name', '')
                    if funder_id:
                        con.execute(
                            """
                            INSERT OR IGNORE INTO grants
                                (openalex_id, funder_id, funder_name_raw, award_id)
                            VALUES (?, ?, ?, '')
                            """,
                            [work_id, funder_id, funder_name],
                        )
                        total_inserted += 1

            next_cursor = data.get('meta', {}).get('next_cursor')
            if not next_cursor:
                break
            cursor = next_cursor

        total_fetched += len(chunk)
        print(f'    {total_fetched}/{len(work_ids)} works fetched...')

    print(f'  Backfill complete: {total_fetched} works fetched, '
          f'{total_inserted} funder rows inserted.')


# ---------------------------------------------------------------------------
# Step 3: Build the funder lookup index and report matches
# ---------------------------------------------------------------------------

def build_funder_index(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Build a mapping from OpenAlex funder ID → canonical name."""
    rows = con.execute("""
        SELECT canonical_name, openalex_id, aliases
        FROM funders
    """).fetchall()

    index = {}
    for canonical, oa_id, aliases in rows:
        # Primary: match by OpenAlex ID (full URL or bare ID)
        if oa_id:
            full_url = (f'https://openalex.org/{oa_id}'
                        if not oa_id.startswith('http') else oa_id)
            bare_id = oa_id.replace('https://openalex.org/', '')
            index[full_url] = canonical
            index[bare_id] = canonical

        # Secondary: alias matching on funder_name_raw
        if aliases:
            for alias in aliases.split('|'):
                alias_clean = alias.strip().lower()
                if alias_clean:
                    index[alias_clean] = canonical
        # Also index canonical name itself
        index[canonical.lower()] = canonical

    return index


def report_matches(con: duckdb.DuckDBPyConnection, funder_index: dict[str, str]):
    """Report how many grant rows match canonical funders."""
    grants = con.execute("""
        SELECT funder_id, funder_name_raw, COUNT(*) as n
        FROM grants
        WHERE funder_id != '' AND funder_id IS NOT NULL
        GROUP BY funder_id, funder_name_raw
        ORDER BY n DESC
    """).fetchall()

    if not grants:
        print('\n  No funder data in grants table yet.')
        return

    matched = 0
    unmatched = 0
    matched_names = {}
    unmatched_names = {}

    for funder_id, funder_name, count in grants:
        canonical = funder_index.get(funder_id)
        if not canonical and funder_name:
            canonical = funder_index.get(funder_name.lower())
        if canonical:
            matched += count
            matched_names[canonical] = matched_names.get(canonical, 0) + count
        else:
            unmatched += count
            unmatched_names[funder_name or funder_id] = \
                unmatched_names.get(funder_name or funder_id, 0) + count

    total = matched + unmatched
    print(f'\n  Grant-funder matches: {matched}/{total} '
          f'({matched/total*100:.0f}%) match a canonical funder')

    print(f'\n  Top canonical funders:')
    for name, n in sorted(matched_names.items(), key=lambda x: -x[1])[:15]:
        print(f'    {n:>4}  {name}')

    if unmatched_names:
        print(f'\n  Top unmatched funders ({unmatched} total):')
        for name, n in sorted(unmatched_names.items(), key=lambda x: -x[1])[:15]:
            print(f'    {n:>4}  {name}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true',
                        help='Backfill only first 100 works')
    args = parser.parse_args()

    con = duckdb.connect(DB)

    print('Step 1: Seeding funders table...')
    seed_funders(con)

    print('Step 2: Backfilling empty grant data from OpenAlex...')
    backfill_grants(con, test=args.test)

    print('Step 3: Matching grants to canonical funders...')
    funder_index = build_funder_index(con)
    report_matches(con, funder_index)

    con.close()
    pipeline_complete('04_funder_normalize')


if __name__ == '__main__':
    main()
