"""
pipeline/01_corpus_pull.py

Fetches all papers from the journal list via the OpenAlex API.
Stores raw metadata + reconstructed abstracts in DuckDB.

Usage:
    uv run python pipeline/01_corpus_pull.py          # full run
    uv run python pipeline/01_corpus_pull.py --test   # 2 pages per journal (~400 rows)

Run overnight:
    caffeinate -i uv run python pipeline/01_corpus_pull.py
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

# ---------------------------------------------------------------------------
# Make `from pipeline.utils import ...` work when run directly
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import pipeline_complete  # noqa: E402

load_dotenv(override=True)

EMAIL   = os.getenv('OPENALEX_EMAIL', 'researcher@example.com')
DB      = 'data/global_health.duckdb'
JOURNAL_CSV = 'data/journal_list.csv'

# OpenAlex polite pool: max 10 req/sec; 0.12 s gap keeps us safely under
RATE_SLEEP  = 0.12
PER_PAGE    = 200

FIELDS = (
    'id,title,abstract_inverted_index,'
    'authorships,funders,publication_year,cited_by_count,'
    'primary_location'
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_journal_issns(path: str) -> list[dict]:
    with open(path) as f:
        rows = [r for r in csv.DictReader(f) if r['included'].strip().upper() == 'TRUE']
    return rows


def reconstruct_abstract(inv: dict | None) -> str:
    """Convert OpenAlex inverted-index abstract to plain text."""
    if not inv:
        return ''
    pos_to_word = {p: w for w, positions in inv.items() for p in positions}
    return ' '.join(pos_to_word[i] for i in sorted(pos_to_word))


@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def fetch_page(issn: str, cursor: str = '*', test: bool = False) -> dict:
    params = {
        'filter':   f'primary_location.source.issn:{issn},publication_year:2010-2024',
        'per-page': PER_PAGE,
        'cursor':   cursor,
        'select':   FIELDS,
    }
    if test:
        params['per-page'] = 50

    r = requests.get(
        'https://api.openalex.org/works',
        params=params,
        headers={'User-Agent': f'mailto:{EMAIL}'},
        timeout=30,
    )
    r.raise_for_status()
    time.sleep(RATE_SLEEP)
    return r.json()


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def already_fetched(con: duckdb.DuckDBPyConnection, issn: str) -> bool:
    """Return True if we already have rows for this journal."""
    n = con.execute(
        "SELECT COUNT(*) FROM works WHERE journal_issn = ?", [issn]
    ).fetchone()[0]
    return n > 0


def insert_batch(con: duckdb.DuckDBPyConnection, rows: list[dict]):
    if not rows:
        return
    con.executemany(
        """
        INSERT OR IGNORE INTO works
            (openalex_id, title, abstract, publication_year,
             journal_issn, cited_by_count)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r['openalex_id'],
                r['title'],
                r['abstract'],
                r['publication_year'],
                r['journal_issn'],
                r['cited_by_count'],
            )
            for r in rows
        ],
    )

    # authorships
    auth_rows = []
    for r in rows:
        for a in r['authorships']:
            auth_rows.append((
                r['openalex_id'],
                a['author_id'],
                a['author_name'],
                a['position'],
                a['institution_id'],
                a['institution_name'],
                a['institution_country'],
            ))
    if auth_rows:
        con.executemany(
            """
            INSERT OR IGNORE INTO authorships
                (openalex_id, author_id, author_name, position,
                 institution_id, institution_name, institution_country)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            auth_rows,
        )

    # funders / grants
    grant_rows = []
    for r in rows:
        for g in r['grants']:
            grant_rows.append((
                r['openalex_id'],
                g['funder_id'],
                g['funder_name'],
                g['award_id'],
            ))
    if grant_rows:
        con.executemany(
            """
            INSERT OR IGNORE INTO grants
                (openalex_id, funder_id, funder_name_raw, award_id)
            VALUES (?, ?, ?, ?)
            """,
            grant_rows,
        )


# ---------------------------------------------------------------------------
# Parse a single OpenAlex work record into a flat dict
# ---------------------------------------------------------------------------

def parse_work(work: dict, issn: str) -> dict:
    authorships = []
    for a in work.get('authorships', []):
        inst   = (a.get('institutions') or [{}])[0]
        author = a.get('author', {})
        authorships.append({
            'author_id':          author.get('id', ''),
            'author_name':        author.get('display_name', ''),
            'position':           a.get('author_position', ''),
            'institution_id':     inst.get('id', ''),
            'institution_name':   inst.get('display_name', ''),
            'institution_country': inst.get('country_code', ''),
        })

    funders = []
    for f in work.get('funders', []):
        funders.append({
            'funder_id':   f.get('id', ''),
            'funder_name': f.get('display_name', ''),
            'award_id':    '',  # award detail lives in 'awards' field; not needed here
        })

    return {
        'openalex_id':     work.get('id', ''),
        'title':           work.get('title', '') or '',
        'abstract':        reconstruct_abstract(work.get('abstract_inverted_index')),
        'publication_year': work.get('publication_year'),
        'journal_issn':    issn,
        'cited_by_count':  work.get('cited_by_count', 0),
        'authorships':     authorships,
        'grants':          funders,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def pull_journal(issn: str, journal_name: str, con: duckdb.DuckDBPyConnection,
                 test: bool = False):
    if already_fetched(con, issn):
        print(f'  {journal_name} ({issn}): already in DB, skipping.')
        return

    print(f'  {journal_name} ({issn}): pulling...')
    cursor  = '*'
    page    = 0
    total   = 0

    while True:
        data       = fetch_page(issn, cursor, test=test)
        results    = data.get('results', [])
        if not results:
            break

        rows = [parse_work(w, issn) for w in results]
        insert_batch(con, rows)
        total  += len(rows)
        page   += 1

        meta        = data.get('meta', {})
        next_cursor = meta.get('next_cursor')

        print(f'    page {page}: +{len(rows)} works (total so far: {total:,})')

        if not next_cursor or (test and page >= 2):
            break
        cursor = next_cursor

    print(f'  → {journal_name}: done. {total:,} works inserted.')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true',
                        help='Pull only 2 pages per journal for testing')
    args = parser.parse_args()

    journals = load_journal_issns(JOURNAL_CSV)
    print(f'Loaded {len(journals)} journals from {JOURNAL_CSV}')
    if args.test:
        print('TEST MODE: max 2 pages per journal')

    con = duckdb.connect(DB)

    # Ensure grants table exists (may not be in 00_setup_db if older version)
    con.execute("""
        CREATE TABLE IF NOT EXISTS grants (
            openalex_id  VARCHAR,
            funder_id    VARCHAR,
            funder_name_raw VARCHAR,
            award_id     VARCHAR,
            PRIMARY KEY (openalex_id, funder_id, award_id)
        )
    """)

    for j in journals:
        pull_journal(j['issn'], j['journal_name'], con, test=args.test)

    con.close()
    pipeline_complete('01_corpus_pull')


if __name__ == '__main__':
    main()
