"""
pipeline/02_topic_classify.py

Classifies all works in the corpus into the topic taxonomy using
claude-haiku-4-5. Async, resumable, writes to DuckDB after every chunk.

Usage:
    uv run python pipeline/02_topic_classify.py          # full run
    uv run python pipeline/02_topic_classify.py --test   # first 100 records only

Run overnight (in parallel with 03_methods_classify.py):
    caffeinate -i uv run python pipeline/02_topic_classify.py
"""

import argparse
import asyncio
import csv
import os
import sys

import anthropic
import duckdb
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import pipeline_complete, truncate_abstract  # noqa: E402

load_dotenv()

DB           = 'data/global_health.duckdb'
TAXONOMY_CSV = 'data/taxonomy/topic_taxonomy.csv'
MODEL        = 'claude-haiku-4-5'
CHUNK_SIZE   = 50   # concurrent requests; saturates Haiku rate limit safely
MAX_TOKENS   = 20   # label only: "A|A04|high" is ~12 chars


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

def build_system_prompt() -> str:
    """Load taxonomy from CSV and build the classification system prompt."""
    with open(TAXONOMY_CSV) as f:
        rows = list(csv.DictReader(f))

    lines = []
    for r in rows:
        lines.append(
            f"{r['subtopic_id']} — {r['subtopic_name']} [{r['category_letter']}]"
        )
    taxonomy_text = '\n'.join(lines)

    return f"""Classify global health research abstracts into the taxonomy below.

Return ONLY this format (no explanation, no preamble):
<category_letter>|<subtopic_id>|<confidence>

Where confidence is: high, med, or low
Example: A|A04|high

If the abstract does not fit any subtopic, return: Z|Z00|low

Taxonomy:
{taxonomy_text}"""


# ---------------------------------------------------------------------------
# Async classification
# ---------------------------------------------------------------------------

client = anthropic.AsyncAnthropic()


@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5))
async def classify_one(openalex_id: str, abstract: str, system: str) -> tuple[str, str]:
    """Returns (openalex_id, raw_label_string)."""
    msg = await client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        messages=[{'role': 'user', 'content': truncate_abstract(abstract)}],
    )
    return openalex_id, msg.content[0].text.strip()


async def classify_batch(
    batch: list[tuple[str, str]], system: str
) -> list[tuple[str, str]]:
    tasks = [classify_one(oid, abstract, system) for oid, abstract in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # On exception, return a low-confidence fallback so the row is still written
    out = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            print(f'  WARNING: classify_one failed for {batch[i][0]}: {r}')
            out.append((batch[i][0], 'Z|Z00|low'))
        else:
            out.append(r)
    return out


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def load_unclassified(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    rows = con.execute("""
        SELECT openalex_id, abstract
        FROM works
        WHERE classified_topic = FALSE
          AND abstract IS NOT NULL
          AND LENGTH(abstract) > 50
        ORDER BY publication_year DESC
    """).fetchall()
    return rows


def parse_label(raw: str) -> tuple[str, str, str]:
    """Parse 'A|A04|high' → (category, subtopic, confidence)."""
    parts = [p.strip() for p in raw.split('|')]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    # Malformed response — mark as unclassified
    return 'Z', 'Z00', 'low'


def write_results(
    con: duckdb.DuckDBPyConnection,
    results: list[tuple[str, str]],
):
    rows = []
    for openalex_id, raw in results:
        category, subtopic, confidence = parse_label(raw)
        rows.append((category, subtopic, confidence, openalex_id))

    con.executemany(
        """
        UPDATE works
        SET topic_category   = ?,
            topic_subtopic   = ?,
            topic_confidence = ?,
            classified_topic = TRUE
        WHERE openalex_id = ?
        """,
        rows,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true',
                        help='Classify only the first 100 unclassified records')
    args = parser.parse_args()

    con   = duckdb.connect(DB)
    rows  = load_unclassified(con)

    if args.test:
        rows = rows[:100]
        print(f'TEST MODE: classifying {len(rows)} records')
    else:
        print(f'Classifying {len(rows):,} unclassified works...')

    if not rows:
        print('Nothing to classify. All works already have topic labels.')
        con.close()
        return

    system = build_system_prompt()
    total  = 0

    for i in range(0, len(rows), CHUNK_SIZE):
        chunk   = rows[i:i + CHUNK_SIZE]
        results = asyncio.run(classify_batch(chunk, system))
        write_results(con, results)
        total += len(chunk)
        pct    = total / len(rows) * 100
        print(f'  {total:,}/{len(rows):,} ({pct:.1f}%) classified')

    con.close()
    pipeline_complete('02_topic_classify')


if __name__ == '__main__':
    main()
