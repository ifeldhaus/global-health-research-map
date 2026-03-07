"""
pipeline/03_methods_classify.py

Classifies all works in the corpus into the methods taxonomy using
claude-haiku-4-5. Async, resumable, writes to DuckDB after every chunk.

Usage:
    uv run python pipeline/03_methods_classify.py          # full run
    uv run python pipeline/03_methods_classify.py --test   # first 100 records only
    uv run python pipeline/03_methods_classify.py --mock   # keyword-based mock (no API)
    uv run python pipeline/03_methods_classify.py --test --mock

Run overnight (in parallel with 02_topic_classify.py):
    caffeinate -i uv run python pipeline/03_methods_classify.py
"""

import argparse
import asyncio
import csv
import os
import random
import re
import sys

import anthropic
import duckdb
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import pipeline_complete, truncate_abstract  # noqa: E402

load_dotenv(override=True)

DB           = 'data/global_health.duckdb'
TAXONOMY_CSV = 'data/taxonomy/methods_taxonomy.csv'
MODEL        = 'claude-haiku-4-5'
CHUNK_SIZE   = 10   # concurrent requests; conservative to avoid rate limits
MAX_TOKENS   = 20   # label only: "M01|high" is ~10 chars
MOCK         = False # set via --mock flag; bypasses API calls


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
            f"{r['method_id']} — {r['method_name']}: {r['method_description']}"
        )
    taxonomy_text = '\n'.join(lines)

    return f"""Classify global health research papers by study methodology.
You will receive the paper's title and abstract. Use BOTH to determine the method;
the title often contains key signals (e.g. "a modelling study", "systematic review").

Return ONLY this format (no explanation, no preamble):
<method_id>|<confidence>

Where confidence is: high, med, or low
Example: M01|high

If the method cannot be determined, return: M18|low

Taxonomy:
{taxonomy_text}"""


# ---------------------------------------------------------------------------
# Mock classifier — keyword matching, no API needed
# ---------------------------------------------------------------------------

def _load_taxonomy_keywords() -> list[dict]:
    """Build keyword lists from taxonomy names and descriptions for mock matching."""
    with open(TAXONOMY_CSV) as f:
        rows = list(csv.DictReader(f))
    entries = []
    for r in rows:
        # Combine method name and description for richer keyword pool
        text = f"{r['method_name']} {r['method_description']}".lower()
        # Remove parenthetical abbreviations to get real words
        text_clean = re.sub(r'\([^)]*\)', '', text)
        words = [w.strip('.,&:') for w in re.split(r'[\s/]+', text_clean)]
        keywords = [w for w in words if len(w) >= 4 and w not in {
            'with', 'from', 'into', 'that', 'this', 'have', 'been',
            'their', 'than', 'also', 'were', 'does', 'such', 'other',
            'methods', 'study', 'data', 'analysis',
        }]
        entries.append({
            'method_id': r['method_id'],
            'keywords': keywords,
        })
    return entries


_MOCK_TAXONOMY: list[dict] = []


def mock_classify(title: str, abstract: str) -> str:
    """Classify by keyword overlap with taxonomy method names/descriptions."""
    global _MOCK_TAXONOMY
    if not _MOCK_TAXONOMY:
        _MOCK_TAXONOMY = _load_taxonomy_keywords()

    text = f"{title} {abstract}".lower()
    best_score = 0
    best_entry = None

    for entry in _MOCK_TAXONOMY:
        score = sum(1 for kw in entry['keywords'] if kw in text)
        if score > best_score:
            best_score = score
            best_entry = entry

    if best_entry and best_score >= 2:
        conf = 'high' if best_score >= 4 else 'med' if best_score >= 3 else 'low'
        return f"{best_entry['method_id']}|{conf}"

    # Fallback: pick a random method with low confidence
    entry = random.choice(_MOCK_TAXONOMY)
    return f"{entry['method_id']}|low"


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
async def classify_one(openalex_id: str, title: str, abstract: str, system: str) -> tuple[str, str]:
    """Returns (openalex_id, raw_label_string)."""
    user_content = f"Title: {title}\n\nAbstract: {truncate_abstract(abstract)}"
    msg = await client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system,
        messages=[{'role': 'user', 'content': user_content}],
    )
    return openalex_id, msg.content[0].text.strip()


async def classify_batch(
    batch: list[tuple[str, str, str]], system: str
) -> list[tuple[str, str]]:
    # Mock mode: skip API entirely, use keyword matching
    if MOCK:
        return [(oid, mock_classify(title, abstract)) for oid, title, abstract in batch]

    tasks = [classify_one(oid, title, abstract, system) for oid, title, abstract in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    # On exception, return a low-confidence fallback so the row is still written
    out = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            print(f'  WARNING: classify_one failed for {batch[i][0]}: {r}')
            out.append((batch[i][0], 'M18|low'))
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
        WHERE classified_method = FALSE
          AND abstract IS NOT NULL
          AND LENGTH(abstract) > 50
        ORDER BY publication_year DESC
    """).fetchall()
    return rows


def parse_label(raw: str) -> tuple[str, str]:
    """Parse 'M01|high' → (method_id, confidence).

    Handles common model response variants:
    - 'M01|high'           → standard 2-part format
    - 'M01|high\\n...'     → extra text after label (take first line)
    - 'M01'                → missing confidence (default to 'med')
    - 'M1|high'            → single-digit ID (normalize to M01)
    """
    # Take only the first line — model sometimes appends explanation
    first_line = raw.split('\n')[0].strip()
    parts = [p.strip() for p in first_line.split('|')]

    valid_conf = {'high', 'med', 'low'}
    valid_ids = {f'M{i:02d}' for i in range(1, 19)}

    if len(parts) >= 2:
        method_id, conf = parts[0], parts[1]
        if conf not in valid_conf:
            conf = 'low'
        # Normalize single-digit: M1 → M01
        if re.match(r'^M\d$', method_id):
            method_id = f'M0{method_id[1]}'
        if method_id in valid_ids:
            return method_id, conf

    if len(parts) == 1:
        method_id = parts[0]
        # Normalize single-digit: M1 → M01
        if re.match(r'^M\d$', method_id):
            method_id = f'M0{method_id[1]}'
        if method_id in valid_ids:
            return method_id, 'med'

    # Malformed response — mark as unclear
    return 'M18', 'low'


def write_results(
    con: duckdb.DuckDBPyConnection,
    results: list[tuple[str, str]],
):
    rows = []
    for openalex_id, raw in results:
        method_id, confidence = parse_label(raw)
        rows.append((method_id, confidence, openalex_id))

    con.executemany(
        """
        UPDATE works
        SET method_type       = ?,
            method_confidence = ?,
            classified_method = TRUE
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

    con   = duckdb.connect(DB)
    rows  = load_unclassified(con)

    if args.test:
        rows = rows[:100]

    mode_parts = []
    if args.test:
        mode_parts.append('TEST')
    if MOCK:
        mode_parts.append('MOCK')
    mode_label = f' [{" + ".join(mode_parts)}]' if mode_parts else ''
    print(f'Classifying {len(rows):,} unclassified works (methods)...{mode_label}')

    if not rows:
        print('Nothing to classify. All works already have method labels.')
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
    pipeline_complete('03_methods_classify')


if __name__ == '__main__':
    main()
