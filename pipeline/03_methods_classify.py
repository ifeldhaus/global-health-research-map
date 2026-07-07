"""
pipeline/03_methods_classify.py

Classifies all works in the corpus into the methods taxonomy using the
Anthropic API (claude-haiku-4-5 by default; override with CLASSIFIER_MODEL).
Async, resumable, writes to DuckDB after every chunk.

Usage:
    uv run python pipeline/03_methods_classify.py          # full run
    uv run python pipeline/03_methods_classify.py --test   # first 100 records only
    uv run python pipeline/03_methods_classify.py --mock   # keyword-based mock (no API)
    uv run python pipeline/03_methods_classify.py --test --mock

Run overnight (in parallel with 02_topic_classify.py):
    caffeinate -i uv run python pipeline/03_methods_classify.py
"""

import argparse
import csv
import os
import random
import re
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.llm_classify import (  # noqa: E402
    JUNK_ABSTRACT_PATTERNS,
    MIN_ABSTRACT_LENGTH,
    get_model,
    mode_label,
    run_classification,
)
from pipeline.utils import pipeline_complete  # noqa: E402

from dotenv import load_dotenv

load_dotenv(override=True)

DB           = 'data/global_health.duckdb'
TAXONOMY_CSV = 'data/taxonomy/methods_taxonomy.csv'
CHUNK_SIZE   = 10   # concurrent requests; conservative to avoid rate limits
MAX_TOKENS   = 20   # label only: "M01|high" is ~10 chars


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

Rules:
- Classify by the paper's primary study design, not by secondary techniques
  mentioned in passing.
- Review types: a structured synthesis with a systematic search (with or without
  meta-analysis) is M05; a scoping/mapping review following a scoping protocol
  is M13; a review without a systematic search protocol is M14.
- M04 vs M16: primary data collected from individual respondents at one time
  point is M04; analyses of aggregate population-level rates, burden estimates,
  or trends are M16.
- Use M12 (Secondary Data Analysis) only when no more specific design applies
  to the analysis — e.g. a cross-sectional analysis of DHS data is M04.
- If the method cannot be determined, return: M18|low

Return ONLY this format (no explanation, no preamble):
<method_id>|<confidence>

Where confidence is: high, med, or low
Example: M01|high

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
# DB helpers
# ---------------------------------------------------------------------------

def load_unclassified(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str]]:
    junk_clauses = ' '.join(
        f"AND abstract NOT LIKE '{pat}'" for pat in JUNK_ABSTRACT_PATTERNS
    )
    rows = con.execute(f"""
        SELECT openalex_id, title, abstract
        FROM works
        WHERE classified_method = FALSE
          AND abstract IS NOT NULL
          AND LENGTH(abstract) > {MIN_ABSTRACT_LENGTH}
          {junk_clauses}
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
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true',
                        help='Classify only the first 100 unclassified records')
    parser.add_argument('--mock', action='store_true',
                        help='Use keyword-based mock classifier (no API calls)')
    args = parser.parse_args()

    con   = duckdb.connect(DB)
    rows  = load_unclassified(con)

    if args.test:
        rows = rows[:100]

    print(f'Classifying {len(rows):,} unclassified works (methods)...'
          f'{mode_label(args.test, args.mock)}')
    if not args.mock:
        print(f'  Model: {get_model()}')

    if not rows:
        print('Nothing to classify. All works already have method labels.')
        con.close()
        return

    completed = run_classification(
        con, rows, build_system_prompt(), write_results,
        max_tokens=MAX_TOKENS,
        chunk_size=CHUNK_SIZE,
        mock_fn=mock_classify if args.mock else None,
    )

    con.close()
    if completed:
        pipeline_complete('03_methods_classify')


if __name__ == '__main__':
    main()
