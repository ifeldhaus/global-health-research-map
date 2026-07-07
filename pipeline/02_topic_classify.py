"""
pipeline/02_topic_classify.py

Classifies all works in the corpus into the topic taxonomy using the
Anthropic API (claude-haiku-4-5 by default; override with CLASSIFIER_MODEL).
Async, resumable, writes to DuckDB after every chunk.

Usage:
    uv run python pipeline/02_topic_classify.py          # full run
    uv run python pipeline/02_topic_classify.py --test   # first 100 records only
    uv run python pipeline/02_topic_classify.py --mock   # keyword-based mock (no API)
    uv run python pipeline/02_topic_classify.py --test --mock

Run overnight (in parallel with 03_methods_classify.py):
    caffeinate -i uv run python pipeline/02_topic_classify.py
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
TAXONOMY_CSV = 'data/taxonomy/topic_taxonomy.csv'
CHUNK_SIZE   = 10   # concurrent requests; conservative to avoid rate limits
MAX_TOKENS   = 20   # label only: "A|A04|high" is ~12 chars


# ---------------------------------------------------------------------------
# Taxonomy
# ---------------------------------------------------------------------------

def load_taxonomy() -> list[dict]:
    with open(TAXONOMY_CSV) as f:
        return list(csv.DictReader(f))


def valid_labels(rows: list[dict]) -> tuple[set[str], set[str]]:
    """Return (valid category letters, valid subtopic ids) incl. Z sentinel."""
    categories = {r['category_letter'] for r in rows} | {'Z'}
    subtopics = {r['subtopic_id'] for r in rows} | {'Z00'}
    # '<letter>00' fallbacks for category-only responses
    subtopics |= {f'{c}00' for c in categories}
    return categories, subtopics


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

def build_system_prompt() -> str:
    """Build the classification prompt with the taxonomy grouped by category,
    so the model sees what each category letter means."""
    rows = load_taxonomy()

    lines = []
    current_letter = None
    for r in rows:
        if r['category_letter'] != current_letter:
            current_letter = r['category_letter']
            lines.append(f"\n{current_letter} — {r['category_name']}")
        lines.append(f"  {r['subtopic_id']}  {r['subtopic_name']}")
    taxonomy_text = '\n'.join(lines)

    return f"""Classify global health research papers into the taxonomy below.
You will receive the paper's title and abstract. Use BOTH to determine the topic;
the title often contains key signals about the subject area.

Rules:
- Classify by the paper's PRIMARY research focus — the main subject the study
  investigates, not the study setting or a secondary theme.
- If a paper spans multiple topics, choose the one most central to the research
  question, and use the subtopic list to decide between adjacent categories.
- If the paper does not fit any subtopic, return: Z|Z00|low

Return ONLY this format (no explanation, no preamble):
<category_letter>|<subtopic_id>|<confidence>

Where confidence is: high, med, or low
Example: A|A04|high

Taxonomy (15 categories, letter — category name, then subtopics):
{taxonomy_text}"""


# ---------------------------------------------------------------------------
# Mock classifier — keyword matching, no API needed
# ---------------------------------------------------------------------------

def _load_taxonomy_keywords() -> list[dict]:
    """Build keyword lists from taxonomy subtopic names for mock matching."""
    entries = []
    for r in load_taxonomy():
        # Split subtopic name into searchable keywords, drop short words
        name = r['subtopic_name'].lower()
        # Remove parenthetical abbreviations to get real words
        name_clean = re.sub(r'\([^)]*\)', '', name)
        words = [w.strip('.,&') for w in re.split(r'[\s/]+', name_clean)]
        keywords = [w for w in words if len(w) >= 4 and w not in {
            'with', 'from', 'into', 'that', 'this', 'have', 'been',
            'their', 'than', 'also', 'were', 'does', 'such', 'other',
        }]
        entries.append({
            'category': r['category_letter'],
            'subtopic': r['subtopic_id'],
            'keywords': keywords,
        })
    return entries


_MOCK_TAXONOMY: list[dict] = []


def mock_classify(title: str, abstract: str) -> str:
    """Classify paper by keyword overlap with taxonomy subtopic names."""
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
        return f"{best_entry['category']}|{best_entry['subtopic']}|{conf}"

    # Fallback: pick a random subtopic with low confidence
    entry = random.choice(_MOCK_TAXONOMY)
    return f"{entry['category']}|{entry['subtopic']}|low"


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
        WHERE classified_topic = FALSE
          AND abstract IS NOT NULL
          AND LENGTH(abstract) > {MIN_ABSTRACT_LENGTH}
          {junk_clauses}
        ORDER BY publication_year DESC
    """).fetchall()
    return rows


_VALID_CATEGORIES, _VALID_SUBTOPICS = valid_labels(load_taxonomy())


def parse_label(raw: str) -> tuple[str, str, str]:
    """Parse 'A|A04|high' → (category, subtopic, confidence).

    Handles common model response variants:
    - 'A|A04|high'           → standard 3-part format
    - 'A04|A04|high'         → subtopic echoed as category (extract letter)
    - 'A|A04|high\\n...'     → extra text after label (take first line)
    - 'A04|high'             → missing category letter (infer from subtopic)
    - 'A|high'               → missing subtopic (category + confidence only)

    Any label not in the taxonomy is coerced: unknown subtopic falls back to
    '<category>00'; unknown category falls back to Z|Z00.
    """
    # Take only the first line — model sometimes appends explanation
    first_line = raw.split('\n')[0].strip()
    parts = [p.strip() for p in first_line.split('|')]

    valid_conf = {'high', 'med', 'low'}

    cat, sub, conf = None, None, 'low'

    if len(parts) >= 3:
        cat, sub, conf = parts[0], parts[1], parts[2]
        if conf not in valid_conf:
            conf = 'low'
        # Model echoed subtopic as category: 'A04|A04|high'
        if len(cat) >= 2 and cat[0].isalpha() and cat[0].isupper():
            cat = cat[0]

    elif len(parts) == 2:
        a, b = parts[0], parts[1]
        # Case: 'A04|high' — subtopic + confidence, missing category letter
        if len(a) >= 2 and a[0].isalpha() and a[0].isupper() and b in valid_conf:
            cat, sub, conf = a[0], a, b
        # Case: 'A|high' — category + confidence, missing subtopic
        elif len(a) == 1 and a.isalpha() and a.isupper() and b in valid_conf:
            cat, sub, conf = a, f'{a}00', b

    # Validate against the taxonomy
    if cat not in _VALID_CATEGORIES:
        return 'Z', 'Z00', 'low'
    if sub not in _VALID_SUBTOPICS:
        sub = f'{cat}00'
    return cat, sub, conf


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
    parser.add_argument('--mock', action='store_true',
                        help='Use keyword-based mock classifier (no API calls)')
    args = parser.parse_args()

    con   = duckdb.connect(DB)
    rows  = load_unclassified(con)

    if args.test:
        rows = rows[:100]

    print(f'Classifying {len(rows):,} unclassified works...'
          f'{mode_label(args.test, args.mock)}')
    if not args.mock:
        print(f'  Model: {get_model()}')

    if not rows:
        print('Nothing to classify. All works already have topic labels.')
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
        pipeline_complete('02_topic_classify')


if __name__ == '__main__':
    main()
