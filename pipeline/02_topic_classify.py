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
- PRIMARY = the single most central topic — the main subject the study
  investigates, not the study setting or a passing theme.
- SECONDARY (optional, RARE): most papers have ONE clear topic — leave the
  secondary empty. Add a secondary ONLY when the paper gives roughly EQUAL weight
  to two genuinely distinct areas across BOTH its objectives and its findings
  (expect this for at most ~1 in 4 papers). If you can name a single dominant
  topic, use only the primary. A secondary is NOT for the study setting, the
  population, a data source, or a theme mentioned in passing. When in doubt, omit
  it. The secondary may share the primary's category with a DIFFERENT subtopic.
- J vs I (financing vs system): if the core subject is FINANCING — funding,
  expenditure, health insurance financing, economic growth, resource pooling,
  development assistance, cost — the primary is J, even when set in a health
  system or framed around UHC. Use I only when the core subject is the DELIVERY
  SYSTEM itself: service delivery, governance, workforce, quality, or UHC as a
  system of care.
- Disease vs population: when a paper centers on a specific disease, pathogen, or
  vaccine, the PRIMARY is the disease category (C infectious, D HIV/TB/malaria,
  E NTD); use the population category (A maternal, B child) as a SECONDARY if that
  life-stage is a substantial focus. Use A or B as PRIMARY only when the
  life-stage/population itself — not a specific disease — is the focus.
- Z: use Z only when NO substantive category fits — global-health governance,
  diplomacy, geopolitics, decolonisation, or meta-commentary on the field itself.
  Do not force these into I (Health Systems).
- If the paper does not fit any subtopic, return Z|Z00 as the primary.

Return ONLY this pipe-delimited format (no explanation, no preamble):
<category>|<subtopic>|<confidence>|<secondary_category>|<secondary_subtopic>
Use a single hyphen for BOTH secondary fields when there is no secondary.
confidence is: high, med, or low
Examples:
  A|A04|high|-|-           (single topic)
  D|D03|high|G|G02         (primary HIV, secondary mental health)
  I|I01|med|I|I05          (same category, two distinct subtopics)

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


def _coerce_pair(cat, sub):
    """Validate a (category, subtopic) pair against the taxonomy. Returns the
    coerced pair, or None if the category is invalid/empty."""
    if not cat or cat in ('-', 'NONE', 'None'):
        return None
    # Model echoed subtopic as category: 'A04' → 'A'
    if len(cat) >= 2 and cat[0].isalpha() and cat[0].isupper():
        cat = cat[0]
    if cat not in _VALID_CATEGORIES:
        return None
    if not sub or sub not in _VALID_SUBTOPICS:
        sub = f'{cat}00'
    return cat, sub


def parse_label(raw: str) -> tuple[str, str, str, str, str]:
    """Parse the (multi-label) topic response.

    Format: '<cat>|<sub>|<conf>[|<cat2>|<sub2>]'
      'A|A04|high'            → single topic
      'D|D03|high|G|G02'      → primary + secondary
      'I|I01|med|I|I05'       → same category, two subtopics
    Secondary fields of '-' / '' / 'NONE' mean no secondary.
    Returns (category, subtopic, confidence, category_2, subtopic_2); the two
    secondary fields are '' when absent. Robust to the same malformed variants
    the single-label parser handled.
    """
    first_line = raw.split('\n')[0].strip()
    parts = [p.strip() for p in first_line.split('|')]
    valid_conf = {'high', 'med', 'low'}

    cat, sub, conf = None, None, 'low'
    if len(parts) >= 3:
        cat, sub, conf = parts[0], parts[1], parts[2]
        if conf not in valid_conf:
            conf = 'low'
        if len(cat) >= 2 and cat[0].isalpha() and cat[0].isupper():
            cat = cat[0]
    elif len(parts) == 2:
        a, b = parts[0], parts[1]
        if len(a) >= 2 and a[0].isalpha() and a[0].isupper() and b in valid_conf:
            cat, sub, conf = a[0], a, b
        elif len(a) == 1 and a.isalpha() and a.isupper() and b in valid_conf:
            cat, sub, conf = a, f'{a}00', b

    primary = _coerce_pair(cat, sub) or ('Z', 'Z00')
    cat, sub = primary

    cat2, sub2 = '', ''
    if len(parts) >= 5:
        sec = _coerce_pair(parts[3], parts[4])
        # ignore a secondary that duplicates the primary exactly
        if sec and sec != (cat, sub):
            cat2, sub2 = sec
    return cat, sub, conf, cat2, sub2


def write_results(
    con: duckdb.DuckDBPyConnection,
    results: list[tuple[str, str]],
):
    cols = [r[1] for r in con.execute("PRAGMA table_info('works')").fetchall()]
    if 'topic_category_2' not in cols:
        con.execute("ALTER TABLE works ADD COLUMN topic_category_2 VARCHAR")
    if 'topic_subtopic_2' not in cols:
        con.execute("ALTER TABLE works ADD COLUMN topic_subtopic_2 VARCHAR")

    rows = []
    for openalex_id, raw in results:
        category, subtopic, confidence, cat2, sub2 = parse_label(raw)
        rows.append((category, subtopic, confidence,
                     cat2 or None, sub2 or None, openalex_id))

    con.executemany(
        """
        UPDATE works
        SET topic_category   = ?,
            topic_subtopic   = ?,
            topic_confidence = ?,
            topic_category_2 = ?,
            topic_subtopic_2 = ?,
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
