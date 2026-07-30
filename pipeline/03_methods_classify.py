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
  is M13; a review without a systematic search protocol is M14. PROSPERO
  registration or a PRISMA flow diagram → M05.
- The M04 / M16 / M12 split uses a PROVENANCE GATE, then an output test:
  STEP 1 — Did the authors COLLECT the data themselves (their own fieldwork,
  survey, trial, or specimens)?
    → Yes: classify by DESIGN — M01 RCT, M02 quasi-experimental, M03 cohort,
      M04 cross-sectional survey, M06 qualitative, M17 case study, etc.
      A primary cross-sectional survey the authors fielded is M04.
  STEP 2 — Did they instead ANALYZE an EXISTING standardized dataset they did
  not collect (DHS, SAGE, GBD, MICS, national/administrative survey, registry)?
    → Descriptive quantity (prevalence, incidence, mortality, DALYs, burden,
      coverage, or its distribution/trend) → M16. Example: global burden of a
      disease from GBD → M16.
    → Analytical relationship (determinants, associations, risk-factor models,
      econometric/financing analysis, development assistance, cross-national
      comparison) → M12. Example: mining DHS for determinants of unmet need,
      or whether smaller countries get more aid per capita → M12.
  So: fielding a survey → M04; mining DHS for relationships → M12; using DHS to
  report prevalence/burden → M16. Two checks: (1) did they collect it?
  (2) descriptive or analytical? Longitudinal follow-up of the same individuals
  is M03 regardless of provenance.
- M06 vs M09: a study applying a NAMED implementation-science framework
  (e.g. RE-AIM, CFIR, NPT) to study uptake/scale-up is M09; qualitative or
  mixed-methods work WITHOUT such a framework is M06.
- M06 vs M15: if the paper reports its OWN qualitative data (interviews, focus
  groups, ethnography), it is M06 — even if framed as a "perspective." M15 is
  opinion/commentary/editorial with NO primary data.
- M01 vs M02: randomized assignment is M01; a controlled but non-randomized
  comparison (pre/post, difference-in-differences, interrupted time series) is M02.
- M03 vs M04: follow-up of a defined population over time is M03; single
  time-point measurement is M04.
- M04 vs M18: a single-timepoint study that enrolls and measures a defined
  sample is M04, not M18. Reserve M18 only when no design can be determined.
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
