"""
pipeline/02_topic_classify.py

Classifies all works in the corpus into the topic taxonomy using
claude-haiku-4-5. Async, resumable, writes to DuckDB after every chunk.

Usage:
    uv run python pipeline/02_topic_classify.py          # full run
    uv run python pipeline/02_topic_classify.py --test   # first 100 records only
    uv run python pipeline/02_topic_classify.py --mock   # keyword-based mock (no API)
    uv run python pipeline/02_topic_classify.py --test --mock

Run overnight (in parallel with 03_methods_classify.py):
    caffeinate -i uv run python pipeline/02_topic_classify.py
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
TAXONOMY_CSV = 'data/taxonomy/topic_taxonomy.csv'
MODEL        = 'claude-haiku-4-5'
CHUNK_SIZE   = 10   # concurrent requests; conservative to avoid rate limits
MAX_TOKENS   = 20   # label only: "A|A04|high" is ~12 chars
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
            f"{r['subtopic_id']} — {r['subtopic_name']} [{r['category_letter']}]"
        )
    taxonomy_text = '\n'.join(lines)

    return f"""Classify global health research papers into the taxonomy below.
You will receive the paper's title and abstract. Use BOTH to determine the topic;
the title often contains key signals about the subject area.

Return ONLY this format (no explanation, no preamble):
<category_letter>|<subtopic_id>|<confidence>

Where confidence is: high, med, or low
Example: A|A04|high

If the paper does not fit any subtopic, return: Z|Z00|low

Taxonomy:
{taxonomy_text}"""


# ---------------------------------------------------------------------------
# Mock classifier — keyword matching, no API needed
# ---------------------------------------------------------------------------

def _load_taxonomy_keywords() -> list[dict]:
    """Build keyword lists from taxonomy subtopic names for mock matching."""
    with open(TAXONOMY_CSV) as f:
        rows = list(csv.DictReader(f))
    entries = []
    for r in rows:
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


class BillingError(Exception):
    """Raised when the API returns a billing/credits error."""
    pass


async def classify_batch(
    batch: list[tuple[str, str, str]], system: str
) -> list[tuple[str, str]]:
    # Mock mode: skip API entirely, use keyword matching
    if MOCK:
        return [(oid, mock_classify(title, abstract)) for oid, title, abstract in batch]

    tasks = [classify_one(oid, title, abstract, system) for oid, title, abstract in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Check for billing errors first — if any task hit it, abort the
    # entire batch without writing anything.
    for r in results:
        if isinstance(r, Exception):
            err_msg = str(r).lower()
            if 'credit balance' in err_msg or 'billing' in err_msg:
                raise BillingError(
                    'API credit balance too low. The script will stop now.\n'
                    'Top up credits at https://console.anthropic.com/settings/billing\n'
                    'then re-run this script — it will resume where it left off.'
                )

    # Only include successful results; skip failures so they remain
    # unclassified and will be retried on the next run.
    out = []
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            oid = batch[i][0]
            print(f'  WARNING: skipping {oid} (will retry next run): {r}')
        else:
            out.append(r)
    return out


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

# Boilerplate patterns that appear in the abstract field but aren't real
# abstracts (e.g. journal descriptions stored by OpenAlex).
JUNK_ABSTRACT_PATTERNS = [
    'Annals of Global Health is a peer-reviewed%',
    'Welcome to Annals of Global Health%',
]


def load_unclassified(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str]]:
    junk_clauses = ' '.join(
        f"AND abstract NOT LIKE '{pat}'" for pat in JUNK_ABSTRACT_PATTERNS
    )
    rows = con.execute(f"""
        SELECT openalex_id, title, abstract
        FROM works
        WHERE classified_topic = FALSE
          AND abstract IS NOT NULL
          AND LENGTH(abstract) > 50
          {junk_clauses}
        ORDER BY publication_year DESC
    """).fetchall()
    return rows


def parse_label(raw: str) -> tuple[str, str, str]:
    """Parse 'A|A04|high' → (category, subtopic, confidence).

    Handles common model response variants:
    - 'A|A04|high'           → standard 3-part format
    - 'A04|A04|high'         → subtopic echoed as category (extract letter)
    - 'A|A04|high\\n...'     → extra text after label (take first line)
    - 'A04|high'             → missing category letter (infer from subtopic)
    - 'A|high'               → missing subtopic (category + confidence only)
    """
    # Take only the first line — model sometimes appends explanation
    first_line = raw.split('\n')[0].strip()
    parts = [p.strip() for p in first_line.split('|')]

    valid_conf = {'high', 'med', 'low'}

    if len(parts) >= 3:
        cat, sub, conf = parts[0], parts[1], parts[2]
        if conf not in valid_conf:
            conf = 'low'
        # Standard: 'A|A04|high'
        if len(cat) == 1 and cat.isalpha() and cat.isupper():
            return cat, sub, conf
        # Model echoed subtopic as category: 'A04|A04|high'
        if len(cat) >= 2 and cat[0].isalpha() and cat[0].isupper():
            return cat[0], sub, conf

    if len(parts) == 2:
        a, b = parts[0], parts[1]
        # Case: 'A04|high' — subtopic + confidence, missing category letter
        if len(a) >= 2 and a[0].isalpha() and a[0].isupper() and b in valid_conf:
            return a[0], a, b
        # Case: 'A|high' — category + confidence, missing subtopic
        if len(a) == 1 and a.isalpha() and a.isupper() and b in valid_conf:
            return a, f'{a}00', b

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
    print(f'Classifying {len(rows):,} unclassified works...{mode_label}')

    if not rows:
        print('Nothing to classify. All works already have topic labels.')
        con.close()
        return

    system = build_system_prompt()
    total  = 0

    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        try:
            results = asyncio.run(classify_batch(chunk, system))
        except BillingError as e:
            print(f'\n✗ {e}')
            print(f'  Progress saved: {total:,}/{len(rows):,} classified so far.')
            con.close()
            return
        if results:
            write_results(con, results)
        total += len(results)
        pct    = total / len(rows) * 100
        print(f'  {total:,}/{len(rows):,} ({pct:.1f}%) classified')

    con.close()
    pipeline_complete('02_topic_classify')


if __name__ == '__main__':
    main()
