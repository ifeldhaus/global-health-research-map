"""
validation/01_sample.py

Draws a stratified validation sample from the classified corpus and exports
it as CSV for hand-labeling. The sample is proportionally stratified by
journal and publication year to ensure representative coverage.

Usage:
    uv run python validation/01_sample.py              # standard 200-paper sample
    uv run python validation/01_sample.py --n 100      # custom sample size
    uv run python validation/01_sample.py --test        # 20-paper sample for testing
    uv run python validation/01_sample.py --force       # overwrite existing sample

Next steps after running:
    1. Open validation/validation_sample.csv in Google Sheets
    2. Add columns: human_topic_category, human_subtopic, agree_topic,
                     human_method, agree_method
    3. Hand-label 200 abstracts across 3 sessions (~70 each)
    4. Save as validation/validation_sample_labeled.csv
"""

import argparse
import csv
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import DB  # noqa: E402

SAMPLE_PATH = Path('validation/validation_sample.csv')
LABELED_PATH = Path('validation/validation_sample_labeled.csv')
TAXONOMY_DIR = Path('data/taxonomy')


# ---------------------------------------------------------------------------
# Taxonomy label loaders (for summary output)
# ---------------------------------------------------------------------------

def load_topic_labels() -> dict[str, str]:
    path = TAXONOMY_DIR / 'topic_taxonomy.csv'
    labels = {}
    if path.exists():
        with open(path) as f:
            for row in csv.DictReader(f):
                labels[row['category_letter']] = row['category_name']
    return labels


def load_method_labels() -> dict[str, str]:
    path = TAXONOMY_DIR / 'methods_taxonomy.csv'
    labels = {}
    if path.exists():
        with open(path) as f:
            for row in csv.DictReader(f):
                labels[row['method_id']] = row['method_name']
    return labels


# ---------------------------------------------------------------------------
# Draw sample
# ---------------------------------------------------------------------------

def draw_sample(con: duckdb.DuckDBPyConnection, n: int) -> pd.DataFrame:
    """Draw a stratified random sample of n papers from the classified corpus.

    Stratification is proportional by journal_issn, ensuring every journal
    in the corpus is represented in the sample. Within each journal stratum,
    rows are sampled randomly.
    """
    # Get total classified works per journal
    journal_counts = con.execute("""
        SELECT journal_issn, COUNT(*) AS n
        FROM works
        WHERE classified_topic = TRUE
          AND classified_method = TRUE
          AND abstract IS NOT NULL
          AND LENGTH(abstract) > 50
        GROUP BY journal_issn
        ORDER BY n DESC
    """).fetchdf()

    total_classified = journal_counts['n'].sum()
    if total_classified == 0:
        print('ERROR: No works are classified for both topic and method.')
        print('Run pipeline scripts 02 and 03 first.')
        sys.exit(1)

    # Cap sample at available classified works
    actual_n = min(n, total_classified)
    if actual_n < n:
        print(f'  NOTE: Only {total_classified} classified works available; '
              f'sampling {actual_n} instead of {n}.')

    # Proportional allocation per journal (at least 1 per journal)
    journal_counts['allocation'] = (
        (journal_counts['n'] / total_classified * actual_n)
        .clip(lower=1)
        .round()
        .astype(int)
    )

    # Adjust to hit exact target
    while journal_counts['allocation'].sum() > actual_n:
        # Remove from largest allocation
        idx = journal_counts['allocation'].idxmax()
        journal_counts.loc[idx, 'allocation'] -= 1
    while journal_counts['allocation'].sum() < actual_n:
        # Add to smallest allocation
        idx = journal_counts['allocation'].idxmin()
        journal_counts.loc[idx, 'allocation'] += 1

    # Sample from each journal stratum
    frames = []
    for _, row in journal_counts.iterrows():
        issn = row['journal_issn']
        alloc = int(row['allocation'])
        if alloc <= 0:
            continue

        stratum = con.execute("""
            SELECT openalex_id, title, abstract,
                   journal_issn, publication_year,
                   topic_category, topic_subtopic, topic_confidence,
                   method_type, method_confidence,
                   study_country, country_confidence
            FROM works
            WHERE classified_topic = TRUE
              AND classified_method = TRUE
              AND abstract IS NOT NULL
              AND LENGTH(abstract) > 50
              AND journal_issn = ?
            ORDER BY random()
            LIMIT ?
        """, [issn, alloc]).fetchdf()
        frames.append(stratum)

    sample = pd.concat(frames, ignore_index=True)

    # Shuffle the combined sample
    sample = sample.sample(frac=1, random_state=42).reset_index(drop=True)

    return sample


# ---------------------------------------------------------------------------
# Print distribution summary
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame):
    """Print distribution summary of the sample."""
    topic_labels = load_topic_labels()
    method_labels = load_method_labels()

    print(f'\n  Sample size: {len(df)}')

    # Journal distribution
    print('\n  Papers per journal:')
    for issn, count in df['journal_issn'].value_counts().items():
        print(f'    {issn}: {count}')

    # Year distribution
    print('\n  Papers per year:')
    year_counts = df['publication_year'].value_counts().sort_index()
    for year, count in year_counts.items():
        print(f'    {int(year)}: {count}')

    # Topic distribution
    print('\n  Papers per topic category:')
    for cat, count in df['topic_category'].value_counts().sort_index().items():
        name = topic_labels.get(cat, cat)
        print(f'    {cat} ({name}): {count}')

    # Method distribution
    print('\n  Papers per method:')
    for method, count in df['method_type'].value_counts().sort_index().head(10).items():
        name = method_labels.get(method, method)
        print(f'    {method} ({name}): {count}')

    # Confidence distribution
    print('\n  Topic confidence:')
    for conf, count in df['topic_confidence'].value_counts().items():
        print(f'    {conf}: {count}')

    print('\n  Method confidence:')
    for conf, count in df['method_confidence'].value_counts().items():
        print(f'    {conf}: {count}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Draw stratified validation sample from classified corpus'
    )
    parser.add_argument(
        '--n', type=int, default=200,
        help='Number of papers to sample (default: 200)',
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Test mode: draw only 20 papers',
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Overwrite existing sample file if it exists',
    )
    args = parser.parse_args()

    sample_size = 20 if args.test else args.n

    print(f'Validation Sample — drawing {sample_size} papers')
    if args.test:
        print('  TEST MODE')

    # Check for existing sample
    if SAMPLE_PATH.exists() and not args.force:
        print(f'\n  WARNING: {SAMPLE_PATH} already exists.')
        print('  Use --force to overwrite, or delete it manually.')
        print('  (Overwriting would invalidate any hand-labeling in progress.)')
        sys.exit(1)

    # Connect and draw sample
    con = duckdb.connect(DB, read_only=True)

    # Quick status check
    total = con.execute('SELECT COUNT(*) FROM works').fetchone()[0]
    classified_both = con.execute("""
        SELECT COUNT(*) FROM works
        WHERE classified_topic = TRUE AND classified_method = TRUE
          AND abstract IS NOT NULL AND LENGTH(abstract) > 50
    """).fetchone()[0]
    print(f'  Database: {total:,} total works, {classified_both:,} classified (topic + method)')

    if classified_both == 0:
        print('\n  ERROR: No works classified for both topic and method.')
        print('  Run the classification pipeline (02 + 03) first.')
        con.close()
        sys.exit(1)

    # Draw sample
    sample = draw_sample(con, sample_size)
    con.close()

    # Print summary
    print_summary(sample)

    # Save
    SAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
    sample.to_csv(SAMPLE_PATH, index=False)
    print(f'\n  Sample saved to {SAMPLE_PATH}')

    # Instructions
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    NEXT STEPS                                ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  1. Open {str(SAMPLE_PATH):<38s}   in Google Sheets  ║
║                                                              ║
║  2. Add these columns after the existing ones:               ║
║     • human_topic_category  (your topic category, e.g. "A")  ║
║     • human_subtopic        (your subtopic, e.g. "A04")      ║
║     • agree_topic           (1 = agree, 0 = disagree)        ║
║     • human_method          (your method, e.g. "M01")        ║
║     • agree_method          (1 = agree, 0 = disagree)        ║
║                                                              ║
║  3. Label abstracts across 3 sessions:                       ║
║     • Session 1 (~4 hrs): abstracts 1–70                     ║
║     • Session 2 (~4 hrs): abstracts 71–140                   ║
║     • Session 3 (~4 hrs): abstracts 141–200                  ║
║                                                              ║
║  4. Do NOT look at the LLM labels before making your own     ║
║     judgment — blind labeling prevents bias.                  ║
║                                                              ║
║  5. Save the labeled file as:                                ║
║     {str(LABELED_PATH):<52s}        ║
║                                                              ║
║  6. Then run:                                                ║
║     uv run python validation/02_kappa.py                     ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
""")


if __name__ == '__main__':
    main()
