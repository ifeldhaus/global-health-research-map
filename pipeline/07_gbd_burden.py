"""
pipeline/07_gbd_burden.py

Loads IHME Global Burden of Disease (GBD) data into the gbd_burden table
and populates the topic_burden_map from the taxonomy CSV.

The IHME CSV(s) must be downloaded manually from the GBD Results Tool
(https://vizhub.healthdata.org/gbd-results/) because IHME requires a
free account for downloads.

The IHME tool limits how many parameters can be selected at once, so
download in 3 batches (one per metric: Number, Percent, Rate). Save
all CSVs to data/gbd/ — this script auto-detects and merges them.

See docs/gbd_burden_methodology.md for full design rationale.

Usage:
    uv run python pipeline/07_gbd_burden.py                          # auto-detect CSVs in data/gbd/
    uv run python pipeline/07_gbd_burden.py --file a.csv b.csv       # specific file(s)
    uv run python pipeline/07_gbd_burden.py --test                   # load years >= 2015 only

Prerequisites:
    - Download IHME GBD Results CSVs to data/gbd/  (see --help for instructions)
    - data/taxonomy/topic_burden_map.csv exists
"""

import argparse
import csv
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import DB, notify  # noqa: E402

GBD_DIR = Path('data/gbd')
MAP_CSV = Path('data/taxonomy/topic_burden_map.csv')

# Columns the IHME GBD Results Tool CSV must contain
REQUIRED_COLUMNS = {'measure_name', 'location_name', 'sex_name', 'age_name',
                    'cause_name', 'metric_name', 'year', 'val'}

DOWNLOAD_INSTRUCTIONS = """
╔══════════════════════════════════════════════════════════════════╗
║  No IHME GBD CSVs found in data/gbd/                           ║
║  Please download manually (one-time, ~5 minutes):              ║
║                                                                  ║
║  1. Go to https://vizhub.healthdata.org/gbd-results/            ║
║  2. Create a free account / sign in                              ║
║  3. For ALL three downloads, select:                             ║
║     • Measure:  DALYs (Disability-Adjusted Life Years) + Deaths  ║
║     • Cause:    All causes                                       ║
║     • Location: Global                                           ║
║     • Age:      All ages                                         ║
║     • Sex:      Both                                             ║
║     • Year:     Select all (or at minimum 2010-2021)             ║
║  4. Download THREE times, changing only the Metric each time:    ║
║     • Download 1: Metric = Number                                ║
║     • Download 2: Metric = Percent                               ║
║     • Download 3: Metric = Rate                                  ║
║  5. Save all CSVs to data/gbd/                                   ║
║  6. Re-run this script                                           ║
╚══════════════════════════════════════════════════════════════════╝
"""


# ---------------------------------------------------------------------------
# Step 1: Find and validate IHME CSVs
# ---------------------------------------------------------------------------

def find_ihme_csvs(explicit_paths: list[str] | None = None) -> list[Path]:
    """Find IHME GBD CSVs, either from explicit paths or by scanning data/gbd/."""
    if explicit_paths:
        paths = []
        for p in explicit_paths:
            path = Path(p)
            if path.exists():
                paths.append(path)
            else:
                print(f'WARNING: Specified file not found: {path}')
        return paths

    if not GBD_DIR.exists():
        return []

    # Find all CSVs in data/gbd/ that have IHME columns
    valid = []
    for csv_path in sorted(GBD_DIR.glob('*.csv')):
        try:
            df = pd.read_csv(csv_path, nrows=1)
            if REQUIRED_COLUMNS.issubset(set(df.columns)):
                valid.append(csv_path)
        except Exception:
            continue

    return valid


def validate_and_concat(csv_paths: list[Path]) -> pd.DataFrame:
    """Read, validate, and concatenate multiple IHME CSVs."""
    frames = []
    for csv_path in csv_paths:
        print(f'  Reading {csv_path.name}...')
        df = pd.read_csv(csv_path)

        missing = REQUIRED_COLUMNS - set(df.columns)
        if missing:
            print(f'  ERROR: {csv_path.name} is missing columns: {missing}')
            sys.exit(1)

        metric = df['metric_name'].unique()
        measures = df['measure_name'].unique()
        print(f'    {len(df):,} rows | metric: {", ".join(metric)} | '
              f'measures: {", ".join(measures)} | '
              f'years: {df["year"].min()}-{df["year"].max()} | '
              f'causes: {df["cause_name"].nunique()}')
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)

    # Deduplicate in case of overlapping downloads
    key_cols = ['measure_name', 'metric_name', 'location_name', 'sex_name',
                'age_name', 'cause_name', 'year']
    before = len(combined)
    combined = combined.drop_duplicates(subset=key_cols, keep='last')
    if len(combined) < before:
        print(f'  Removed {before - len(combined):,} duplicate rows')

    print(f'\n  Combined: {len(combined):,} rows')
    print(f'  Measures: {sorted(combined["measure_name"].unique())}')
    print(f'  Metrics:  {sorted(combined["metric_name"].unique())}')
    print(f'  Years:    {combined["year"].min()}-{combined["year"].max()}')
    print(f'  Causes:   {combined["cause_name"].nunique()} unique')

    return combined


# ---------------------------------------------------------------------------
# Step 2: Parse and load gbd_burden
# ---------------------------------------------------------------------------

def load_gbd_burden(con: duckdb.DuckDBPyConnection, df: pd.DataFrame,
                    test: bool = False):
    """Load IHME data into gbd_burden table."""

    # Normalise measure names to short labels
    measure_map = {
        'DALYs (Disability-Adjusted Life Years)': 'DALYs',
        'Deaths': 'Deaths',
    }

    # Filter to known measures only
    known_measures = set(measure_map.keys())
    filtered = df[df['measure_name'].isin(known_measures)].copy()

    if len(filtered) == 0:
        print('ERROR: No rows match known measure names.')
        print(f'  Found: {sorted(df["measure_name"].unique())}')
        print(f'  Expected one of: {sorted(known_measures)}')
        sys.exit(1)

    filtered['measure_short'] = filtered['measure_name'].map(measure_map)

    if test:
        filtered = filtered[filtered['year'] >= 2015]

    print(f'\n  {len(filtered):,} rows after filtering '
          f'({filtered["cause_name"].nunique()} causes × '
          f'{filtered["year"].nunique()} years × '
          f'{filtered["measure_short"].nunique()} measures × '
          f'{filtered["metric_name"].nunique()} metrics)')

    # Build records for insertion
    records = []
    for _, row in filtered.iterrows():
        records.append((
            row['cause_name'],
            row['location_name'],
            int(row['year']),
            row['measure_short'],
            row['metric_name'],
            row['sex_name'],
            row['age_name'],
            float(row['val']),
            float(row['upper']) if pd.notna(row.get('upper')) else None,
            float(row['lower']) if pd.notna(row.get('lower')) else None,
        ))

    # Drop and recreate table to ensure schema is current
    con.execute('DROP TABLE IF EXISTS gbd_burden')
    con.execute("""
        CREATE TABLE gbd_burden (
            cause       VARCHAR,
            region      VARCHAR,
            year        INTEGER,
            measure     VARCHAR,
            metric      VARCHAR,
            sex         VARCHAR,
            age_group   VARCHAR,
            val         DOUBLE,
            upper       DOUBLE,
            lower       DOUBLE,
            PRIMARY KEY (cause, region, year, measure, metric, sex, age_group)
        )
    """)

    con.executemany(
        'INSERT INTO gbd_burden (cause, region, year, measure, metric, sex, '
        'age_group, val, upper, lower) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        records,
    )

    loaded = con.execute('SELECT COUNT(*) FROM gbd_burden').fetchone()[0]
    causes = con.execute('SELECT COUNT(DISTINCT cause) FROM gbd_burden').fetchone()[0]
    measures = con.execute(
        'SELECT DISTINCT measure, metric FROM gbd_burden ORDER BY measure, metric'
    ).fetchdf()
    years = con.execute(
        'SELECT MIN(year), MAX(year) FROM gbd_burden'
    ).fetchone()

    print(f'  Loaded {loaded:,} rows into gbd_burden '
          f'({causes} causes, {years[0]}-{years[1]})')
    print('  Measure × Metric combinations:')
    for _, row in measures.iterrows():
        count = con.execute(
            'SELECT COUNT(*) FROM gbd_burden WHERE measure = ? AND metric = ?',
            [row['measure'], row['metric']]
        ).fetchone()[0]
        print(f'    {row["measure"]} / {row["metric"]}: {count:,} rows')


# ---------------------------------------------------------------------------
# Step 3: Load topic_burden_map + validate
# ---------------------------------------------------------------------------

def load_topic_burden_map(con: duckdb.DuckDBPyConnection):
    """Load topic-to-GBD-cause mapping and validate against gbd_burden."""
    if not MAP_CSV.exists():
        print(f'ERROR: Mapping file not found at {MAP_CSV}')
        sys.exit(1)

    # Recreate table with composite PK (in case schema is outdated)
    con.execute('DROP TABLE IF EXISTS topic_burden_map')
    con.execute("""
        CREATE TABLE topic_burden_map (
            topic_category  VARCHAR,
            topic_name      VARCHAR,
            gbd_cause       VARCHAR,
            notes           VARCHAR,
            PRIMARY KEY (topic_category, gbd_cause)
        )
    """)

    # Read and insert
    with open(MAP_CSV, 'r') as f:
        reader = csv.DictReader(f)
        records = [
            (row['topic_category'], row['topic_name'],
             row['gbd_cause'], row['notes'])
            for row in reader
        ]

    con.executemany(
        'INSERT INTO topic_burden_map (topic_category, topic_name, gbd_cause, notes) '
        'VALUES (?, ?, ?, ?)',
        records,
    )

    loaded = con.execute('SELECT COUNT(*) FROM topic_burden_map').fetchone()[0]
    topics = con.execute(
        'SELECT COUNT(DISTINCT topic_category) FROM topic_burden_map'
    ).fetchone()[0]
    print(f'\n  Loaded {loaded} mappings covering {topics} topic categories')

    # Validate: check that all gbd_cause values match actual causes in gbd_burden
    unmatched = con.execute("""
        SELECT DISTINCT tbm.gbd_cause
        FROM topic_burden_map tbm
        WHERE tbm.gbd_cause NOT IN (
            SELECT DISTINCT cause FROM gbd_burden
        )
    """).fetchdf()

    if len(unmatched) > 0:
        print(f'\n  ⚠️  WARNING: {len(unmatched)} cause(s) in topic_burden_map '
              f'do not match any cause in gbd_burden:')
        for _, row in unmatched.iterrows():
            print(f'     • "{row["gbd_cause"]}"')
        print('\n  Available causes in gbd_burden:')
        causes = con.execute(
            'SELECT DISTINCT cause FROM gbd_burden ORDER BY cause'
        ).fetchdf()
        for _, row in causes.iterrows():
            print(f'     • "{row["cause"]}"')
        print('\n  → Update data/taxonomy/topic_burden_map.csv to use exact '
              'cause names from above.')
    else:
        print('  ✓ All cause names in topic_burden_map match gbd_burden')

    # Show matched summary (using Number metric for DALYs)
    matched = con.execute("""
        SELECT tbm.topic_category, tbm.topic_name,
               COUNT(DISTINCT tbm.gbd_cause) AS n_causes,
               SUM(g.total_dalys) AS total_dalys
        FROM topic_burden_map tbm
        LEFT JOIN (
            SELECT cause, SUM(val) AS total_dalys
            FROM gbd_burden
            WHERE measure = 'DALYs' AND metric = 'Number'
            GROUP BY cause
        ) g ON tbm.gbd_cause = g.cause
        GROUP BY tbm.topic_category, tbm.topic_name
        ORDER BY tbm.topic_category
    """).fetchdf()
    print('\n  Topic → GBD burden summary (DALYs, Number, all years summed):')
    for _, row in matched.iterrows():
        dalys = f'{row["total_dalys"]:,.0f}' if pd.notna(row['total_dalys']) else 'NO MATCH'
        print(f'    {row["topic_category"]} ({row["topic_name"]}): '
              f'{row["n_causes"]} cause(s), total DALYs = {dalys}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Load IHME GBD burden data into DuckDB',
        epilog=DOWNLOAD_INSTRUCTIONS,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--file', type=str, nargs='+', default=None,
        help='Path(s) to IHME GBD Results CSV(s) (default: auto-detect in data/gbd/)',
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Test mode: load only years >= 2015',
    )
    args = parser.parse_args()

    print('GBD Burden Loader')
    print(f'  Database: {DB}')
    print(f'  Mapping:  {MAP_CSV}')

    # Step 1: Find CSVs
    csv_paths = find_ihme_csvs(args.file)
    if not csv_paths:
        GBD_DIR.mkdir(parents=True, exist_ok=True)
        print(DOWNLOAD_INSTRUCTIONS)
        sys.exit(1)

    print(f'\n  Found {len(csv_paths)} CSV file(s):')
    for p in csv_paths:
        print(f'    • {p}')

    if args.test:
        print('  Mode: TEST (years >= 2015 only)')

    # Validate and concatenate
    print('\n── Reading and validating CSVs ──')
    df = validate_and_concat(csv_paths)

    # Step 2: Load gbd_burden
    print('\n── Step 1: Loading GBD burden data ──')
    con = duckdb.connect(DB)
    load_gbd_burden(con, df, test=args.test)

    # Step 3: Load topic_burden_map + validate
    print('\n── Step 2: Loading topic-burden mapping ──')
    load_topic_burden_map(con)

    con.close()

    notify(
        title='07_gbd_burden complete',
        message='GBD data and topic mapping loaded into DuckDB.',
    )
    print('\n✓ GBD burden data loaded. Lens C analyses are now enabled.')


if __name__ == '__main__':
    main()
