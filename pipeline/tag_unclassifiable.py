"""
pipeline/tag_unclassifiable.py

Adds a `classification_note` column to the works table and tags papers that
cannot be classified with the reason why.  This ensures they are not treated
as missing data in the findings write-up or dashboard.

Values:
    NULL                    — classified (or classifiable); no note needed
    'no_abstract'           — OpenAlex has no abstract for this paper
    'insufficient_abstract' — abstract field is present but too short / junk
                              (≤ 50 chars: funder names, "None.", registration IDs, etc.)
    'boilerplate_abstract'  — abstract field contains journal boilerplate text
                              instead of a real abstract (e.g. Annals of Global Health)

The script is idempotent — safe to re-run at any time (e.g. after new data
loads or after running classification scripts).

Usage:
    uv run python pipeline/tag_unclassifiable.py              # dry-run
    uv run python pipeline/tag_unclassifiable.py --confirm    # apply tags
"""

import argparse
import duckdb

DB = 'data/global_health.duckdb'

# Must match the threshold in 02_topic_classify.py and 03_methods_classify.py
MIN_ABSTRACT_LENGTH = 50

# Boilerplate patterns — journal descriptions stored by OpenAlex instead of
# real abstracts.  Must stay in sync with JUNK_ABSTRACT_PATTERNS in
# 02_topic_classify.py.
BOILERPLATE_PATTERNS = [
    "Annals of Global Health is a peer-reviewed%",
    "Welcome to Annals of Global Health%",
]


def main():
    parser = argparse.ArgumentParser(
        description='Tag papers that cannot be classified with the reason why'
    )
    parser.add_argument(
        '--confirm', action='store_true',
        help='Actually apply the tags.  Without this flag the script only '
             'shows what would happen (dry-run).',
    )
    args = parser.parse_args()

    con = duckdb.connect(DB)

    # ── Ensure column exists ─────────────────────────────────────────
    cols = [r[0] for r in con.execute('DESCRIBE works').fetchall()]
    if 'classification_note' not in cols:
        if not args.confirm:
            print('Column `classification_note` does not exist yet.  '
                  'It will be added on --confirm.')
        else:
            con.execute('ALTER TABLE works ADD COLUMN classification_note VARCHAR')
            print('Added column `classification_note` to works table.')
    else:
        print('Column `classification_note` already exists.')

    # ── Build boilerplate SQL clause ─────────────────────────────────
    bp_or_clauses = ' OR '.join(
        f"abstract LIKE '{pat}'" for pat in BOILERPLATE_PATTERNS
    )
    bp_where = f"({bp_or_clauses})"

    # ── Count what would be tagged ───────────────────────────────────
    no_abstract = con.execute("""
        SELECT COUNT(*) FROM works
        WHERE (abstract IS NULL OR TRIM(abstract) = '')
    """).fetchone()[0]

    insufficient = con.execute(f"""
        SELECT COUNT(*) FROM works
        WHERE abstract IS NOT NULL
          AND TRIM(abstract) != ''
          AND LENGTH(TRIM(abstract)) <= {MIN_ABSTRACT_LENGTH}
    """).fetchone()[0]

    boilerplate = con.execute(f"""
        SELECT COUNT(*) FROM works
        WHERE abstract IS NOT NULL
          AND LENGTH(TRIM(abstract)) > {MIN_ABSTRACT_LENGTH}
          AND {bp_where}
    """).fetchone()[0]

    classifiable = con.execute(f"""
        SELECT COUNT(*) FROM works
        WHERE abstract IS NOT NULL
          AND LENGTH(TRIM(abstract)) > {MIN_ABSTRACT_LENGTH}
          AND NOT {bp_where}
    """).fetchone()[0]

    total = no_abstract + insufficient + boilerplate + classifiable

    print()
    print(f'Total works:              {total:,}')
    print(f'  no_abstract:            {no_abstract:,}  '
          f'({no_abstract/total*100:.1f}%)')
    print(f'  insufficient_abstract:  {insufficient:,}  '
          f'({insufficient/total*100:.1f}%)')
    print(f'  boilerplate_abstract:   {boilerplate:,}  '
          f'({boilerplate/total*100:.1f}%)')
    print(f'  classifiable (NULL):    {classifiable:,}  '
          f'({classifiable/total*100:.1f}%)')

    if not args.confirm:
        print()
        print('DRY RUN — no changes made.')
        print('Re-run with --confirm to apply tags.')
        con.close()
        return

    # ── Apply tags ───────────────────────────────────────────────────
    # Reset all to NULL first (idempotent)
    con.execute('UPDATE works SET classification_note = NULL')

    n1 = con.execute("""
        UPDATE works
        SET classification_note = 'no_abstract'
        WHERE abstract IS NULL OR TRIM(abstract) = ''
    """).fetchone()[0]

    n2 = con.execute(f"""
        UPDATE works
        SET classification_note = 'insufficient_abstract'
        WHERE abstract IS NOT NULL
          AND TRIM(abstract) != ''
          AND LENGTH(TRIM(abstract)) <= {MIN_ABSTRACT_LENGTH}
    """).fetchone()[0]

    n3 = con.execute(f"""
        UPDATE works
        SET classification_note = 'boilerplate_abstract'
        WHERE abstract IS NOT NULL
          AND LENGTH(TRIM(abstract)) > {MIN_ABSTRACT_LENGTH}
          AND {bp_where}
          AND classification_note IS NULL
    """).fetchone()[0]

    con.close()

    print()
    print(f'✓ Tagged {n1:,} papers as no_abstract')
    print(f'✓ Tagged {n2:,} papers as insufficient_abstract')
    print(f'✓ Tagged {n3:,} papers as boilerplate_abstract')
    print(f'✓ {classifiable:,} papers left as NULL (classifiable)')


if __name__ == '__main__':
    main()
