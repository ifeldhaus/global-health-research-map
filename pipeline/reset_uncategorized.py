"""
pipeline/reset_uncategorized.py

Resets low-confidence Z (Other/Uncategorized) topic classifications so they
can be reclassified on the next run of 02_topic_classify.py.

This is intentionally a SEPARATE script — it is never called automatically.
Run it only when you want to give the classifier another pass at papers it
previously couldn't categorise.  On the final run, if some papers still end
up as Z, that's fine — they stay in the full dataset.

Usage:
    uv run python pipeline/reset_uncategorized.py              # dry-run (preview only)
    uv run python pipeline/reset_uncategorized.py --confirm    # actually reset
    uv run python pipeline/reset_uncategorized.py --all-z      # include Z|med and Z|high too
"""

import argparse
import duckdb

DB = 'data/global_health.duckdb'


def main():
    parser = argparse.ArgumentParser(
        description='Reset uncategorized (Z) topic classifications for reclassification'
    )
    parser.add_argument(
        '--confirm', action='store_true',
        help='Actually perform the reset.  Without this flag the script only '
             'shows what would happen (dry-run).',
    )
    parser.add_argument(
        '--all-z', action='store_true',
        help='Reset ALL Z classifications regardless of confidence.  '
             'By default only Z|low are reset.',
    )
    args = parser.parse_args()

    con = duckdb.connect(DB)

    # ── Current state ────────────────────────────────────────────────
    total = con.execute('SELECT COUNT(*) FROM works').fetchone()[0]
    classified = con.execute(
        'SELECT COUNT(*) FROM works WHERE classified_topic = TRUE'
    ).fetchone()[0]
    unclassified = con.execute(
        'SELECT COUNT(*) FROM works WHERE classified_topic = FALSE'
    ).fetchone()[0]

    z_by_conf = con.execute("""
        SELECT topic_confidence, COUNT(*)
        FROM works
        WHERE topic_category = 'Z' AND classified_topic = TRUE
        GROUP BY topic_confidence
        ORDER BY topic_confidence
    """).fetchall()

    print('Current database state')
    print(f'  Total works:      {total:,}')
    print(f'  Classified:       {classified:,}')
    print(f'  Unclassified:     {unclassified:,}')
    print()
    print('  Z (Other/Uncategorized) breakdown:')
    z_total = 0
    for conf, n in z_by_conf:
        print(f'    Z|{conf:4s}  {n:,}')
        z_total += n
    print(f'    {"total":>6s}  {z_total:,}')

    # ── Determine which rows to reset ────────────────────────────────
    if args.all_z:
        condition = "topic_category = 'Z' AND classified_topic = TRUE"
        label = 'all Z classifications'
        to_reset = z_total
    else:
        condition = ("topic_category = 'Z' AND topic_confidence = 'low' "
                     "AND classified_topic = TRUE")
        label = 'Z|low classifications only'
        to_reset = sum(n for conf, n in z_by_conf if conf == 'low')

    print()
    print(f'Target: {label}')
    print(f'  Papers to reset:  {to_reset:,}')

    if to_reset == 0:
        print('  Nothing to reset.')
        con.close()
        return

    after_classified = classified - to_reset
    after_unclassified = unclassified + to_reset
    print(f'  After reset:      {after_classified:,} classified, '
          f'{after_unclassified:,} unclassified')

    # ── Execute or dry-run ───────────────────────────────────────────
    if not args.confirm:
        print()
        print('DRY RUN — no changes made.')
        print('Re-run with --confirm to apply the reset.')
        con.close()
        return

    n_updated = con.execute(f"""
        UPDATE works
        SET topic_category  = NULL,
            topic_subtopic  = NULL,
            topic_confidence = NULL,
            classified_topic = FALSE
        WHERE {condition}
    """).fetchone()[0]

    con.close()

    print()
    print(f'✓ Reset {n_updated:,} papers.  They will be reclassified on the '
          f'next run of 02_topic_classify.py.')


if __name__ == '__main__':
    main()
