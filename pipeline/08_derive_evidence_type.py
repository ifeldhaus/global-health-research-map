"""
pipeline/08_derive_evidence_type.py

Derives the `evidence_type` column on `works` from `method_type`, grouping the
18-code method taxonomy into three analysis groups:

    empirical      primary studies (trials, observational, modelling, econ, ...)
    synthesis      systematic & scoping reviews
    non_empirical  narrative reviews, commentary/editorial/perspective, other/unclear

Commentary/editorial/perspective (M15) alone is ~28% of the classified corpus.
Left in, it distorts "where research happens" analyses, so lenses A/B/D restrict
to evidence_type = 'empirical'. Lens C stratifies attention (all) vs evidence
(empirical). The mapping lives in pipeline/utils.py (single source of truth).

Usage:
    uv run python pipeline/08_derive_evidence_type.py
"""

import os
import sys

import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from pipeline.utils import DB, EVIDENCE_TYPE_MAP  # noqa: E402


def main():
    con = duckdb.connect(DB)

    cols = [r[1] for r in con.execute("PRAGMA table_info('works')").fetchall()]
    if 'evidence_type' not in cols:
        con.execute("ALTER TABLE works ADD COLUMN evidence_type VARCHAR")

    # Reset, then set from the mapping. Classified-but-unmapped codes (should be
    # none) fall through to non_empirical; unclassified stay NULL.
    con.execute("UPDATE works SET evidence_type = NULL")
    con.execute("""
        UPDATE works
        SET evidence_type = CASE
            WHEN method_type IS NULL THEN NULL
            ELSE 'non_empirical'
        END
    """)
    for code, grp in EVIDENCE_TYPE_MAP.items():
        con.execute(
            "UPDATE works SET evidence_type = ? WHERE method_type = ?",
            [grp, code],
        )

    rows = con.execute("""
        SELECT evidence_type, COUNT(*) c
        FROM works WHERE evidence_type IS NOT NULL
        GROUP BY 1 ORDER BY c DESC
    """).fetchall()
    total = sum(c for _, c in rows)
    con.close()

    print(f'evidence_type populated for {total:,} classified works:')
    for grp, c in rows:
        print(f'  {grp:<14} {c:>6,} ({100*c/total:.1f}%)')


if __name__ == '__main__':
    main()
