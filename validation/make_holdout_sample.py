"""Draw a blind validation holdout sample from the classified corpus.

Stratified proportionally by journal. Optionally restrict to a subset of
journals (e.g. a set newly added to the corpus) and exclude works already
labeled in a previous round, so successive holdouts stay disjoint and the
pooled set keeps one sampling density.

Examples:
    # 60 papers from the five journals added in the 16-journal expansion,
    # excluding anything already labeled:
    uv run python validation/make_holdout_sample.py \
        --journals 1654-9880,1876-3405,2397-0642,2414-6447,2210-6014 \
        --n 60 --exclude validation/labels_so_far.csv --out validation/holdout_sample.json

    # 150 papers from across all journals:
    uv run python validation/make_holdout_sample.py --n 150 --out holdout.json

The output JSON feeds make_labeling_tool.py. Hand labels are exported by the
tool and are NOT produced here; they live outside version control.
"""
import argparse
import csv
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB = str(ROOT / 'data' / 'global_health.duckdb')
JOURNALS_CSV = ROOT / 'data' / 'journal_list.csv'


def load_exclude(path: str) -> set[str]:
    ids: set[str] = set()
    if not path:
        return ids
    for r in csv.DictReader(open(path)):
        oid = (r.get('openalex_id') or '').split('/')[-1]
        if oid:
            ids.add(oid)
    return ids


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--journals', default='all',
                    help="comma-separated ISSNs, or 'all' (default)")
    ap.add_argument('--n', type=int, default=150, help='total sample size')
    ap.add_argument('--exclude', default='',
                    help='labels CSV whose openalex_ids to exclude')
    ap.add_argument('--out', default='validation/holdout_sample.json')
    ap.add_argument('--seed', type=int, default=42, help='shuffle seed (reproducible)')
    args = ap.parse_args()

    names = {r['issn']: r['journal_name'] for r in csv.DictReader(open(JOURNALS_CSV))}
    issns = list(names) if args.journals == 'all' else [s.strip() for s in args.journals.split(',')]
    exclude = load_exclude(args.exclude)

    con = duckdb.connect(DB, read_only=True)
    con.execute(f'SELECT setseed({(args.seed % 1000) / 1000.0})')

    # available pool per journal (usable abstract, not already labeled)
    pool = {}
    for issn in issns:
        n = con.execute(
            "SELECT COUNT(*) FROM works WHERE journal_issn = ? "
            "AND abstract IS NOT NULL AND LENGTH(abstract) > 50", [issn]).fetchone()[0]
        pool[issn] = n
    total = sum(pool.values()) or 1
    alloc = {i: max(1, round(pool[i] / total * args.n)) for i in issns if pool[i]}
    while sum(alloc.values()) > args.n:
        alloc[max(alloc, key=alloc.get)] -= 1
    while sum(alloc.values()) < args.n:
        alloc[min(alloc, key=alloc.get)] += 1

    papers = []
    for issn, k in alloc.items():
        rows = con.execute(
            "SELECT openalex_id, title, abstract, publication_year, journal_issn "
            "FROM works WHERE journal_issn = ? AND abstract IS NOT NULL "
            "AND LENGTH(abstract) > 50 ORDER BY random() LIMIT ?", [issn, k * 3]).fetchall()
        picked = 0
        for oid, title, ab, yr, ji in rows:
            if oid.split('/')[-1] in exclude:
                continue
            papers.append({'id': oid, 'title': title, 'abstract': ab,
                           'year': yr, 'journal': names.get(ji, ji)})
            picked += 1
            if picked >= k:
                break
    # deterministic interleave (seed-free order that mixes journals)
    papers.sort(key=lambda p: p['id'][::-1])
    con.close()

    Path(args.out).write_text(json.dumps(papers, ensure_ascii=False, indent=0))
    from collections import Counter
    print(f'wrote {args.out}: {len(papers)} papers')
    print('by journal:', dict(Counter(p['journal'] for p in papers)))


if __name__ == '__main__':
    main()
