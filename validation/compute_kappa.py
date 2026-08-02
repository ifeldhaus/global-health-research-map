"""
validation/compute_kappa.py

Canonical validation for the final label set. Reproduces the reported kappas
from the DB (model labels) and the two hand-labeled gold files.

Final label set (all shipped axes are the FIRST-RUN, untuned classifier, so
neither gold set was used to build them → the pooled 345 is a clean held-out
validation for every axis):
  - topic_category / topic_subtopic  : first-run single-label (PRIMARY)
  - topic_category_2 / topic_subtopic_2 : secondary enrichment layer, grafted
        from the multi-label pass (redundant-with-primary dropped). Descriptive
        only; not part of the primary validation.
  - method_type      : first-run
  - study_country    : original

Gold (consolidated into one file with a `source` column):
  validation/validation_labels_all.csv  405 labels across three rounds
    - dev        199  development labels
    - sealed     146  held-out (sealed) labels, drawn excluding the 199
    - expansion   60  held-out labels from the five journals added in the
                      16-journal expansion (1 SKIP)
Pooled = 404 usable, distinct papers, all against the first-run classifier.

Usage:  uv run python validation/compute_kappa.py
"""
import csv
import os
from collections import Counter
import duckdb

DB = 'data/global_health.duckdb'
GOLDS = [('validation/validation_labels_all.csv', 'all')]


def clean(x):
    x = (x or '').strip()
    return '' if x in ('-', 'NONE', 'None') else x


def kappa(pairs):
    h = [a for a, b in pairs]
    m = [b for a, b in pairs]
    n = len(h)
    po = sum(1 for a, b in zip(h, m) if a == b) / n
    cats = set(h) | set(m)
    ph, pm = Counter(h), Counter(m)
    pe = sum((ph[c] / n) * (pm[c] / n) for c in cats)
    return n, po, (po - pe) / (1 - pe) if pe < 1 else 1.0


def cset(s):
    return set(x for x in (s or '').split('|') if x)


def main():
    gold = {}
    for f, src in GOLDS:
        for r in csv.DictReader(open(f)):
            if r['human_topic_category'] == 'SKIP':
                continue
            r['_src'] = src
            gold[r['openalex_id'].split('/')[-1]] = r

    con = duckdb.connect(DB, read_only=True)
    rows = con.execute(
        """SELECT regexp_replace(openalex_id,'.*/',''), topic_category, topic_subtopic,
                  topic_category_2, method_type, study_country
           FROM works WHERE regexp_replace(openalex_id,'.*/','') IN ?""",
        [list(gold)]).fetchall()
    con.close()
    db = {r[0]: r for r in rows}

    ids = [k for k in gold if k in db]
    tp = kappa([(gold[k]['human_topic_category'], db[k][1]) for k in ids])
    ts = kappa([(gold[k]['human_subtopic'], db[k][2]) for k in ids if db[k][2]])
    mp = kappa([(gold[k]['human_method'], db[k][4]) for k in ids
                if gold[k]['human_method'] not in ('', 'SKIP') and db[k][4]])
    cids = [k for k in ids if db[k][5]]
    cp = kappa([((sorted(cset(gold[k]['human_country']))[0] if cset(gold[k]['human_country']) else ''),
                 (sorted(cset(db[k][5]))[0] if cset(db[k][5]) else '')) for k in cids])
    cex = sum(1 for k in cids if cset(gold[k]['human_country']) == cset(db[k][5])) / len(cids)
    sx = sum(1 for k in ids if
             ({gold[k]['human_topic_category']} | ({gold[k]['human_topic_category_2']}
                if clean(gold[k]['human_topic_category_2']) else set()))
             == ({db[k][1]} | ({db[k][3]} if db[k][3] else set()))) / len(ids)

    print(f'Validation set: N={len(ids)} hand-labeled papers\n')
    print(f'  Topic — primary category   kappa = {tp[2]:.3f}   (agreement {tp[1]:.3f})')
    print(f'  Topic — primary subtopic   kappa = {ts[2]:.3f}   (agreement {ts[1]:.3f})')
    print(f'  Topic — set incl. secondary  exact-match = {sx:.3f}')
    print(f'  Methods                    kappa = {mp[2]:.3f}   (agreement {mp[1]:.3f})')
    print(f'  Country                    kappa = {cp[2]:.3f}   (exact-set {cex:.3f})')
    print('\n(Cohen\'s kappa; country by primary-country with set-exact shown. All axes are the '
          'first-run/untuned classifier, so the full validation set is a clean held-out check.)')

    # ---- emit the paper's validation table (tab:validation) ----------------
    def descr(k):
        return ('almost perfect' if k >= 0.81 else 'substantial' if k >= 0.61
                else 'moderate' if k >= 0.41 else 'fair' if k >= 0.21 else 'slight')
    tex = [r'\begin{table}[ht]', r'\centering',
           f'\\caption{{Classification agreement against {len(ids)} blind hand-labeled works.}}',
           r'\label{tab:validation}', r'\begin{tabular}{lrl}', r'\toprule',
           r"Label axis & Cohen's $\kappa$ & Agreement \\", r'\midrule',
           f'Topic, primary category & {tp[2]:.2f} & {descr(tp[2])} ' + r'\\',
           f'Topic, primary subtopic & {ts[2]:.2f} & {descr(ts[2])} ' + r'\\',
           f'Study design            & {mp[2]:.2f} & {descr(mp[2])} ' + r'\\',
           f'Study country           & {cp[2]:.2f} & {descr(cp[2])} ' + r'\\',
           r'\bottomrule', r'\end{tabular}', r'\end{table}']
    out = os.path.join(os.path.dirname(__file__), '..', 'paper', 'validation_table.tex')
    if os.path.isdir(os.path.dirname(out)):
        open(out, 'w').write('\n'.join(tex) + '\n')
        print(f'\nwrote {os.path.normpath(out)}')


if __name__ == '__main__':
    main()
