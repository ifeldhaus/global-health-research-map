"""
Draw a blind validation holdout from the FIVE newly-added journals only, and
regenerate the sealed labeling tool for it. Reuses the taxonomy payload
(cats/methods/countries) and full UI from the existing sealed_label_tool.html,
swapping in the new papers and namespacing the localStorage key so it does not
collide with the prior (11-journal) sealed labeling.

Run: uv run python validation/make_expansion_holdout.py
Outputs:
  validation/sealed_label_tool_expansion.html   (deliver to labeler)
  validation/sealed_expansion_ids.csv           (ids, for later kappa join)
"""
import csv
import json
import re
from pathlib import Path

import duckdb

DB = 'data/global_health.duckdb'
SRC_HTML = Path('validation/archive/sealed_label_tool.html')
OUT_HTML = Path('validation/sealed_label_tool_expansion.html')
OUT_IDS = Path('validation/sealed_expansion_ids.csv')
KEY = 'ghrm_sealed_expansion_v1'

# The five newly-added journals (ISSN -> name), sampled in isolation.
NEW = {
    '1654-9880': 'Global Health Action',
    '1876-3405': 'International Health',
    '2397-0642': 'Global Health Research and Policy',
    '2414-6447': 'Global Health Journal',
    '2210-6014': 'Journal of Epidemiology and Global Health',
}
# Match the ORIGINAL validation's sampling density so the combined set stays
# proportional across all 16 journals. Original: 345 papers over the classifiable
# 11-journal corpus. Target for the new journals = same density x their pool.
ORIG_N = 345

c = duckdb.connect(DB, read_only=True)

# Per-journal available pool (abstract present and substantive)
pool = {}
for issn in NEW:
    n = c.execute(
        """SELECT COUNT(*) FROM works
           WHERE journal_issn = ? AND abstract IS NOT NULL AND LENGTH(abstract) > 50""",
        [issn]).fetchone()[0]
    pool[issn] = n
total_pool = sum(pool.values())

# Classifiable pool of the 11 ORIGINAL journals (already classified) = original density denom.
orig_pool = c.execute(
    """SELECT COUNT(*) FROM works
       WHERE journal_issn NOT IN ('1654-9880','1876-3405','2397-0642','2414-6447','2210-6014')
         AND abstract IS NOT NULL AND LENGTH(abstract) > 50""").fetchone()[0]
TARGET = round(ORIG_N * total_pool / orig_pool)
print(f'original density = {ORIG_N}/{orig_pool} = {ORIG_N/orig_pool*100:.3f}%')
print('available pool per journal:', pool, 'total', total_pool)
print(f'proportional TARGET for 5 new journals = {TARGET}')

# Proportional allocation across the five (round; no artificial floor)
alloc = {issn: max(1, round(pool[issn] / total_pool * TARGET)) for issn in NEW}
# trim to exactly TARGET
while sum(alloc.values()) > TARGET:
    k = max(alloc, key=lambda x: alloc[x]); alloc[k] -= 1
while sum(alloc.values()) < TARGET:
    k = min(alloc, key=lambda x: alloc[x]); alloc[k] += 1
print('allocation:', alloc, 'total', sum(alloc.values()))

papers = []
for issn, k in alloc.items():
    rows = c.execute(
        """SELECT openalex_id, title, abstract, publication_year, journal_issn
           FROM works
           WHERE journal_issn = ? AND abstract IS NOT NULL AND LENGTH(abstract) > 50
           ORDER BY random()
           LIMIT ?""", [issn, k]).fetchall()
    for oid, title, ab,  yr, ji in rows:
        papers.append({'id': oid, 'title': title, 'abstract': ab,
                       'year': yr, 'journal': NEW.get(ji, ji)})

# Deterministic shuffle so journals are interleaved (seed-free: sort by id hash)
papers.sort(key=lambda p: p['id'][::-1])
print('drawn:', len(papers))

# ---- Rebuild the sealed HTML with the new papers -------------------------
html = SRC_HTML.read_text()
lines = html.split('\n')
# locate the `const B={...};` line
bi = next(i for i, ln in enumerate(lines) if ln.startswith('const B='))
old_B = json.loads(re.match(r'const B=(\{.*\});?\s*$', lines[bi]).group(1))
new_B = {'papers': papers, 'cats': old_B['cats'],
         'methods': old_B['methods'], 'countries': old_B['countries']}
lines[bi] = 'const B=' + json.dumps(new_B, ensure_ascii=False) + ';'

# namespace the localStorage key so it does not collide with prior labeling
out = '\n'.join(lines).replace("const KEY='ghrm_sealed_v1';", f"const KEY='{KEY}';")
assert f"const KEY='{KEY}';" in out, 'KEY replacement failed'
OUT_HTML.write_text(out)
print('wrote', OUT_HTML)

with open(OUT_IDS, 'w', newline='') as f:
    w = csv.writer(f); w.writerow(['openalex_id', 'journal_issn'])
    # re-derive issn from journal name map (reverse)
    rev = {v: k for k, v in NEW.items()}
    for p in papers:
        w.writerow([p['id'].split('/')[-1], rev.get(p['journal'], '')])
print('wrote', OUT_IDS, f'({len(papers)} ids)')
