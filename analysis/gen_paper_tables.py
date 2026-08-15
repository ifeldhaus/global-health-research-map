"""Regenerate EVERY count-bearing LaTeX table the paper \\inputs, from the DB.
Reproduces the exact structure of the hand-frozen tables so the Overleaf draft
can be refreshed for the expanded (16-journal) corpus. Run AFTER classification,
enrichment, and the summary-notebook rebuild.

Run: uv run python paper/gen_paper_tables.py

Writes (into paper/):
  topic_category_table.tex  topic_full_table.tex
  study_country_top_table.tex  study_country_full_table.tex
  funder_top_table.tex  funder_full_table.tex
  institution_top_table.tex  institution_full_table.tex
  study_design_table.tex
  commentary_journal_table.tex  commentary_inst_table.tex
Static (NOT regenerated): crosswalk_table.tex, taxonomy_table.tex.
"""
import csv
from collections import defaultdict
from pathlib import Path

import duckdb
import pycountry

ROOT = Path(__file__).parent.parent
DB = str(ROOT / 'data/global_health.duckdb')
OUT = ROOT / 'paper'
TAX = ROOT / 'data/taxonomy'

# Research-article subset (== evidence empirical/synthesis AND topic != Z).
SUB = ("classified_topic AND topic_category NOT IN ('Z') "
       "AND classified_method AND method_type NOT IN ('M14','M15','M18')")
COMM = "classified_method AND method_type = 'M15'"   # commentary/editorial/perspective

c = duckdb.connect(DB, read_only=True)
RA = c.execute(f'SELECT COUNT(*) FROM works w WHERE {SUB}').fetchone()[0]
COMM_N = c.execute(f'SELECT COUNT(*) FROM works w WHERE {COMM}').fetchone()[0]
# FUNDED = research articles with >=1 grant record (63.9% of research articles acknowledged a funder)
FUNDED = c.execute(
    f'SELECT COUNT(DISTINCT w.openalex_id) FROM works w JOIN grants g '
    f'ON w.openalex_id=g.openalex_id WHERE {SUB}').fetchone()[0]
RAf, COMMf, FUNDf = f'{RA:,}', f'{COMM_N:,}', f'{FUNDED:,}'
# Burden-mapped research articles (topics with a GBD counterpart) — the denominator
# for the burden table's publication share; cited in both the topic and burden captions.
_lmic_c = ("('World Bank Low Income','World Bank Lower Middle Income',"
           "'World Bank Upper Middle Income')")
_gy_c = c.execute(f"SELECT MAX(year) FROM gbd_burden WHERE measure='DALYs' "
                  f"AND metric='Number' AND region IN {_lmic_c}").fetchone()[0]
_mapped_cats = [r[0] for r in c.execute(
    f"SELECT DISTINCT tbm.topic_category FROM topic_burden_map tbm "
    f"JOIN gbd_burden g ON tbm.gbd_cause=g.cause WHERE g.measure='DALYs' "
    f"AND g.metric='Number' AND g.region IN {_lmic_c} AND g.year={_gy_c}").fetchall()]
MAPPED = c.execute(
    f"SELECT COUNT(*) FROM works w WHERE {SUB} AND topic_category IN "
    f"({','.join(chr(39)+x+chr(39) for x in _mapped_cats)})").fetchone()[0]
MAPPEDf = f'{MAPPED:,}'
print(f'RA={RA}  COMMENTARY(M15)={COMM_N}  FUNDED={FUNDED}  BURDEN-MAPPED={MAPPED}')


def esc(s):
    s = str(s)
    for a, b in [('&', r'\&'), ('%', r'\%'), ('_', r'\_'), ('#', r'\#')]:
        s = s.replace(a, b)
    return s


def write(name, lines):
    (OUT / name).write_text('\n'.join(lines) + '\n')
    print('wrote', name)


def cname(cc):
    """ISO alpha-2 -> display country name, matching pycountry official name."""
    if not cc:
        return cc
    try:
        return pycountry.countries.get(alpha_2=cc).name
    except Exception:
        return cc


# ---- taxonomy loaders ------------------------------------------------------
topic_cat = {}      # letter -> category_name
subtopics = defaultdict(list)   # letter -> [(sub_id, sub_name)]
for r in csv.DictReader(open(TAX / 'topic_taxonomy.csv')):
    topic_cat[r['category_letter']] = r['category_name']
    subtopics[r['category_letter']].append((r['subtopic_id'], r['subtopic_name']))
method_name = {r['method_id']: r['method_name']
               for r in csv.DictReader(open(TAX / 'methods_taxonomy.csv'))}
journal_name = {r['issn']: r['journal_name']
                for r in csv.DictReader(open(ROOT / 'data/journal_list.csv'))}

FUNDER_CC = {'Multilateral': 'Intl.'}   # display remap for funder country column


def fcc(x):
    return FUNDER_CC.get(x, x)


# ===========================================================================
# 1. Topic category (research articles, exclude Z), ordered by n desc
# ===========================================================================
rows = c.execute(f"""SELECT topic_category, COUNT(*) n FROM works w WHERE {SUB}
                     GROUP BY 1 ORDER BY n DESC""").fetchall()
o = [r'\begin{table}[ht]', r'\centering',
     f'\\caption{{Research articles by topic category (n = {RAf}). Percentages are of '
     f'all {RAf} research articles; the disease-burden comparison in '
     r'Table~\ref{tab:burden} instead uses as its denominator the '
     f'{MAPPEDf} articles mapped to a burden category, so a topic\'s share is higher '
     r'there. Full category-and-subtopic distribution in Table~\ref{tab:topic-full}.}',
     r'\label{tab:topic-dist}', r'\begin{tabular}{@{}lrr@{}}', r'\toprule',
     r'Topic category & $n$ & \% \\', r'\midrule']
for cat, n in rows:
    o.append(f'{esc(topic_cat.get(cat, cat))} & {n:,} & {100*n/RA:.1f} ' + r'\\')
o += [r'\bottomrule', r'\end{tabular}', r'\end{table}']
write('topic_category_table.tex', o)

# ===========================================================================
# 2. Topic full: categories (by n desc) with subtopics (taxonomy order)
# ===========================================================================
cat_counts = dict(c.execute(f"SELECT topic_category, COUNT(*) FROM works w WHERE {SUB} GROUP BY 1").fetchall())
sub_counts = dict(c.execute(f"SELECT topic_subtopic, COUNT(*) FROM works w WHERE {SUB} GROUP BY 1").fetchall())
# works with a research method but no fitting topic category (Z), outside SUB
Z_RES = c.execute("SELECT COUNT(*) FROM works WHERE classified_topic "
                  "AND topic_category='Z' AND classified_method "
                  "AND method_type NOT IN ('M14','M15','M18')").fetchone()[0]
o = [r'\begin{longtable}{@{}p{0.60\textwidth}rr@{}}',
     f'\\caption{{Full topic taxonomy and distribution of research articles (n = {RAf}); '
     r'subtopics with no articles are shown as 0. A further '
     f'{Z_RES:,} research-method works matched no category (Z, uncategorized) and are '
     r'listed at the foot, outside the research-article subset and its percentages.}'
     r'\label{tab:topic-full}\\',
     r'\toprule Category / subtopic & $n$ & \% \\ \midrule \endfirsthead',
     r'\multicolumn{3}{@{}l}{\emph{Table~\ref{tab:topic-full} continued}}\\ \toprule Category / subtopic & $n$ & \% \\ \midrule \endhead',
     r'\bottomrule \endfoot']
for cat in sorted(topic_cat, key=lambda k: -cat_counts.get(k, 0)):
    if cat == 'Z':
        continue
    n = cat_counts.get(cat, 0)
    o.append(f'\\addlinespace\\textbf{{{esc(topic_cat[cat])}}} & \\textbf{{{n:,}}} & \\textbf{{{100*n/RA:.1f}}} ' + r'\\')
    for sid, sname in subtopics[cat]:
        sn = sub_counts.get(sid, 0)
        o.append(f'\\quad {esc(sname)} & {sn:,} & {100*sn/RA:.1f} ' + r'\\')
o.append(r'\addlinespace\midrule')
o.append(f'\\emph{{Uncategorized / none fits (Z)}} & {Z_RES:,} & --- ' + r'\\')
o.append(r'\end{longtable}')
write('topic_full_table.tex', o)

# ===========================================================================
# 3+4. Study countries (a work counts toward each country it studies)
# ===========================================================================
q = f"""SELECT country, COUNT(*) n FROM (
  SELECT TRIM(UNNEST(string_split(w.study_country,'|'))) AS country
  FROM works w WHERE {SUB}
) WHERE country NOT IN ('GLOBAL','UNKNOWN','') GROUP BY 1 ORDER BY n DESC"""
crows = c.execute(q).fetchall()
# top 20
o = [r'\begin{table}[ht]', r'\centering',
     f'\\caption{{Top 20 study countries by research articles (a work counts toward each '
     f'country it studies; \\% of research articles, n = {RAf}). '
     r'Full list in Table~\ref{tab:countries-full}.}',
     r'\label{tab:countries}', r'\begin{tabular}{@{}lrr@{}}', r'\toprule',
     r'Country & $n$ & \% \\', r'\midrule']
for cc, n in crows[:20]:
    o.append(f'{esc(cname(cc))} & {n:,} & {100*n/RA:.1f} ' + r'\\')
o += [r'\bottomrule', r'\end{tabular}', r'\end{table}']
write('study_country_top_table.tex', o)
# full
o = [r'\begin{longtable}{@{}lrr@{}}',
     f'\\caption{{All {len(crows)} study countries by research articles '
     f'(\\% of research articles, n = {RAf}).}}\\label{{tab:countries-full}}\\\\',
     r'\toprule Country & $n$ & \% \\ \midrule \endfirsthead',
     r'\multicolumn{3}{@{}l}{\emph{Table~\ref{tab:countries-full} continued}}\\ \toprule Country & $n$ & \% \\ \midrule \endhead',
     r'\bottomrule \endfoot']
for cc, n in crows:
    o.append(f'{esc(cname(cc))} & {n:,} & {100*n/RA:.1f} ' + r'\\')
o.append(r'\end{longtable}')
write('study_country_full_table.tex', o)

# ===========================================================================
# 5+6. Funders — counts computed LIVE from grants (distinct research articles per
# canonical funder, % of FUNDED). funder_summary is stale on counts; its
# funder_category / funder_country are stable attributes and are reused here,
# keyed by canonical name.
# ===========================================================================
_fmeta = {f: (cat, cc) for f, cat, cc in c.execute(
    "SELECT funder, funder_category, funder_country FROM funder_summary").fetchall()}
_fj = "REPLACE(g.funder_id,'https://openalex.org/','') = fu.openalex_id"
frows = c.execute(f"""
    WITH ra AS (SELECT openalex_id FROM works w WHERE {SUB})
    SELECT fu.canonical_name, COUNT(DISTINCT ra.openalex_id) n
    FROM ra JOIN grants g ON ra.openalex_id = g.openalex_id
    JOIN funders fu ON {_fj}
    GROUP BY 1 ORDER BY 2 DESC, 1""").fetchall()
NF = len(frows)


def _frow(f, n):
    cat, cc = _fmeta.get(f, ('', ''))
    return f'{esc(f)} & {esc(cat)} & {esc(fcc(cc))} & {n:,} & {100*n/FUNDED:.1f} ' + r'\\'


# top 15
o = [r'\begin{table}[ht]', r'\centering',
     f'\\caption{{Top 15 funders of research articles (\\% of funded articles, n = {FUNDf}). '
     r'Full list in Table~\ref{tab:funders-full}.}',
     r'\label{tab:funders}', r'\begin{tabular}{@{}p{0.46\textwidth}llrr@{}}', r'\toprule',
     r'Funder & Type & Country & $n$ & \% \\', r'\midrule']
for f, n in frows[:15]:
    o.append(_frow(f, n))
o += [r'\bottomrule', r'\end{tabular}', r'\end{table}']
write('funder_top_table.tex', o)
# full top 50
o = [r'\begin{longtable}{@{}p{0.50\textwidth}llrr@{}}',
     f'\\caption{{Top 50 funders of research articles (of {NF}; \\% of funded articles, n = {FUNDf}).}}'
     r'\label{tab:funders-full}\\',
     r'\toprule Funder & Type & Country & $n$ & \% \\ \midrule \endfirsthead',
     r'\multicolumn{5}{@{}l}{\emph{continued}}\\ \toprule Funder & Type & Country & $n$ & \% \\ \midrule \endhead',
     r'\bottomrule \endfoot']
for f, n in frows[:50]:
    o.append(_frow(f, n))
o.append(r'\end{longtable}')
write('funder_full_table.tex', o)

# ===========================================================================
# 7+8. Institutions leading research articles (first/last author, distinct works)
# ===========================================================================
c.execute(f"""CREATE TEMP TABLE lead AS
  SELECT DISTINCT a.institution_id iid, a.institution_name nm, a.institution_country cc, a.openalex_id wid
  FROM authorships a JOIN works w ON a.openalex_id=w.openalex_id
  WHERE {SUB} AND a.position IN ('first','last')
    AND a.institution_id IS NOT NULL AND a.institution_id<>'' AND a.institution_name<>''""")
irows = c.execute("SELECT nm, ANY_VALUE(cc) cc, COUNT(DISTINCT wid) n "
                  "FROM lead GROUP BY iid, nm ORDER BY n DESC").fetchall()
NI = c.execute("SELECT COUNT(DISTINCT iid) FROM lead").fetchone()[0]
# top 20 (with institution country)
o = [r'\begin{table}[ht]', r'\centering',
     f'\\caption{{Top 20 institutions by research articles led (first or last author; '
     f'\\% of research articles, n = {RAf}). Full ranking of the top 50 in Table~\\ref{{tab:institutions-full}}.}}',
     r'\label{tab:institutions}', r'\begin{tabular}{@{}p{0.52\textwidth}p{0.18\textwidth}rr@{}}', r'\toprule',
     r'Institution & Country & $n$ & \% \\', r'\midrule']
for nm, cc, n in irows[:20]:
    o.append(f'{esc(nm)} & {esc(cname(cc))} & {n:,} & {100*n/RA:.1f} ' + r'\\')
o += [r'\bottomrule', r'\end{tabular}', r'\end{table}']
write('institution_top_table.tex', o)
# full top 50 (with institution country)
o = [r'\begin{longtable}{@{}p{0.56\textwidth}p{0.20\textwidth}rr@{}}',
     f'\\caption{{Top 50 institutions by research articles led (of {NI:,}; first or last author; '
     f'\\% of research articles, n = {RAf}).}}\\label{{tab:institutions-full}}\\\\',
     r'\toprule Institution & Country & $n$ & \% \\ \midrule \endfirsthead',
     r'\multicolumn{4}{@{}l}{\emph{continued}}\\ \toprule Institution & Country & $n$ & \% \\ \midrule \endhead',
     r'\bottomrule \endfoot']
for nm, cc, n in irows[:50]:
    o.append(f'{esc(nm)} & {esc(cname(cc))} & {n:,} & {100*n/RA:.1f} ' + r'\\')
o.append(r'\end{longtable}')
write('institution_full_table.tex', o)

# ===========================================================================
# 9. Study design, tiered
# ===========================================================================
TIER_ORDER = ['Experimental and synthesis designs',
              'Observational and analytic designs',
              'Descriptive and exploratory designs']


def tier_of(name):
    n = name.lower()
    if any(k in n for k in ['randomized', 'quasi-experimental', 'systematic review', 'meta-analysis']):
        return TIER_ORDER[0]
    if any(k in n for k in ['cohort', 'longitudinal', 'modeling', 'simulation', 'economic evaluation',
                            'implementation', 'geospatial', 'remote sensing', 'machine learning',
                            'artificial intelligence', 'secondary data']):
        return TIER_ORDER[1]
    return TIER_ORDER[2]


counts = dict(c.execute(f"SELECT method_type, COUNT(*) FROM works w WHERE {SUB} GROUP BY 1").fetchall())
by_tier = defaultdict(list)
for mid, nm in method_name.items():
    if mid in ('M14', 'M15', 'M18'):
        continue
    by_tier[tier_of(nm)].append((nm, counts.get(mid, 0)))
o = [r'\begin{table}[ht]', r'\centering',
     f'\\caption{{Distribution of research articles by study design (n = {RAf}).}}',
     r'\label{tab:design-dist}', r'\begin{tabular}{@{}lrr@{}}', r'\toprule',
     r'Study design & $n$ & \% \\', r'\midrule']
for tier in TIER_ORDER:
    o.append(r'\multicolumn{3}{@{}l}{\textit{' + esc(tier) + r'}}\\')
    for nm, n in sorted(by_tier[tier], key=lambda x: -x[1]):
        o.append(f'\\quad {esc(nm)} & {n:,} & {100*n/RA:.1f} ' + r'\\')
o += [r'\bottomrule', r'\end{tabular}', r'\end{table}']
write('study_design_table.tex', o)

# ---- Study designs by topic (evidence strength; exp/synth share highlighted) ----
tier_idx = {TIER_ORDER[0]: 0, TIER_ORDER[1]: 1, TIER_ORDER[2]: 2}
dt = c.execute(f"SELECT topic_category, method_type, COUNT(*) FROM works w WHERE {SUB} GROUP BY 1,2").fetchall()
dagg = {}
for cat, mt, n in dt:
    if cat == 'Z' or mt in ('M14', 'M15', 'M18') or mt not in method_name:
        continue
    d = dagg.setdefault(cat, [0, 0, 0, 0])
    d[tier_idx[tier_of(method_name[mt])]] += n
    d[3] += n
o = [r'\begin{table}[ht]', r'\centering', r'\small',
     f'\\caption{{Study designs by topic (research articles, n = {RAf}). Percentages are of each '
     r"topic's research articles. Experimental/synthesis designs are randomized, quasi-experimental, "
     r'systematic-review, and meta-analysis studies.}',
     r'\label{tab:design-topic}',
     r'\begin{tabular}{@{}p{0.40\textwidth}rrrr@{}}', r'\toprule',
     r'Topic & $n$ & Exp./syn., \% & Obs., \% & Desc., \% \\', r'\midrule']
for cat in sorted(dagg, key=lambda k: -dagg[k][0] / dagg[k][3]):
    e, ob, de, tot = dagg[cat]
    o.append(f'{esc(topic_cat.get(cat, cat))} & {tot:,} & {100*e/tot:.1f} & '
             f'{100*ob/tot:.1f} & {100*de/tot:.1f} ' + r'\\')
o += [r'\bottomrule', r'\end{tabular}', r'\end{table}']
write('design_topic_table.tex', o)

# ===========================================================================
# 10. Commentary by journal (% of each journal's total output)
# ===========================================================================
jtot = dict(c.execute("SELECT journal_issn, COUNT(*) FROM works GROUP BY 1").fetchall())
jcomm = c.execute(f"SELECT journal_issn, COUNT(*) n FROM works w WHERE {COMM} GROUP BY 1 ORDER BY n DESC").fetchall()
o = [r'\begin{table}[ht]', r'\centering',
     f'\\caption{{Commentary, editorials, and perspectives by journal (n = {COMMf}; '
     r"\% is of each journal's total output).}",
     r'\label{tab:commentary-journal}', r'\begin{tabular}{@{}lrr@{}}', r'\toprule',
     r'Journal & Commentary, $n$ & \% of journal \\', r'\midrule']
for issn, n in jcomm:
    pct = 100 * n / jtot.get(issn, n)
    o.append(f'\\emph{{{esc(journal_name.get(issn, issn))}}} & {n:,} & {pct:.0f} ' + r'\\')
o += [r'\bottomrule', r'\end{tabular}', r'\end{table}']
write('commentary_journal_table.tex', o)

# ===========================================================================
# 11. Commentary by institution (first/last author, with country), top 15
# ===========================================================================
c.execute(f"""CREATE TEMP TABLE clead AS
  SELECT DISTINCT a.institution_id iid, a.institution_name nm, a.institution_country cc, a.openalex_id wid
  FROM authorships a JOIN works w ON a.openalex_id=w.openalex_id
  WHERE {COMM} AND a.position IN ('first','last')
    AND a.institution_id IS NOT NULL AND a.institution_id<>'' AND a.institution_name<>''""")
crows2 = c.execute("""SELECT nm, ANY_VALUE(cc) cc, COUNT(DISTINCT wid) n
                      FROM clead GROUP BY iid, nm ORDER BY n DESC LIMIT 15""").fetchall()
o = [r'\begin{table}[ht]', r'\centering',
     f'\\caption{{Institutions publishing the most commentary, editorials, and perspectives '
     f'(first or last author; \\% of commentary works, n = {COMMf}).}}',
     r'\label{tab:commentary-inst}', r'\begin{tabular}{@{}p{0.56\textwidth}p{0.20\textwidth}rr@{}}',
     r'\toprule', r'Institution & Country & $n$ & \% \\', r'\midrule']
for nm, cc, n in crows2:
    o.append(f'{esc(nm)} & {esc(cname(cc))} & {n:,} & {100*n/COMM_N:.1f} ' + r'\\')
o += [r'\bottomrule', r'\end{tabular}', r'\end{table}']
write('commentary_inst_table.tex', o)

# ===========================================================================
# 12. Research attention vs disease burden (pub share vs LMIC DALY burden share)
# ===========================================================================
BURDEN_LABEL = {  # short display names used in tab:burden
    'D': 'HIV/AIDS/Tuberculosis/Malaria', 'F': 'Non-Communicable Diseases',
    'C': 'Infectious Disease (other)', 'A': 'Maternal \\& Reproductive Health',
    'G': 'Mental Health \\& Substance Use', 'E': 'Neglected Tropical Diseases',
    'M': 'Surgical, Trauma \\& Emergency Care', 'H': 'Nutrition \\& Micronutrient Deficiency'}
_lmic = ("('World Bank Low Income','World Bank Lower Middle Income',"
         "'World Bank Upper Middle Income')")
_gy = c.execute(f"SELECT MAX(year) FROM gbd_burden WHERE measure='DALYs' "
                f"AND metric='Number' AND region IN {_lmic}").fetchone()[0]
_bur = dict(c.execute(
    f"SELECT tbm.topic_category, SUM(g.val) FROM topic_burden_map tbm "
    f"JOIN gbd_burden g ON tbm.gbd_cause=g.cause WHERE g.measure='DALYs' "
    f"AND g.metric='Number' AND g.region IN {_lmic} AND g.year={_gy} GROUP BY 1").fetchall())
_btot = sum(_bur.values())
_incat = ','.join(f"'{x}'" for x in _bur)
_pub = dict(c.execute(f"SELECT topic_category, COUNT(*) FROM works w WHERE {SUB} "
                      f"AND topic_category IN ({_incat}) GROUP BY 1").fetchall())
_ptot = sum(_pub.values())
o = [r'\begin{table}[ht]', r'\centering',
     f'\\caption{{Research attention and disease burden, by topic. Publication share '
     f'is the percentage among the {MAPPEDf} research articles mapped to a '
     f'disease-burden category, not of all {RAf} research articles; the eight topics '
     r'shown are those with a Global Burden of Disease counterpart '
     r"(Table~\ref{tab:crosswalk}), so a topic's share here is higher than its share "
     r'of the full corpus in Table~\ref{tab:topic-dist}. LMIC burden share is each '
     r"topic's share of low- and middle-income disability-adjusted life years (GBD 2023).}",
     r'\label{tab:burden}', r'\begin{tabular}{lrr}', r'\toprule',
     r'Topic & Publication share, \% & LMIC burden share, \% \\', r'\midrule']
for cat in sorted(_bur, key=lambda k: -_pub.get(k, 0) / _ptot):
    o.append(f'{BURDEN_LABEL.get(cat, cat)} & {100*_pub.get(cat,0)/_ptot:.1f} & '
             f'{100*_bur[cat]/_btot:.1f} ' + r'\\')
o += [r'\bottomrule', r'\end{tabular}', r'\end{table}']
write('burden_table.tex', o)

# ===========================================================================
# 13. Corpus by journal (full corpus; coverage-from = first year with >=10 works)
# ===========================================================================
TOT = c.execute('SELECT COUNT(*) FROM works').fetchone()[0]
usable_case = (
    "CASE WHEN abstract IS NULL OR TRIM(abstract)='' THEN 0 "
    "WHEN LENGTH(TRIM(abstract))<=50 THEN 0 "
    "WHEN abstract LIKE 'Annals of Global Health is a peer-reviewed%' "
    "OR abstract LIKE 'Welcome to Annals of Global Health%' THEN 0 ELSE 1 END")
jrows = c.execute(
    f"SELECT journal_issn, COUNT(*) n, SUM({usable_case}) u FROM works GROUP BY 1 ORDER BY n DESC"
).fetchall()
o = [r'\begin{table}[ht]', r'\centering', r'\caption{Corpus by journal.}',
     r'\label{tab:corpus}', r'\resizebox{\textwidth}{!}{%', r'\begin{tabular}{@{}lrrrr@{}}', r'\toprule',
     r'Journal & Coverage & Works, $n$ & \% of corpus & Usable, \% \\',
     r'\midrule']
tot_u = 0
for issn, n, u in jrows:
    cov = c.execute(
        'SELECT MIN(publication_year) FROM (SELECT publication_year FROM works '
        'WHERE journal_issn=? GROUP BY 1 HAVING COUNT(*)>=10)', [issn]).fetchone()[0]
    cov = max(cov or 2010, 2010)
    tot_u += u
    o.append(f'\\emph{{{esc(journal_name.get(issn, issn))}}} & {cov} & {n:,} & '
             f'{100*n/TOT:.1f} & {100*u/n:.1f} ' + r'\\')
o += [r'\midrule',
      f'\\textbf{{Total}} & & \\textbf{{{TOT:,}}} & \\textbf{{100}} & '
      f'\\textbf{{{100*tot_u/TOT:.1f}}} ' + r'\\',
      r'\bottomrule', r'\end{tabular}}', r'\end{table}']
write('corpus_table.tex', o)

print('\nAll paper tables regenerated.')
