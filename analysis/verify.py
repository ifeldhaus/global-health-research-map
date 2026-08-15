"""
Independently recompute every headline value and figure series in the paper directly
from the DuckDB database, and check each against the number stated in the manuscript.

This is a self-contained audit: it does not read the paper's tables or figures, it
re-derives each quantity from the raw data using the definitions in the Methods, then
compares. A reviewer can run it in one command and see, line by line, whether the
paper's numbers reproduce.

    uv run python analysis/verify.py

Reads data/global_health.duckdb (the full database, rebuildable from OpenAlex via the
pipeline/ scripts). Uses the World Bank income map in dashboard/constants.py.
"""
from pathlib import Path
import sys

import duckdb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "dashboard"))
from constants import HIGH_INCOME_ISO2  # noqa: E402

DB = REPO / "data" / "global_health.duckdb"
c = duckdb.connect(str(DB), read_only=True)
HI = tuple(HIGH_INCOME_ISO2)

# Research-article subset (Methods): classified to a topic other than uncategorized,
# and to an empirical or synthesis design (commentary, narrative review, and
# undeterminable design excluded).
SUB = ("classified_topic AND topic_category NOT IN ('Z') "
       "AND classified_method AND method_type NOT IN ('M14','M15','M18')")
COMM = "classified_method AND method_type = 'M15'"

PASS, FAIL = 0, 0


def check(label, computed, expected, tol=0.05):
    global PASS, FAIL
    ok = abs(computed - expected) <= tol
    PASS, FAIL = PASS + ok, FAIL + (not ok)
    print(f"  [{'PASS' if ok else 'FAIL'}] {label:52} computed={computed:<10} paper={expected}")


def scalar(q, *p):
    return c.execute(q, list(p)).fetchone()[0]


print("=" * 78)
print("CORPUS AND SUBSET")
print("=" * 78)
TOT = scalar("SELECT COUNT(*) FROM works")
RA = scalar(f"SELECT COUNT(*) FROM works w WHERE {SUB}")
COMM_N = scalar(f"SELECT COUNT(*) FROM works w WHERE {COMM}")
usable = scalar("SELECT COUNT(*) FROM works WHERE NOT(abstract IS NULL OR TRIM(abstract)='' "
                "OR LENGTH(TRIM(abstract))<=50 OR abstract LIKE 'Annals of Global Health is a peer-reviewed%' "
                "OR abstract LIKE 'Welcome to Annals of Global Health%')")
check("total works", TOT, 39905, 0)
check("research articles", RA, 23468, 0)
check("commentary (M15)", COMM_N, 8871, 0)
check("usable-abstract works", usable, 35136, 0)
check("usable-abstract %", round(100 * usable / TOT, 1), 88.0)
check("journals", scalar("SELECT COUNT(DISTINCT journal_issn) FROM works"), 16, 0)

print("=" * 78)
print("FUNDING")
print("=" * 78)
FUNDED = scalar(f"SELECT COUNT(DISTINCT w.openalex_id) FROM works w JOIN grants g "
                f"ON w.openalex_id=g.openalex_id WHERE {SUB}")
check("funded research articles", FUNDED, 14999, 0)
check("funded % of research articles", round(100 * FUNDED / RA, 1), 63.9)
J = "REPLACE(g.funder_id,'https://openalex.org/','') = fu.openalex_id"
rank = c.execute(f"""WITH ra AS (SELECT openalex_id FROM works w WHERE {SUB})
    SELECT fu.canonical_name, COUNT(DISTINCT ra.openalex_id) n FROM ra
    JOIN grants g ON ra.openalex_id=g.openalex_id JOIN funders fu ON {J}
    GROUP BY 1 ORDER BY 2 DESC, 1""").fetchall()
top5 = scalar(f"""WITH ra AS (SELECT openalex_id FROM works w WHERE {SUB})
    SELECT COUNT(DISTINCT ra.openalex_id) FROM ra JOIN grants g ON ra.openalex_id=g.openalex_id
    JOIN funders fu ON {J} WHERE fu.canonical_name IN ({','.join(['?']*5)})""", *[x[0] for x in rank[:5]])
check("Gates share of funded", round(100 * dict(rank)['Bill & Melinda Gates Foundation'] / FUNDED, 1), 13.5)
check("top-5 funders (union) share", round(100 * top5 / FUNDED, 1), 42.0)
# funder gini (articles per funder)
vals = sorted(x[1] for x in rank)
n = len(vals)
gini = (2 * sum((i + 1) * v for i, v in enumerate(vals)) / (n * sum(vals))) - (n + 1) / n
check("funding Gini", round(gini, 2), 0.79)
# funder-type shares (of FUNDED)
for typ, exp in [('Government', 53), ('Philanthropic', 29), ('Multilateral', 20)]:
    nt = scalar(f"""WITH ra AS (SELECT openalex_id FROM works w WHERE {SUB})
        SELECT COUNT(DISTINCT ra.openalex_id) FROM ra JOIN grants g ON ra.openalex_id=g.openalex_id
        JOIN funders fu ON {J} JOIN funder_summary fs ON fs.funder=fu.canonical_name
        WHERE fs.funder_category=?""", typ)
    check(f"funder type: {typ}", round(100 * nt / FUNDED), exp, 0.5)

print("=" * 78)
print("GEOGRAPHIC LEADERSHIP")
print("=" * 78)
sc_all = scalar(f"SELECT COUNT(*) FROM works w WHERE {SUB} AND study_country IS NOT NULL "
                f"AND study_country NOT IN ('GLOBAL','UNKNOWN') AND study_country NOT LIKE '%|%'")
check("single-country research articles", sc_all, 16042, 0)
lead = c.execute(f"""WITH sc AS (SELECT w.openalex_id id, w.study_country sctry FROM works w
      WHERE {SUB} AND study_country IS NOT NULL AND study_country NOT IN ('GLOBAL','UNKNOWN')
      AND study_country NOT LIKE '%|%'),
    lab AS (SELECT sc.id,
      MAX(CASE WHEN a.institution_country=sc.sctry THEN 1 ELSE 0 END) any_local,
      MAX(CASE WHEN a.position='last' AND a.institution_country=sc.sctry THEN 1 ELSE 0 END) last_local
      FROM sc JOIN authorships a ON sc.id=a.openalex_id
      WHERE a.institution_country IS NOT NULL AND a.institution_country<>'' GROUP BY sc.id)
    SELECT COUNT(*) n,
      100.0*SUM(CASE WHEN last_local=1 THEN 1 ELSE 0 END)/COUNT(*),
      100.0*SUM(CASE WHEN any_local=1 AND last_local=0 THEN 1 ELSE 0 END)/COUNT(*),
      100.0*SUM(CASE WHEN any_local=0 THEN 1 ELSE 0 END)/COUNT(*) FROM lab""").fetchone()
check("single-country w/ resolvable affiliation", lead[0], 15826, 0)
check("local-led %", round(lead[1], 1), 41.0)
check("collaborative %", round(lead[2], 1), 41.8)
check("externally-led %", round(lead[3], 1), 17.2)


def ext_band(lo, hi):
    r = c.execute(f"""WITH sc AS (SELECT w.openalex_id id, w.study_country sctry, w.publication_year yr FROM works w
        WHERE {SUB} AND study_country IS NOT NULL AND study_country NOT IN ('GLOBAL','UNKNOWN')
        AND study_country NOT LIKE '%|%' AND w.publication_year BETWEEN {lo} AND {hi}),
      lab AS (SELECT sc.id, MAX(CASE WHEN a.institution_country=sc.sctry THEN 1 ELSE 0 END) hl
        FROM sc JOIN authorships a ON sc.id=a.openalex_id
        WHERE a.institution_country IS NOT NULL AND a.institution_country<>'' GROUP BY sc.id)
      SELECT 100.0*SUM(CASE WHEN hl=0 THEN 1 ELSE 0 END)/COUNT(*) FROM lab""").fetchone()[0]
    return round(r, 1)


check("externally-led 2010-2013", ext_band(2010, 2013), 21.7)
check("externally-led 2022-2025", ext_band(2022, 2025), 12.7)

print("=" * 78)
print("ATTENTION VS BURDEN")
print("=" * 78)
lmic = ("('World Bank Low Income','World Bank Lower Middle Income','World Bank Upper Middle Income')")
gy = scalar(f"SELECT MAX(year) FROM gbd_burden WHERE measure='DALYs' AND metric='Number' AND region IN {lmic}")
bur = dict(c.execute(f"SELECT tbm.topic_category, SUM(g.val) FROM topic_burden_map tbm "
                     f"JOIN gbd_burden g ON tbm.gbd_cause=g.cause WHERE g.measure='DALYs' "
                     f"AND g.metric='Number' AND g.region IN {lmic} AND g.year={gy} GROUP BY 1").fetchall())
btot = sum(bur.values())
incat = ','.join(f"'{k}'" for k in bur)
pub = dict(c.execute(f"SELECT topic_category, COUNT(*) FROM works w WHERE {SUB} "
                     f"AND topic_category IN ({incat}) GROUP BY 1").fetchall())
ptot = sum(pub.values())
check("burden-mapped research articles", ptot, 12927, 0)
check("NCD publication share", round(100 * pub['F'] / ptot, 1), 20.7)
check("NCD burden share", round(100 * bur['F'] / btot, 1), 53.9)
check("HIV/TB/malaria publication share", round(100 * pub['D'] / ptot, 1), 25.2)
check("HIV/TB/malaria burden share", round(100 * bur['D'] / btot, 1), 9.5)

print("=" * 78)
print("STUDY DESIGNS")
print("=" * 78)
import csv  # noqa: E402
mn = {r['method_id']: r['method_name'] for r in csv.DictReader(open(REPO / 'data/taxonomy/methods_taxonomy.csv'))}


def tier(name):
    n = name.lower()
    if any(k in n for k in ['randomized', 'quasi-experimental', 'systematic review', 'meta-analysis']):
        return 'exp'
    if any(k in n for k in ['cohort', 'longitudinal', 'modeling', 'simulation', 'economic evaluation',
                            'implementation', 'geospatial', 'remote sensing', 'machine learning',
                            'artificial intelligence', 'secondary data']):
        return 'obs'
    return 'desc'


cm = dict(c.execute(f"SELECT method_type, COUNT(*) FROM works w WHERE {SUB} GROUP BY 1").fetchall())
agg = {'exp': 0, 'obs': 0, 'desc': 0}
for mid, name in mn.items():
    if mid in ('M14', 'M15', 'M18'):
        continue
    agg[tier(name)] += cm.get(mid, 0)
check("descriptive/exploratory %", round(100 * agg['desc'] / RA, 1), 59.2)
check("observational/analytic %", round(100 * agg['obs'] / RA, 1), 25.3)
check("experimental/synthesis %", round(100 * agg['exp'] / RA, 1), 15.6)

print("=" * 78)
print("PRODUCING INSTITUTIONS")
print("=" * 78)
c.execute(f"""CREATE TEMP TABLE lead AS SELECT DISTINCT a.institution_id iid, a.openalex_id wid
    FROM authorships a JOIN works w ON a.openalex_id=w.openalex_id
    WHERE {SUB} AND a.position IN ('first','last') AND a.institution_id IS NOT NULL
    AND a.institution_id<>'' AND a.institution_name<>''""")
counts = sorted((x[0] for x in c.execute("SELECT COUNT(DISTINCT wid) FROM lead GROUP BY iid").fetchall()))
ni = len(counts)
gi = (2 * sum((i + 1) * v for i, v in enumerate(counts)) / (ni * sum(counts))) - (ni + 1) / ni
check("distinct lead institutions", ni, 5219, 0)
check("institution Gini", round(gi, 2), 0.73)
check("top-10% institution share", round(100 * sum(counts[-max(1, ni // 10):]) / sum(counts), 1), 67.7)
for yr, exp in [(2010, 30.8), (2025, 35.5)]:
    s = scalar(f"""WITH pc AS (SELECT DISTINCT a.openalex_id wid, a.institution_country cc
        FROM authorships a JOIN works w ON a.openalex_id=w.openalex_id
        WHERE {SUB} AND w.publication_year={yr} AND a.position IN ('first','last')
        AND a.institution_country IS NOT NULL AND a.institution_country<>''),
      pg AS (SELECT wid, MAX(CASE WHEN cc IN {HI} THEN 0 ELSE 1 END) hs,
        MAX(CASE WHEN cc IN {HI} THEN 1 ELSE 0 END) hn FROM pc GROUP BY 1)
      SELECT ROUND(100.0*SUM(CASE WHEN hs=1 AND hn=0 THEN 1 ELSE 0 END)/COUNT(*),1) FROM pg""")
    check(f"LMIC-led share {yr}", s, exp)

print("=" * 78)
print("AUTHOR GENDER (first vs last)")
print("=" * 78)
for col, exp in [('gender_first', 45.0), ('gender_last', 34.2)]:
    r = dict(c.execute(f"SELECT split_part({col},'|',1) g, COUNT(*) FROM works "
                       f"WHERE {col} IS NOT NULL GROUP BY 1").fetchall())
    check(f"{col} female %", round(100 * r.get('female', 0) / sum(r.values()), 1), exp)

print("=" * 78)
print(f"RESULT: {PASS} passed, {FAIL} failed")
print("Validation kappa (N=404, topic 0.67 / methods 0.65 / country 0.93) is checked "
      "separately by: uv run python validation/compute_kappa.py")
print("=" * 78)
sys.exit(1 if FAIL else 0)
