"""Cross-cutting analyses for the PLOS GPH rework: linking the five concentration
lenses. A1 = funding x geographic leadership. A2 = funding x attention-vs-burden.
Reuses the paper's canonical definitions (SUB research-article subset, FUNDED
denominator, leadership via all-author-position affiliation)."""
import duckdb
from pathlib import Path

DB = Path(__file__).resolve().parents[1] / "data" / "global_health.duckdb"
con = duckdb.connect(str(DB), read_only=True)

SUB = ("classified_topic AND topic_category NOT IN ('Z') AND classified_method "
       "AND method_type NOT IN ('M14','M15','M18')")
FJ = "REPLACE(g.funder_id,'https://openalex.org/','') = fu.openalex_id"

def rule(t): print("\n" + "="*70 + f"\n{t}\n" + "="*70)

# ---- A1: funding x geographic leadership -----------------------------------
rule("A1  FUNDING x GEOGRAPHIC LEADERSHIP (single-country RA, resolvable affil)")
# leadership label per single-country paper (any_local / last_local over ALL authorships)
lead = con.execute(f"""
  WITH sc AS (SELECT w.openalex_id id, w.study_country sctry FROM works w
      WHERE {SUB} AND study_country IS NOT NULL AND study_country NOT IN ('GLOBAL','UNKNOWN')
      AND study_country NOT LIKE '%|%'),
    lab AS (SELECT sc.id,
      MAX(CASE WHEN a.institution_country=sc.sctry THEN 1 ELSE 0 END) any_local,
      MAX(CASE WHEN a.position='last' AND a.institution_country=sc.sctry THEN 1 ELSE 0 END) last_local
      FROM sc JOIN authorships a ON sc.id=a.openalex_id
      WHERE a.institution_country IS NOT NULL AND a.institution_country<>'' GROUP BY sc.id)
  SELECT id,
    CASE WHEN last_local=1 THEN 'local-led'
         WHEN any_local=1 THEN 'collaborative'
         ELSE 'externally-led' END AS lead
  FROM lab
""").df()
print(f"leadership set N = {len(lead):,}")

# funded flag per paper
funded = con.execute("""SELECT DISTINCT openalex_id id FROM grants""").df()
lead['funded'] = lead['id'].isin(set(funded['id']))
print("\nfunded rate by leadership category:")
g = lead.groupby('lead')['funded'].agg(['size','sum'])
for k,row in g.iterrows():
    print(f"  {k:15} n={int(row['size']):5,}  funded={int(row['sum']):5,}  ({100*row['sum']/row['size']:.1f}%)")
overall = lead['funded'].mean()*100
print(f"  {'ALL':15} n={len(lead):5,}  funded rate {overall:.1f}%")

# funder-CATEGORY composition by leadership (share of each leadership group's funded papers naming a category)
rule("A1b  Funder-category share within each leadership group (of that group's papers)")
cat = con.execute(f"""
  WITH sc AS (SELECT w.openalex_id id, w.study_country sctry FROM works w
      WHERE {SUB} AND study_country IS NOT NULL AND study_country NOT IN ('GLOBAL','UNKNOWN')
      AND study_country NOT LIKE '%|%'),
    lab AS (SELECT sc.id,
      MAX(CASE WHEN a.institution_country=sc.sctry THEN 1 ELSE 0 END) any_local,
      MAX(CASE WHEN a.position='last' AND a.institution_country=sc.sctry THEN 1 ELSE 0 END) last_local
      FROM sc JOIN authorships a ON sc.id=a.openalex_id
      WHERE a.institution_country IS NOT NULL AND a.institution_country<>'' GROUP BY sc.id),
    lead AS (SELECT id, CASE WHEN last_local=1 THEN 'local-led' WHEN any_local=1 THEN 'collaborative' ELSE 'externally-led' END lead FROM lab),
    fund AS (SELECT DISTINCT ra.id, fu.funder_category cat
             FROM lead ra JOIN grants g ON ra.id=g.openalex_id JOIN funders fu ON {FJ})
  SELECT l.lead, f.cat, COUNT(DISTINCT f.id) n
  FROM lead l JOIN fund f ON l.id=f.id
  GROUP BY 1,2 ORDER BY 1,3 DESC
""").df()
tot = lead.groupby('lead').size()
for ld in ['local-led','collaborative','externally-led']:
    sub = cat[cat['lead']==ld]
    print(f"\n  {ld} (n={tot[ld]:,}):")
    for _,r in sub.iterrows():
        print(f"     {r['cat']:14} {int(r['n']):5,}  ({100*r['n']/tot[ld]:.1f}% of group)")

# externally-led share among papers funded by each top funder vs baseline
rule("A1c  Externally-led share among single-country papers funded by each top funder")
print(f"  baseline externally-led (all single-country w/ affil): "
      f"{100*(lead['lead']=='externally-led').mean():.1f}%")
topf = con.execute(f"""
  WITH sc AS (SELECT w.openalex_id id, w.study_country sctry FROM works w
      WHERE {SUB} AND study_country IS NOT NULL AND study_country NOT IN ('GLOBAL','UNKNOWN')
      AND study_country NOT LIKE '%|%'),
    lab AS (SELECT sc.id,
      MAX(CASE WHEN a.institution_country=sc.sctry THEN 1 ELSE 0 END) any_local
      FROM sc JOIN authorships a ON sc.id=a.openalex_id
      WHERE a.institution_country IS NOT NULL AND a.institution_country<>'' GROUP BY sc.id),
    fund AS (SELECT DISTINCT ra.id, fu.canonical_name f, fu.funder_category cat
             FROM (SELECT id, any_local FROM lab) ra JOIN grants g ON ra.id=g.openalex_id JOIN funders fu ON {FJ})
  SELECT f.f, f.cat, COUNT(DISTINCT f.id) n,
         100.0*SUM(CASE WHEN l.any_local=0 THEN 1 ELSE 0 END)/COUNT(DISTINCT f.id) ext_pct
  FROM fund f JOIN lab l ON f.id=l.id
  GROUP BY 1,2 HAVING COUNT(DISTINCT f.id)>=100 ORDER BY n DESC LIMIT 12
""").df()
for _,r in topf.iterrows():
    print(f"  {r['f'][:38]:38} {r['cat'][:12]:12} n={int(r['n']):4,}  externally-led {r['ext_pct']:.1f}%")

# ---- A2: funding x attention-vs-burden -------------------------------------
rule("A2  FUNDING x ATTENTION-vs-BURDEN (funder-type mix by topic, with burden ratio)")
TAX = {'A':'Maternal/reproductive','B':'Child','C':'Other infectious','D':'HIV/TB/malaria',
 'E':'NTD','F':'Noncommunicable','G':'Mental health','H':'Nutrition','I':'Health systems',
 'J':'Health economics','K':'Climate','L':'Conflict','M':'Surgical/emergency',
 'N':'Epidemiology','O':'Research methods'}
# pub share per topic (of RA) + funder-type mix among that topic's funded RA
rows = con.execute(f"""
  WITH ra AS (SELECT openalex_id id, topic_category tc FROM works w WHERE {SUB}),
    tot AS (SELECT COUNT(*) n FROM ra),
    fund AS (SELECT DISTINCT ra.id, ra.tc, fu.funder_category cat
             FROM ra JOIN grants g ON ra.id=g.openalex_id JOIN funders fu ON {FJ})
  SELECT ra.tc,
    COUNT(DISTINCT ra.id) n_ra,
    100.0*COUNT(DISTINCT ra.id)/(SELECT n FROM tot) pub_share,
    COUNT(DISTINCT CASE WHEN f.cat='Government' THEN f.id END) gov,
    COUNT(DISTINCT CASE WHEN f.cat='Philanthropic' THEN f.id END) phil,
    COUNT(DISTINCT CASE WHEN f.cat='Multilateral' THEN f.id END) multi,
    COUNT(DISTINCT f.id) funded
  FROM ra LEFT JOIN fund f ON ra.id=f.id
  GROUP BY ra.tc ORDER BY pub_share DESC
""").df()
burden=None
print(f"{'topic':22} {'pubR%':>6} {'gov%':>6} {'phil%':>6} {'multi%':>6}   (of topic funded RA)")
for _,r in rows.iterrows():
    fn=r['funded'] or 1
    print(f"{TAX.get(r['tc'],r['tc'])[:22]:22} {r['pub_share']:6.1f} "
          f"{100*r['gov']/fn:6.1f} {100*r['phil']/fn:6.1f} {100*r['multi']/fn:6.1f}")
# top individual funder per key topic (over- vs under-attended)
rule("A2b  Top 3 funders for the over- vs under-attended topics")
for tc,label in [('D','HIV/TB/malaria (over-attended)'),('F','Noncommunicable (under-attended)'),
                 ('A','Maternal/reproductive'),('G','Mental health')]:
    tf = con.execute(f"""
      WITH ra AS (SELECT openalex_id id FROM works w WHERE {SUB} AND topic_category='{tc}')
      SELECT fu.canonical_name f, fu.funder_category cat, COUNT(DISTINCT ra.id) n
      FROM ra JOIN grants g ON ra.id=g.openalex_id JOIN funders fu ON {FJ}
      GROUP BY 1,2 ORDER BY n DESC LIMIT 3
    """).df()
    print(f"  {label}:  " + " | ".join(f"{r['f'][:26]} ({r['cat'][:4]}, {int(r['n'])})" for _,r in tf.iterrows()))
