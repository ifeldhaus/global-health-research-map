"""Audit harness for the PLOS cross-cutting + small-community numbers. Independently
recomputes every value that appears in the manuscript and asserts it against the
exact figure in the prose. Anchors reconcile to verify.py's certified counts.
Exit 0 and 'ALL PASS' only if every number matches. Run: python analysis/verify_crosscutting.py"""
import duckdb, numpy as np
from pathlib import Path
from scipy.stats import chi2_contingency
from statsmodels.stats.contingency_tables import Table2x2, StratifiedTable
from statsmodels.stats.proportion import proportion_confint
import statsmodels.formula.api as smf
import pandas as pd

con=duckdb.connect(str(Path(__file__).resolve().parents[1]/"data"/"global_health.duckdb"),read_only=True)
SUB=("classified_topic AND topic_category NOT IN ('Z') AND classified_method "
     "AND method_type NOT IN ('M14','M15','M18')")
FJ="REPLACE(g.funder_id,'https://openalex.org/','') = fu.openalex_id"
FAILS=[]
def check(name, got, exp, tol=0.0):
    ok = (got==exp) if tol==0 else (abs(got-exp)<=tol)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name:52} got={got}  exp={exp}")
    if not ok: FAILS.append(name)

print("ANCHORS (must match verify.py-certified figures)")
RA=con.execute(f"SELECT COUNT(*) FROM works w WHERE {SUB}").fetchone()[0]
check("research articles (SUB)", RA, 23468)
base=con.execute(f"""
  WITH sc AS (SELECT w.openalex_id id, w.study_country sctry, w.topic_category tc FROM works w
      WHERE {SUB} AND study_country IS NOT NULL AND study_country NOT IN ('GLOBAL','UNKNOWN') AND study_country NOT LIKE '%|%'),
    lab AS (SELECT sc.id, sc.tc, MAX(CASE WHEN a.institution_country=sc.sctry THEN 1 ELSE 0 END) any_local
      FROM sc JOIN authorships a ON sc.id=a.openalex_id
      WHERE a.institution_country IS NOT NULL AND a.institution_country<>'' GROUP BY sc.id, sc.tc)
  SELECT id, tc, CASE WHEN any_local=0 THEN 1 ELSE 0 END ext FROM lab""").df()
check("single-country resolvable (n)", len(base), 15826)
check("externally-led baseline %", round(100*base.ext.mean(),1), 17.2, 0.05)

fund=con.execute(f"SELECT DISTINCT g.openalex_id id, fu.canonical_name f FROM grants g JOIN funders fu ON {FJ}").df()

print("\nA1 DESCRIPTIVES: externally-led % by funder (+ Wilson 95% CI)")
EXP={'National Institutes of Health':(1005,11.1,9.3,13.2),'Wellcome Trust':(928,10.3,8.5,12.5),
 'MRC UK':(897,11.7,9.8,14.0),'Fogarty International Center':(564,9.4,7.3,12.1),
 'USAID':(635,24.9,21.7,28.4),'Bill & Melinda Gates Foundation':(1137,17.1,15.0,19.4)}
for fn,(en,ep,elo,ehi) in EXP.items():
    ids=set(fund[fund.f==fn].id); sub=base[base.id.isin(ids)]
    n=len(sub); k=int(sub.ext.sum()); lo,hi=proportion_confint(k,n,method='wilson')
    check(f"{fn[:26]} n", n, en)
    check(f"{fn[:26]} ext%", round(100*k/n,1), ep, 0.05)
    check(f"{fn[:26]} CI", (round(100*lo,1),round(100*hi,1)), (elo,ehi), 0)

print("\nA1 ROBUSTNESS (supplement): topic-adjusted OR = logistic OR (two methods agree)")
for fn,exp_or in [('National Institutes of Health',0.67),('USAID',1.54),('Bill & Melinda Gates Foundation',0.95)]:
    base['exp']=base.id.isin(set(fund[fund.f==fn].id)).astype(int)
    strata=[]
    for tc,grp in base.groupby('tc'):
        a=int(((grp.exp==1)&(grp.ext==1)).sum()); b=int(((grp.exp==1)&(grp.ext==0)).sum())
        c=int(((grp.exp==0)&(grp.ext==1)).sum()); d=int(((grp.exp==0)&(grp.ext==0)).sum())
        if (a+b)>0 and (c+d)>0 and (a+c)>0 and (b+d)>0: strata.append([[a,b],[c,d]])
    ormh=StratifiedTable(np.array(strata).transpose(1,2,0).astype(float)).oddsratio_pooled
    orlog=np.exp(smf.logit('ext ~ exp + C(tc)',data=base).fit(disp=0).params['exp'])
    check(f"{fn[:22]} CMH adjOR", round(ormh,2), exp_or, 0.01)
    check(f"{fn[:22]} CMH vs logit agree", round(abs(ormh-orlog),2), 0.0, 0.03)

print("\nCONFOUNDING CRITERIA")
ct=pd.crosstab(base.tc, base.ext); chi2,p,dof,_=chi2_contingency(ct.values)
check("topic<->external chi2", round(chi2), 351, 2)
check("topic<->external CramerV", round(np.sqrt(chi2/(len(base)*(min(ct.shape)-1))),3), 0.149, 0.003)
byt=base.groupby('tc').ext.mean()
check("external range low %", round(100*byt.min(),1), 9.4, 0.1)
check("external range high %", round(100*byt.max(),1), 35.4, 0.1)

print("\nA2: funder-type shares + residuals at the burden gaps")
raf=con.execute(f"""
  WITH ra AS (SELECT openalex_id id, topic_category tc FROM works w WHERE {SUB}),
    fc AS (SELECT DISTINCT ra.id, ra.tc,
      MAX(CASE WHEN fu.funder_category='Government' THEN 1 ELSE 0 END) gov,
      MAX(CASE WHEN fu.funder_category='Multilateral' THEN 1 ELSE 0 END) multi
      FROM ra JOIN grants g ON ra.id=g.openalex_id JOIN funders fu ON {FJ} GROUP BY 1,2)
  SELECT * FROM fc""").df()
def share(tc,col):
    s=raf[raf.tc==tc]; return round(100*s[col].sum()/len(s),1)
check("HIV/TB/mal gov %", share('D','gov'), 79.1, 0.05)
check("NCD multi %", share('F','multi'), 18.3, 0.05)
check("Mental multi %", share('G','multi'), 11.5, 0.05)
ctm=pd.crosstab(raf.tc, raf.multi); chi2,p,dof,exp=chi2_contingency(ctm.values)
resid=(ctm.values-exp)/np.sqrt(exp); col1=list(ctm.columns).index(1); tcs=list(ctm.index)
check("Mental multi residual", round(resid[tcs.index('G'),col1],1), -5.3, 0.15)
check("NCD multi residual", round(resid[tcs.index('F'),col1],1), -4.8, 0.15)

print("\nSMALL COMMUNITY (institution concentration, two methods)")
inst=con.execute(f"""
  WITH ra AS (SELECT openalex_id id FROM works w WHERE {SUB}),
    lead AS (SELECT DISTINCT a.institution_id iid, a.openalex_id id FROM authorships a JOIN ra ON a.openalex_id=ra.id
             WHERE a.position IN ('first','last') AND a.institution_id IS NOT NULL AND a.institution_id<>'')
  SELECT iid, COUNT(DISTINCT id) n FROM lead GROUP BY 1 ORDER BY n DESC""").df()
check("distinct lead institutions", len(inst), 5219)
tot=inst.n.sum(); cum=inst.n.cumsum()
k25=int((cum<=0.25*tot).sum())+1; k50=int((cum<=0.50*tot).sum())+1
check("institutions for 25% (pandas)", k25, 34)
check("institutions for 50% (pandas)", k50, 186)
# independent 2nd method: SQL window
sqlk=con.execute(f"""
  WITH ra AS (SELECT openalex_id id FROM works w WHERE {SUB}),
    lead AS (SELECT DISTINCT a.institution_id iid, a.openalex_id id FROM authorships a JOIN ra ON a.openalex_id=ra.id
             WHERE a.position IN ('first','last') AND a.institution_id IS NOT NULL AND a.institution_id<>''),
    c AS (SELECT iid, COUNT(DISTINCT id) n FROM lead GROUP BY 1),
    w AS (SELECT iid, n, SUM(n) OVER (ORDER BY n DESC) run, SUM(n) OVER () tot FROM c)
  SELECT SUM(CASE WHEN run <= 0.50*tot THEN 1 ELSE 0 END)+1 FROM w""").fetchone()[0]
check("institutions for 50% (SQL window)", sqlk, 186)
la=con.execute(f"""
  WITH ra AS (SELECT openalex_id id FROM works w WHERE {SUB})
  SELECT a.author_id, COUNT(DISTINCT a.openalex_id) n FROM authorships a JOIN ra ON a.openalex_id=ra.id
  WHERE a.position='last' AND a.author_id IS NOT NULL AND a.author_id<>'' GROUP BY 1 ORDER BY n DESC""").df()
check("distinct last authors", len(la), 14278)
clt=la.n.cumsum(); k50a=int((clt<=0.5*la.n.sum()).sum())+1
check("last-authors for 50% (~3200)", k50a, 3174, 3)

print("\n" + ("ALL PASS" if not FAILS else f"FAILURES: {FAILS}"))
import sys; sys.exit(1 if FAILS else 0)
