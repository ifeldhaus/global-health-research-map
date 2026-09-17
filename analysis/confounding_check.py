"""Demonstrate that topic meets the confounding criteria for the funder->leadership
analysis: (1) topic associated with the OUTCOME (externally-led), (2) topic
associated with the EXPOSURE (funder), and (3) crude vs adjusted OR shift = evidence
confounding was operating. Justifies why stratified/adjusted estimation is required."""
import duckdb, numpy as np, pandas as pd
from pathlib import Path
from scipy.stats import chi2_contingency
DB=Path(__file__).resolve().parents[1]/"data"/"global_health.duckdb"
con=duckdb.connect(str(DB),read_only=True)
SUB=("classified_topic AND topic_category NOT IN ('Z') AND classified_method "
     "AND method_type NOT IN ('M14','M15','M18')")
FJ="REPLACE(g.funder_id,'https://openalex.org/','') = fu.openalex_id"
TAX={'A':'Maternal','B':'Child','C':'OtherInf','D':'HIV/TB/mal','E':'NTD','F':'NCD','G':'Mental',
 'H':'Nutrition','I':'HealthSys','J':'HealthEcon','K':'Climate','L':'Conflict','M':'Surgical',
 'N':'Epi','O':'ResMethods'}
def V(chi2,n,ct): return np.sqrt(chi2/(n*(min(ct.shape)-1)))

base=con.execute(f"""
  WITH sc AS (SELECT w.openalex_id id, w.study_country sctry, w.topic_category tc FROM works w
      WHERE {SUB} AND study_country IS NOT NULL AND study_country NOT IN ('GLOBAL','UNKNOWN')
      AND study_country NOT LIKE '%|%'),
    lab AS (SELECT sc.id, sc.tc,
      MAX(CASE WHEN a.institution_country=sc.sctry THEN 1 ELSE 0 END) any_local
      FROM sc JOIN authorships a ON sc.id=a.openalex_id
      WHERE a.institution_country IS NOT NULL AND a.institution_country<>'' GROUP BY sc.id, sc.tc)
  SELECT id, tc, CASE WHEN any_local=0 THEN 1 ELSE 0 END ext FROM lab
""").df()
fund=con.execute(f"""SELECT DISTINCT g.openalex_id id, fu.canonical_name f FROM grants g JOIN funders fu ON {FJ}""").df()
N=len(base)

print("CRITERION 1  Topic <-> OUTCOME (externally-led)")
ct=pd.crosstab(base.tc, base.ext); chi2,p,dof,_=chi2_contingency(ct.values)
byt=base.groupby('tc').ext.mean().sort_values()
print(f"  chi2={chi2:.1f} dof={dof} p={p:.2e} CramerV={V(chi2,N,ct):.3f}")
print(f"  externally-led ranges {100*byt.min():.1f}% ({TAX[byt.index[0]]}) to {100*byt.max():.1f}% ({TAX[byt.index[-1]]})")

print("\nCRITERION 2  Topic <-> EXPOSURE (funder), for the key funders")
for fn in ['National Institutes of Health','USAID','Wellcome Trust','Bill & Melinda Gates Foundation']:
    base['exp']=base.id.isin(set(fund[fund.f==fn].id)).astype(int)
    ct=pd.crosstab(base.tc, base.exp); chi2,p,dof,_=chi2_contingency(ct.values)
    # topic concentration: top topic share among this funder's papers vs field
    fp=base[base.exp==1]; top=fp.tc.value_counts(normalize=True)
    field=base.tc.value_counts(normalize=True)
    t0=top.index[0]
    print(f"  {fn[:30]:30} chi2={chi2:6.1f} p={p:.1e} V={V(chi2,N,ct):.3f} | "
          f"top topic {TAX[t0]} {100*top.iloc[0]:.0f}% of its papers vs {100*field[t0]:.0f}% field")
con.close()
