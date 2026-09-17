"""Inferential statistics for the PLOS cross-cutting analyses A1 (funding x
leadership) and A2 (funding x attention-vs-burden). Every proportion carries a
Wilson 95% CI; associations use chi-square with Cramer's V; the funder->leadership
claim is adjusted for topic (the specialization confounder) via Cochran-Mantel-
Haenszel pooled odds ratios (Breslow-Day/Tarone homogeneity), cross-checked with
topic-fixed-effects logistic regression. Observational; associational only."""
import duckdb, numpy as np, pandas as pd
from pathlib import Path
from scipy.stats import chi2_contingency
from statsmodels.stats.contingency_tables import Table2x2, StratifiedTable
from statsmodels.stats.proportion import proportion_confint
from statsmodels.stats.multitest import multipletests
import statsmodels.formula.api as smf

DB = Path(__file__).resolve().parents[1] / "data" / "global_health.duckdb"
con = duckdb.connect(str(DB), read_only=True)
SUB = ("classified_topic AND topic_category NOT IN ('Z') AND classified_method "
       "AND method_type NOT IN ('M14','M15','M18')")
FJ = "REPLACE(g.funder_id,'https://openalex.org/','') = fu.openalex_id"
def wci(k,n): lo,hi=proportion_confint(k,n,method='wilson'); return f"{100*k/n:.1f}% [{100*lo:.1f}-{100*hi:.1f}]"
def rule(t): print("\n"+"="*74+f"\n{t}\n"+"="*74)

# ================= STEP 0: base frame + reconcile to validated figures =======
rule("STEP 0  Base frame: single-country RA with resolvable affiliation")
base = con.execute(f"""
  WITH sc AS (SELECT w.openalex_id id, w.study_country sctry, w.topic_category tc FROM works w
      WHERE {SUB} AND study_country IS NOT NULL AND study_country NOT IN ('GLOBAL','UNKNOWN')
      AND study_country NOT LIKE '%|%'),
    lab AS (SELECT sc.id, sc.tc,
      MAX(CASE WHEN a.institution_country=sc.sctry THEN 1 ELSE 0 END) any_local,
      MAX(CASE WHEN a.position='last' AND a.institution_country=sc.sctry THEN 1 ELSE 0 END) last_local
      FROM sc JOIN authorships a ON sc.id=a.openalex_id
      WHERE a.institution_country IS NOT NULL AND a.institution_country<>'' GROUP BY sc.id, sc.tc)
  SELECT id, tc, any_local, last_local FROM lab
""").df()
base['lead'] = np.where(base.last_local==1,'local', np.where(base.any_local==1,'collab','external'))
base['external'] = (base.lead=='external').astype(int)
N=len(base)
print(f"N = {N:,}  (validated target 15,826: {'MATCH' if N==15826 else 'MISMATCH'})")
for k,v in base['lead'].value_counts().items():
    print(f"  {k:9} {v:5,}  {wci(v,N)}")
print("  validated targets: local 41.0%, collab 41.8%, external 17.2%")

# funder membership per paper (canonical + category)
fund = con.execute(f"""SELECT DISTINCT g.openalex_id id, fu.canonical_name f, fu.funder_category cat
                       FROM grants g JOIN funders fu ON {FJ}""").df()
base['funded'] = base['id'].isin(set(fund['id']))

# ================= STEP 1 (A1a): funded rate by leadership ====================
rule("STEP 1 (A1a)  Funded rate by leadership  [Wilson 95% CI; chi-square]")
ct = pd.crosstab(base['lead'], base['funded'])
for ld in ['local','collab','external']:
    k=int(ct.loc[ld,True]); n=int(ct.loc[ld].sum()); print(f"  {ld:9} funded {wci(k,n)}  (n={n:,})")
chi2,p,dof,exp = chi2_contingency(ct.values)
V = np.sqrt(chi2/(N*(min(ct.shape)-1)))
print(f"  chi-square = {chi2:.1f}, dof = {dof}, p = {p:.2e}, Cramer's V = {V:.3f} (small)")
print("  NOTE: collaborative papers have more authors/institutions -> more grant records;")
print("        funded-rate-by-leadership is confounded by team size. Reported, not emphasized.")

# ================= STEP 2 (A1b): funder -> externally-led, topic-adjusted =====
rule("STEP 2 (A1b)  Funder -> externally-led: crude vs TOPIC-ADJUSTED (CMH)")
print("  exposure = paper acknowledges funder X; outcome = externally-led; strata = 15 topics")
print("  OR<1 => funder's papers LESS externally-led (more locally led) than the rest\n")
funders = ['National Institutes of Health','Wellcome Trust','MRC UK','Fogarty International Center',
           'USAID','Bill & Melinda Gates Foundation']
rowsout=[]
for fn in funders:
    ids=set(fund[fund.f==fn].id)
    base['exp']=base['id'].isin(ids).astype(int)
    # crude 2x2: rows exposure(1,0) x cols external(1,0)
    a=int(((base.exp==1)&(base.external==1)).sum()); b=int(((base.exp==1)&(base.external==0)).sum())
    c=int(((base.exp==0)&(base.external==1)).sum()); d=int(((base.exp==0)&(base.external==0)).sum())
    t=Table2x2(np.array([[a,b],[c,d]])); cor,(lo,hi)=t.oddsratio,t.oddsratio_confint()
    exp_ext=wci(a,a+b)
    # topic-stratified CMH
    strata=[]
    for tc,grp in base.groupby('tc'):
        aa=int(((grp.exp==1)&(grp.external==1)).sum()); bb=int(((grp.exp==1)&(grp.external==0)).sum())
        cc=int(((grp.exp==0)&(grp.external==1)).sum()); dd=int(((grp.exp==0)&(grp.external==0)).sum())
        if (aa+bb)>0 and (cc+dd)>0 and (aa+cc)>0 and (bb+dd)>0:
            strata.append([[aa,bb],[cc,dd]])
    st=StratifiedTable(np.array(strata).transpose(1,2,0).astype(float))
    ORmh=st.oddsratio_pooled; cimh=st.oddsratio_pooled_confint()
    cmh=st.test_null_odds(); bd=st.test_equal_odds()
    rowsout.append((fn,a+b,exp_ext,cor,lo,hi,ORmh,cimh[0],cimh[1],cmh.pvalue,bd.pvalue))
# Holm correction across funders on the CMH p-values
pvals=[r[9] for r in rowsout]; rej,padj,_,_=multipletests(pvals,method='holm')
print(f"  baseline externally-led (whole set): {wci(int(base.external.sum()),N)}\n")
print(f"  {'funder':32}{'n':>6} {'ext% [95CI]':>18} {'crudeOR[95CI]':>20} {'adjOR(topic)[95CI]':>22} {'CMHp_holm':>10} {'BDp':>7}")
for (fn,n,ee,cor,lo,hi,ORmh,cl,ch,cmhp,bdp),pa in zip(rowsout,padj):
    print(f"  {fn[:32]:32}{n:6,} {ee:>18} {cor:5.2f}[{lo:.2f}-{hi:.2f}]{'':4}{ORmh:5.2f}[{cl:.2f}-{ch:.2f}]{'':4}{pa:10.3g} {bdp:7.2g}")
print("  (adjOR from Cochran-Mantel-Haenszel across topic strata; BDp=Breslow-Day/Tarone homogeneity;")
print("   CMHp_holm = Holm-adjusted CMH p across the 6 funders)")

# logistic cross-check: external ~ funder + C(topic)
rule("STEP 2b  Logistic cross-check: external ~ funder + C(topic)  (adjusted OR)")
for fn in ['National Institutes of Health','USAID','Bill & Melinda Gates Foundation']:
    base['exp']=base['id'].isin(set(fund[fund.f==fn].id)).astype(int)
    m=smf.logit('external ~ exp + C(tc)', data=base).fit(disp=0)
    orr=np.exp(m.params['exp']); ci=np.exp(m.conf_int().loc['exp'])
    print(f"  {fn[:34]:34} adjOR = {orr:.2f} [{ci[0]:.2f}-{ci[1]:.2f}], p = {m.pvalues['exp']:.3g}")

# ================= STEP 3 (A2): topic x funder-category ======================
rule("STEP 3 (A2)  Topic x funder-category association  [chi-square, Cramer's V, residuals]")
raf = con.execute(f"""
  WITH ra AS (SELECT openalex_id id, topic_category tc FROM works w WHERE {SUB}),
    fc AS (SELECT DISTINCT ra.id, ra.tc,
             MAX(CASE WHEN fu.funder_category='Government' THEN 1 ELSE 0 END) gov,
             MAX(CASE WHEN fu.funder_category='Philanthropic' THEN 1 ELSE 0 END) phil,
             MAX(CASE WHEN fu.funder_category='Multilateral' THEN 1 ELSE 0 END) multi
           FROM ra JOIN grants g ON ra.id=g.openalex_id JOIN funders fu ON {FJ} GROUP BY 1,2)
  SELECT * FROM fc
""").df()
TAX={'A':'Maternal','B':'Child','C':'OtherInfect','D':'HIV/TB/mal','E':'NTD','F':'NCD','G':'Mental',
 'H':'Nutrition','I':'HealthSys','J':'HealthEcon','K':'Climate','L':'Conflict','M':'Surgical',
 'N':'Epidemiology','O':'ResMethods'}
print(f"  funded single+multi RA analyzed: {len(raf):,}\n")
cat_p=[]
for cat in ['gov','phil','multi']:
    ct=pd.crosstab(raf['tc'], raf[cat])
    chi2,p,dof,exp=chi2_contingency(ct.values)
    V=np.sqrt(chi2/(len(raf)*(min(ct.shape)-1)))
    resid=(ct.values-exp)/np.sqrt(exp)  # Pearson residuals for the '1' column
    col1=list(ct.columns).index(1)
    order=np.argsort(resid[:,col1])
    tcs=list(ct.index)
    lo3=[(TAX.get(tcs[i],tcs[i]),resid[i,col1]) for i in order[:3]]
    hi3=[(TAX.get(tcs[i],tcs[i]),resid[i,col1]) for i in order[-3:]][::-1]
    cat_p.append(p)
    print(f"  {cat.upper():5} names-{cat}: chi2={chi2:.1f} dof={dof} p={p:.2e} V={V:.3f}")
    print(f"        most UNDER (resid): "+", ".join(f"{n}{r:+.1f}" for n,r in lo3))
    print(f"        most OVER  (resid): "+", ".join(f"{n}{r:+.1f}" for n,r in hi3))
rej,padj,_,_=multipletests(cat_p,method='holm')
print(f"  Holm-adjusted p across the 3 categories: {[f'{x:.2e}' for x in padj]}")
# key proportions with CI
print("\n  Key proportions (Wilson 95% CI), share of a topic's funded RA naming category:")
for tc,lab in [('F','NCD'),('G','Mental health'),('D','HIV/TB/malaria')]:
    sub=raf[raf.tc==tc]; n=len(sub)
    for cat in ['gov','multi','phil']:
        print(f"    {lab:14} {cat:5} {wci(int(sub[cat].sum()),n)}  (n={n:,})")
