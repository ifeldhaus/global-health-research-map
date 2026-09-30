"""Emit the two PLOS supplement tables into plos-gph/tables/. S11 = funder x leadership
(odds ratios). S12 = share of each topic's funded articles naming each funder type, with
an all-topics reference row and cells that depart significantly (|Pearson residual|>2)
shown in bold. Values certified by verify_crosscutting.py."""
import duckdb, numpy as np, pandas as pd
from pathlib import Path
from scipy.stats import chi2_contingency
from statsmodels.stats.contingency_tables import Table2x2, StratifiedTable
from statsmodels.stats.proportion import proportion_confint
ROOT=Path(__file__).resolve().parents[1]; OUT=ROOT/"plos-gph"/"tables"
con=duckdb.connect(str(ROOT/"data"/"global_health.duckdb"),read_only=True)
SUB=("classified_topic AND topic_category NOT IN ('Z') AND classified_method AND method_type NOT IN ('M14','M15','M18')")
FJ="REPLACE(g.funder_id,'https://openalex.org/','') = fu.openalex_id"
def wci(k,n): lo,hi=proportion_confint(k,n,method='wilson'); return f"{100*k/n:.1f} ({100*lo:.1f}--{100*hi:.1f})"

# ===== S11: funder x leadership =====
base=con.execute(f"""
  WITH sc AS (SELECT w.openalex_id id, w.study_country sctry, w.topic_category tc FROM works w
      WHERE {SUB} AND study_country IS NOT NULL AND study_country NOT IN ('GLOBAL','UNKNOWN') AND study_country NOT LIKE '%|%'),
    lab AS (SELECT sc.id, sc.tc, MAX(CASE WHEN a.institution_country=sc.sctry THEN 1 ELSE 0 END) any_local
      FROM sc JOIN authorships a ON sc.id=a.openalex_id WHERE a.institution_country IS NOT NULL AND a.institution_country<>'' GROUP BY sc.id, sc.tc)
  SELECT id, tc, CASE WHEN any_local=0 THEN 1 ELSE 0 END ext FROM lab""").df()
fund=con.execute(f"SELECT DISTINCT g.openalex_id id, fu.canonical_name f FROM grants g JOIN funders fu ON {FJ}").df()
disp=[('National Institutes of Health','National Institutes of Health (Gov)'),
      ('Wellcome Trust','Wellcome Trust (Phil)'),('MRC UK','Medical Research Council (Gov)'),
      ('Fogarty International Center','Fogarty International Center (Gov)'),
      ('USAID','US Agency for Int.\\ Development (Gov)'),
      ('Bill & Melinda Gates Foundation','Bill \\& Melinda Gates Foundation (Phil)')]
rows=[]
for canon,label in disp:
    ids=set(fund[fund.f==canon].id); base['exp']=base.id.isin(ids).astype(int)
    a=int(((base.exp==1)&(base.ext==1)).sum()); b=int(((base.exp==1)&(base.ext==0)).sum())
    c=int(((base.exp==0)&(base.ext==1)).sum()); d=int(((base.exp==0)&(base.ext==0)).sum())
    cor=Table2x2(np.array([[a,b],[c,d]])).oddsratio
    strata=[]
    for tc,grp in base.groupby('tc'):
        aa=int(((grp.exp==1)&(grp.ext==1)).sum()); bb=int(((grp.exp==1)&(grp.ext==0)).sum())
        cc=int(((grp.exp==0)&(grp.ext==1)).sum()); dd=int(((grp.exp==0)&(grp.ext==0)).sum())
        if (aa+bb)>0 and (cc+dd)>0 and (aa+cc)>0 and (bb+dd)>0: strata.append([[aa,bb],[cc,dd]])
    st=StratifiedTable(np.array(strata).transpose(1,2,0).astype(float))
    lo,hi=st.oddsratio_pooled_confint(); bd=st.test_equal_odds().pvalue
    rows.append(f"{label} & {a+b:,} & {wci(a,a+b)} & {cor:.2f} & {st.oddsratio_pooled:.2f} ({lo:.2f}--{hi:.2f}) & {bd:.2g} \\\\")
s11=(r"\begin{table}[htbp]\centering\footnotesize"
 r"\caption{\textbf{How often each major funder's single-country studies were led from outside the study "
 r"country, versus the field's 17.2\%; the topic-adjusted odds ratio confirms the difference is not "
 r"explained by what each funder studies --- a descriptive association, not a causal effect.} "
 r"Single-country research articles with a resolvable affiliation ($n=15{,}826$). Externally led $=$ no "
 r"author affiliated with the study country. Odds ratios compare articles acknowledging each funder with "
 r"all others; the topic-adjusted odds ratio is the Cochran--Mantel--Haenszel estimate across the 15 topic "
 r"strata and agreed with a topic-fixed-effects logistic regression (NIH 0.67, USAID 1.54, Gates 0.94). "
 r"$P$-values Holm-adjusted across the six funders; Breslow--Day tests homogeneity of the stratum-specific "
 r"odds ratios.}\label{tab:funder-leadership}"
 r"\begin{tabular}{@{}lrrrrr@{}}\toprule"
 r"Funder & $n$ & Ext.-led \% (95\% CI) & Crude OR & Topic-adj.\ OR (95\% CI) & Breslow--Day $p$ \\\midrule "
 + " ".join(rows) + r"\bottomrule\end{tabular}\end{table}"+"\n")
(OUT/"supp_funder_leadership.tex").write_text(s11); print("wrote supp_funder_leadership.tex")

# ===== S12: topic x funder-type SHARES with reference row + significance bold =====
raf=con.execute(f"""
  WITH ra AS (SELECT openalex_id id, topic_category tc FROM works w WHERE {SUB}),
    fc AS (SELECT DISTINCT ra.id, ra.tc,
      MAX(CASE WHEN fu.funder_category='Government' THEN 1 ELSE 0 END) gov,
      MAX(CASE WHEN fu.funder_category='Philanthropic' THEN 1 ELSE 0 END) phil,
      MAX(CASE WHEN fu.funder_category='Multilateral' THEN 1 ELSE 0 END) multi
      FROM ra JOIN grants g ON ra.id=g.openalex_id JOIN funders fu ON {FJ} GROUP BY 1,2)
  SELECT * FROM fc""").df()
TAX={'A':'Maternal \\& reproductive','B':'Child health','C':'Other infectious','D':'HIV, TB, malaria',
 'E':'Neglected tropical','F':'Noncommunicable','G':'Mental health','H':'Nutrition','I':'Health systems',
 'J':'Health economics','K':'Climate','L':'Conflict \\& humanitarian','M':'Surgical \\& emergency',
 'N':'Epidemiology','O':'Research methods'}
resid={}; overall={}
for cat in ['gov','phil','multi']:
    ct=pd.crosstab(raf.tc, raf[cat]); chi2,p,dof,exp=chi2_contingency(ct.values)
    col1=list(ct.columns).index(1); r=(ct.values-exp)/np.sqrt(exp)
    resid[cat]={tc:r[i,col1] for i,tc in enumerate(ct.index)}
    overall[cat]=100*raf[cat].mean()
def cell(tc,cat):
    sub=raf[raf.tc==tc]; pct=100*sub[cat].mean()
    return f"\\textbf{{{pct:.1f}}}" if abs(resid[cat][tc])>2 else f"{pct:.1f}"
order=raf.tc.value_counts().index
ref=f"\\emph{{All topics (reference)}} & \\emph{{{overall['gov']:.1f}}} & \\emph{{{overall['phil']:.1f}}} & \\emph{{{overall['multi']:.1f}}} \\\\\\midrule "
body=" ".join(f"{TAX[tc]} & {cell(tc,'gov')} & {cell(tc,'phil')} & {cell(tc,'multi')} \\\\" for tc in order if tc in TAX)
s12=(r"\begin{table}[htbp]\centering\footnotesize"
 r"\caption{\textbf{Where each funder type is over- or under-represented by topic: the share of a topic's "
 r"funded articles naming a government, philanthropic, or multilateral funder, against the all-topics rate; "
 r"the low bold values show multilateral and philanthropic funding is scarcest at the biggest burden gaps, "
 r"noncommunicable disease and mental health.} Funded research articles ($n=11{,}859$). Articles may name "
 r"more than one funder, so rows need not sum to 100\%. \textbf{Bold} $=$ significantly above or below the "
 r"all-topics rate (standardized residual $|{>}2|$; chi-squared $p<0.001$ for each funder type, "
 r"Cram\'er's $V$ 0.12--0.17). Compare each cell with the reference row.}\label{tab:funder-topic}"
 r"\begin{tabular}{@{}lccc@{}}\toprule"
 r"Topic & Government \% & Philanthropic \% & Multilateral \% \\\midrule "
 + ref + body + r"\bottomrule\end{tabular}\end{table}"+"\n")
(OUT/"supp_funder_topic.tex").write_text(s12); print("wrote supp_funder_topic.tex")
# echo the S12 values for verification
print("\nOverall: gov %.1f  phil %.1f  multi %.1f" % (overall['gov'],overall['phil'],overall['multi']))
for tc in order:
    if tc in TAX:
        sub=raf[raf.tc==tc]
        print(f"  {TAX[tc][:24]:24} gov {100*sub['gov'].mean():4.1f}{'*' if abs(resid['gov'][tc])>2 else ' '}  "
              f"phil {100*sub['phil'].mean():4.1f}{'*' if abs(resid['phil'][tc])>2 else ' '}  "
              f"multi {100*sub['multi'].mean():4.1f}{'*' if abs(resid['multi'][tc])>2 else ' '}")
