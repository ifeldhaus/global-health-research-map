"""
Appendix figures: dashboard analyses not shown in the main-text figures.
Run: uv run python paper/figures/make_appendix_figures.py
Writes figA_*.pdf and figA_*.png to paper/figures/.
"""
from pathlib import Path
import sys
import duckdb
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "dashboard"))
from constants import HIGH_INCOME_ISO2, income_tier, INCOME_TIER_ORDER, INCOME_TIER_COLORS  # noqa

DB = REPO / "data" / "global_health.duckdb"
OUT = REPO / "paper" / "figures"
Y0, Y1 = 2010, 2025
SUB = ("classified_topic AND topic_category NOT IN ('Z') "
       "AND classified_method AND method_type NOT IN ('M14','M15','M18')")
OKABE = ['#0072B2', '#E69F00', '#009E73', '#D55E00', '#CC79A7', '#56B4E9', '#F0E442', '#999999']
mpl.rcParams.update({'figure.dpi': 120, 'savefig.dpi': 300, 'font.size': 9.5,
                     'font.family': 'sans-serif', 'axes.spines.top': False,
                     'axes.spines.right': False, 'axes.grid': True, 'grid.alpha': 0.2,
                     'grid.linewidth': 0.5, 'legend.frameon': False})
con = duckdb.connect(str(DB), read_only=True)

from scipy.cluster.hierarchy import linkage, leaves_list  # noqa: E402


def cluster_order(M):
    """Hierarchical-clustering leaf order for rows and columns, so similar
    profiles sit next to each other and blocks of structure become visible."""
    R = np.nan_to_num(M)
    ro = list(leaves_list(linkage(R, method='average', metric='euclidean'))) if R.shape[0] > 2 else list(range(R.shape[0]))
    co = list(leaves_list(linkage(R.T, method='average', metric='euclidean'))) if R.shape[1] > 2 else list(range(R.shape[1]))
    return ro, co


def annotate_cells(ax, M, thr, vmax):
    """Print the value in cells at or above |thr|; white text on dark cells."""
    for i in range(M.shape[0]):
        for j in range(M.shape[1]):
            v = M[i, j]
            if np.isfinite(v) and abs(v) >= thr:
                ax.text(j, i, f'{v:.0f}', ha='center', va='center', fontsize=6,
                        color='white' if abs(v) >= 0.55 * vmax else '#222222')


def save(fig, name):
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f"{name}.{ext}", bbox_inches='tight')
    plt.close(fig)
    print('wrote', name)


# A1 -- funder concentration over time, shown as the effective number of funders
# (1/HHI): the count of equally-sized funders that would give the same
# concentration. Lower = more concentrated. More legible than raw HHI (~0.03-0.04).
def a1():
    r = con.execute("SELECT publication_year,hhi,n_funders FROM funder_hhi_by_year "
                    f"WHERE publication_year BETWEEN {Y0} AND {Y1} ORDER BY 1").fetchall()
    yr = [x[0] for x in r]
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(yr, [1 / x[1] for x in r], color=OKABE[0], marker='o', lw=2,
            label='Effective number of funders (1/HHI)')
    ax.plot(yr, [x[2] for x in r], color='#999999', marker='.', lw=1.5,
            linestyle='--', label='Distinct funders')
    ax.set_xlabel('Year'); ax.set_ylabel('Number of funders')
    ax.set_ylim(0, None)
    ax.legend(loc='upper left', fontsize=9)
    save(fig, 'figA_funder_concentration')


# A2 -- funder x topic heatmap (top funders, row-normalized share across topics)
def a2():
    funders = [x[0] for x in con.execute(
        "SELECT funder,SUM(n_papers) s FROM funder_topic_summary GROUP BY 1 ORDER BY s DESC LIMIT 12").fetchall()]
    allt = [x[0] for x in con.execute(f"SELECT topic_category FROM works w WHERE {SUB} GROUP BY 1 ORDER BY COUNT(*) DESC").fetchall()]
    M = np.zeros((len(funders), len(allt)))
    idx = {t: i for i, t in enumerate(allt)}
    for f, t, n in con.execute("SELECT funder,topic_category,SUM(n_papers) FROM funder_topic_summary GROUP BY 1,2").fetchall():
        if f in funders and t in idx:
            M[funders.index(f), idx[t]] = n
    M = M / M.sum(axis=1, keepdims=True) * 100
    tnames = {x[0]: x[1] for x in con.execute("SELECT DISTINCT topic_category,topic_name FROM topic_method_matrix").fetchall()}
    xlabels = [tnames.get(t, t) for t in allt]
    ro, co = cluster_order(M)
    M = M[np.ix_(ro, co)]; funders = [funders[i] for i in ro]; xlabels = [xlabels[j] for j in co]
    fig, ax = plt.subplots(figsize=(9.5, 6.5))
    im = ax.imshow(M, aspect='auto', cmap='Blues')
    ax.set_xticks(range(len(xlabels))); ax.set_xticklabels(xlabels, fontsize=7.5, rotation=90)
    ax.set_yticks(range(len(funders))); ax.set_yticklabels(funders, fontsize=7.5)
    annotate_cells(ax, M, thr=5, vmax=np.nanmax(M))
    fig.colorbar(im, ax=ax, label="% of funder's articles", shrink=0.8)
    save(fig, 'figA_funder_topic')


# A3 -- attention vs burden dumbbell (mapped topics; computed from DB, matches tab:burden)
BURDEN_LABEL = {'D': 'HIV/AIDS/Tuberculosis/Malaria', 'F': 'Non-Communicable Diseases',
                'C': 'Infectious Disease (other)', 'A': 'Maternal & Reproductive Health',
                'G': 'Mental Health & Substance Use', 'E': 'Neglected Tropical Diseases',
                'M': 'Surgical, Trauma & Emergency Care', 'H': 'Nutrition & Micronutrient Deficiency'}
def a3():
    lmic = ("('World Bank Low Income','World Bank Lower Middle Income',"
            "'World Bank Upper Middle Income')")
    gy = con.execute(f"SELECT MAX(year) FROM gbd_burden WHERE measure='DALYs' "
                     f"AND metric='Number' AND region IN {lmic}").fetchone()[0]
    burd = dict(con.execute(
        f"SELECT tbm.topic_category, SUM(g.val) FROM topic_burden_map tbm "
        f"JOIN gbd_burden g ON tbm.gbd_cause=g.cause WHERE g.measure='DALYs' "
        f"AND g.metric='Number' AND g.region IN {lmic} AND g.year={gy} GROUP BY 1").fetchall())
    btot = sum(burd.values())
    incat = ','.join(f"'{x}'" for x in burd)
    pubd = dict(con.execute(f"SELECT topic_category, COUNT(*) FROM works w WHERE {SUB} "
                            f"AND topic_category IN ({incat}) GROUP BY 1").fetchall())
    ptot = sum(pubd.values())
    data = sorted(((BURDEN_LABEL.get(k, k), 100*pubd.get(k, 0)/ptot, 100*burd[k]/btot)
                   for k in burd), key=lambda d: -d[2])
    names = [d[0] for d in data]
    pub = [d[1] for d in data]
    bur = [d[2] for d in data]
    y = range(len(names))
    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    for i in y:
        ax.plot([pub[i], bur[i]], [i, i], color='#CCCCCC', lw=2, zorder=1)
    ax.scatter(pub, list(y), color=OKABE[1], label='Publication share', zorder=2)
    ax.scatter(bur, list(y), color=OKABE[0], label='LMIC burden share', zorder=2)
    ax.set_yticks(list(y)); ax.set_yticklabels(names, fontsize=8)
    ax.set_xlabel('Share (%)'); ax.invert_yaxis()
    ax.legend(loc='lower right', fontsize=8.5)
    save(fig, 'figA_attention_burden')


# A4 -- pre vs post COVID topic share change
def a4():
    r = con.execute("SELECT topic_name,net_change FROM covid_displacement WHERE net_change IS NOT NULL ORDER BY net_change").fetchall()
    names = [x[0] for x in r]; val = [x[1] * 100 for x in r]  # proportion -> percentage points
    colors = [OKABE[2] if v >= 0 else OKABE[3] for v in val]
    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.barh(range(len(names)), val, color=colors)
    ax.set_yticks(range(len(names))); ax.set_yticklabels(names, fontsize=8)
    ax.axvline(0, color='black', lw=0.8)
    ax.set_xlabel('Net change in publication share, pre- vs post-COVID (pp)')
    save(fig, 'figA_covid_displacement')


# A5 -- design x topic heatmap (row-normalized within topic)
def a5():
    topics = con.execute("SELECT DISTINCT topic_category,topic_name FROM topic_method_matrix ORDER BY 1").fetchall()
    methods = [x for x in con.execute("SELECT DISTINCT method_type,method_name FROM topic_method_matrix ORDER BY 1").fetchall()]
    ti = {t[0]: i for i, t in enumerate(topics)}
    mi = {m[0]: i for i, m in enumerate(methods)}
    M = np.zeros((len(topics), len(methods)))
    for tc, tn, mt, mn, n in con.execute("SELECT topic_category,topic_name,method_type,method_name,n_papers FROM topic_method_matrix").fetchall():
        M[ti[tc], mi[mt]] = n
    M = M / np.clip(M.sum(axis=1, keepdims=True), 1, None) * 100
    tlabels = [t[1] for t in topics]; mlabels = [m[1] for m in methods]
    ro, co = cluster_order(M)
    M = M[np.ix_(ro, co)]; tlabels = [tlabels[i] for i in ro]; mlabels = [mlabels[j] for j in co]
    fig, ax = plt.subplots(figsize=(10.5, 6.5))
    im = ax.imshow(M, aspect='auto', cmap='Greens')
    ax.set_xticks(range(len(mlabels))); ax.set_xticklabels(mlabels, fontsize=7, rotation=90)
    ax.set_yticks(range(len(tlabels))); ax.set_yticklabels(tlabels, fontsize=8)
    annotate_cells(ax, M, thr=8, vmax=np.nanmax(M))
    fig.colorbar(im, ax=ax, label="% of topic's articles", shrink=0.8)
    save(fig, 'figA_design_topic')


# A6 -- collaboration breadth by topic
def a6():
    r = con.execute("SELECT topic_name,mean_institutions,intl_rate FROM collaboration_density ORDER BY mean_institutions DESC").fetchall()
    names = [x[0] for x in r]; mi = [x[1] for x in r]
    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.barh(range(len(names)), mi, color=OKABE[0])
    ax.set_yticks(range(len(names))); ax.set_yticklabels(names, fontsize=8); ax.invert_yaxis()
    ax.set_xlabel('Mean distinct institutions per article')
    save(fig, 'figA_collaboration')


# A7 -- leadership by income tier over time (lead institution tier, single-tier papers)
def a7():
    hi = tuple(HIGH_INCOME_ISO2)
    rows = con.execute(f"""
      SELECT w.publication_year yr, a.institution_country cc
      FROM authorships a JOIN works w ON a.openalex_id=w.openalex_id
      WHERE {SUB} AND w.publication_year BETWEEN {Y0} AND {Y1}
        AND a.position IN ('first','last') AND a.institution_country IS NOT NULL AND a.institution_country<>''""").fetchall()
    # per paper we approximate by counting lead-author-institution tiers
    from collections import defaultdict
    yrt = defaultdict(lambda: defaultdict(int))
    for yr, cc in rows:
        t = income_tier(cc) or 'Unknown'
        yrt[yr][t] += 1
    yrs = sorted(yrt)
    order = INCOME_TIER_ORDER
    data = {t: [100 * yrt[y].get(t, 0) / sum(yrt[y].values()) for y in yrs] for t in order}
    fig, ax = plt.subplots(figsize=(7.4, 4.3))
    ax.stackplot(yrs, [data[t] for t in order], labels=order,
                 colors=[INCOME_TIER_COLORS[t] for t in order], alpha=0.9)
    ax.set_xlabel('Year'); ax.set_ylabel('Share of lead-author institutions (%)')
    ax.set_ylim(0, 100); ax.set_xlim(Y0, Y1)
    ax.legend(ncol=2, fontsize=8, loc='upper center', bbox_to_anchor=(0.5, -0.13))
    save(fig, 'figA_income_tier_leadership')


# A8 -- gender of first and last authors over time (% female of inferred)
def a8():
    rows = con.execute(f"""SELECT publication_year yr,
        SUM(CASE WHEN gender_first LIKE 'female%' THEN 1 ELSE 0 END) ff,
        SUM(CASE WHEN gender_first LIKE 'male%' THEN 1 ELSE 0 END) mf,
        SUM(CASE WHEN gender_last LIKE 'female%' THEN 1 ELSE 0 END) fl,
        SUM(CASE WHEN gender_last LIKE 'male%' THEN 1 ELSE 0 END) ml
      FROM works WHERE publication_year BETWEEN {Y0} AND {Y1} GROUP BY 1 ORDER BY 1""").fetchall()
    yr = [r[0] for r in rows]
    first = [100 * r[1] / (r[1] + r[2]) if (r[1] + r[2]) else None for r in rows]
    last = [100 * r[3] / (r[3] + r[4]) if (r[3] + r[4]) else None for r in rows]
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.plot(yr, first, color=OKABE[1], marker='o', lw=2, label='First author')
    ax.plot(yr, last, color=OKABE[0], marker='s', lw=2, label='Last author')
    ax.axhline(50, color='#999999', ls='--', lw=0.8)
    ax.set_xlabel('Year'); ax.set_ylabel('Inferred female (% of inferred)')
    ax.legend(loc='lower right', fontsize=9)
    save(fig, 'figA_gender_year')


# A9 -- usable-abstract rate by journal
def a9():
    import csv
    jl = {r['issn']: r['journal_name'] for r in csv.DictReader(open(REPO / 'data' / 'journal_list.csv'))}
    rows = con.execute("""SELECT journal_issn,
        100.0*SUM(CASE WHEN classified_topic THEN 1 ELSE 0 END)/COUNT(*) rate
        FROM works GROUP BY 1""").fetchall()
    rows = sorted(rows, key=lambda r: r[1])
    names = [jl.get(r[0], r[0]) for r in rows]; rate = [r[1] for r in rows]
    fig, ax = plt.subplots(figsize=(7.5, 4.3))
    ax.barh(range(len(names)), rate, color=OKABE[5])
    ax.set_yticks(range(len(names))); ax.set_yticklabels(names, fontsize=8)
    ax.set_xlabel('Usable-abstract rate (%)'); ax.set_xlim(0, 100)
    save(fig, 'figA_abstract_availability')


if __name__ == '__main__':
    for fn in (a1, a2, a3, a4, a5, a6, a7, a8, a9):
        try:
            fn()
        except Exception as e:
            print('ERR', fn.__name__, e)
    print('done ->', OUT)


# ==== Additional figures to mirror remaining dashboard charts ==============
from collections import defaultdict as _dd


def a10_parachute_by_topic():
    hi = tuple(HIGH_INCOME_ISO2)
    rows = con.execute(f"""
      WITH ra AS (SELECT openalex_id, topic_category, study_country FROM works w WHERE {SUB}
                  AND study_country NOT IN ('GLOBAL','UNKNOWN') AND study_country NOT LIKE '%|%'),
      lead AS (SELECT ra.topic_category tc, ra.openalex_id id,
                 MAX(CASE WHEN a.institution_country = ra.study_country THEN 1 ELSE 0 END) has_local
               FROM ra JOIN authorships a USING(openalex_id) GROUP BY 1,2)
      SELECT tc, 100.0*SUM(CASE WHEN has_local=0 THEN 1 ELSE 0 END)/COUNT(*) ext, COUNT(*) n
      FROM lead GROUP BY 1 HAVING COUNT(*)>=50 ORDER BY ext DESC""").fetchall()
    tn = {x[0]: x[1] for x in con.execute("SELECT DISTINCT topic_category,topic_name FROM topic_method_matrix").fetchall()}
    names = [tn.get(r[0], r[0]) for r in rows]; val = [r[1] for r in rows]
    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.barh(range(len(names)), val, color=OKABE[3])
    ax.set_yticks(range(len(names))); ax.set_yticklabels(names, fontsize=8); ax.invert_yaxis()
    ax.set_xlabel('Externally-led share of single-country research (%)')
    save(fig, 'figA_parachute_by_topic')


def a11_methods_gap():
    rows = con.execute("SELECT topic,method,z_score FROM methods_gap_matrix").fetchall()
    topics = sorted({r[0] for r in rows}); methods = sorted({r[1] for r in rows})
    ti = {t: i for i, t in enumerate(topics)}; mi = {m: i for i, m in enumerate(methods)}
    M = np.full((len(topics), len(methods)), np.nan)
    for t, m, z in rows:
        M[ti[t], mi[m]] = z
    lim = float(np.nanpercentile(np.abs(M[np.isfinite(M)]), 95))  # cap so one outlier cell doesn't wash out the rest
    ro, co = cluster_order(M)
    M = M[np.ix_(ro, co)]; topics = [topics[i] for i in ro]; methods = [methods[j] for j in co]
    fig, ax = plt.subplots(figsize=(10.5, 6.5))
    im = ax.imshow(M, aspect='auto', cmap='RdBu_r', vmin=-lim, vmax=lim)
    ax.set_xticks(range(len(methods))); ax.set_xticklabels(methods, fontsize=7, rotation=90)
    ax.set_yticks(range(len(topics))); ax.set_yticklabels(topics, fontsize=8)
    annotate_cells(ax, M, thr=3, vmax=lim)
    fig.colorbar(im, ax=ax, label='z-score (red: used more than expected)', shrink=0.8)
    save(fig, 'figA_methods_gap')


def a12_method_over_time():
    def tier_of(n):
        n = n.lower()
        if any(k in n for k in ['randomized', 'quasi-experimental', 'systematic review', 'meta-analysis']):
            return 'Experimental & synthesis'
        if any(k in n for k in ['cohort', 'longitudinal', 'modeling', 'simulation', 'economic', 'implementation', 'geospatial', 'remote', 'machine', 'artificial', 'secondary data']):
            return 'Observational & analytic'
        return 'Descriptive & exploratory'
    import csv
    mt = {r['method_id']: r['method_name'] for r in csv.DictReader(open(REPO / 'data' / 'taxonomy' / 'methods_taxonomy.csv'))}
    rows = con.execute(f"SELECT publication_year, method_type, COUNT(*) FROM works w WHERE {SUB} AND publication_year BETWEEN {Y0} AND {Y1} GROUP BY 1,2").fetchall()
    yrt = _dd(lambda: _dd(int))
    for yr, mt_, n in rows:
        if mt_ in mt:
            yrt[yr][tier_of(mt[mt_])] += n
    yrs = sorted(yrt); order = ['Experimental & synthesis', 'Observational & analytic', 'Descriptive & exploratory']
    data = {t: [100 * yrt[y].get(t, 0) / sum(yrt[y].values()) for y in yrs] for t in order}
    fig, ax = plt.subplots(figsize=(7.4, 4.3))
    ax.stackplot(yrs, [data[t] for t in order], labels=order, colors=[OKABE[0], OKABE[2], OKABE[1]], alpha=0.9)
    ax.set_xlabel('Year'); ax.set_ylabel('Share of research articles (%)')
    ax.set_ylim(0, 100); ax.set_xlim(Y0, Y1)
    ax.legend(ncol=3, fontsize=8, loc='upper center', bbox_to_anchor=(0.5, -0.13))
    save(fig, 'figA_method_over_time')


def a13_rising_institutions():
    con.execute(f"""CREATE TEMP TABLE _lead AS SELECT DISTINCT a.institution_id iid, a.institution_name nm,
        a.institution_country cc, a.openalex_id wid, w.publication_year yr
      FROM authorships a JOIN works w ON a.openalex_id=w.openalex_id
      WHERE {SUB} AND a.position IN ('first','last') AND a.institution_id IS NOT NULL AND a.institution_id<>'' AND a.institution_name<>''""")
    rows = con.execute("""SELECT ANY_VALUE(nm) nm, ANY_VALUE(cc) cc,
        COUNT(DISTINCT CASE WHEN yr BETWEEN 2010 AND 2013 THEN wid END) early,
        COUNT(DISTINCT CASE WHEN yr BETWEEN 2022 AND 2025 THEN wid END) recent
      FROM _lead GROUP BY iid""").fetchall()
    # fastest-rising = largest growth in articles led (recent minus early)
    data = sorted(((r[0], r[1], r[3] - r[2]) for r in rows), key=lambda x: -x[2])[:15]
    names = [d[0] for d in data]; growth = [d[2] for d in data]
    tiers = [income_tier(d[1]) or 'Unknown' for d in data]
    colors = [INCOME_TIER_COLORS.get(t, '#999999') for t in tiers]
    fig, ax = plt.subplots(figsize=(7.8, 5.2))
    ax.barh(range(len(names)), growth, color=colors)
    ax.set_yticks(range(len(names))); ax.set_yticklabels(names, fontsize=8); ax.invert_yaxis()
    ax.set_xlabel('Growth in research articles led, 2010--2013 to 2022--2025')
    import matplotlib.patches as mpatches
    handles = [mpatches.Patch(color=INCOME_TIER_COLORS[t], label=t) for t in INCOME_TIER_ORDER]
    ax.legend(handles=handles, fontsize=8, loc='lower right', title='Income tier', title_fontsize=8)
    save(fig, 'figA_rising_institutions')


def a14_collaboration_hubs():
    con.execute(f"""CREATE TEMP TABLE _ap AS SELECT DISTINCT a.openalex_id wid, a.institution_id iid, a.institution_name nm
      FROM authorships a JOIN works w ON a.openalex_id=w.openalex_id
      WHERE {SUB} AND a.institution_id IS NOT NULL AND a.institution_id<>'' AND a.institution_name<>''""")
    rows = con.execute("""SELECT x.nm, COUNT(DISTINCT y.iid) partners
      FROM _ap x JOIN _ap y ON x.wid=y.wid AND x.iid<>y.iid
      GROUP BY x.iid, x.nm ORDER BY partners DESC LIMIT 15""").fetchall()
    names = [r[0] for r in rows]; val = [r[1] for r in rows]
    fig, ax = plt.subplots(figsize=(7.8, 5))
    ax.barh(range(len(names)), val, color=OKABE[0])
    ax.set_yticks(range(len(names))); ax.set_yticklabels(names, fontsize=8); ax.invert_yaxis()
    ax.set_xlabel('Distinct partner institutions')
    save(fig, 'figA_collaboration_hubs')


def a15_abstract_by_year():
    rows = con.execute(f"""SELECT publication_year,
        100.0*SUM(CASE WHEN classified_topic THEN 1 ELSE 0 END)/COUNT(*) rate
      FROM works WHERE publication_year BETWEEN {Y0} AND {Y1} GROUP BY 1 ORDER BY 1""").fetchall()
    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot([r[0] for r in rows], [r[1] for r in rows], color=OKABE[5], marker='o', lw=2)
    ax.set_xlabel('Year'); ax.set_ylabel('Usable-abstract rate (%)'); ax.set_ylim(0, 100)
    save(fig, 'figA_abstract_by_year')


def a16_external_source_countries():
    import pycountry
    OVR = {'XK': 'Kosovo', 'EU': 'European Union'}
    def cn(cc):
        if cc in OVR: return OVR[cc]
        try:
            o = pycountry.countries.get(alpha_2=cc); return (getattr(o, 'common_name', None) or o.name) if o else cc
        except Exception: return cc
    rows = con.execute("""SELECT first_author_country, SUM(n_papers) n FROM author_study_country_flows
      WHERE first_author_country <> study_country AND study_country NOT IN ('GLOBAL','UNKNOWN')
        AND first_author_country NOT IN ('GLOBAL','UNKNOWN','')
      GROUP BY 1 ORDER BY n DESC LIMIT 15""").fetchall()
    names = [cn(r[0]) for r in rows]; val = [r[1] for r in rows]
    fig, ax = plt.subplots(figsize=(7.5, 5))
    ax.barh(range(len(names)), val, color=OKABE[3])
    ax.set_yticks(range(len(names))); ax.set_yticklabels(names, fontsize=8); ax.invert_yaxis()
    ax.set_xlabel('First-author papers on a different study country')
    save(fig, 'figA_external_source_countries')


for _fn in (a10_parachute_by_topic, a11_methods_gap, a12_method_over_time,
            a13_rising_institutions, a14_collaboration_hubs, a15_abstract_by_year, a16_external_source_countries):
    try:
        _fn()
    except Exception as _e:
        print('ERR', _fn.__name__, _e)
