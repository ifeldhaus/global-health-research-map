"""
paper/figures/make_figures.py

Generate the paper's time-trend figures as vector PDF (for LaTeX/Overleaf) plus
PNG (for quick preview), directly from the full DuckDB database, so the figures
reproduce the numbers reported in the Results.

Run from the repo root:
    uv run python paper/figures/make_figures.py

Figures (2010-2025 full years; 2026 is a partial year and is excluded):
  fig1_publications_by_year   Publications by year, stacked by journal
  fig2_funder_concentration   Funding concentration (HHI + top-funder share) by year
  fig3_externally_led         Externally-led (parachute) share over time, overall + by region
  fig4_topic_share            Topic share of research articles over time (key movers)
  fig5_north_south_leadership Global North vs South research leadership over time
"""

from pathlib import Path
import sys

import duckdb
import matplotlib as mpl
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "dashboard"))
from constants import HIGH_INCOME_ISO2  # noqa: E402

DB = REPO / "data" / "global_health.duckdb"
OUT = REPO / "paper" / "figures"
OUT.mkdir(parents=True, exist_ok=True)

Y0, Y1 = 2010, 2025  # full-year window; 2026 partial excluded

# Research-article subset used across the analytic lenses (n = 19,035).
SUB = ("classified_topic AND topic_category NOT IN ('Z') "
       "AND classified_method AND method_type NOT IN ('M14','M15','M18')")

# Colorblind-safe palettes.
SAFE = ['#88CCEE', '#CC6677', '#DDCC77', '#117733', '#332288', '#AA4499',
        '#44AA99', '#999933', '#882255', '#661100', '#6699CC']  # Paul Tol
OKABE = ['#0072B2', '#E69F00', '#009E73', '#D55E00', '#CC79A7', '#56B4E9',
         '#F0E442', '#000000']

mpl.rcParams.update({
    'figure.dpi': 120,
    'savefig.dpi': 300,
    'font.size': 10,
    'font.family': 'sans-serif',
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.grid': True,
    'grid.alpha': 0.25,
    'grid.linewidth': 0.5,
    'legend.frameon': False,
    'axes.titlesize': 11,
    'axes.titleweight': 'bold',
})

con = duckdb.connect(str(DB), read_only=True)


def save(fig, name):
    for ext in ('pdf', 'png'):
        fig.savefig(OUT / f"{name}.{ext}", bbox_inches='tight')
    plt.close(fig)
    print("wrote", name)


# ---------------------------------------------------------------- Figure 1
def fig1_publications_by_year():
    jn = {r[0]: r[1] for r in con.execute(
        "SELECT issn, journal_name FROM read_csv_auto(?)",
        [str(REPO / "data" / "journal_list.csv")]).fetchall()}
    order = [r[0] for r in con.execute(
        "SELECT journal_issn FROM works GROUP BY 1 ORDER BY COUNT(*) DESC").fetchall()]
    years = list(range(Y0, Y1 + 1))
    fig, ax = plt.subplots(figsize=(7.6, 4.4))
    bottom = [0] * len(years)
    for i, issn in enumerate(order):
        vals = dict(con.execute(
            "SELECT publication_year, COUNT(*) FROM works "
            "WHERE journal_issn = ? AND publication_year BETWEEN ? AND ? GROUP BY 1",
            [issn, Y0, Y1]).fetchall())
        y = [vals.get(yr, 0) for yr in years]
        ax.bar(years, y, bottom=bottom, label=jn.get(issn, issn),
               color=SAFE[i % len(SAFE)], width=0.82, edgecolor='white', linewidth=0.3)
        bottom = [b + v for b, v in zip(bottom, y)]
    ax.set_xlabel('Year'); ax.set_ylabel('Publications')
    ax.set_title('Publications by year, by journal')
    ax.legend(ncol=2, fontsize=7.5, loc='upper left')
    ax.set_xlim(Y0 - 0.6, Y1 + 0.6)
    save(fig, 'fig1_publications_by_year')


# ---------------------------------------------------------------- Figure 2
def fig2_funder_concentration():
    rows = con.execute(
        "SELECT publication_year, hhi, top_share FROM funder_hhi_by_year "
        "WHERE publication_year BETWEEN ? AND ? ORDER BY 1", [Y0, Y1]).fetchall()
    yrs = [r[0] for r in rows]
    hhi = [r[1] for r in rows]
    top = [r[2] * 100 if r[2] is not None and r[2] <= 1 else r[2] for r in rows]
    fig, ax = plt.subplots(figsize=(7.2, 4.4))
    l1, = ax.plot(yrs, hhi, color=OKABE[0], marker='o', lw=2, label='Concentration (HHI)')
    ax.set_xlabel('Year'); ax.set_ylabel('Funding concentration (HHI)', color=OKABE[0])
    ax.tick_params(axis='y', labelcolor=OKABE[0])
    ax2 = ax.twinx(); ax2.grid(False)
    l2, = ax2.plot(yrs, top, color=OKABE[1], marker='s', lw=2, ls='--',
                   label='Top-funder share (%)')
    ax2.set_ylabel('Top-funder share (%)', color=OKABE[1])
    ax2.tick_params(axis='y', labelcolor=OKABE[1])
    ax.set_title('Funding concentration over time')
    ax.legend(handles=[l1, l2], loc='upper right', fontsize=8.5)
    save(fig, 'fig2_funder_concentration')


# ---------------------------------------------------------------- Figure 3
def fig3_externally_led():
    ov = con.execute(
        "SELECT publication_year, 100.0*parachute/total FROM parachute_index "
        "WHERE publication_year BETWEEN ? AND ? AND total > 0 ORDER BY 1",
        [Y0, Y1]).fetchall()
    reg = con.execute(
        "SELECT study_region, publication_year, 100.0*parachute/total, total "
        "FROM parachute_by_region WHERE publication_year BETWEEN ? AND ? ORDER BY 1,2",
        [Y0, Y1]).fetchall()
    names = {'AFRO': 'Africa', 'AMRO': 'Americas', 'EMRO': 'E. Mediterranean',
             'EURO': 'Europe', 'SEARO': 'South-East Asia', 'WPRO': 'W. Pacific'}
    fig, ax = plt.subplots(figsize=(7.4, 4.4))
    regions = sorted({r[0] for r in reg})
    for i, rg in enumerate(regions):
        pts = [(r[1], r[2]) for r in reg if r[0] == rg and r[3] >= 20]
        if len(pts) >= 3:
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=OKABE[i % len(OKABE)],
                    lw=1.2, alpha=0.7, marker='.', ms=4, label=names.get(rg, rg))
    ax.plot([r[0] for r in ov], [r[1] for r in ov], color='black', lw=2.6,
            marker='o', ms=4, label='All regions', zorder=5)
    ax.set_xlabel('Year'); ax.set_ylabel('Externally-led share (%)')
    ax.set_title('Externally-led (parachute) research over time')
    ax.legend(ncol=2, fontsize=8, loc='upper right')
    ax.set_ylim(bottom=0)
    save(fig, 'fig3_externally_led')


# ---------------------------------------------------------------- Figure 4
def fig4_topic_share():
    # highlight the key movers; gray the rest
    movers = {
        'Non-Communicable': OKABE[3],
        'HIV': OKABE[0],
        'Health Systems': OKABE[2],
        'Child': OKABE[1],
        'Mental Health': OKABE[4],
        'Neglected Tropical': OKABE[5],
    }
    rows = con.execute(
        "SELECT topic_name, publication_year, pub_share FROM topic_year_counts "
        "WHERE publication_year BETWEEN ? AND ? ORDER BY 1,2", [Y0, Y1]).fetchall()
    series = {}
    for name, yr, share in rows:
        if not name:
            continue
        series.setdefault(name, []).append((yr, share * 100 if share <= 1 else share))
    fig, ax = plt.subplots(figsize=(7.6, 4.6))
    # gray background series
    for name, pts in series.items():
        if not any(k in name for k in movers):
            ax.plot([p[0] for p in pts], [p[1] for p in pts],
                    color='#CCCCCC', lw=1, alpha=0.6, zorder=1)
    # highlighted movers
    for key, color in movers.items():
        match = next((n for n in series if key in n), None)
        if match:
            pts = series[match]
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=color, lw=2.4,
                    marker='.', ms=5, label=match, zorder=3)
    ax.set_xlabel('Year'); ax.set_ylabel('Share of research articles (%)')
    ax.set_title('Topic share over time')
    ax.legend(fontsize=7.8, loc='upper right', ncol=1)
    ax.set_ylim(bottom=0)
    save(fig, 'fig4_topic_share')


# ---------------------------------------------------------------- Figure 5
def fig5_north_south_leadership():
    hi = tuple(HIGH_INCOME_ISO2)
    q = f"""WITH pc AS (
        SELECT DISTINCT a.openalex_id wid, w.publication_year yr, a.institution_country cc
        FROM authorships a JOIN works w ON a.openalex_id = w.openalex_id
        WHERE w.publication_year BETWEEN {Y0} AND {Y1}
          AND (w.topic_category IS NULL OR w.topic_category NOT IN ('Z'))
          AND (w.method_type IS NULL OR w.method_type NOT IN ('M14','M15','M18'))
          AND a.position IN ('first','last')
          AND a.institution_country IS NOT NULL AND a.institution_country <> ''),
    pg AS (SELECT wid, yr,
             MAX(CASE WHEN cc IN {hi} THEN 1 ELSE 0 END) has_n,
             MAX(CASE WHEN cc IN {hi} THEN 0 ELSE 1 END) has_s FROM pc GROUP BY 1,2)
    SELECT yr,
      100.0*SUM(CASE WHEN has_s=1 AND has_n=0 THEN 1 ELSE 0 END)/COUNT(*) south,
      100.0*SUM(CASE WHEN has_s=1 AND has_n=1 THEN 1 ELSE 0 END)/COUNT(*) mixed,
      100.0*SUM(CASE WHEN has_n=1 AND has_s=0 THEN 1 ELSE 0 END)/COUNT(*) north
    FROM pg GROUP BY yr ORDER BY yr"""
    rows = con.execute(q).fetchall()
    yrs = [r[0] for r in rows]
    south = [r[1] for r in rows]; both = [r[2] for r in rows]; north = [r[3] for r in rows]
    fig, ax = plt.subplots(figsize=(7.4, 4.4))
    ax.stackplot(yrs, south, both, north,
                 labels=['Low- & middle-income led', 'Both (mixed)', 'High-income led'],
                 colors=['#E69F00', '#BBBBBB', '#0072B2'], alpha=0.9)
    ax.set_xlabel('Year'); ax.set_ylabel('Share of research led (%)')
    ax.set_title('Global North vs South research leadership over time')
    ax.set_ylim(0, 100); ax.set_xlim(Y0, Y1)
    ax.legend(loc='center right', fontsize=8.5)
    save(fig, 'fig5_north_south_leadership')


if __name__ == '__main__':
    fig1_publications_by_year()
    fig2_funder_concentration()
    fig3_externally_led()
    fig4_topic_share()
    fig5_north_south_leadership()
    print("done ->", OUT)
