"""
dashboard/pages/institutions.py

Institutions: Who produces global health research?

Section arc (macro story -> drill-down tools -> supplementary):
- Top institutions by output, with a first/last-author summary bar
- Where research is led: Global North vs South over time
- Rising institutions (recent-momentum scatter)
- Research influence by citations
- Collaboration hubs (breadth of partnerships)
- Institution dossier and head-to-head comparison (drill-down tools)
- Commentary & editorial publications (the discourse lens)
"""

import plotly.express as px
import streamlit as st

from dashboard.components import (
    check_data_ready, metric_row, section_header, download_csv_button,
    page_subtitle,
)
from dashboard.constants import (
    TOPIC_LABELS, METHOD_LABELS, TOPIC_COLORS, NON_EMPIRICAL_METHODS,
    UNCATEGORIZED_TOPICS, QUAL_PALETTE, CHART_TEMPLATE, CHART_HEIGHT,
    CHART_HEIGHT_TALL, iso2_to_country_name, HIGH_INCOME_ISO2,
    country_color_map, institution_label, income_group,
    income_tier, INCOME_TIER_COLORS, INCOME_TIER_ORDER,
)
from dashboard.db import (
    query_df, query_scalar, build_where_clause, shared_institution_names,
)


def page():
    st.title('Institutions')
    page_subtitle(
        'Who produces global health research? How concentrated is '
        'production, and which institutions are on the rise?'
    )

    if not check_data_ready():
        return

    year_range = st.session_state.get('year_range', (2010, 2025))
    topics = st.session_state.get('selected_topics', [])
    where, where_params = build_where_clause(
        year_range=year_range, topics=topics or None)
    # Exclude uncategorized topics and non-empirical methods from analysis
    uc_placeholders = ', '.join(['?'] * len(UNCATEGORIZED_TOPICS))
    uc_clause = f" AND (w.topic_category IS NULL OR w.topic_category NOT IN ({uc_placeholders}))"
    ne_placeholders = ', '.join(['?'] * len(NON_EMPIRICAL_METHODS))
    ne_clause = (f" AND (w.method_type IS NULL "
                 f"OR w.method_type NOT IN ({ne_placeholders}))")
    # Restrict to the research-article subset: exclude papers with no usable
    # abstract (never classified). Their content could not be verified and the
    # records are more likely to be non-articles, so they are dropped from all
    # institution analyses, consistent with the rest of the analytic lenses.
    base_where = (f"WHERE TRUE {where}{uc_clause}{ne_clause} "
                  f"AND w.classified_topic AND w.classified_method")
    params = where_params + list(UNCATEGORIZED_TOPICS) + list(NON_EMPIRICAL_METHODS)

    # Umbrella names (e.g. "Ministry of Health") that OpenAlex shares across
    # many national institution IDs; disambiguated by country everywhere below.
    shared_names = shared_institution_names()

    def _label(df):
        """Apply the shared-name country disambiguation to a name/country df."""
        df['institution'] = [
            institution_label(n, c, shared_names)
            for n, c in zip(df['institution'], df['country'])
        ]
        return df

    # ------------------------------------------------------------------
    # Shared production data (computed once; reused by the summary bar and
    # the rising-institutions scatter). Grouped by institution_id so each
    # distinct entity is counted separately even when names collide.
    # ------------------------------------------------------------------
    df_all_inst = query_df(
        f"""SELECT ANY_VALUE(a.institution_name) AS institution,
                   ANY_VALUE(a.institution_country) AS country,
                   COUNT(DISTINCT a.openalex_id) AS n
            FROM authorships a
            JOIN works w ON a.openalex_id = w.openalex_id
            {base_where}
            AND a.institution_name IS NOT NULL
            AND a.institution_name != ''
            AND a.position IN ('first', 'last')
            GROUP BY a.institution_id
            ORDER BY n DESC""",
        tuple(params),
    )
    if not df_all_inst.empty:
        _label(df_all_inst)

    total_papers_count = query_scalar(
        f"SELECT COUNT(*) FROM works w {base_where}", tuple(params)
    ) or 0
    min_inst_papers = max(3, min(20, int(total_papers_count * 0.005)))

    # Rising = recent momentum, not a split of the whole period. Compare a
    # trailing window against the window immediately before it, so "rising"
    # reflects the last few years rather than a decade-old surge.
    span = int(year_range[1]) - int(year_range[0]) + 1
    win = max(1, min(3, span // 2))
    recent_lo = int(year_range[1]) - win + 1
    prior_hi = recent_lo - 1
    prior_lo = prior_hi - win + 1

    df_inst_growth = query_df(
        f"""WITH yearly AS (
                SELECT a.institution_id AS iid,
                       ANY_VALUE(a.institution_name) AS name,
                       ANY_VALUE(a.institution_country) AS country,
                       w.publication_year AS year,
                       COUNT(DISTINCT w.openalex_id) AS n
                FROM authorships a
                JOIN works w ON a.openalex_id = w.openalex_id
                {base_where}
                AND a.institution_name IS NOT NULL
                AND a.institution_name != ''
                AND a.position IN ('first', 'last')
                GROUP BY a.institution_id, w.publication_year
            ),
            inst_total AS (
                SELECT iid, SUM(n) AS total FROM yearly GROUP BY iid
                HAVING total >= ?
            ),
            windows AS (
                SELECT y.iid,
                       ANY_VALUE(y.name) AS name,
                       ANY_VALUE(y.country) AS country,
                       SUM(CASE WHEN y.year >= ?
                           THEN y.n ELSE 0 END) AS recent,
                       SUM(CASE WHEN y.year BETWEEN ? AND ?
                           THEN y.n ELSE 0 END) AS earlier
                FROM yearly y
                JOIN inst_total it ON y.iid = it.iid
                GROUP BY y.iid
            )
            SELECT wd.name AS institution,
                   wd.country AS country,
                   it.total,
                   wd.recent, wd.earlier,
                   CASE WHEN wd.earlier > 0
                        THEN (wd.recent - wd.earlier) * 100.0 / wd.earlier
                        ELSE 100 END AS growth_pct
            FROM windows wd
            JOIN inst_total it ON wd.iid = it.iid
            WHERE wd.earlier > 0
            ORDER BY it.total DESC""",
        tuple(params + [min_inst_papers, recent_lo, prior_lo, prior_hi]),
    )
    if not df_inst_growth.empty:
        df_inst_growth = df_inst_growth[
            df_inst_growth['institution'].str.strip().astype(bool)
        ].copy()
        _label(df_inst_growth)

    # ------------------------------------------------------------------
    # Summary bar (top-of-page indicators; no section header of its own)
    # ------------------------------------------------------------------
    if not df_all_inst.empty:
        n_inst = len(df_all_inst)
        total_fl = int(df_all_inst['n'].sum())
        top_inst = df_all_inst.iloc[0]
        top_share = top_inst['n'] / total_fl * 100 if total_fl else 0
        top10_k = max(1, n_inst // 10)
        top10_share = (df_all_inst['n'].head(top10_k).sum() / total_fl * 100
                       if total_fl else 0)

        # Fastest riser: growth rate is only meaningful off a non-trivial
        # base, so require a solid earlier-window count and degrade
        # gracefully when filters shrink the pool.
        riser = None
        if not df_inst_growth.empty:
            pool = df_inst_growth
            for floor in (8, 5, 3):
                candidate = df_inst_growth[df_inst_growth['earlier'] >= floor]
                if not candidate.empty:
                    pool = candidate
                    break
            riser = pool.sort_values('growth_pct', ascending=False).iloc[0]

        # Allow long institution names to wrap in the metric value.
        st.markdown(
            '<style>'
            '[data-testid="stMetricValue"],'
            '[data-testid="stMetricValue"] > div,'
            '[data-testid="stMetricValue"] p{'
            'white-space:normal !important;overflow:visible !important;'
            'text-overflow:clip !important;overflow-wrap:anywhere;'
            'line-height:1.25}'
            '[data-testid="stMetricValue"] p{font-size:1.5rem;margin:0}'
            '</style>',
            unsafe_allow_html=True,
        )
        mc = st.columns(4)
        with mc[0]:
            st.metric('Most prolific institution', top_inst['institution'])
            st.caption(f"{top_share:.1f}% of first/last-author papers")
        with mc[1]:
            if riser is not None:
                st.metric('Fastest-rising institution', riser['institution'])
                st.caption(
                    f"output up {riser['growth_pct']:.0f}%, {recent_lo}–"
                    f"{int(year_range[1])} vs {prior_lo}–{prior_hi}"
                )
            else:
                st.metric('Fastest-rising institution', '—')
        mc[2].metric('Top 10% share', f"{top10_share:.0f}%")
        mc[2].caption('of papers, from the most prolific 10% of institutions')
        mc[3].metric('Institutions producing research', f"{n_inst:,}")

        st.info(
            'Production is credited to an institution when its researchers are '
            '**first or last author**, the positions that usually signal study '
            'leadership. Papers where the institution appears only in a middle '
            'position are not counted here.',
            icon=':material/info:',
        )

    # ------------------------------------------------------------------
    # Top institutions
    # ------------------------------------------------------------------
    section_header(
        'Top Institutions by Paper Count',
        'Institutions ranked by total papers with first or last authorship.',
    )

    col1, _ = st.columns([1, 3])
    with col1:
        top_n = st.slider('Show top N', 10, 50, 25, key='inst_top_n')

    df_inst = query_df(
        f"""SELECT a.institution_id AS iid,
                   ANY_VALUE(a.institution_name) AS institution,
                   ANY_VALUE(a.institution_country) AS country,
                   COUNT(DISTINCT a.openalex_id) AS n_papers
            FROM authorships a
            JOIN works w ON a.openalex_id = w.openalex_id
            {base_where}
            AND a.institution_name IS NOT NULL
            AND a.institution_name != ''
            AND a.position IN ('first', 'last')
            GROUP BY a.institution_id
            ORDER BY n_papers DESC
            LIMIT ?""",
        tuple(params + [top_n]),
    )

    if not df_inst.empty:
        _label(df_inst)
        total_papers = query_scalar(
            f"SELECT COUNT(*) FROM works w {base_where}", tuple(params)
        )

        top3_share = (df_inst.head(3)['n_papers'].sum() / total_papers * 100
                       if total_papers else 0)

        metric_row([
            ('Top 3 Share', f"{top3_share:.1f}%", None),
            ('Countries Represented',
             int(df_inst['country'].nunique()), None),
        ])

        df_inst['country_name'] = df_inst['country'].apply(iso2_to_country_name)
        fig = px.bar(
            df_inst, y='institution', x='n_papers', orientation='h',
            color='country_name',
            color_discrete_map=country_color_map(
                df_inst['country_name'].unique()),
            labels={'n_papers': 'Papers', 'institution': '',
                    'country_name': 'Country'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=max(500, len(df_inst) * 25),
            yaxis={'categoryorder': 'total ascending'},
            legend=dict(font=dict(size=9)),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Where research is led: Global North vs South, over time
    # ------------------------------------------------------------------
    section_header(
        'Where Research Is Led: Global North vs South',
        'Share of research led (first or last author) by high-income versus '
        'low- and middle-income institutions, by year.',
    )

    hic_list = sorted(HIGH_INCOME_ISO2)
    hic_ph = ', '.join(['?'] * len(hic_list))
    df_ns = query_df(
        f"""WITH pc AS (
                SELECT DISTINCT a.openalex_id AS wid,
                       w.publication_year AS yr,
                       a.institution_country AS cc
                FROM authorships a
                JOIN works w ON a.openalex_id = w.openalex_id
                {base_where}
                AND a.position IN ('first', 'last')
                AND a.institution_country IS NOT NULL
                AND a.institution_country <> ''
            ),
            pg AS (
                SELECT wid, yr,
                       MAX(CASE WHEN cc IN ({hic_ph}) THEN 1 ELSE 0 END) AS has_n,
                       MAX(CASE WHEN cc IN ({hic_ph}) THEN 0 ELSE 1 END) AS has_s
                FROM pc GROUP BY wid, yr
            )
            SELECT yr,
                   SUM(CASE WHEN has_n = 1 AND has_s = 0 THEN 1 ELSE 0 END) AS north,
                   SUM(CASE WHEN has_s = 1 AND has_n = 0 THEN 1 ELSE 0 END) AS south,
                   SUM(CASE WHEN has_n = 1 AND has_s = 1 THEN 1 ELSE 0 END) AS both
            FROM pg GROUP BY yr ORDER BY yr""",
        tuple(params + hic_list + hic_list),
    )

    if not df_ns.empty:
        df_ns['total'] = df_ns['north'] + df_ns['south'] + df_ns['both']
        df_ns = df_ns[df_ns['total'] >= 30]  # suppress thin years

    if not df_ns.empty:
        for col in ('north', 'south', 'both'):
            df_ns[col + '_pct'] = df_ns[col] / df_ns['total'] * 100

        first, last = df_ns.iloc[0], df_ns.iloc[-1]
        metric_row([
            (f"LMIC-led share, {int(last['yr'])}",
             f"{last['south_pct']:.0f}%", None),
            (f"Change since {int(first['yr'])}",
             f"{last['south_pct'] - first['south_pct']:+.0f} pp", None),
        ])

        label = {'south_pct': 'Low- & middle-income led',
                 'both_pct': 'Both (mixed)',
                 'north_pct': 'High-income led'}
        long = df_ns.melt(
            id_vars='yr',
            value_vars=['south_pct', 'both_pct', 'north_pct'],
            var_name='grp', value_name='pct')
        long['Led by'] = long['grp'].map(label)
        color_map = {'Low- & middle-income led': '#E69F00',
                     'Both (mixed)': '#BBBBBB',
                     'High-income led': '#0072B2'}
        order = ['Low- & middle-income led', 'Both (mixed)', 'High-income led']
        fig = px.area(
            long, x='yr', y='pct', color='Led by',
            category_orders={'Led by': order},
            color_discrete_map=color_map,
            labels={'yr': 'Year', 'pct': 'Share of research led (%)'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=CHART_HEIGHT, yaxis_range=[0, 100],
            legend=dict(orientation='h', y=1.1, title_text=''),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            'Each paper is assigned by the income group of its lead (first or '
            'last author) institutions: **Low- & middle-income led** when all '
            'lead institutions are in low- or middle-income countries, **High-'
            'income led** when all are in high-income countries, and **Both** '
            'when it spans the two. Groups follow the World Bank income '
            'classification (FY2024): low- and middle-income combines the low, '
            'lower-middle, and upper-middle income groups; high-income is the '
            'World Bank high-income group. Shares are of lead-institution '
            'papers; they describe where research is based, not credit within '
            'any single paper.'
        )
    else:
        st.info(
            'Not enough papers with institution-country data for this view '
            'under the selected filters.',
            icon=':material/info:',
        )

    # ------------------------------------------------------------------
    # Rising institutions scatter
    # ------------------------------------------------------------------
    section_header(
        'Rising Institutions',
        f'X = total papers, Y = recent growth ({recent_lo}–{int(year_range[1])} '
        f'vs {prior_lo}–{prior_hi}). Quadrants: established leaders '
        '(high volume, moderate growth), rising stars (growing fast), '
        'declining (negative growth), and niche (low volume, stable).',
    )

    if not df_inst_growth.empty:
        # Cap extreme growth values for visualization
        df_inst_growth['growth_capped'] = df_inst_growth['growth_pct'].clip(-100, 500)
        # Color by World Bank income tier so the legend stays legible and ties
        # to the North/South story.
        df_inst_growth['Income tier'] = (
            df_inst_growth['country'].apply(income_tier).fillna('Unknown'))

        # Reference lines and axis bounds come from the full qualifying set so
        # they stay fixed as the user filters.
        med_x = df_inst_growth['total'].median()
        x_max = df_inst_growth['total'].max()
        y_abs_max = max(abs(df_inst_growth['growth_capped'].min()),
                        abs(df_inst_growth['growth_capped'].max()), 10) * 1.2

        picks = st.multiselect(
            'Focus on specific institutions (optional):',
            options=sorted(df_inst_growth['institution'].tolist()),
            default=[], key='rising_focus',
            help='Leave empty to show all qualifying institutions.',
        )
        plot_df = (df_inst_growth[df_inst_growth['institution'].isin(picks)]
                   if picks else df_inst_growth).copy()

        fig = px.scatter(
            plot_df,
            x='total', y='growth_capped',
            color='Income tier',
            color_discrete_map={**INCOME_TIER_COLORS, 'Unknown': '#999999'},
            category_orders={'Income tier': INCOME_TIER_ORDER + ['Unknown']},
            hover_name='institution',
            hover_data={'growth_pct': ':.0f', 'recent': True, 'earlier': True,
                        'total': True, 'growth_capped': False,
                        'Income tier': False},
            labels={
                'total': 'Total Papers',
                'growth_capped': 'Growth Rate (%)',
            },
            template=CHART_TEMPLATE,
        )

        # Quadrant lines (references fixed to the full qualifying set)
        fig.add_hline(y=0, line_dash='dash', line_color='gray')
        fig.add_vline(x=med_x, line_dash='dash', line_color='gray')

        # Quadrant annotations positioned relative to axis bounds
        fig.add_annotation(
            x=x_max * 0.75, y=y_abs_max * 0.75,
            text='Established Leaders', showarrow=False,
            font=dict(color='gray', size=10),
        )
        fig.add_annotation(
            x=med_x * 0.3, y=y_abs_max * 0.75,
            text='Rising Stars', showarrow=False,
            font=dict(color='#009E73', size=10),
        )
        fig.add_annotation(
            x=x_max * 0.75, y=-y_abs_max * 0.75,
            text='Declining', showarrow=False,
            font=dict(color='#D55E00', size=10),
        )
        fig.add_annotation(
            x=med_x * 0.3, y=-y_abs_max * 0.75,
            text='Niche / Stable', showarrow=False,
            font=dict(color='gray', size=10),
        )

        fig.update_traces(marker=dict(size=8, opacity=0.7))
        fig.update_layout(
            height=CHART_HEIGHT_TALL,
            xaxis=dict(range=[0, x_max * 1.05]),
            yaxis=dict(range=[-y_abs_max, y_abs_max]),
            legend=dict(
                font=dict(size=10),
                title_text='Income tier',
                itemsizing='constant',
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

        n_qualified = len(df_inst_growth)
        st.caption(
            f'All {n_qualified} institutions with at least {min_inst_papers} '
            f'papers are shown by default; use the selector above to focus on '
            f'specific ones. The X-axis is total output volume; the Y-axis is '
            f'how much that output grew (or shrank) from its {prior_lo}–'
            f'{prior_hi} output to its {recent_lo}–{int(year_range[1])} output, '
            f'so it reflects recent momentum rather than change across the '
            f'whole period. **Rising Stars** (top-left) are smaller '
            f'institutions growing fast; **Established Leaders** (top-right) '
            f'combine high volume with growth; **Declining** (bottom-right) '
            f'have high output but are losing momentum. Points are colored by '
            f'the World Bank income group of the institution’s country (low, '
            f'lower-middle, upper-middle, or high income).'
        )
    else:
        st.info(
            f'No institutions meet the minimum paper threshold '
            f'({min_inst_papers} papers) for growth analysis. '
            f'This threshold adapts to corpus size: with more papers loaded, '
            f'more institutions will qualify.',
            icon=':material/info:',
        )

    # ------------------------------------------------------------------
    # Research influence (citations to research output)
    # ------------------------------------------------------------------
    section_header(
        'Research Influence by Citations',
        'Whose research the field cites most. Total citations capture overall '
        'influence; citations per paper capture impact per study.',
    )

    infl_mode = st.radio(
        'Rank institutions by',
        ['Total citations', 'Citations per paper'],
        horizontal=True, key='influence_rank_mode',
        help='Total = overall influence footprint (favors large producers). '
             'Per paper = average citations per research article, where '
             'smaller institutions can rank.',
    )

    df_infl_all = query_df(
        f"""WITH pi AS (
                SELECT a.institution_id AS iid,
                       a.openalex_id AS wid,
                       ANY_VALUE(a.institution_name) AS name,
                       ANY_VALUE(a.institution_country) AS country,
                       ANY_VALUE(w.cited_by_count) AS cites
                FROM authorships a
                JOIN works w ON a.openalex_id = w.openalex_id
                {base_where}
                AND a.institution_name IS NOT NULL
                AND a.institution_name != ''
                AND a.position IN ('first', 'last')
                GROUP BY a.institution_id, a.openalex_id
            )
            SELECT ANY_VALUE(name) AS institution,
                   ANY_VALUE(country) AS country,
                   COUNT(*) AS n_papers,
                   SUM(cites) AS total_citations,
                   AVG(cites) AS avg_citations
            FROM pi
            GROUP BY iid""",
        tuple(params),
    )

    if not df_infl_all.empty:
        _label(df_infl_all)
        if infl_mode == 'Total citations':
            df_infl = df_infl_all.sort_values(
                'total_citations', ascending=False).head(20).copy()
            xcol, xlab = 'total_citations', 'Total citations to research'
            top = df_infl.iloc[0]
            val_label = 'Citations to their research'
            val_str = f"{int(top['total_citations']):,}"
            top_label = 'Most-cited research (total)'
        else:
            # Impact per paper needs a stable base of research output.
            pool = df_infl_all
            for floor in (30, 15, 8):
                cand = df_infl_all[df_infl_all['n_papers'] >= floor]
                if not cand.empty:
                    pool = cand
                    break
            df_infl = pool.sort_values(
                'avg_citations', ascending=False).head(20).copy()
            df_infl['avg_citations'] = df_infl['avg_citations'].round(1)
            xcol, xlab = 'avg_citations', 'Citations per research paper'
            top = df_infl.iloc[0]
            val_label = 'Citations per paper'
            val_str = f"{top['avg_citations']:.0f}"
            top_label = 'Highest citations per paper'

        metric_row([
            (top_label, top['institution'], None),
            (val_label, val_str, None),
        ])

        df_infl['country_name'] = df_infl['country'].apply(iso2_to_country_name)
        fig = px.bar(
            df_infl, y='institution', x=xcol, orientation='h',
            color='country_name',
            color_discrete_map=country_color_map(
                df_infl['country_name'].unique()),
            hover_data=['n_papers'],
            labels={xcol: xlab, 'institution': '', 'country_name': 'Country',
                    'n_papers': 'Research papers'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=max(400, len(df_infl) * 28),
            yaxis={'categoryorder': 'total ascending'},
            legend=dict(font=dict(size=9)),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            'Citations to each institution’s research articles (commentary '
            'excluded). **Total citations** rewards both output and being '
            'cited, so large producers lead; **Citations per paper** is the '
            'average per article, surfacing influence per study. Citations '
            'accrue over time, so recent work is undercounted. Hover for each '
            'institution’s research-paper count.'
        )
    else:
        st.info(
            'No research papers with citation data for the selected filters.',
            icon=':material/info:',
        )

    # ------------------------------------------------------------------
    # Collaboration hubs (breadth of institutional partnerships)
    # ------------------------------------------------------------------
    section_header(
        'Collaboration Hubs',
        'Institutions that co-author with the widest range of partners: the '
        'connectors that hold the research network together.',
    )

    df_hubs = query_df(
        f"""WITH pairs AS (
                SELECT DISTINCT a1.institution_id AS iid,
                       a2.institution_id AS pid
                FROM authorships a1
                JOIN authorships a2 ON a1.openalex_id = a2.openalex_id
                JOIN works w ON a1.openalex_id = w.openalex_id
                {base_where}
                AND a1.institution_name IS NOT NULL AND a1.institution_name != ''
                AND a2.institution_name IS NOT NULL AND a2.institution_name != ''
                AND a1.institution_id <> a2.institution_id
            ),
            cc AS (
                SELECT institution_id AS iid,
                       ANY_VALUE(institution_name) AS name,
                       ANY_VALUE(institution_country) AS country
                FROM authorships
                WHERE institution_name IS NOT NULL AND institution_name != ''
                GROUP BY institution_id
            )
            SELECT cc.name AS institution,
                   cc.country AS country,
                   COUNT(DISTINCT p.pid) AS partners
            FROM pairs p JOIN cc ON p.iid = cc.iid
            GROUP BY p.iid, cc.name, cc.country
            ORDER BY partners DESC
            LIMIT 20""",
        tuple(params),
    )

    if not df_hubs.empty:
        _label(df_hubs)
        metric_row([
            ('Most-connected institution',
             df_hubs.iloc[0]['institution'], None),
            ('Distinct partner institutions',
             f"{int(df_hubs.iloc[0]['partners']):,}", None),
        ])

        df_hubs['country_name'] = df_hubs['country'].apply(iso2_to_country_name)
        fig = px.bar(
            df_hubs, y='institution', x='partners', orientation='h',
            color='country_name',
            color_discrete_map=country_color_map(
                df_hubs['country_name'].unique()),
            labels={'partners': 'Distinct partner institutions',
                    'institution': '', 'country_name': 'Country'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=max(400, len(df_hubs) * 28),
            yaxis={'categoryorder': 'total ascending'},
            legend=dict(font=dict(size=9)),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            'The number of distinct partner institutions each has co-authored '
            'with, across all author positions: a measure of network reach and '
            'brokerage. Institutions are counted by their OpenAlex ID, so each '
            'national body (for example a specific Ministry of Health) is its '
            'own entity rather than a merged umbrella.'
        )
    else:
        st.info(
            'No collaboration data for the selected filters.',
            icon=':material/info:',
        )

    # ------------------------------------------------------------------
    # Institution dossier (drill-down)
    # ------------------------------------------------------------------
    if not df_inst.empty:
        section_header(
            'Institution Dossier',
            'Select an institution to see its topic specialization, '
            'growth trajectory, and geographic reach.',
        )

        iid_by_label = dict(zip(df_inst['institution'], df_inst['iid']))
        selected_inst = st.selectbox(
            'Select institution:',
            options=df_inst['institution'].tolist(),
            key='selected_institution',
        )
        selected_iid = iid_by_label.get(selected_inst)

        if selected_inst:
            col_a, col_b = st.columns(2)

            # Topic specialization (radar-like bar chart)
            with col_a:
                df_topic = query_df(
                    f"""SELECT w.topic_category AS cat,
                               COUNT(DISTINCT w.openalex_id) AS n
                        FROM authorships a
                        JOIN works w ON a.openalex_id = w.openalex_id
                        {base_where}
                        AND a.institution_id = ?
                        AND a.position IN ('first', 'last')
                        AND w.topic_category IS NOT NULL
                        GROUP BY w.topic_category
                        ORDER BY n DESC""",
                    tuple(params + [selected_iid]),
                )
                if not df_topic.empty:
                    df_topic['label'] = df_topic['cat'].map(
                        lambda c: TOPIC_LABELS.get(c, c)
                    )
                    fig = px.bar(
                        df_topic, y='label', x='n', orientation='h',
                        color='cat', color_discrete_map=TOPIC_COLORS,
                        title='Topic Specialization',
                        labels={'n': 'Papers', 'label': ''},
                        template=CHART_TEMPLATE,
                    )
                    fig.update_layout(
                        height=max(300, len(df_topic) * 28),
                        showlegend=False,
                        yaxis={'categoryorder': 'total ascending'},
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # Method profile
            with col_b:
                df_method = query_df(
                    f"""SELECT w.method_type AS method,
                               COUNT(DISTINCT w.openalex_id) AS n
                        FROM authorships a
                        JOIN works w ON a.openalex_id = w.openalex_id
                        {base_where}
                        AND a.institution_id = ?
                        AND a.position IN ('first', 'last')
                        AND w.method_type IS NOT NULL
                        GROUP BY w.method_type
                        ORDER BY n DESC""",
                    tuple(params + [selected_iid]),
                )
                if not df_method.empty:
                    df_method['label'] = df_method['method'].map(
                        lambda m: METHOD_LABELS.get(m, m)
                    )
                    fig = px.bar(
                        df_method, y='label', x='n', orientation='h',
                        title='Methods Profile',
                        labels={'n': 'Papers', 'label': ''},
                        template=CHART_TEMPLATE,
                    )
                    fig.update_traces(marker_color='#009E73')
                    fig.update_layout(
                        height=max(300, len(df_method) * 28),
                        yaxis={'categoryorder': 'total ascending'},
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # Growth trajectory
            df_growth = query_df(
                f"""SELECT w.publication_year AS year,
                           COUNT(DISTINCT w.openalex_id) AS n
                    FROM authorships a
                    JOIN works w ON a.openalex_id = w.openalex_id
                    {base_where}
                    AND a.institution_id = ?
                    AND a.position IN ('first', 'last')
                    GROUP BY w.publication_year
                    ORDER BY year""",
                tuple(params + [selected_iid]),
            )

            if not df_growth.empty:
                fig = px.line(
                    df_growth, x='year', y='n',
                    title=f'Publication Trend: {selected_inst}',
                    labels={'year': 'Year', 'n': 'Papers'},
                    template=CHART_TEMPLATE, markers=True,
                )
                fig.update_traces(line=dict(color='#0072B2', width=2))
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

            # Top collaborating institutions
            df_collabs = query_df(
                f"""SELECT ANY_VALUE(a2.institution_name) AS institution,
                           ANY_VALUE(a2.institution_country) AS country,
                           COUNT(DISTINCT w.openalex_id) AS n
                    FROM authorships a1
                    JOIN authorships a2 ON a1.openalex_id = a2.openalex_id
                    JOIN works w ON a1.openalex_id = w.openalex_id
                    {base_where}
                    AND a1.institution_id = ?
                    AND a2.institution_name IS NOT NULL
                    AND a2.institution_name != ''
                    AND a2.institution_id <> ?
                    GROUP BY a2.institution_id
                    ORDER BY n DESC LIMIT 10""",
                tuple(params + [selected_iid, selected_iid]),
            )
            if not df_collabs.empty:
                _label(df_collabs)
                df_collabs = df_collabs.rename(
                    columns={'institution': 'collaborator'})
                st.markdown(f'#### Top Collaborating Institutions')
                fig = px.bar(
                    df_collabs, y='collaborator', x='n', orientation='h',
                    labels={'n': 'Co-authored Papers', 'collaborator': ''},
                    template=CHART_TEMPLATE,
                )
                fig.update_traces(marker_color='#E69F00')
                fig.update_layout(
                    height=max(300, len(df_collabs) * 30),
                    yaxis={'categoryorder': 'total ascending'},
                )
                st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Head-to-head comparison (drill-down)
    # ------------------------------------------------------------------
    if not df_inst.empty:
        section_header(
            'Head-to-Head Comparison',
            'Select 2–3 institutions to compare their topic portfolios '
            'and research output profiles.',
        )

        iid_by_label = dict(zip(df_inst['institution'], df_inst['iid']))
        inst_options = [i for i in df_inst['institution'].tolist() if i.strip()]
        compare_insts = st.multiselect(
            'Select institutions to compare:',
            options=inst_options,
            default=inst_options[:min(2, len(inst_options))],
            max_selections=3,
            key='compare_institutions',
        )

        if len(compare_insts) >= 2:
            compare_iids = [iid_by_label[i] for i in compare_insts
                            if i in iid_by_label]
            placeholders = ', '.join(['?'] * len(compare_iids))
            df_compare = query_df(
                f"""SELECT ANY_VALUE(a.institution_name) AS institution,
                           ANY_VALUE(a.institution_country) AS country,
                           w.topic_category AS cat,
                           COUNT(DISTINCT w.openalex_id) AS n
                    FROM authorships a
                    JOIN works w ON a.openalex_id = w.openalex_id
                    {base_where}
                    AND a.institution_id IN ({placeholders})
                    AND a.position IN ('first', 'last')
                    AND w.topic_category IS NOT NULL
                    GROUP BY a.institution_id, w.topic_category""",
                tuple(params + compare_iids),
            )

            if not df_compare.empty:
                _label(df_compare)
                # Normalize to percentages
                totals = df_compare.groupby('institution')['n'].sum().reset_index()
                totals.columns = ['institution', 'total']
                df_compare = df_compare.merge(totals, on='institution')
                df_compare['pct'] = (
                    df_compare['n'] / df_compare['total'] * 100
                ).round(1)
                df_compare['topic'] = df_compare['cat'].map(
                    lambda c: TOPIC_LABELS.get(c, c)
                )

                fig = px.bar(
                    df_compare, x='topic', y='pct', color='institution',
                    barmode='group',
                    color_discrete_sequence=QUAL_PALETTE,
                    labels={'pct': 'Portfolio Share (%)', 'topic': '',
                            'institution': 'Institution'},
                    template=CHART_TEMPLATE,
                )
                fig.update_layout(
                    height=CHART_HEIGHT, xaxis_tickangle=-45,
                    legend=dict(orientation='h', y=1.15, font=dict(size=9)),
                )
                st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Commentary & Editorial use by institutions
    # ------------------------------------------------------------------
    # This is the ONLY section that includes commentary/editorials. Every
    # other analysis excludes them via ne_clause, so this needs its own
    # WHERE that keeps the sidebar filters but selects only these methods.
    comm_where = f"WHERE TRUE {where} AND w.method_type IN ({ne_placeholders})"
    comm_params = where_params + list(NON_EMPIRICAL_METHODS)

    section_header(
        'Commentary & Editorial Publications',
        'Every other analysis on this dashboard excludes commentary, '
        'editorials, and perspectives. This section looks only at them, to '
        'show which institutions publish the most of this non-empirical, '
        'discourse-shaping work.',
    )

    total_ne = query_scalar(
        f"SELECT COUNT(*) FROM works w {comm_where}", tuple(comm_params)
    ) or 0

    rank_mode = st.radio(
        'Rank institutions by',
        ['Volume of commentary', 'Citations to commentary'],
        horizontal=True, key='commentary_rank_mode',
        help='Volume = who publishes the most. Citations = whose commentary '
             'the field engages with most.',
    )

    if rank_mode == 'Volume of commentary':
        df_ne_inst = query_df(
            f"""SELECT ANY_VALUE(a.institution_name) AS institution,
                       ANY_VALUE(a.institution_country) AS country,
                       COUNT(DISTINCT a.openalex_id) AS n_papers
                FROM authorships a
                JOIN works w ON a.openalex_id = w.openalex_id
                {comm_where}
                AND a.institution_name IS NOT NULL
                AND a.institution_name != ''
                AND a.position IN ('first', 'last')
                GROUP BY a.institution_id
                ORDER BY n_papers DESC
                LIMIT 20""",
            tuple(comm_params),
        )

        if not df_ne_inst.empty:
            _label(df_ne_inst)
            metric_row([
                ('Total Commentary/Editorials', total_ne, None),
                ('Most commentary published',
                 df_ne_inst.iloc[0]['institution'], None),
            ])

            df_ne_inst['country_name'] = df_ne_inst['country'].apply(
                iso2_to_country_name)
            fig = px.bar(
                df_ne_inst, y='institution', x='n_papers', orientation='h',
                color='country_name',
                color_discrete_map=country_color_map(
                    df_ne_inst['country_name'].unique()),
                labels={'n_papers': 'Commentary/editorial pieces',
                        'institution': '', 'country_name': 'Country'},
                template=CHART_TEMPLATE,
            )
            fig.update_layout(
                height=max(400, len(df_ne_inst) * 28),
                yaxis={'categoryorder': 'total ascending'},
                legend=dict(font=dict(size=9)),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption(
                'Institutions that publish the most commentary, editorials, '
                'and perspectives. This is raw volume, so the largest research '
                'producers tend to lead. **Citations to commentary** instead '
                'shows whose contributions the field engages with most.'
            )
        else:
            st.info(
                'No commentary/editorial publications found for the selected '
                'filters.',
                icon=':material/info:',
            )
    else:
        # Discourse impact: total citations to an institution's commentary and
        # editorials. Captures both how much it contributes to the conversation
        # and how widely those contributions are picked up. Dedupe paper-
        # institution pairs first so a first+last match cannot double-count.
        df_disc = query_df(
            f"""WITH pi AS (
                    SELECT a.institution_id AS iid,
                           a.openalex_id AS wid,
                           ANY_VALUE(a.institution_name) AS name,
                           ANY_VALUE(a.institution_country) AS country,
                           ANY_VALUE(w.cited_by_count) AS cites
                    FROM authorships a
                    JOIN works w ON a.openalex_id = w.openalex_id
                    {comm_where}
                    AND a.institution_name IS NOT NULL
                    AND a.institution_name != ''
                    AND a.position IN ('first', 'last')
                    GROUP BY a.institution_id, a.openalex_id
                )
                SELECT ANY_VALUE(name) AS institution,
                       ANY_VALUE(country) AS country,
                       COUNT(*) AS n_papers,
                       SUM(cites) AS total_citations
                FROM pi
                GROUP BY iid
                ORDER BY total_citations DESC
                LIMIT 20""",
            tuple(comm_params),
        )

        if not df_disc.empty:
            _label(df_disc)
            metric_row([
                ('Total Commentary/Editorials', total_ne, None),
                ('Most-cited commentary voice',
                 df_disc.iloc[0]['institution'], None),
            ])

            df_disc['country_name'] = df_disc['country'].apply(
                iso2_to_country_name)
            fig = px.bar(
                df_disc, y='institution', x='total_citations', orientation='h',
                color='country_name',
                color_discrete_map=country_color_map(
                    df_disc['country_name'].unique()),
                hover_data=['n_papers'],
                labels={'total_citations': 'Citations to their commentary',
                        'institution': '', 'country_name': 'Country',
                        'n_papers': 'Commentary pieces'},
                template=CHART_TEMPLATE,
            )
            fig.update_layout(
                height=max(400, len(df_disc) * 28),
                yaxis={'categoryorder': 'total ascending'},
                legend=dict(font=dict(size=9)),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption(
                'Total citations to each institution’s commentary, editorials, '
                'and perspectives, ranking the most-engaged-with voices in the '
                'field’s discourse. It rewards both publishing often and being '
                'cited widely, so an institution rises by shaping the '
                'conversation, not by the kind of journal it publishes in. '
                'Hover for the number of pieces behind each bar.'
            )
        else:
            st.info(
                'No commentary/editorial publications found for the selected '
                'filters.',
                icon=':material/info:',
            )
