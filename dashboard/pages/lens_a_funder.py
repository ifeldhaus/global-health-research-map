"""
dashboard/pages/lens_a_funder.py

Lens A (Funder Power): Does funder concentration determine what gets studied?

Analytical interactions:
- Funder dossier drill-down (click funder → portfolio, geography, trend)
- Comparative funder analysis (2-3 funders side-by-side)
- Funding-research gap scatter (pub share vs funding share)
- HHI decomposition (click year → who drove the change)
- Funder×topic heatmap with trend drill-down
- Unfunded analysis (who does research without funding)
"""

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from dashboard.components import (
    check_data_ready, metric_row, section_header, download_csv_button,
    page_subtitle,
)
from dashboard.constants import (
    TOPIC_COLORS, TOPIC_LABELS, FUNDER_CATEGORY_COLORS,
    NON_EMPIRICAL_METHODS, UNCATEGORIZED_TOPICS, iso2_to_country_name,
    CHART_TEMPLATE, CHART_HEIGHT, CHART_HEIGHT_TALL, DIVERGING_COLORSCALE,
    funder_display_name, WHO_REGIONS, WHO_REGION_NAMES, WHO_REGION_COLORS,
    QUAL_PALETTE,
)
from dashboard.db import query_df, query_scalar, build_where_clause


def page():
    st.title('Funder Power')
    page_subtitle(
        'Who funds global health research? How concentrated is that funding? '
        'What do the largest funders support?'
    )

    if not check_data_ready(require_topics=True):
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
    base_where = f"WHERE TRUE {where}{uc_clause}{ne_clause}"
    params = where_params + list(UNCATEGORIZED_TOPICS) + list(NON_EMPIRICAL_METHODS)

    # Funder-acknowledgment rates for the scoping note, computed from the
    # current filter so they track the applied years and topics. Denominators
    # match the paper: research articles (usable abstract, classified to a
    # topic and an empirical or review design) versus non-empirical works.
    def _ack_rate(cond, cond_params):
        denom = query_scalar(
            f"SELECT COUNT(*) FROM works w WHERE TRUE {where} AND {cond}",
            tuple(where_params + cond_params)) or 0
        funded = query_scalar(
            f"""SELECT COUNT(DISTINCT w.openalex_id) FROM works w
                JOIN grants g ON w.openalex_id = g.openalex_id
                WHERE TRUE {where} AND {cond}""",
            tuple(where_params + cond_params)) or 0
        return (100 * funded / denom) if denom else 0
    _art_cond = (f"w.classified_topic AND w.topic_category NOT IN ({uc_placeholders}) "
                 f"AND w.classified_method AND w.method_type NOT IN ({ne_placeholders})")
    rate_article = _ack_rate(_art_cond,
                             list(UNCATEGORIZED_TOPICS) + list(NON_EMPIRICAL_METHODS))
    rate_nonemp = _ack_rate(f"w.method_type IN ({ne_placeholders})",
                            list(NON_EMPIRICAL_METHODS))

    # Total funded papers in the current filter: denominator reused below.
    # Counts works with at least one grant (any funder, whether or not the
    # funder_id resolves to the funders table), matching the paper's "funded
    # articles" denominator (n = 14,999). Restricting to resolvable funders
    # would shrink the denominator and inflate every share below.
    total_funded = query_scalar(
        f"""SELECT COUNT(DISTINCT g.openalex_id)
            FROM grants g
            JOIN works w ON g.openalex_id = w.openalex_id
            {base_where}""",
        tuple(params),
    ) or 0

    # ------------------------------------------------------------------
    # 1. Concentration framing: metrics sit directly under the intro info
    #    box (no section header of their own).
    # ------------------------------------------------------------------
    df_rank = query_df(
        f"""SELECT f.canonical_name AS funder,
                   COUNT(DISTINCT g.openalex_id) AS n
            FROM grants g
            JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
            JOIN works w ON g.openalex_id = w.openalex_id
            {base_where}
            GROUP BY f.canonical_name
            ORDER BY n DESC""",
        tuple(params),
    )

    if not df_rank.empty and total_funded:
        def _footprint(names):
            """Share of funded papers naming at least one of `names`."""
            if not names:
                return 0.0
            ph = ', '.join(['?'] * len(names))
            hit = query_scalar(
                f"""SELECT COUNT(DISTINCT g.openalex_id)
                    FROM grants g
                    JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
                    JOIN works w ON g.openalex_id = w.openalex_id
                    {base_where} AND f.canonical_name IN ({ph})""",
                tuple(params + list(names)),
            ) or 0
            return 100 * hit / total_funded

        top5 = _footprint(df_rank['funder'].head(5).tolist())
        top10 = _footprint(df_rank['funder'].head(10).tolist())
        mc = st.columns(3)
        mc[0].metric('Distinct funders', f"{len(df_rank):,}")
        mc[1].metric('Top 5 funders', f"{top5:.0f}%",
                     help='Share of funded papers naming at least one of the '
                          'five largest funders.')
        mc[2].metric('Top 10 funders', f"{top10:.0f}%",
                     help='Share of funded papers naming at least one of the '
                          'ten largest funders.')
    # ------------------------------------------------------------------
    # Scoping note: under the summary indicators, above the first section.
    # ------------------------------------------------------------------
    st.info(
        'This page covers the **research corpus only**: commentary, editorials, '
        'and perspectives are excluded. They are opinion and discourse rather '
        'than funded research projects, and name a funder far less often '
        f'({rate_nonemp:.0f}%, vs **{rate_article:.0f}% of research articles**). '
        'Acknowledgment is also incompletely '
        'recorded (and less so in earlier years), so these figures reflect '
        '*recorded* funding, not all funding.\n\n'
        'Every figure counts papers that **acknowledge** a funder, a proxy for a '
        'funder\'s footprint and clout in a research space, **not a measure of '
        'money**. OpenAlex records which funders are named on a paper, never how '
        'much they gave. "Funding share" therefore means the share of '
        'grant-acknowledging papers, not dollars.',
        icon=':material/info:',
    )

    # ------------------------------------------------------------------
    # 2. Top funders (who, individually)
    # ------------------------------------------------------------------
    section_header(
        'Top Funders by Paper Count',
        'The individual organizations named on the most research papers.',
    )
    top_n = st.slider('Top N funders', 5, 30, 15, key='funder_top_n')

    df_funders = query_df(
        f"""SELECT f.canonical_name AS funder,
                   f.funder_category AS category,
                   f.funder_country AS country,
                   COUNT(DISTINCT g.openalex_id) AS n_papers
            FROM grants g
            JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
            JOIN works w ON g.openalex_id = w.openalex_id
            {base_where}
            GROUP BY f.canonical_name, f.funder_category, f.funder_country
            ORDER BY n_papers DESC
            LIMIT ?""",
        tuple(params + [top_n]),
    )

    selected_funder = None
    if df_funders.empty:
        st.info(
            'No funder data available for the selected filters. '
            'This may indicate that grant records have not yet been loaded, '
            'or that no funded papers match the current filter criteria. '
            'Run the grants pipeline stage to populate funder data.',
            icon=':material/info:',
        )

    if not df_funders.empty:
        # Display label appends the base country for generically-named funders
        # (skips multilateral bodies and names that already state their country).
        df_funders['funder_label'] = [
            funder_display_name(n, c)
            for n, c in zip(df_funders['funder'], df_funders['country'])
        ]
        label_map = dict(zip(df_funders['funder'], df_funders['funder_label']))

        fig = px.bar(
            df_funders, y='funder_label', x='n_papers', orientation='h',
            color='category',
            color_discrete_map=FUNDER_CATEGORY_COLORS,
            labels={'n_papers': 'Papers', 'funder_label': '', 'category': 'Type'},
            template=CHART_TEMPLATE,
            hover_data=['country'],
        )
        fig.update_layout(
            height=max(400, len(df_funders) * 30),
            yaxis={'categoryorder': 'total ascending'},
        )
        st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # 3 + 4. Funder geography and type, side by side
    # ------------------------------------------------------------------
    df_nation = query_df(
        f"""SELECT f.funder_country AS country,
                   COUNT(DISTINCT g.openalex_id) AS n
            FROM grants g
            JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
            JOIN works w ON g.openalex_id = w.openalex_id
            {base_where} AND f.funder_country IS NOT NULL
              AND f.funder_country != ''
            GROUP BY f.funder_country
            ORDER BY n DESC
            LIMIT 12""",
        tuple(params),
    )

    df_type = query_df(
        f"""SELECT f.funder_category AS category,
                   COUNT(DISTINCT g.openalex_id) AS n
            FROM grants g
            JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
            JOIN works w ON g.openalex_id = w.openalex_id
            {base_where} AND f.funder_category IS NOT NULL
            GROUP BY f.funder_category
            ORDER BY n DESC""",
        tuple(params),
    )

    _SIDE_HEIGHT = 400
    col_geo, col_type = st.columns(2)

    with col_geo:
        section_header(
            'Papers by Funder Country',
            'Funded papers naming a funder based in each country or bloc.',
        )
        if not df_nation.empty and total_funded:
            df_nation['pct'] = df_nation['n'] / total_funded * 100
            df_nation['label'] = df_nation['country'].replace(
                {'Multilateral': 'Multilateral / UN'})
            fig = px.bar(
                df_nation, y='label', x='pct', orientation='h',
                labels={'pct': 'Share of funded papers (%)', 'label': ''},
                template=CHART_TEMPLATE,
            )
            fig.update_traces(marker_color='#2171b5')
            fig.update_layout(height=_SIDE_HEIGHT,
                              yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

    with col_type:
        section_header(
            'Funder Types',
            'What kinds of organizations fund the research.',
        )
        if not df_type.empty and total_funded:
            df_type['pct'] = df_type['n'] / total_funded * 100
            fig = px.bar(
                df_type, y='category', x='pct', orientation='h',
                color='category', color_discrete_map=FUNDER_CATEGORY_COLORS,
                labels={'pct': 'Share of funded papers (%)',
                        'category': ''},
                template=CHART_TEMPLATE,
            )
            fig.update_layout(height=_SIDE_HEIGHT, showlegend=False,
                              yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

    if not df_nation.empty or not df_type.empty:
        st.caption(
            'Both charts show the share of funded papers naming a funder of a '
            'given kind: by home country (including multilateral bodies such '
            'as the WHO or EU) on the left, by organization type on the right. '
            'A paper can name several funders, so it is counted under more than '
            'one bar and the bars do not sum to 100%.'
        )

    # ------------------------------------------------------------------
    # 5. Funder momentum: who is rising or fading
    # ------------------------------------------------------------------
    section_header(
        'Which Funders Are Rising or Fading?',
        'Change in each major funder\'s footprint between two periods. Use the '
        'sliders to choose the periods to compare.',
    )

    y0, y1 = year_range
    if y1 > y0:
        span = y1 - y0
        win = min(4, max(1, (span + 1) // 2))  # default window length (years)
        def_early = (y0, y0 + win - 1)
        def_recent = (y1 - win + 1, y1)

        sc1, sc2 = st.columns(2)
        with sc1:
            e0, e1 = st.slider(
                'Earlier period', min_value=y0, max_value=y1,
                value=def_early, key='mom_early')
        with sc2:
            r0, r1 = st.slider(
                'Recent period', min_value=y0, max_value=y1,
                value=def_recent, key='mom_recent')

        mom_totals = query_df(
            f"""SELECT
                    COUNT(DISTINCT CASE WHEN w.publication_year BETWEEN ? AND ?
                        THEN g.openalex_id END) AS early_total,
                    COUNT(DISTINCT CASE WHEN w.publication_year BETWEEN ? AND ?
                        THEN g.openalex_id END) AS recent_total
                FROM grants g
                JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
                JOIN works w ON g.openalex_id = w.openalex_id
                {base_where}""",
            tuple([e0, e1, r0, r1] + params),
        )
        early_total = int(mom_totals.iloc[0]['early_total'] or 0)
        recent_total = int(mom_totals.iloc[0]['recent_total'] or 0)

        df_mom = query_df(
            f"""SELECT f.canonical_name AS funder, f.funder_country AS country,
                    COUNT(DISTINCT CASE WHEN w.publication_year BETWEEN ? AND ?
                        THEN g.openalex_id END) AS early_n,
                    COUNT(DISTINCT CASE WHEN w.publication_year BETWEEN ? AND ?
                        THEN g.openalex_id END) AS recent_n
                FROM grants g
                JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
                JOIN works w ON g.openalex_id = w.openalex_id
                {base_where}
                GROUP BY f.canonical_name, f.funder_country""",
            tuple([e0, e1, r0, r1] + params),
        )

        if not df_mom.empty and early_total and recent_total:
            df_mom['total_n'] = df_mom['early_n'] + df_mom['recent_n']
            df_mom = df_mom.sort_values('total_n', ascending=False).head(12)
            df_mom['early_share'] = df_mom['early_n'] / early_total * 100
            df_mom['recent_share'] = df_mom['recent_n'] / recent_total * 100
            df_mom['delta'] = df_mom['recent_share'] - df_mom['early_share']
            df_mom['label'] = [funder_display_name(n, c)
                               for n, c in zip(df_mom['funder'], df_mom['country'])]
            df_mom['dir'] = df_mom['delta'].apply(
                lambda d: 'Rising' if d >= 0 else 'Fading')
            df_mom = df_mom.sort_values('delta')
            fig = px.bar(
                df_mom, y='label', x='delta', orientation='h', color='dir',
                color_discrete_map={'Rising': '#0072B2', 'Fading': '#D55E00'},
                labels={'delta': 'Change in footprint (percentage points)',
                        'label': '', 'dir': ''},
                template=CHART_TEMPLATE,
            )
            fig.update_layout(
                height=max(360, len(df_mom) * 30),
                yaxis={'categoryorder': 'total ascending'},
                showlegend=False,
            )
            fig.add_vline(x=0, line_width=1, line_color='#888')
            st.plotly_chart(fig, use_container_width=True)
            st.caption(
                f'Footprint = share of funded papers naming that funder. Bars '
                f'compare **{r0}–{r1}** with **{e0}–{e1}**.'
            )

    # ------------------------------------------------------------------
    # 6. Funding composition by topic
    # ------------------------------------------------------------------
    section_header(
        'Funding Composition by Topic',
        'For each research area, the share of its funded papers attributed to '
        'each of the largest funders; the rest are grouped as "Other funders". '
        'Topics are ordered by how funder-concentrated they are.',
    )

    df_tf = query_df(
        f"""SELECT w.topic_category AS cat,
                   f.canonical_name AS funder, f.funder_country AS country,
                   COUNT(DISTINCT g.openalex_id) AS n
            FROM grants g
            JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
            JOIN works w ON g.openalex_id = w.openalex_id
            {base_where} AND w.topic_category IS NOT NULL
            GROUP BY w.topic_category, f.canonical_name, f.funder_country""",
        tuple(params),
    )

    if df_tf.empty:
        st.info('No funder–topic data available (grant records needed).',
                icon=':material/info:')
    else:
        TOP_N = 8
        top_funders = (
            df_tf.groupby(['funder', 'country'])['n'].sum()
            .sort_values(ascending=False).head(TOP_N).reset_index()
        )
        disp = {r.funder: funder_display_name(r.funder, r.country)
                for r in top_funders.itertuples()}

        df_tf = df_tf.copy()
        df_tf['fgroup'] = df_tf['funder'].map(lambda f: disp.get(f, 'Other funders'))
        topic_tot = df_tf.groupby('cat')['n'].sum()
        comp = df_tf.groupby(['cat', 'fgroup'], as_index=False)['n'].sum()
        comp['share'] = comp.apply(
            lambda r: 100 * r['n'] / topic_tot[r['cat']], axis=1
        )
        comp['label'] = comp['cat'].map(lambda c: TOPIC_LABELS.get(c, c))

        # Funder segments: largest funder first, "Other funders" last.
        order = [disp[f] for f in top_funders['funder']] + ['Other funders']
        colors = {name: QUAL_PALETTE[i % len(QUAL_PALETTE)]
                  for i, name in enumerate(order[:-1])}
        colors['Other funders'] = '#cccccc'

        # Tufte: order topics by named-funder concentration so the grey
        # "Other funders" tail forms a clean, readable staircase.
        named_share = (comp[comp['fgroup'] != 'Other funders']
                       .groupby('label')['share'].sum())
        topic_order = named_share.sort_values().index.tolist()

        fig = px.bar(
            comp, y='label', x='share', color='fgroup', orientation='h',
            category_orders={'fgroup': order, 'label': topic_order},
            color_discrete_map=colors,
            labels={'share': "Share of the topic's funded papers (%)",
                    'label': '', 'fgroup': 'Funder'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            barmode='stack',
            height=max(450, comp['label'].nunique() * 34),
            legend=dict(font=dict(size=10), title_text=''),
            xaxis_range=[0, 100],
            yaxis={'categoryorder': 'array', 'categoryarray': topic_order},
            bargap=0.25,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # 7. Funder dossier: header, then the picker below it, then the charts
    # ------------------------------------------------------------------
    selected_funder = None
    if not df_funders.empty:
        section_header(
            'Funder Dossier',
            'Pick a funder for its topic portfolio, geographic footprint, and '
            'funding trend.',
        )
        selected_funder = st.selectbox(
            'Select a funder to explore:',
            options=df_funders['funder'].tolist(),
            format_func=lambda f: label_map.get(f, f),
            key='selected_funder',
        )

    if selected_funder:
        col_a, col_b = st.columns(2)

        # Topic portfolio for this funder
        with col_a:
            df_portfolio = query_df(
                f"""SELECT w.topic_category AS cat, COUNT(DISTINCT w.openalex_id) AS n
                    FROM grants g
                    JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
                    JOIN works w ON g.openalex_id = w.openalex_id
                    {base_where}
                    AND f.canonical_name = ?
                    AND w.topic_category IS NOT NULL
                    GROUP BY w.topic_category
                    ORDER BY n DESC""",
                tuple(params + [selected_funder]),
            )
            if not df_portfolio.empty:
                df_portfolio['label'] = df_portfolio['cat'].map(
                    lambda c: TOPIC_LABELS.get(c, c)
                )
                fig = px.pie(
                    df_portfolio, values='n', names='label',
                    color='cat', color_discrete_map=TOPIC_COLORS,
                    title='Topic Portfolio',
                    template=CHART_TEMPLATE,
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

        # Geographic footprint
        with col_b:
            df_geo = query_df(
                f"""SELECT w.study_country AS country, COUNT(DISTINCT w.openalex_id) AS n
                    FROM grants g
                    JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
                    JOIN works w ON g.openalex_id = w.openalex_id
                    {base_where}
                    AND f.canonical_name = ?
                    AND w.study_country IS NOT NULL
                    AND w.study_country NOT IN ('GLOBAL', 'UNKNOWN')
                    GROUP BY w.study_country
                    ORDER BY n DESC LIMIT 15""",
                tuple(params + [selected_funder]),
            )
            if not df_geo.empty:
                df_geo['name'] = df_geo['country'].apply(iso2_to_country_name)
                df_geo['region'] = (
                    df_geo['country'].map(WHO_REGIONS).map(WHO_REGION_NAMES)
                    .fillna('Other')
                )
                fig = px.bar(
                    df_geo, y='name', x='n', orientation='h', color='region',
                    color_discrete_map=WHO_REGION_COLORS,
                    title='Top Study Countries',
                    labels={'n': 'Papers', 'name': '', 'region': 'WHO region'},
                    template=CHART_TEMPLATE,
                )
                fig.update_layout(
                    height=400,
                    yaxis={'categoryorder': 'total ascending'},
                    legend=dict(orientation='h', yanchor='bottom', y=1.02,
                                xanchor='left', x=0, title_text='',
                                font=dict(size=9)),
                )
                st.plotly_chart(fig, use_container_width=True)

        # Funding trend over time
        df_trend = query_df(
            f"""SELECT w.publication_year AS year,
                       COUNT(DISTINCT w.openalex_id) AS n
                FROM grants g
                JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
                JOIN works w ON g.openalex_id = w.openalex_id
                {base_where}
                AND f.canonical_name = ?
                GROUP BY w.publication_year
                ORDER BY year""",
            tuple(params + [selected_funder]),
        )
        if not df_trend.empty:
            fig = px.line(
                df_trend, x='year', y='n',
                title=f'Funding Trend: {selected_funder}',
                labels={'year': 'Year', 'n': 'Papers Funded'},
                template=CHART_TEMPLATE, markers=True,
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Comparative funder analysis
    # ------------------------------------------------------------------
    if not df_funders.empty:
        section_header(
            'Comparative Funder Analysis',
            'Select 2\u20133 funders to compare their topic portfolios side by side.',
        )

        compare_funders = st.multiselect(
            'Select funders to compare:',
            options=df_funders['funder'].tolist(),
            default=df_funders['funder'].tolist()[:2],
            max_selections=3,
            key='compare_funders',
        )

        if len(compare_funders) >= 2:
            placeholders = ', '.join(['?'] * len(compare_funders))
            df_compare = query_df(
                f"""SELECT f.canonical_name AS funder,
                           w.topic_category AS cat,
                           COUNT(DISTINCT w.openalex_id) AS n
                    FROM grants g
                    JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
                    JOIN works w ON g.openalex_id = w.openalex_id
                    {base_where}
                    AND f.canonical_name IN ({placeholders})
                    AND w.topic_category IS NOT NULL
                    GROUP BY f.canonical_name, w.topic_category""",
                tuple(params + compare_funders),
            )

            if not df_compare.empty:
                # Normalize to percentages within each funder
                totals = df_compare.groupby('funder')['n'].sum().reset_index()
                totals.columns = ['funder', 'total']
                df_compare = df_compare.merge(totals, on='funder')
                df_compare['pct'] = (df_compare['n'] / df_compare['total'] * 100).round(1)
                df_compare['topic'] = df_compare['cat'].map(
                    lambda c: TOPIC_LABELS.get(c, c)
                )

                fig = px.bar(
                    df_compare, x='topic', y='pct', color='funder',
                    barmode='group',
                    color_discrete_sequence=QUAL_PALETTE,
                    labels={'pct': 'Portfolio Share (%)', 'topic': ''},
                    template=CHART_TEMPLATE,
                )
                fig.update_layout(
                    height=CHART_HEIGHT, xaxis_tickangle=-45,
                    legend=dict(orientation='h', y=1.15),
                )
                st.plotly_chart(fig, use_container_width=True)
                st.caption(
                    'Portfolio share = percentage of each funder\'s papers in '
                    'that topic.'
                )


