"""
dashboard/pages/lens_a_funder.py

Lens A — Funder Power: Does funder concentration determine what gets studied?

Analytical interactions:
- Funder dossier drill-down (click funder → portfolio, geography, trend)
- Comparative funder analysis (2-3 funders side-by-side)
- Funding-research gap scatter (pub share vs funding share)
- HHI decomposition (click year → who drove the change)
- Funder×topic heatmap with trend drill-down
- Unfunded analysis (who does research without funding)
"""

import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from dashboard.components import (
    check_data_ready, metric_row, section_header, download_csv_button,
)
from dashboard.constants import (
    TOPIC_COLORS, TOPIC_LABELS, FUNDER_CATEGORY_COLORS,
    NON_EMPIRICAL_METHODS, UNCATEGORIZED_TOPICS, iso2_to_country_name,
    CHART_TEMPLATE, CHART_HEIGHT, CHART_HEIGHT_TALL, DIVERGING_COLORSCALE,
)
from dashboard.db import query_df, query_scalar, build_where_clause


def page():
    st.title('Funder Power')
    st.caption(
        'Does funder concentration determine what gets studied? '
        'Is it increasing over time?'
    )

    if not check_data_ready(require_topics=True):
        return

    year_range = st.session_state.get('year_range', (2010, 2024))
    topics = st.session_state.get('selected_topics', [])
    where, params = build_where_clause(year_range=year_range, topics=topics or None)
    # Exclude uncategorized topics and non-empirical methods from analysis
    uc_placeholders = ', '.join(['?'] * len(UNCATEGORIZED_TOPICS))
    uc_clause = f" AND (w.topic_category IS NULL OR w.topic_category NOT IN ({uc_placeholders}))"
    ne_placeholders = ', '.join(['?'] * len(NON_EMPIRICAL_METHODS))
    ne_clause = (f" AND (w.method_type IS NULL "
                 f"OR w.method_type NOT IN ({ne_placeholders}))")
    base_where = f"WHERE TRUE {where}{uc_clause}{ne_clause}"
    params = params + list(UNCATEGORIZED_TOPICS) + list(NON_EMPIRICAL_METHODS)

    # ------------------------------------------------------------------
    # Page-level controls
    # ------------------------------------------------------------------
    col1, col2 = st.columns([1, 2])
    with col1:
        top_n = st.slider('Top N funders', 5, 30, 15, key='funder_top_n')

    # ------------------------------------------------------------------
    # Top funders
    # ------------------------------------------------------------------
    section_header(
        'Top Funders by Paper Count',
        'Click a funder to see their research portfolio below.',
    )

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
        fig = px.bar(
            df_funders, y='funder', x='n_papers', orientation='h',
            color='category',
            color_discrete_map=FUNDER_CATEGORY_COLORS,
            labels={'n_papers': 'Papers', 'funder': '', 'category': 'Type'},
            template=CHART_TEMPLATE,
            hover_data=['country'],
        )
        fig.update_layout(
            height=max(400, len(df_funders) * 30),
            yaxis={'categoryorder': 'total ascending'},
        )
        st.plotly_chart(fig, use_container_width=True)

        # Funder selector for drill-down
        selected_funder = st.selectbox(
            'Select a funder to explore:',
            options=df_funders['funder'].tolist(),
            key='selected_funder',
        )

    # ------------------------------------------------------------------
    # Funder dossier drill-down
    # ------------------------------------------------------------------
    if selected_funder:
        section_header(
            f'Funder Dossier: {selected_funder}',
            'Topic portfolio, geographic footprint, and funding trend.',
        )

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
                fig = px.bar(
                    df_geo, y='name', x='n', orientation='h',
                    title='Top Study Countries',
                    labels={'n': 'Papers', 'name': ''},
                    template=CHART_TEMPLATE,
                )
                fig.update_traces(marker_color='#ff7f0e')
                fig.update_layout(
                    height=400,
                    yaxis={'categoryorder': 'total ascending'},
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
                    'that topic. Differences reveal strategic priorities.'
                )

    # ------------------------------------------------------------------
    # Funding–research gap scatter
    # ------------------------------------------------------------------
    section_header(
        'Funding vs. Research Attention',
        'Topics below the diagonal are heavily researched but weakly funded; '
        'above are funder-driven.',
    )

    df_scatter = query_df(
        f"""WITH pub_share AS (
                SELECT topic_category AS cat,
                       COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS share
                FROM works w {base_where} AND topic_category IS NOT NULL
                GROUP BY topic_category
            ),
            fund_share AS (
                SELECT w.topic_category AS cat,
                       COUNT(DISTINCT g.openalex_id) * 1.0 /
                       SUM(COUNT(DISTINCT g.openalex_id)) OVER () AS share
                FROM grants g
                JOIN works w ON g.openalex_id = w.openalex_id
                {base_where.replace('WHERE TRUE', 'WHERE TRUE')}
                AND w.topic_category IS NOT NULL
                GROUP BY w.topic_category
            )
            SELECT p.cat, p.share AS pub_share, COALESCE(f.share, 0) AS fund_share
            FROM pub_share p
            LEFT JOIN fund_share f ON p.cat = f.cat""",
        tuple(params + params),
    )

    if not df_scatter.empty:
        df_scatter['label'] = df_scatter['cat'].map(
            lambda c: TOPIC_LABELS.get(c, c)
        )
        df_scatter['pub_pct'] = (df_scatter['pub_share'] * 100).round(1)
        df_scatter['fund_pct'] = (df_scatter['fund_share'] * 100).round(1)

        fig = px.scatter(
            df_scatter, x='pub_pct', y='fund_pct',
            color='label',
            color_discrete_map={TOPIC_LABELS.get(k, k): v
                                for k, v in TOPIC_COLORS.items()},
            labels={'pub_pct': 'Publication Share (%)',
                    'fund_pct': 'Funding Share (%)', 'label': 'Topic'},
            template=CHART_TEMPLATE,
        )
        # Diagonal line (parity)
        max_val = max(df_scatter['pub_pct'].max(), df_scatter['fund_pct'].max()) * 1.1
        fig.add_shape(
            type='line', x0=0, y0=0, x1=max_val, y1=max_val,
            line=dict(color='gray', dash='dash'),
        )
        fig.update_traces(marker_size=12)
        fig.update_layout(
            height=CHART_HEIGHT,
            legend=dict(font=dict(size=9), title_text=''),
        )

        # Add quadrant annotations
        fig.add_annotation(
            x=max_val * 0.75, y=max_val * 0.25,
            text='Self-sustaining<br>(high research, low funding)',
            showarrow=False, font=dict(color='gray', size=10),
        )
        fig.add_annotation(
            x=max_val * 0.25, y=max_val * 0.75,
            text='Funder-driven<br>(low research, high funding)',
            showarrow=False, font=dict(color='gray', size=10),
        )

        st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # HHI concentration trend
    # ------------------------------------------------------------------
    section_header(
        'Funder Concentration Over Time (HHI)',
        'Herfindahl\u2013Hirschman Index: higher = more concentrated. '
        'Click a year to see which funders drove the change.',
    )

    df_hhi = query_df(
        f"""WITH funder_year AS (
                SELECT w.publication_year AS year,
                       f.canonical_name AS funder,
                       COUNT(DISTINCT g.openalex_id) AS n
                FROM grants g
                JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
                JOIN works w ON g.openalex_id = w.openalex_id
                {base_where}
                GROUP BY w.publication_year, f.canonical_name
            ),
            year_total AS (
                SELECT year, SUM(n) AS total FROM funder_year GROUP BY year
            ),
            shares AS (
                SELECT fy.year, fy.funder, fy.n,
                       fy.n * 1.0 / yt.total AS share
                FROM funder_year fy
                JOIN year_total yt ON fy.year = yt.year
            )
            SELECT year,
                   SUM(share * share) AS hhi,
                   COUNT(DISTINCT funder) AS n_funders,
                   MAX(share) AS top_share
            FROM shares
            GROUP BY year
            ORDER BY year""",
        tuple(params),
    )

    if df_hhi.empty:
        st.info(
            'No funder concentration data available. Grant records are '
            'needed to compute the HHI index. Ensure the grants pipeline '
            'stage has been run.',
            icon=':material/info:',
        )

    if not df_hhi.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Scatter(
                x=df_hhi['year'], y=df_hhi['hhi'],
                mode='lines+markers', name='HHI',
                line=dict(color='#d62728', width=2),
                hovertemplate='Year: %{x}<br>HHI: %{y:.4f}<br>Top funder share: %{customdata:.1%}',
                customdata=df_hhi['top_share'],
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Bar(
                x=df_hhi['year'], y=df_hhi['n_funders'],
                name='Unique Funders', opacity=0.3,
                marker_color='#2171b5',
            ),
            secondary_y=True,
        )
        fig.update_layout(
            template=CHART_TEMPLATE, height=CHART_HEIGHT,
            legend=dict(orientation='h', y=1.12),
        )
        fig.update_yaxes(title_text='HHI', secondary_y=False)
        fig.update_yaxes(title_text='Unique Funders', secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

        st.info(
            '**How to read this chart:** The Herfindahl\u2013Hirschman Index '
            '(HHI) measures market concentration by summing the squared '
            'funding shares of all funders in a given year. Values range '
            'from 0 to 1: below 0.15 indicates low concentration (many '
            'funders with roughly equal shares), 0.15\u20130.25 indicates '
            'moderate concentration, and above 0.25 indicates high '
            'concentration (a few funders dominate). The red line tracks '
            'HHI over time, while the blue bars show how many unique '
            'funders were active each year. A rising HHI with stable '
            'funder counts suggests a few funders are capturing larger '
            'shares; a falling HHI suggests diversification.',
            icon=':material/info:',
        )

    # ------------------------------------------------------------------
    # Unfunded papers analysis
    # ------------------------------------------------------------------
    section_header(
        'Unfunded Papers by Topic',
        'Papers with no identified funder. High rates may indicate '
        'self-funded research or incomplete funder data.',
    )

    df_unfunded = query_df(
        f"""SELECT w.topic_category AS cat,
                   COUNT(*) AS total,
                   SUM(CASE WHEN g.openalex_id IS NULL THEN 1 ELSE 0 END) AS unfunded
            FROM works w
            LEFT JOIN grants g ON w.openalex_id = g.openalex_id
            {base_where} AND w.topic_category IS NOT NULL
            GROUP BY w.topic_category
            ORDER BY cat""",
        tuple(params),
    )

    if not df_unfunded.empty:
        df_unfunded['pct_unfunded'] = (
            df_unfunded['unfunded'] / df_unfunded['total'] * 100
        ).round(1)
        df_unfunded['label'] = df_unfunded['cat'].map(
            lambda c: TOPIC_LABELS.get(c, c)
        )

        fig = px.bar(
            df_unfunded.sort_values('pct_unfunded', ascending=True),
            y='label', x='pct_unfunded', orientation='h',
            labels={'pct_unfunded': 'Unfunded (%)', 'label': ''},
            template=CHART_TEMPLATE,
            color='pct_unfunded',
            color_continuous_scale='Reds',
        )
        fig.update_layout(
            height=max(400, len(df_unfunded) * 35),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)
        download_csv_button(
            df_unfunded[['cat', 'label', 'total', 'unfunded', 'pct_unfunded']],
            'unfunded_by_topic.csv',
        )

    # ------------------------------------------------------------------
    # Supplementary: Commentary & Editorial use by funders
    # ------------------------------------------------------------------
    ne_placeholders = ', '.join(['?'] * len(NON_EMPIRICAL_METHODS))
    section_header(
        'Commentary & Editorial Publications',
        'How do funders use commentary, editorials, and perspectives? '
        'These non-empirical publications are excluded from the '
        'analytical lenses but reveal how funders shape discourse.',
    )

    df_ne_funders = query_df(
        f"""SELECT f.canonical_name AS funder,
                   f.funder_category AS category,
                   COUNT(DISTINCT g.openalex_id) AS n_papers
            FROM grants g
            JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
            JOIN works w ON g.openalex_id = w.openalex_id
            {base_where}
            AND w.method_type IN ({ne_placeholders})
            GROUP BY f.canonical_name, f.funder_category
            ORDER BY n_papers DESC
            LIMIT 15""",
        tuple(params + list(NON_EMPIRICAL_METHODS)),
    )

    if not df_ne_funders.empty:
        total_ne = query_scalar(
            f"""SELECT COUNT(*)
                FROM works w {base_where}
                AND w.method_type IN ({ne_placeholders})""",
            tuple(params + list(NON_EMPIRICAL_METHODS)),
        )
        funded_ne = df_ne_funders['n_papers'].sum()

        metric_row([
            ('Total Commentary/Editorial Papers', total_ne or 0, None),
            ('Funded Commentary/Editorials', int(funded_ne), None),
            ('Top Funder (Commentary)', df_ne_funders.iloc[0]['funder'], None),
        ])

        fig = px.bar(
            df_ne_funders, y='funder', x='n_papers', orientation='h',
            color='category',
            color_discrete_map=FUNDER_CATEGORY_COLORS,
            labels={'n_papers': 'Commentary/Editorial Papers',
                    'funder': '', 'category': 'Type'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=max(400, len(df_ne_funders) * 30),
            yaxis={'categoryorder': 'total ascending'},
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            'Commentary, editorials, and perspective pieces funded by each '
            'funder. These non-empirical publications can signal a funder\'s '
            'strategic priorities and influence on the research discourse.'
        )
    else:
        st.info(
            'No commentary/editorial publications found with funder data '
            'for the selected filters.',
            icon=':material/info:',
        )
