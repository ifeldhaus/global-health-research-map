"""
dashboard/pages/institutions.py

Institutions — Who produces global health research?

Analytical interactions:
- Institution dossier (click institution → topic radar, growth, funders, reach)
- Head-to-head comparison (select 2–3 institutions → parallel comparison)
- Rising institutions scatter (total papers × growth rate)
- Geographic concentration Lorenz curve (Gini coefficient)
"""

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from dashboard.components import (
    check_data_ready, metric_row, section_header, download_csv_button,
)
from dashboard.constants import (
    TOPIC_LABELS, METHOD_LABELS, TOPIC_COLORS, NON_EMPIRICAL_METHODS,
    UNCATEGORIZED_TOPICS, QUAL_PALETTE, CHART_TEMPLATE, CHART_HEIGHT,
    CHART_HEIGHT_TALL, iso2_to_country_name,
)
from dashboard.db import query_df, query_scalar, build_where_clause


def page():
    st.title('Institutions')
    st.caption(
        'Who produces global health research? How concentrated is '
        'production, and which institutions are on the rise?'
    )

    if not check_data_ready():
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
        f"""SELECT a.institution_name AS institution,
                   a.institution_country AS country,
                   COUNT(DISTINCT a.openalex_id) AS n_papers
            FROM authorships a
            JOIN works w ON a.openalex_id = w.openalex_id
            {base_where}
            AND a.institution_name IS NOT NULL
            AND a.institution_name != ''
            AND a.position IN ('first', 'last')
            GROUP BY a.institution_name, a.institution_country
            ORDER BY n_papers DESC
            LIMIT ?""",
        tuple(params + [top_n]),
    )

    if not df_inst.empty:
        total_papers = query_scalar(
            f"SELECT COUNT(*) FROM works w {base_where}", tuple(params)
        )

        top3_share = (df_inst.head(3)['n_papers'].sum() / total_papers * 100
                       if total_papers else 0)

        # Use columns with markdown instead of metric_row
        # (st.metric truncates long institution names)
        m_cols = st.columns(3)
        with m_cols[0]:
            st.markdown('**Top Institution**')
            st.markdown(f"{df_inst.iloc[0]['institution']}")
        with m_cols[1]:
            st.metric('Top 3 Share', f"{top3_share:.1f}%")
        with m_cols[2]:
            st.metric('Countries Represented',
                       int(df_inst['country'].nunique()))


        df_inst['country_name'] = df_inst['country'].apply(iso2_to_country_name)
        fig = px.bar(
            df_inst, y='institution', x='n_papers', orientation='h',
            color='country_name',
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
    # Institution dossier
    # ------------------------------------------------------------------
    if not df_inst.empty:
        section_header(
            'Institution Dossier',
            'Select an institution to see its topic specialization, '
            'growth trajectory, and geographic reach.',
        )

        selected_inst = st.selectbox(
            'Select institution:',
            options=df_inst['institution'].tolist(),
            key='selected_institution',
        )

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
                        AND a.institution_name = ?
                        AND a.position IN ('first', 'last')
                        AND w.topic_category IS NOT NULL
                        GROUP BY w.topic_category
                        ORDER BY n DESC""",
                    tuple(params + [selected_inst]),
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
                        AND a.institution_name = ?
                        AND a.position IN ('first', 'last')
                        AND w.method_type IS NOT NULL
                        GROUP BY w.method_type
                        ORDER BY n DESC""",
                    tuple(params + [selected_inst]),
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
                    fig.update_traces(marker_color='#2ca02c')
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
                    AND a.institution_name = ?
                    AND a.position IN ('first', 'last')
                    GROUP BY w.publication_year
                    ORDER BY year""",
                tuple(params + [selected_inst]),
            )

            if not df_growth.empty:
                fig = px.line(
                    df_growth, x='year', y='n',
                    title=f'Publication Trend: {selected_inst}',
                    labels={'year': 'Year', 'n': 'Papers'},
                    template=CHART_TEMPLATE, markers=True,
                )
                fig.update_traces(line=dict(color='#1f77b4', width=2))
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)

            # Top collaborating institutions
            df_collabs = query_df(
                f"""SELECT a2.institution_name AS collaborator,
                           COUNT(DISTINCT w.openalex_id) AS n
                    FROM authorships a1
                    JOIN authorships a2 ON a1.openalex_id = a2.openalex_id
                    JOIN works w ON a1.openalex_id = w.openalex_id
                    {base_where}
                    AND a1.institution_name = ?
                    AND a2.institution_name IS NOT NULL
                    AND a2.institution_name != ''
                    AND a2.institution_name != ?
                    GROUP BY a2.institution_name
                    ORDER BY n DESC LIMIT 10""",
                tuple(params + [selected_inst, selected_inst]),
            )
            if not df_collabs.empty:
                st.markdown(f'#### Top Collaborating Institutions')
                fig = px.bar(
                    df_collabs, y='collaborator', x='n', orientation='h',
                    labels={'n': 'Co-authored Papers', 'collaborator': ''},
                    template=CHART_TEMPLATE,
                )
                fig.update_traces(marker_color='#ff7f0e')
                fig.update_layout(
                    height=max(300, len(df_collabs) * 30),
                    yaxis={'categoryorder': 'total ascending'},
                )
                st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Head-to-head comparison
    # ------------------------------------------------------------------
    if not df_inst.empty:
        section_header(
            'Head-to-Head Comparison',
            'Select 2\u20133 institutions to compare their topic portfolios '
            'and research output profiles.',
        )

        inst_options = [i for i in df_inst['institution'].tolist() if i.strip()]
        compare_insts = st.multiselect(
            'Select institutions to compare:',
            options=inst_options,
            default=inst_options[:min(2, len(inst_options))],
            max_selections=3,
            key='compare_institutions',
        )

        if len(compare_insts) >= 2:
            placeholders = ', '.join(['?'] * len(compare_insts))
            df_compare = query_df(
                f"""SELECT a.institution_name AS institution,
                           w.topic_category AS cat,
                           COUNT(DISTINCT w.openalex_id) AS n
                    FROM authorships a
                    JOIN works w ON a.openalex_id = w.openalex_id
                    {base_where}
                    AND a.institution_name IN ({placeholders})
                    AND a.position IN ('first', 'last')
                    AND w.topic_category IS NOT NULL
                    GROUP BY a.institution_name, w.topic_category""",
                tuple(params + compare_insts),
            )

            if not df_compare.empty:
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
    # Rising institutions scatter
    # ------------------------------------------------------------------
    section_header(
        'Rising Institutions',
        'X = total papers, Y = growth rate. Quadrants: established leaders '
        '(high volume, moderate growth), rising stars (growing fast), '
        'declining (negative growth), and niche (low volume, stable).',
    )

    # Compute midpoint year for splitting first/second halves
    mid_year = (year_range[0] + year_range[1]) / 2.0

    # Adaptive minimum papers threshold based on corpus size
    total_papers_count = query_scalar(
        f"SELECT COUNT(*) FROM works w {base_where}", tuple(params)
    ) or 0
    min_inst_papers = max(3, min(20, int(total_papers_count * 0.005)))

    df_inst_growth = query_df(
        f"""WITH yearly AS (
                SELECT a.institution_name AS inst,
                       w.publication_year AS year,
                       COUNT(DISTINCT w.openalex_id) AS n
                FROM authorships a
                JOIN works w ON a.openalex_id = w.openalex_id
                {base_where}
                AND a.institution_name IS NOT NULL
                AND a.institution_name != ''
                AND a.position IN ('first', 'last')
                GROUP BY a.institution_name, w.publication_year
            ),
            inst_total AS (
                SELECT inst, SUM(n) AS total FROM yearly GROUP BY inst
                HAVING total >= ?
            ),
            halves AS (
                SELECT y.inst,
                       SUM(CASE WHEN y.year <= ?
                           THEN y.n ELSE 0 END) AS first_half,
                       SUM(CASE WHEN y.year > ?
                           THEN y.n ELSE 0 END) AS second_half
                FROM yearly y
                JOIN inst_total it ON y.inst = it.inst
                GROUP BY y.inst
            )
            SELECT h.inst AS institution,
                   it.total,
                   h.first_half, h.second_half,
                   CASE WHEN h.first_half > 0
                        THEN (h.second_half - h.first_half) * 100.0 / h.first_half
                        ELSE 100 END AS growth_pct
            FROM halves h
            JOIN inst_total it ON h.inst = it.inst
            WHERE h.first_half > 0
            ORDER BY it.total DESC""",
        tuple(params + [min_inst_papers, mid_year, mid_year]),
    )

    if not df_inst_growth.empty:
        # Filter out any blank institution names
        df_inst_growth = df_inst_growth[
            df_inst_growth['institution'].str.strip().astype(bool)
        ].copy()

    if not df_inst_growth.empty:
        # Cap extreme growth values for visualization
        df_inst_growth['growth_capped'] = df_inst_growth['growth_pct'].clip(-100, 500)

        plot_df = df_inst_growth.head(50).copy()

        # Color-code each institution for the legend
        n_colors = len(QUAL_PALETTE)
        plot_df['color_idx'] = [i % n_colors for i in range(len(plot_df))]

        fig = px.scatter(
            plot_df,
            x='total', y='growth_capped',
            color='institution',
            color_discrete_sequence=QUAL_PALETTE,
            hover_data=['growth_pct', 'first_half', 'second_half'],
            labels={
                'total': 'Total Papers',
                'growth_capped': 'Growth Rate (%)',
                'institution': 'Institution',
            },
            template=CHART_TEMPLATE,
        )

        # Quadrant lines
        med_x = plot_df['total'].median()
        fig.add_hline(y=0, line_dash='dash', line_color='gray')
        fig.add_vline(x=med_x, line_dash='dash', line_color='gray')

        # Determine y-axis bounds that center zero in the visible range
        y_min = plot_df['growth_capped'].min()
        y_max = plot_df['growth_capped'].max()
        y_abs_max = max(abs(y_min), abs(y_max), 10) * 1.2

        # Quadrant annotations positioned relative to axis bounds
        x_max = plot_df['total'].max()
        fig.add_annotation(
            x=x_max * 0.75, y=y_abs_max * 0.75,
            text='Established Leaders', showarrow=False,
            font=dict(color='gray', size=10),
        )
        fig.add_annotation(
            x=med_x * 0.3, y=y_abs_max * 0.75,
            text='Rising Stars', showarrow=False,
            font=dict(color='#2ca02c', size=10),
        )
        fig.add_annotation(
            x=x_max * 0.75, y=-y_abs_max * 0.75,
            text='Declining', showarrow=False,
            font=dict(color='#d62728', size=10),
        )
        fig.add_annotation(
            x=med_x * 0.3, y=-y_abs_max * 0.75,
            text='Niche / Stable', showarrow=False,
            font=dict(color='gray', size=10),
        )

        fig.update_traces(marker=dict(size=10, opacity=0.8))
        fig.update_layout(
            height=CHART_HEIGHT_TALL,
            yaxis=dict(range=[-y_abs_max, y_abs_max]),
            legend=dict(
                font=dict(size=8),
                title_text='Institution',
                itemsizing='constant',
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

        n_displayed = len(plot_df)
        n_qualified = len(df_inst_growth)
        st.info(
            f'**How to read this chart:** Each dot is an institution with at '
            f'least {min_inst_papers} papers (threshold adapts to corpus size). '
            f'The X-axis shows total output volume; the Y-axis '
            f'shows how much that output grew (or shrank) between the first and '
            f'second halves of the {int(year_range[0])}\u2013{int(year_range[1])} '
            f'window. **Rising Stars** (top-left) are smaller institutions '
            f'growing fast \u2014 potential emerging hubs. **Established Leaders** '
            f'(top-right) combine high volume with growth. **Declining** '
            f'(bottom-right) institutions have high output but are losing '
            f'momentum. '
            f'Currently showing {n_displayed} of {n_qualified} qualifying '
            f'institutions.',
            icon=':material/info:',
        )
    else:
        st.info(
            f'No institutions meet the minimum paper threshold '
            f'({min_inst_papers} papers) for growth analysis. '
            f'This threshold adapts to corpus size — with more papers loaded, '
            f'more institutions will qualify.',
            icon=':material/info:',
        )

    # ------------------------------------------------------------------
    # Geographic concentration (Lorenz curve)
    # ------------------------------------------------------------------
    section_header(
        'Research Production Concentration',
        'Lorenz curve showing how concentrated global health research '
        'production is across institutions. The Gini coefficient '
        'measures inequality (0 = perfect equality, 1 = one institution '
        'produces everything).',
    )

    df_all_inst = query_df(
        f"""SELECT a.institution_name AS institution,
                   COUNT(DISTINCT a.openalex_id) AS n
            FROM authorships a
            JOIN works w ON a.openalex_id = w.openalex_id
            {base_where}
            AND a.institution_name IS NOT NULL
            AND a.position IN ('first', 'last')
            GROUP BY a.institution_name
            ORDER BY n ASC""",
        tuple(params),
    )

    if not df_all_inst.empty and len(df_all_inst) > 1:
        values = df_all_inst['n'].values.astype(float)
        n = len(values)

        # Lorenz curve
        sorted_vals = np.sort(values)
        cumulative = np.cumsum(sorted_vals) / sorted_vals.sum()
        population_pct = np.arange(1, n + 1) / n

        # Add origin point
        lorenz_x = np.insert(population_pct, 0, 0) * 100
        lorenz_y = np.insert(cumulative, 0, 0) * 100

        # Gini coefficient
        gini = 1 - 2 * np.trapezoid(cumulative, population_pct)

        # Key concentration metrics
        top_10_pct = (sorted_vals[-max(1, n // 10):].sum() /
                      sorted_vals.sum() * 100)
        top_1_pct = (sorted_vals[-max(1, n // 100):].sum() /
                     sorted_vals.sum() * 100)

        metric_row([
            ('Gini Coefficient', f'{gini:.3f}', None),
            ('Top 10% Share', f'{top_10_pct:.1f}%', None),
            ('Top 1% Share', f'{top_1_pct:.1f}%', None),
            ('Total Institutions', n, None),
        ])

        fig = go.Figure()

        # Lorenz curve
        fig.add_trace(go.Scatter(
            x=lorenz_x, y=lorenz_y,
            mode='lines', name='Actual distribution',
            line=dict(color='#d62728', width=2),
            fill='tozeroy', fillcolor='rgba(214, 39, 40, 0.1)',
        ))

        # Perfect equality line
        fig.add_trace(go.Scatter(
            x=[0, 100], y=[0, 100],
            mode='lines', name='Perfect equality',
            line=dict(color='gray', dash='dash'),
        ))

        fig.update_layout(
            template=CHART_TEMPLATE, height=CHART_HEIGHT,
            xaxis_title='Cumulative % of Institutions',
            yaxis_title='Cumulative % of Papers',
            title=f'Lorenz Curve (Gini = {gini:.3f})',
            legend=dict(x=0.05, y=0.95),
        )
        st.plotly_chart(fig, use_container_width=True)

        st.info(
            '**How to read this chart:** The dashed diagonal represents '
            'perfect equality \u2014 every institution producing the same number '
            'of papers. The red curve shows the actual distribution. The '
            'further it bows below the diagonal, the more concentrated '
            'research production is among a few institutions. '
            f'A **Gini coefficient of {gini:.3f}** indicates '
            + ('moderate' if gini < 0.6 else 'high' if gini < 0.8 else 'very high')
            + ' concentration. For context: '
            f'the top 1% of institutions ({max(1, n // 100)}) produce '
            f'{top_1_pct:.1f}% of all papers, and the top 10% produce '
            f'{top_10_pct:.1f}%. This chart responds to the sidebar filters '
            '\u2014 try filtering by topic or year range to see how '
            'concentration varies across research areas.',
            icon=':material/info:',
        )

    # ------------------------------------------------------------------
    # Supplementary: Commentary & Editorial use by institutions
    # ------------------------------------------------------------------
    ne_placeholders = ', '.join(['?'] * len(NON_EMPIRICAL_METHODS))
    section_header(
        'Commentary & Editorial Publications',
        'How do institutions use commentary, editorials, and perspectives '
        'to influence research discourse? These non-empirical publications '
        'are excluded from the analytical lenses above.',
    )

    df_ne_inst = query_df(
        f"""SELECT a.institution_name AS institution,
                   a.institution_country AS country,
                   COUNT(DISTINCT a.openalex_id) AS n_papers
            FROM authorships a
            JOIN works w ON a.openalex_id = w.openalex_id
            {base_where}
            AND w.method_type IN ({ne_placeholders})
            AND a.institution_name IS NOT NULL
            AND a.institution_name != ''
            AND a.position IN ('first', 'last')
            GROUP BY a.institution_name, a.institution_country
            ORDER BY n_papers DESC
            LIMIT 20""",
        tuple(params + list(NON_EMPIRICAL_METHODS)),
    )

    if not df_ne_inst.empty:
        total_ne = query_scalar(
            f"""SELECT COUNT(*)
                FROM works w {base_where}
                AND w.method_type IN ({ne_placeholders})""",
            tuple(params + list(NON_EMPIRICAL_METHODS)),
        )

        metric_row([
            ('Total Commentary/Editorials', total_ne or 0, None),
            ('Top Institution (Commentary)',
             df_ne_inst.iloc[0]['institution'], None),
        ])

        df_ne_inst['country_name'] = df_ne_inst['country'].apply(iso2_to_country_name)
        fig = px.bar(
            df_ne_inst, y='institution', x='n_papers', orientation='h',
            color='country_name',
            labels={'n_papers': 'Commentary/Editorial Papers',
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
            'Institutions ranked by commentary, editorial, and perspective '
            'publications. High commentary output can signal thought leadership '
            'and influence on research priorities and policy discourse.'
        )
    else:
        st.info(
            'No commentary/editorial publications found for the selected '
            'filters.',
            icon=':material/info:',
        )
