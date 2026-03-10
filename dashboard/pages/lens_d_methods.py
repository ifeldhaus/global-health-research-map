"""
dashboard/pages/lens_d_methods.py

Lens D — Methods Gaps: Where are the missing study designs?

Analytical interactions:
- Z-score heatmap (topic × method, with gap narrative on click)
- Methods transfer scorecard (top 20 gaps ranked by impact)
- Country-method profile (country's methods vs global average)
- Method adoption trajectory (how methods spread across topics over time)
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
    UNCATEGORIZED_TOPICS,
    CHART_TEMPLATE, CHART_HEIGHT, CHART_HEIGHT_TALL, DIVERGING_COLORSCALE,
)
from dashboard.db import query_df, query_scalar, build_where_clause


def page():
    st.title('Methods Gaps')
    st.caption(
        'Where are the missing study designs? Which topic\u2013method '
        'combinations are under-represented relative to what we\'d expect?'
    )

    if not check_data_ready(require_topics=True, require_methods=True):
        return

    year_range = st.session_state.get('year_range', (2010, 2024))
    topics = st.session_state.get('selected_topics', [])
    where, params = build_where_clause(year_range=year_range, topics=topics or None)

    # Exclude non-empirical publications (commentary/editorials) from analysis
    ne_placeholders = ', '.join(['?'] * len(NON_EMPIRICAL_METHODS))
    ne_clause = (f" AND (w.method_type IS NULL "
                 f"OR w.method_type NOT IN ({ne_placeholders}))")
    # Exclude uncategorized topics from visualizations
    uc_placeholders = ', '.join(['?'] * len(UNCATEGORIZED_TOPICS))
    uc_clause = f" AND (w.topic_category IS NULL OR w.topic_category NOT IN ({uc_placeholders}))"
    base_where = f"WHERE TRUE {where}{ne_clause}{uc_clause}"
    params = params + list(NON_EMPIRICAL_METHODS) + list(UNCATEGORIZED_TOPICS)

    # ------------------------------------------------------------------
    # Methods overview
    # ------------------------------------------------------------------
    section_header(
        'Methods Usage Overview',
        'Distribution of study designs across the corpus.',
    )

    df_methods = query_df(
        f"""SELECT method_type AS method, COUNT(*) AS n
            FROM works w
            {base_where} AND method_type IS NOT NULL
            GROUP BY method_type ORDER BY n DESC""",
        tuple(params),
    )

    if not df_methods.empty:
        total_classified = df_methods['n'].sum()
        df_methods['pct'] = (df_methods['n'] / total_classified * 100).round(1)
        df_methods['label'] = df_methods['method'].map(
            lambda m: METHOD_LABELS.get(m, m)
        )

        # Top methods metric
        top3 = df_methods.head(3)
        metric_row([
            (top3.iloc[0]['label'], f"{top3.iloc[0]['pct']:.0f}%", None),
            (top3.iloc[1]['label'], f"{top3.iloc[1]['pct']:.0f}%", None),
            (top3.iloc[2]['label'], f"{top3.iloc[2]['pct']:.0f}%", None),
            ('Total Classified', int(total_classified), None),
        ])

    # ------------------------------------------------------------------
    # Topic × Method z-score heatmap
    # ------------------------------------------------------------------
    section_header(
        'Topic \u00d7 Method Z-Score Heatmap',
        'Each cell shows how many standard deviations the observed count '
        'differs from the expected count (based on row and column marginals). '
        'Blue = under-represented gap. Red = over-represented.',
    )

    df_cross = query_df(
        f"""SELECT topic_category AS cat, method_type AS method,
                   COUNT(*) AS n
            FROM works w
            {base_where}
            AND topic_category IS NOT NULL
            AND method_type IS NOT NULL
            GROUP BY topic_category, method_type""",
        tuple(params),
    )

    if not df_cross.empty:
        # Build contingency table
        pivot = df_cross.pivot_table(
            index='cat', columns='method', values='n', fill_value=0,
        )

        # Compute expected values under independence
        row_sums = pivot.sum(axis=1)
        col_sums = pivot.sum(axis=0)
        grand_total = pivot.values.sum()

        expected = np.outer(row_sums, col_sums) / grand_total
        expected_df = pd.DataFrame(
            expected, index=pivot.index, columns=pivot.columns,
        )

        # Z-scores: (observed - expected) / sqrt(expected)
        with np.errstate(divide='ignore', invalid='ignore'):
            z_scores = (pivot.values - expected) / np.sqrt(expected)
            z_scores = np.nan_to_num(z_scores, nan=0.0)

        z_df = pd.DataFrame(z_scores, index=pivot.index, columns=pivot.columns)

        # Rename axes for display
        z_display = z_df.copy()
        z_display.index = [TOPIC_LABELS.get(c, c) for c in z_display.index]
        z_display.columns = [METHOD_LABELS.get(m, m) for m in z_display.columns]

        # Filter out methods with very few papers (noise)
        # Use lower threshold for small datasets
        min_method_count = min(10, max(2, int(grand_total * 0.005)))
        keep_methods = col_sums[col_sums >= min_method_count].index
        z_filtered = z_df[keep_methods]
        z_filtered_display = z_filtered.copy()
        z_filtered_display.index = [
            TOPIC_LABELS.get(c, c) for c in z_filtered_display.index
        ]
        z_filtered_display.columns = [
            METHOD_LABELS.get(m, m) for m in z_filtered_display.columns
        ]

        fig = px.imshow(
            z_filtered_display.values,
            x=z_filtered_display.columns.tolist(),
            y=z_filtered_display.index.tolist(),
            color_continuous_scale=DIVERGING_COLORSCALE,
            color_continuous_midpoint=0,
            zmin=-4, zmax=4,
            labels={'color': 'Z-Score'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=max(500, len(z_filtered_display) * 45),
            xaxis_tickangle=-45,
        )
        st.plotly_chart(fig, use_container_width=True)

        # ------------------------------------------------------------------
        # Gap narrative drill-down
        # ------------------------------------------------------------------
        st.markdown('#### Gap Opportunity Explorer')
        st.caption(
            'Select a topic and method to see a narrative summary of the gap.'
        )

        col1, col2 = st.columns(2)
        with col1:
            gap_topic = st.selectbox(
                'Topic:',
                options=sorted(pivot.index.tolist()),
                format_func=lambda c: TOPIC_LABELS.get(c, c),
                key='gap_topic',
            )
        with col2:
            gap_method = st.selectbox(
                'Method:',
                options=sorted(
                    [m for m in pivot.columns if m in keep_methods],
                ),
                format_func=lambda m: METHOD_LABELS.get(m, m),
                key='gap_method',
            )

        if gap_topic and gap_method:
            observed = int(pivot.loc[gap_topic, gap_method])
            exp = expected_df.loc[gap_topic, gap_method]
            z = z_df.loc[gap_topic, gap_method]

            topic_total = int(row_sums[gap_topic])
            method_total = int(col_sums[gap_method])

            # Most common method for this topic
            topic_methods = pivot.loc[gap_topic]
            most_common = topic_methods.idxmax()
            most_common_pct = (
                topic_methods[most_common] / topic_methods.sum() * 100
            )

            topic_name = TOPIC_LABELS.get(gap_topic, gap_topic)
            method_name = METHOD_LABELS.get(gap_method, gap_method)
            mc_name = METHOD_LABELS.get(most_common, most_common)

            if z < -1.5:
                assessment = (
                    f'**Under-represented gap** (z = {z:.1f}). '
                    f'Only {observed} papers used {method_name} for '
                    f'{topic_name}, vs. {exp:.0f} expected under independence. '
                    f'This topic has {topic_total:,} total papers, of which '
                    f'{most_common_pct:.0f}% use the most common method '
                    f'({mc_name}). A new {method_name} study in this area '
                    f'would fill a methodological gap.'
                )
                st.warning(assessment, icon=':material/lightbulb:')
            elif z > 1.5:
                assessment = (
                    f'**Over-represented** (z = {z:.1f}). '
                    f'{observed} papers used {method_name} for {topic_name}, '
                    f'vs. {exp:.0f} expected. This topic\u2013method combination '
                    f'is well-covered.'
                )
                st.success(assessment, icon=':material/check_circle:')
            else:
                assessment = (
                    f'**Near expected** (z = {z:.1f}). '
                    f'{observed} papers used {method_name} for {topic_name} '
                    f'(expected: {exp:.0f}). No major gap or surplus.'
                )
                st.info(assessment, icon=':material/balance:')

        # ------------------------------------------------------------------
        # Methods transfer scorecard
        # ------------------------------------------------------------------
        section_header(
            'Methods Transfer Scorecard',
            'Top methodological gaps ranked by potential impact '
            '(z-score magnitude \u00d7 topic paper volume). Where would a '
            'single new study have the most impact?',
        )

        # Build scorecard from z-scores
        # Use adaptive threshold: -1.5 for large datasets, -1.0 for small
        z_threshold = -1.5 if grand_total >= 5000 else -1.0
        gap_records = []
        for cat in z_df.index:
            for method in keep_methods:
                z_val = z_df.loc[cat, method]
                if z_val < z_threshold:  # gaps
                    obs = int(pivot.loc[cat, method])
                    exp_val = expected_df.loc[cat, method]
                    topic_vol = int(row_sums[cat])
                    impact = abs(z_val) * np.log1p(topic_vol)
                    gap_records.append({
                        'topic': TOPIC_LABELS.get(cat, cat),
                        'method': METHOD_LABELS.get(method, method),
                        'cat': cat,
                        'method_id': method,
                        'observed': obs,
                        'expected': round(exp_val),
                        'z_score': round(z_val, 2),
                        'topic_volume': topic_vol,
                        'impact_score': round(impact, 1),
                    })

        if gap_records:
            df_gaps = pd.DataFrame(gap_records).sort_values(
                'impact_score', ascending=False
            ).head(20)

            st.dataframe(
                df_gaps[['topic', 'method', 'observed', 'expected',
                         'z_score', 'topic_volume', 'impact_score']].rename(
                    columns={
                        'topic': 'Topic', 'method': 'Method',
                        'observed': 'Observed', 'expected': 'Expected',
                        'z_score': 'Z-Score', 'topic_volume': 'Topic Papers',
                        'impact_score': 'Impact Score',
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
            download_csv_button(df_gaps, 'methods_gaps_scorecard.csv')
        else:
            st.info(
                f'No significant methodological gaps found '
                f'(z-score < {z_threshold}) with current filters. '
                f'This may indicate a small corpus ({int(grand_total):,} '
                f'classified papers) where method distributions are '
                f'relatively uniform, or that the selected topic/method '
                f'filters are too narrow.',
                icon=':material/info:',
            )

    # ------------------------------------------------------------------
    # Country-method profile
    # ------------------------------------------------------------------
    section_header(
        'Country\u2013Method Profile',
        'Compare a country\'s method usage distribution against the '
        'global average. Diverging bars show over/under-use.',
    )

    df_countries = query_df(
        f"""SELECT study_country AS country, COUNT(*) AS n
            FROM works w {base_where}
            AND study_country IS NOT NULL AND study_country != 'GLOBAL'
            AND method_type IS NOT NULL
            GROUP BY study_country ORDER BY n DESC LIMIT 30""",
        tuple(params),
    )

    if not df_countries.empty:
        selected_country = st.selectbox(
            'Select study country:',
            options=df_countries['country'].tolist(),
            key='method_country',
        )

        if selected_country:
            # Country method distribution
            df_country_methods = query_df(
                f"""SELECT method_type AS method, COUNT(*) AS n
                    FROM works w {base_where}
                    AND study_country = ? AND method_type IS NOT NULL
                    GROUP BY method_type""",
                tuple(params + [selected_country]),
            )

            # Global method distribution
            df_global_methods = query_df(
                f"""SELECT method_type AS method, COUNT(*) AS n
                    FROM works w {base_where}
                    AND method_type IS NOT NULL
                    GROUP BY method_type""",
                tuple(params),
            )

            if not df_country_methods.empty and not df_global_methods.empty:
                country_total = df_country_methods['n'].sum()
                global_total = df_global_methods['n'].sum()

                df_country_methods['pct'] = (
                    df_country_methods['n'] / country_total * 100
                )
                df_global_methods['pct'] = (
                    df_global_methods['n'] / global_total * 100
                )

                df_compare = df_country_methods[['method', 'pct']].rename(
                    columns={'pct': 'country_pct'}
                ).merge(
                    df_global_methods[['method', 'pct']].rename(
                        columns={'pct': 'global_pct'}
                    ),
                    on='method', how='outer',
                ).fillna(0)

                df_compare['divergence'] = (
                    df_compare['country_pct'] - df_compare['global_pct']
                ).round(1)
                df_compare['label'] = df_compare['method'].map(
                    lambda m: METHOD_LABELS.get(m, m)
                )
                df_compare = df_compare.sort_values('divergence')

                colors = ['#2ca02c' if v > 0 else '#d62728'
                          for v in df_compare['divergence']]

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    y=df_compare['label'],
                    x=df_compare['divergence'],
                    orientation='h',
                    marker_color=colors,
                    hovertemplate=(
                        '%{y}<br>'
                        f'{selected_country}: ' + '%{customdata[0]:.1f}%<br>'
                        'Global: %{customdata[1]:.1f}%<br>'
                        'Divergence: %{x:+.1f} pp'
                        '<extra></extra>'
                    ),
                    customdata=df_compare[['country_pct', 'global_pct']].values,
                ))
                fig.add_vline(x=0, line_color='gray')
                fig.update_layout(
                    template=CHART_TEMPLATE,
                    height=max(400, len(df_compare) * 30),
                    xaxis_title=f'Divergence from Global Average (pp)',
                    title=f'{selected_country} vs Global Method Usage',
                )
                st.plotly_chart(fig, use_container_width=True)

                st.caption(
                    f'Green = {selected_country} uses this method more than '
                    f'the global average. Red = less than average. '
                    f'Based on {country_total:,} papers from {selected_country} '
                    f'vs. {global_total:,} globally.'
                )

    # ------------------------------------------------------------------
    # Method adoption trajectory
    # ------------------------------------------------------------------
    section_header(
        'Method Adoption Over Time',
        'Select a method to see how its usage has spread across '
        'topics over time.',
    )

    if not df_methods.empty:
        selected_method = st.selectbox(
            'Select method:',
            options=df_methods['method'].tolist(),
            format_func=lambda m: METHOD_LABELS.get(m, m),
            key='adoption_method',
        )

        if selected_method:
            df_adoption = query_df(
                f"""SELECT publication_year AS year,
                           topic_category AS cat,
                           COUNT(*) AS n
                    FROM works w
                    {base_where}
                    AND method_type = ?
                    AND topic_category IS NOT NULL
                    GROUP BY publication_year, topic_category
                    ORDER BY year, cat""",
                tuple(params + [selected_method]),
            )

            if not df_adoption.empty:
                df_adoption['label'] = df_adoption['cat'].map(
                    lambda c: TOPIC_LABELS.get(c, c)
                )

                # Stacked area showing adoption across topics
                fig = px.area(
                    df_adoption, x='year', y='n', color='label',
                    color_discrete_sequence=list(TOPIC_COLORS.values()),
                    labels={'year': 'Year', 'n': 'Papers', 'label': 'Topic'},
                    title=f'{METHOD_LABELS.get(selected_method, selected_method)} '
                          f'Adoption by Topic',
                    template=CHART_TEMPLATE,
                )
                fig.update_layout(
                    height=CHART_HEIGHT,
                    legend=dict(font=dict(size=10)),
                )
                fig.update_xaxes(dtick=1)  # whole years only
                st.plotly_chart(fig, use_container_width=True)

                # Growth rates by topic for this method
                growth_data = []
                for cat in df_adoption['cat'].unique():
                    cat_data = df_adoption[df_adoption['cat'] == cat]
                    if len(cat_data) >= 3:
                        first_half = cat_data[
                            cat_data['year'] <= cat_data['year'].median()
                        ]['n'].mean()
                        second_half = cat_data[
                            cat_data['year'] > cat_data['year'].median()
                        ]['n'].mean()
                        if first_half > 0:
                            growth = ((second_half - first_half) /
                                      first_half * 100)
                            growth_data.append({
                                'topic': TOPIC_LABELS.get(cat, cat),
                                'growth_pct': round(growth, 1),
                                'total': int(cat_data['n'].sum()),
                            })

                # Count how many topics were excluded and why
                all_topics = df_adoption['cat'].unique()
                n_total_topics = len(all_topics)

                if growth_data:
                    df_growth = pd.DataFrame(growth_data).sort_values(
                        'growth_pct', ascending=True,
                    )

                    fig = px.bar(
                        df_growth, y='topic', x='growth_pct',
                        orientation='h',
                        labels={
                            'growth_pct': 'Growth (%)',
                            'topic': '',
                        },
                        title=f'{METHOD_LABELS.get(selected_method, selected_method)} '
                              f'Growth Rate by Topic',
                        template=CHART_TEMPLATE,
                        color='growth_pct',
                        color_continuous_scale='RdYlGn',
                        color_continuous_midpoint=0,
                    )
                    fig.update_layout(
                        height=max(350, len(df_growth) * 30),
                        coloraxis_showscale=False,
                    )
                    st.plotly_chart(fig, use_container_width=True)

                    n_shown = len(df_growth)
                    n_excluded = n_total_topics - n_shown
                    if n_excluded > 0:
                        st.caption(
                            f'Showing {n_shown} of {n_total_topics} topics. '
                            f'{n_excluded} topic(s) excluded because they have '
                            f'fewer than 3 years of data for this method or '
                            f'zero papers in the first half of the time period, '
                            f'making growth rates incalculable. '
                            f'Growth = change in average annual papers between '
                            f'the first and second halves of the time period.'
                        )
                    else:
                        st.caption(
                            'Growth = change in average annual papers between '
                            'the first and second halves of the time period.'
                        )
                else:
                    st.info(
                        f'No growth data available for '
                        f'{METHOD_LABELS.get(selected_method, selected_method)}. '
                        f'This method may have too few papers across topics to '
                        f'compute reliable growth rates (requires 3+ years of '
                        f'data per topic).',
                        icon=':material/info:',
                    )
