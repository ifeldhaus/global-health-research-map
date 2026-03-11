"""
dashboard/pages/lens_c_topics.py

Lens C — Topic Trends: Are we researching what matters most?

Analytical interactions:
- DALYs vs Deaths side-by-side (dual bar: do conclusions change by measure?)
- Research intensity decomposition (click topic → pub share + burden share trends)
- COVID counterfactual (pre-2020 trend projected forward vs actual)
- Topic displacement (zero-sum: which topics gained/lost share)
- Fashionability vs intensity quadrant scatter
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
    TOPIC_COLORS, TOPIC_LABELS, NON_EMPIRICAL_METHODS, UNCATEGORIZED_TOPICS,
    QUAL_PALETTE, CHART_TEMPLATE, CHART_HEIGHT, CHART_HEIGHT_TALL,
)
from dashboard.db import query_df, query_scalar, build_where_clause, table_exists


def page():
    st.title('Topic Trends')
    st.caption(
        'Are we researching what matters most? How does research attention '
        'align with disease burden — and does it depend on how you measure it?'
    )

    if not check_data_ready(require_topics=True, require_gbd=True):
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

    has_gbd = table_exists('gbd_burden') and table_exists('topic_burden_map')

    # Track research intensity data across sections
    df_ri = None

    # ------------------------------------------------------------------
    # Topic publication volume over time
    # ------------------------------------------------------------------
    section_header(
        'Topic Publication Volume Over Time',
        'How has the volume of research across topics changed year by year?',
    )

    df_volume = query_df(
        f"""SELECT w.publication_year AS year,
                   w.topic_category AS cat,
                   COUNT(*) AS n
            FROM works w
            {base_where} AND w.topic_category IS NOT NULL
            GROUP BY w.publication_year, w.topic_category
            ORDER BY year, cat""",
        tuple(params),
    )

    if not df_volume.empty:
        df_volume['label'] = df_volume['cat'].map(
            lambda c: TOPIC_LABELS.get(c, c)
        )

        fig = px.area(
            df_volume, x='year', y='n', color='label',
            color_discrete_sequence=list(TOPIC_COLORS.values()),
            labels={'year': 'Year', 'n': 'Papers', 'label': 'Topic'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=CHART_HEIGHT, legend=dict(font=dict(size=10)),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Research intensity: DALYs vs Deaths side-by-side
    # ------------------------------------------------------------------
    if has_gbd:
        section_header(
            'Research Intensity: DALYs vs Deaths',
            'Research intensity ratio = publication share / burden share. '
            'Values > 1 indicate over-researched; < 1 under-researched. '
            'Comparing both burden measures reveals where conclusions diverge.',
        )

        # Which year to use for burden — use the latest available GBD year
        # that's within the publication year range
        gbd_year = query_scalar(
            "SELECT MAX(year) FROM gbd_burden WHERE measure = 'DALYs' "
            "AND metric = 'Number'"
        )

        if gbd_year:
            # Compute publication share by topic
            df_pub_share = query_df(
                f"""SELECT w.topic_category AS cat,
                           COUNT(*) AS n,
                           COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS pub_share
                    FROM works w
                    {base_where} AND w.topic_category IS NOT NULL
                    GROUP BY w.topic_category""",
                tuple(params),
            )

            # Compute burden share by topic (using topic_burden_map)
            df_burden_dalys = query_df(
                f"""SELECT tbm.topic_category AS cat,
                           SUM(g.val) AS burden_val,
                           SUM(g.val) * 1.0 /
                               SUM(SUM(g.val)) OVER () AS burden_share
                    FROM topic_burden_map tbm
                    JOIN gbd_burden g ON tbm.gbd_cause = g.cause
                    WHERE g.measure = 'DALYs' AND g.metric = 'Number'
                      AND g.year = ? AND g.sex = 'Both'
                      AND g.age_group = 'All ages'
                      AND g.region = 'Global'
                    GROUP BY tbm.topic_category""",
                (gbd_year,),
            )

            df_burden_deaths = query_df(
                f"""SELECT tbm.topic_category AS cat,
                           SUM(g.val) AS burden_val,
                           SUM(g.val) * 1.0 /
                               SUM(SUM(g.val)) OVER () AS burden_share
                    FROM topic_burden_map tbm
                    JOIN gbd_burden g ON tbm.gbd_cause = g.cause
                    WHERE g.measure = 'Deaths' AND g.metric = 'Number'
                      AND g.year = ? AND g.sex = 'Both'
                      AND g.age_group = 'All ages'
                      AND g.region = 'Global'
                    GROUP BY tbm.topic_category""",
                (gbd_year,),
            )

            if (not df_pub_share.empty and not df_burden_dalys.empty
                    and not df_burden_deaths.empty):
                # Merge pub share with both burden measures
                df_ri = df_pub_share[['cat', 'pub_share']].merge(
                    df_burden_dalys[['cat', 'burden_share']].rename(
                        columns={'burden_share': 'burden_dalys'}
                    ),
                    on='cat', how='inner',
                )
                df_ri = df_ri.merge(
                    df_burden_deaths[['cat', 'burden_share']].rename(
                        columns={'burden_share': 'burden_deaths'}
                    ),
                    on='cat', how='inner',
                )

                df_ri['ri_dalys'] = (
                    df_ri['pub_share'] / df_ri['burden_dalys']
                ).round(2)
                df_ri['ri_deaths'] = (
                    df_ri['pub_share'] / df_ri['burden_deaths']
                ).round(2)
                df_ri['label'] = df_ri['cat'].map(
                    lambda c: TOPIC_LABELS.get(c, c)
                )

                # Key metric: most over- and under-researched
                most_over = df_ri.loc[df_ri['ri_dalys'].idxmax()]
                most_under = df_ri.loc[df_ri['ri_dalys'].idxmin()]

                # Check if DALYs and Deaths agree
                dalys_rank = df_ri.sort_values('ri_dalys')['cat'].tolist()
                deaths_rank = df_ri.sort_values('ri_deaths')['cat'].tolist()
                rank_agreement = (
                    dalys_rank[:3] == deaths_rank[:3]  # top 3 under-researched agree
                )

                # Key findings — use columns with markdown for readability
                # (st.metric truncates long topic names)
                ri_cols = st.columns(4)
                with ri_cols[0]:
                    st.markdown('**Most Over-Researched**')
                    st.markdown(
                        f":red[{most_over['label']}] "
                        f"({most_over['ri_dalys']:.1f}x)"
                    )
                with ri_cols[1]:
                    st.markdown('**Most Under-Researched**')
                    st.markdown(
                        f":green[{most_under['label']}] "
                        f"({most_under['ri_dalys']:.1f}x)"
                    )
                with ri_cols[2]:
                    st.markdown('**DALYs vs Deaths**')
                    st.markdown(
                        ':white_check_mark: Top 3 agree'
                        if rank_agreement
                        else ':warning: Rankings differ'
                    )
                with ri_cols[3]:
                    st.markdown('**GBD Reference Year**')
                    st.markdown(f"{int(gbd_year)}")

                # Side-by-side grouped bar chart
                fig = make_subplots(
                    rows=1, cols=2,
                    subplot_titles=['Research Intensity (DALYs-based)',
                                    'Research Intensity (Deaths-based)'],
                    shared_yaxes=True,
                )

                df_sorted = df_ri.sort_values('ri_dalys', ascending=True)

                # DALYs panel
                colors_dalys = [
                    '#d62728' if v > 1.5 else '#2ca02c' if v < 0.5
                    else '#ff7f0e' if v > 1 else '#1f77b4'
                    for v in df_sorted['ri_dalys']
                ]
                fig.add_trace(
                    go.Bar(
                        y=df_sorted['label'], x=df_sorted['ri_dalys'],
                        orientation='h', name='DALYs',
                        marker_color=colors_dalys,
                        hovertemplate='%{y}: %{x:.2f}x<extra>DALYs</extra>',
                    ),
                    row=1, col=1,
                )

                # Deaths panel
                df_sorted_d = df_ri.sort_values('ri_dalys', ascending=True)
                colors_deaths = [
                    '#d62728' if v > 1.5 else '#2ca02c' if v < 0.5
                    else '#ff7f0e' if v > 1 else '#1f77b4'
                    for v in df_sorted_d['ri_deaths']
                ]
                fig.add_trace(
                    go.Bar(
                        y=df_sorted_d['label'], x=df_sorted_d['ri_deaths'],
                        orientation='h', name='Deaths',
                        marker_color=colors_deaths,
                        hovertemplate='%{y}: %{x:.2f}x<extra>Deaths</extra>',
                    ),
                    row=1, col=2,
                )

                # Parity lines at x=1
                for col in [1, 2]:
                    fig.add_vline(
                        x=1, line_dash='dash', line_color='gray',
                        annotation_text='Parity', row=1, col=col,
                    )

                fig.update_layout(
                    template=CHART_TEMPLATE,
                    height=max(500, len(df_ri) * 50),
                    showlegend=False,
                )
                fig.update_xaxes(title_text='Research Intensity Ratio')
                st.plotly_chart(fig, use_container_width=True)

                st.info(
                    '**How to read this chart:** The research intensity ratio '
                    'compares each topic\'s share of publications to its share '
                    'of global disease burden. A ratio of **1.0** (the dashed '
                    'parity line) means a topic receives research attention '
                    'exactly proportional to its burden. **Above 1** = '
                    'over-researched relative to burden; **below 1** = '
                    'under-researched. The left panel uses DALYs '
                    '(disability-adjusted life years, which capture both '
                    'premature death and years lived with disability) while the '
                    'right panel uses Deaths alone. Comparing the two reveals '
                    'topics where the choice of burden measure changes the '
                    'conclusion \u2014 for instance, mental health conditions '
                    'cause significant DALYs but fewer deaths, so a topic may '
                    'appear under-researched by DALYs but adequately covered '
                    'by deaths. Color coding: red (>1.5x), orange (1\u20131.5x), '
                    'blue (0.5\u20131x), green (<0.5x).',
                    icon=':material/info:',
                )

                # Divergence highlights
                df_ri['divergence'] = abs(df_ri['ri_dalys'] - df_ri['ri_deaths'])
                biggest_div = df_ri.sort_values('divergence', ascending=False).head(3)
                if not biggest_div.empty:
                    st.info(
                        '**Biggest divergences between DALYs and Deaths:** '
                        + ', '.join([
                            f"{row['label']} ({row['ri_dalys']:.1f}x DALYs vs "
                            f"{row['ri_deaths']:.1f}x Deaths)"
                            for _, row in biggest_div.iterrows()
                        ]),
                        icon=':material/compare_arrows:',
                    )

                download_csv_button(
                    df_ri[['cat', 'label', 'pub_share', 'burden_dalys',
                           'burden_deaths', 'ri_dalys', 'ri_deaths']],
                    'research_intensity_comparison.csv',
                )

        # ------------------------------------------------------------------
        # Research intensity decomposition
        # ------------------------------------------------------------------
        if has_gbd and not df_volume.empty:
            section_header(
                'Research Intensity Decomposition',
                'Select a topic to see whether changes in research intensity '
                'are driven by publication trends, burden trends, or both.',
            )

            # Build topic selector from available mappings
            mapped_topics = query_df(
                "SELECT DISTINCT topic_category FROM topic_burden_map "
                "ORDER BY topic_category"
            )

            st.info(
                '**How to read this chart:** Research intensity can change '
                'for two reasons: (1) the topic\'s share of publications '
                'changes (left panel), or (2) the disease burden itself '
                'shifts (right panel). By plotting both side by side, you '
                'can see whether a topic is becoming more neglected because '
                'researchers moved away, because the burden grew, or both. '
                'For example, if the left panel (publication share) is flat '
                'but the right panel (burden share) is rising, the topic is '
                'becoming under-researched not because of declining interest '
                'but because the disease is worsening. The auto-generated '
                'interpretation below each pair summarizes the key takeaway.',
                icon=':material/info:',
            )

            if not mapped_topics.empty:
                topic_options_list = mapped_topics['topic_category'].tolist()
                decomp_topic = st.selectbox(
                    'Select topic for decomposition:',
                    options=topic_options_list,
                    format_func=lambda c: TOPIC_LABELS.get(c, c),
                    key='decomp_topic',
                )

                if decomp_topic:
                    # Get annual publication share for this topic
                    df_pub_trend = query_df(
                        f"""WITH yearly_total AS (
                                SELECT publication_year AS year, COUNT(*) AS total
                                FROM works w {base_where}
                                AND topic_category IS NOT NULL
                                GROUP BY publication_year
                            ),
                            yearly_topic AS (
                                SELECT publication_year AS year, COUNT(*) AS n
                                FROM works w {base_where}
                                AND topic_category = ?
                                GROUP BY publication_year
                            )
                            SELECT yt.year,
                                   COALESCE(t.n, 0) AS papers,
                                   yt.total,
                                   COALESCE(t.n, 0) * 100.0 / yt.total AS pub_share
                            FROM yearly_total yt
                            LEFT JOIN yearly_topic t ON yt.year = t.year
                            ORDER BY yt.year""",
                        tuple(params + params + [decomp_topic]),
                    )

                    # Get annual burden share for this topic
                    df_burden_trend = query_df(
                        f"""WITH topic_burden AS (
                                SELECT g.year,
                                       SUM(g.val) AS burden
                                FROM topic_burden_map tbm
                                JOIN gbd_burden g ON tbm.gbd_cause = g.cause
                                WHERE tbm.topic_category = ?
                                  AND g.measure = 'DALYs' AND g.metric = 'Number'
                                  AND g.sex = 'Both' AND g.age_group = 'All ages'
                                  AND g.region = 'Global'
                                GROUP BY g.year
                            ),
                            total_burden AS (
                                SELECT g.year, SUM(g.val) AS total
                                FROM gbd_burden g
                                WHERE g.measure = 'DALYs' AND g.metric = 'Number'
                                  AND g.sex = 'Both' AND g.age_group = 'All ages'
                                  AND g.region = 'Global'
                                  AND g.cause IN (
                                      SELECT DISTINCT gbd_cause
                                      FROM topic_burden_map
                                  )
                                GROUP BY g.year
                            )
                            SELECT tb.year,
                                   tb.burden,
                                   tt.total AS total_burden,
                                   tb.burden * 100.0 / tt.total AS burden_share
                            FROM topic_burden tb
                            JOIN total_burden tt ON tb.year = tt.year
                            ORDER BY tb.year""",
                        (decomp_topic,),
                    )

                    if not df_pub_trend.empty:
                        fig = make_subplots(
                            rows=1, cols=2,
                            subplot_titles=[
                                f'Publication Share: '
                                f'{TOPIC_LABELS.get(decomp_topic, decomp_topic)}',
                                f'Burden Share (DALYs): '
                                f'{TOPIC_LABELS.get(decomp_topic, decomp_topic)}',
                            ],
                        )

                        fig.add_trace(
                            go.Scatter(
                                x=df_pub_trend['year'],
                                y=df_pub_trend['pub_share'],
                                mode='lines+markers',
                                name='Publication Share',
                                line=dict(color='#1f77b4', width=2),
                                fill='tozeroy',
                                fillcolor='rgba(31, 119, 180, 0.1)',
                            ),
                            row=1, col=1,
                        )

                        if not df_burden_trend.empty:
                            fig.add_trace(
                                go.Scatter(
                                    x=df_burden_trend['year'],
                                    y=df_burden_trend['burden_share'],
                                    mode='lines+markers',
                                    name='Burden Share (DALYs)',
                                    line=dict(color='#d62728', width=2),
                                    fill='tozeroy',
                                    fillcolor='rgba(214, 39, 40, 0.1)',
                                ),
                                row=1, col=2,
                            )

                        fig.update_layout(
                            template=CHART_TEMPLATE, height=400,
                        )
                        fig.update_yaxes(title_text='Share (%)')
                        st.plotly_chart(fig, use_container_width=True)

                        # Interpretation
                        if not df_burden_trend.empty and len(df_pub_trend) >= 2:
                            pub_change = (df_pub_trend['pub_share'].iloc[-1]
                                          - df_pub_trend['pub_share'].iloc[0])
                            burden_change = (df_burden_trend['burden_share'].iloc[-1]
                                             - df_burden_trend['burden_share'].iloc[0])

                            if pub_change > 0 and burden_change <= 0:
                                interp = ('Research attention grew while burden share '
                                          'stayed flat or declined \u2014 growing research '
                                          'interest beyond burden.')
                            elif pub_change < 0 and burden_change >= 0:
                                interp = ('Research attention declined while burden '
                                          'stayed flat or grew \u2014 increasing neglect.')
                            elif pub_change < 0 and burden_change < 0:
                                interp = ('Both research and burden shares declined '
                                          '\u2014 declining burden may justify less attention.')
                            else:
                                interp = ('Both research and burden shares grew \u2014 '
                                          'attention is tracking burden.')

                            st.info(
                                f'**Interpretation:** Pub share changed '
                                f'{pub_change:+.1f} pp, burden share changed '
                                f'{burden_change:+.1f} pp. {interp}',
                                icon=':material/analytics:',
                            )

    # ------------------------------------------------------------------
    # COVID displacement analysis
    # ------------------------------------------------------------------
    section_header(
        'COVID Displacement Analysis',
        'How did the COVID-19 pandemic reshape the global health research '
        'agenda? Pre-2020 trends are projected forward to estimate '
        'what would have happened without COVID.',
    )

    df_shares = query_df(
        f"""WITH yearly AS (
                SELECT publication_year AS year, topic_category AS cat,
                       COUNT(*) AS n
                FROM works w
                {base_where} AND topic_category IS NOT NULL
                GROUP BY publication_year, topic_category
            ),
            yearly_total AS (
                SELECT year, SUM(n) AS total FROM yearly GROUP BY year
            )
            SELECT y.year, y.cat, y.n,
                   y.n * 100.0 / yt.total AS share
            FROM yearly y
            JOIN yearly_total yt ON y.year = yt.year
            ORDER BY y.year, y.cat""",
        tuple(params),
    )

    if not df_shares.empty:
        df_shares['label'] = df_shares['cat'].map(
            lambda c: TOPIC_LABELS.get(c, c)
        )

        # Compute pre-COVID (2010-2019) vs COVID/post-COVID (2020+) share changes
        pre_covid = df_shares[df_shares['year'] < 2020].groupby('cat')['share'].mean()
        post_covid = df_shares[df_shares['year'] >= 2020].groupby('cat')['share'].mean()

        df_shift = pre_covid.to_frame('pre').join(
            post_covid.to_frame('post'), how='outer'
        ).fillna(0)
        df_shift['change'] = df_shift['post'] - df_shift['pre']
        df_shift = df_shift.reset_index()
        df_shift.columns = ['cat', 'pre_share', 'post_share', 'change']
        df_shift['label'] = df_shift['cat'].map(
            lambda c: TOPIC_LABELS.get(c, c)
        )

        # Winners and losers
        df_shift_sorted = df_shift.sort_values('change')

        fig = go.Figure()
        colors = ['#2ca02c' if v > 0 else '#d62728'
                  for v in df_shift_sorted['change']]

        fig.add_trace(go.Bar(
            y=df_shift_sorted['label'],
            x=df_shift_sorted['change'],
            orientation='h',
            marker_color=colors,
            hovertemplate=(
                '%{y}<br>'
                'Change: %{x:+.2f} pp<br>'
                '<extra></extra>'
            ),
        ))
        fig.add_vline(x=0, line_color='gray')
        fig.update_layout(
            template=CHART_TEMPLATE,
            height=max(400, len(df_shift) * 35),
            xaxis_title='Change in Publication Share (pp)',
            title='Publication Share Change: Pre-COVID (2010\u20132019) vs '
                  'COVID Era (2020+)',
        )
        st.plotly_chart(fig, use_container_width=True)

        # COVID counterfactual for the most impacted topics
        st.markdown('#### Counterfactual Projection for Key Topics')
        st.caption(
            'Pre-COVID trend (2010\u20132019) projected forward as a dashed line. '
            'The gap between projected and actual shows COVID\'s impact.'
        )

        # Show top 3 most displaced topics
        top_displaced = df_shift.reindex(
            df_shift['change'].abs().sort_values(ascending=False).index
        ).head(4)

        cols = st.columns(2)
        for idx, (_, row) in enumerate(top_displaced.iterrows()):
            cat = row['cat']
            topic_data = df_shares[df_shares['cat'] == cat].copy()

            if len(topic_data) < 5:
                continue

            pre = topic_data[topic_data['year'] < 2020]
            post = topic_data[topic_data['year'] >= 2020]

            with cols[idx % 2]:
                fig = go.Figure()

                # Actual data
                fig.add_trace(go.Scatter(
                    x=topic_data['year'], y=topic_data['share'],
                    mode='lines+markers', name='Actual',
                    line=dict(color=TOPIC_COLORS.get(cat, '#333'), width=2),
                ))

                # Linear trend from pre-COVID projected forward
                if len(pre) >= 3:
                    try:
                        coeffs = np.polyfit(pre['year'], pre['share'], 1)
                        all_years = topic_data['year'].values
                        projected = np.polyval(coeffs, all_years)

                        fig.add_trace(go.Scatter(
                            x=all_years, y=projected,
                            mode='lines', name='Pre-COVID trend',
                            line=dict(color='gray', dash='dash', width=1),
                        ))
                    except Exception:
                        pass

                fig.update_layout(
                    template=CHART_TEMPLATE,
                    height=300,
                    title=f'{TOPIC_LABELS.get(cat, cat)} '
                          f'({row["change"]:+.2f} pp)',
                    showlegend=True,
                    legend=dict(font=dict(size=9)),
                    yaxis_title='Share (%)',
                    xaxis_title='Year',
                    margin=dict(l=20, r=20, t=40, b=20),
                )
                st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Fashionability vs intensity quadrant
    # ------------------------------------------------------------------
    if has_gbd and not df_volume.empty:
        section_header(
            'Fashionability vs Research Intensity',
            'X = fashionability (growth in publication share), '
            'Y = research intensity ratio (pub share / burden share). '
            'Quadrants reveal persistent neglect vs trending topics.',
        )

        # Compute fashionability: slope of publication share over time
        if not df_shares.empty:
            fashion_data = []
            for cat in df_shares['cat'].unique():
                cat_data = df_shares[df_shares['cat'] == cat]
                if len(cat_data) >= 3:
                    try:
                        slope = np.polyfit(cat_data['year'], cat_data['share'], 1)[0]
                        fashion_data.append({
                            'cat': cat,
                            'fashionability': slope,
                        })
                    except Exception:
                        pass

            df_fashion = None
            if fashion_data:
                df_fashion = pd.DataFrame(fashion_data)

                # Merge with research intensity
                if df_ri is not None and not df_ri.empty:
                    df_quad = df_fashion.merge(
                        df_ri[['cat', 'ri_dalys', 'label']], on='cat', how='inner'
                    )

                    if not df_quad.empty:
                        fig = px.scatter(
                            df_quad,
                            x='fashionability', y='ri_dalys',
                            color='label',
                            color_discrete_map={
                                TOPIC_LABELS.get(k, k): v
                                for k, v in TOPIC_COLORS.items()
                            },
                            labels={
                                'fashionability': 'Fashionability '
                                                  '(annual share slope, pp/year)',
                                'ri_dalys': 'Research Intensity Ratio '
                                            '(DALYs-based)',
                                'label': 'Topic',
                            },
                            template=CHART_TEMPLATE,
                        )

                        # Add quadrant lines at medians
                        med_x = df_quad['fashionability'].median()
                        med_y = 1.0  # parity line

                        fig.add_hline(
                            y=med_y, line_dash='dash', line_color='gray',
                        )
                        fig.add_vline(
                            x=0, line_dash='dash', line_color='gray',
                        )

                        # Quadrant labels
                        x_range = (df_quad['fashionability'].max()
                                   - df_quad['fashionability'].min())
                        y_max = df_quad['ri_dalys'].max()

                        fig.add_annotation(
                            x=df_quad['fashionability'].max() - x_range * 0.15,
                            y=y_max * 0.9,
                            text='Trendy + Over-researched',
                            showarrow=False,
                            font=dict(color='gray', size=9),
                        )
                        fig.add_annotation(
                            x=df_quad['fashionability'].min() + x_range * 0.15,
                            y=y_max * 0.9,
                            text='Declining + Over-researched',
                            showarrow=False,
                            font=dict(color='gray', size=9),
                        )
                        fig.add_annotation(
                            x=df_quad['fashionability'].max() - x_range * 0.15,
                            y=0.15,
                            text='Trendy + Under-researched',
                            showarrow=False,
                            font=dict(color='#2ca02c', size=10),
                        )
                        fig.add_annotation(
                            x=df_quad['fashionability'].min() + x_range * 0.15,
                            y=0.15,
                            text='Persistent neglect',
                            showarrow=False,
                            font=dict(color='#d62728', size=10, weight='bold'),
                        )

                        fig.update_traces(marker_size=14)
                        fig.update_layout(
                            height=CHART_HEIGHT_TALL,
                            legend=dict(font=dict(size=9), title_text=''),
                        )
                        st.plotly_chart(fig, use_container_width=True)

                        st.info(
                            '**How to read this chart:** Each dot is a topic '
                            'category. The X-axis measures *fashionability* '
                            '\u2014 whether a topic\'s share of publications is '
                            'growing (right) or shrinking (left) over time. The '
                            'Y-axis measures *research intensity* relative to '
                            'disease burden (DALYs): above 1 means over-researched, '
                            'below 1 means under-researched. **Persistent neglect** '
                            '(bottom-left) is the most concerning: topics that are '
                            'both under-researched AND losing share. '
                            'If the chart appears sparse, it is because only '
                            'topics with both GBD burden data and sufficient '
                            'publication history (3+ years) can be plotted. '
                            'Topics without a GBD burden mapping cannot be '
                            'assessed for research intensity.',
                            icon=':material/info:',
                        )

    # ------------------------------------------------------------------
    # Topic publication share table
    # ------------------------------------------------------------------
    if not df_volume.empty:
        section_header(
            'Detailed Topic Data',
            'Sortable table with publication counts and shares.',
        )

        df_summary = df_volume.groupby(['cat', 'label']).agg(
            total_papers=('n', 'sum'),
            first_year=('year', 'min'),
            last_year=('year', 'max'),
        ).reset_index()

        total_all = df_summary['total_papers'].sum()
        df_summary['share_pct'] = (
            df_summary['total_papers'] / total_all * 100
        ).round(1)
        df_summary = df_summary.sort_values('total_papers', ascending=False)

        st.dataframe(
            df_summary[['label', 'total_papers', 'share_pct']].rename(
                columns={
                    'label': 'Topic',
                    'total_papers': 'Papers', 'share_pct': 'Share (%)',
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
        download_csv_button(df_summary, 'topic_summary.csv')
