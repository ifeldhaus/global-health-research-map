"""
dashboard/pages/lens_d_methods.py

Lens D (Methods Gaps): Where are the missing study designs?

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
    page_subtitle,
)
from dashboard.constants import (
    TOPIC_LABELS, METHOD_LABELS, TOPIC_COLORS, TOPIC_COLORS_BY_LABEL,
    NON_EMPIRICAL_METHODS, UNCATEGORIZED_TOPICS,
    CHART_TEMPLATE, CHART_HEIGHT, CHART_HEIGHT_TALL, DIVERGING_COLORSCALE,
    iso2_to_country_name,
)
from dashboard.db import query_df, query_scalar, build_where_clause


# Study designs grouped by how much weight they can bear for causal or
# effectiveness claims. A simplification (some designs sit on a spectrum),
# labelled as such in the chart caption.
RIGOR_TIERS = {
    'M01': 'Experimental & synthesis',   # RCT
    'M02': 'Experimental & synthesis',   # Quasi-experimental
    'M05': 'Experimental & synthesis',   # Systematic review / meta-analysis
    'M03': 'Observational & analytic',   # Cohort / longitudinal
    'M07': 'Observational & analytic',   # Modeling / simulation
    'M08': 'Observational & analytic',   # Economic evaluation
    'M09': 'Observational & analytic',   # Implementation science
    'M10': 'Observational & analytic',   # Geospatial
    'M11': 'Observational & analytic',   # Machine learning / AI
    'M12': 'Observational & analytic',   # Secondary data analysis
    'M04': 'Descriptive & exploratory',  # Cross-sectional survey
    'M06': 'Descriptive & exploratory',  # Qualitative / mixed methods
    'M13': 'Descriptive & exploratory',  # Scoping review
    'M16': 'Descriptive & exploratory',  # Descriptive epidemiology / ecological
    'M17': 'Descriptive & exploratory',  # Case study / case report
}
RIGOR_ORDER = [
    'Experimental & synthesis',
    'Observational & analytic',
    'Descriptive & exploratory',
]
RIGOR_COLORS = {
    'Experimental & synthesis': '#08519c',
    'Observational & analytic': '#4292c6',
    'Descriptive & exploratory': '#9ecae1',
}


def page():
    st.title('Methods Gaps')
    page_subtitle(
        'Which study designs does global health research rely on, how '
        'rigorous are they, and where are the methodological openings?'
    )

    if not check_data_ready(require_topics=True, require_methods=True):
        return

    year_range = st.session_state.get('year_range', (2010, 2025))
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
    # Summary bar (top-of-page indicators; no section header of its own)
    # ------------------------------------------------------------------
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

        # Top methods: label each by rank so it is clear these are the
        # most-used study designs. Allow long names to wrap in the value.
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
        top3 = df_methods.head(3)
        ordinals = ['Most-used study design', '2nd most-used', '3rd most-used']
        mc = st.columns(4)
        for i in range(min(3, len(top3))):
            with mc[i]:
                st.metric(ordinals[i], top3.iloc[i]['label'])
                st.caption(f"{top3.iloc[i]['pct']:.0f}% of classified papers")
        mc[3].metric('Study designs classified', f"{int(total_classified):,}")

    # ------------------------------------------------------------------
    # Scoping note: under the summary indicators.
    # ------------------------------------------------------------------
    if not df_methods.empty:
        st.info(
            'Every figure here counts **research articles with a classifiable '
            'study design**. Commentary, editorials, and perspectives have no '
            'study design and are excluded, as are papers without a usable '
            'abstract, so this covers a subset of the full corpus.',
            icon=':material/info:',
        )

    # ------------------------------------------------------------------
    # Study designs across the corpus
    # ------------------------------------------------------------------
    section_header(
        'Study Designs Across the Corpus',
        'How the classified research divides across study designs, and how '
        'much of it uses stronger versus more descriptive designs.',
    )

    if not df_methods.empty:
        dfm = df_methods.copy()
        dfm['tier'] = dfm['method'].map(RIGOR_TIERS).fillna(
            'Descriptive & exploratory')

        # Ranked designs, coloured by rigor tier. The shared legend lives on
        # this chart (in the top margin, clear of the bars).
        fig = px.bar(
            dfm.sort_values('pct'), y='label', x='pct', orientation='h',
            color='tier', category_orders={'tier': RIGOR_ORDER},
            color_discrete_map=RIGOR_COLORS,
            labels={'pct': 'Share of classified papers (%)', 'label': '',
                    'tier': ''},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=max(430, len(dfm) * 30),
            margin=dict(t=72),
            legend=dict(orientation='h', yanchor='bottom', y=1.02,
                        xanchor='left', x=0, title_text=''),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Same tiers summed into one 100% stacked bar (no legend of its own).
        tier_share = dfm.groupby('tier')['n'].sum()
        tier_share = (tier_share / tier_share.sum() * 100).reindex(
            RIGOR_ORDER).fillna(0)
        tdf = tier_share.rename('pct').reset_index()
        tdf['y'] = 'Evidence strength'
        fig2 = px.bar(
            tdf, y='y', x='pct', color='tier', orientation='h',
            category_orders={'tier': RIGOR_ORDER},
            color_discrete_map=RIGOR_COLORS,
            labels={'pct': 'Share of classified papers (%)', 'y': '',
                    'tier': ''},
            template=CHART_TEMPLATE,
        )
        fig2.update_traces(texttemplate='%{x:.0f}%', textposition='inside')
        fig2.update_layout(
            height=170, barmode='stack', xaxis_range=[0, 100],
            showlegend=False, margin=dict(t=10),
        )
        st.plotly_chart(fig2, use_container_width=True)
        st.caption(
            'Colour groups each design by how much weight it can bear for '
            'causal or effectiveness claims: **experimental & synthesis** '
            '(trials, quasi-experiments, systematic reviews), **observational '
            '& analytic** (cohorts, secondary analysis, modeling, economic '
            'evaluation), and **descriptive & exploratory** (cross-sectional '
            'surveys, descriptive epidemiology, qualitative, case studies). '
            'The lower bar sums these into the overall evidence strength of '
            'the field. The grouping is a simplification; some designs sit on '
            'a spectrum.'
        )

    # ------------------------------------------------------------------
    # Method mix by topic (evidence strength within each topic)
    # ------------------------------------------------------------------
    section_header(
        'Method Mix by Topic',
        'For each topic, the share of its research using stronger versus more '
        'descriptive study designs.',
    )

    df_tm = query_df(
        f"""SELECT topic_category AS cat, method_type AS method, COUNT(*) AS n
            FROM works w {base_where}
            AND topic_category IS NOT NULL AND method_type IS NOT NULL
            GROUP BY topic_category, method_type""",
        tuple(params),
    )

    if not df_tm.empty:
        df_tm['tier'] = df_tm['method'].map(RIGOR_TIERS).fillna(
            'Descriptive & exploratory')
        agg = df_tm.groupby(['cat', 'tier'], as_index=False)['n'].sum()
        agg['pct'] = agg['n'] / agg.groupby('cat')['n'].transform('sum') * 100
        agg['label'] = agg['cat'].map(lambda c: TOPIC_LABELS.get(c, c))
        # Order topics by their experimental & synthesis share (strongest at top).
        exp = (agg[agg['tier'] == 'Experimental & synthesis']
               .set_index('label')['pct'])
        topic_order = (exp.reindex(agg['label'].unique()).fillna(0)
                       .sort_values(ascending=False).index.tolist())

        fig = px.bar(
            agg, y='label', x='pct', color='tier', orientation='h',
            category_orders={'tier': RIGOR_ORDER, 'label': topic_order},
            color_discrete_map=RIGOR_COLORS,
            labels={'pct': "Share of the topic's papers (%)", 'label': '',
                    'tier': ''},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            barmode='stack', xaxis_range=[0, 100],
            height=max(450, agg['label'].nunique() * 34),
            margin=dict(t=72),
            legend=dict(orientation='h', yanchor='bottom', y=1.02,
                        xanchor='left', x=0, title_text=''),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            'Each topic\'s research split by study-design strength (the same '
            'three groups as above). Topics higher up rely more on '
            'experimental and synthesis designs; those lower down are more '
            'descriptive.'
        )

    # ------------------------------------------------------------------
    # Biggest methodological gaps
    # ------------------------------------------------------------------
    section_header(
        'Biggest Methodological Gaps',
        'Topic and study-design pairs used far less than the field-wide '
        'average would predict.',
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
        # Rank the biggest gaps: topic + method pairs used far less than you
        # would expect if the topic drew on methods like the field overall.
        z_threshold = -1.5 if grand_total >= 5000 else -1.0
        # Require the design to have been used for the topic at least a few
        # times, so we only flag gaps where the design is demonstrably
        # applicable, not absent because it does not fit the topic.
        min_observed = 5
        gap_records = []
        for cat in z_df.index:
            for method in keep_methods:
                z_val = z_df.loc[cat, method]
                obs = int(pivot.loc[cat, method])
                if z_val < z_threshold and obs >= min_observed:
                    exp_val = float(expected_df.loc[cat, method])
                    gap_records.append({
                        'topic': TOPIC_LABELS.get(cat, cat),
                        'method': METHOD_LABELS.get(method, method),
                        'observed': obs,
                        'expected': round(exp_val),
                        'shortfall': round(exp_val - obs),
                        'z_score': round(z_val, 2),
                        'topic_volume': int(row_sums[cat]),
                    })

        if gap_records:
            df_gaps = pd.DataFrame(gap_records).sort_values(
                'shortfall', ascending=False).head(12)
            df_gaps['pair'] = df_gaps['method'] + '  \u00b7  ' + df_gaps['topic']

            fig = px.bar(
                df_gaps.sort_values('shortfall'),
                y='pair', x='shortfall', orientation='h',
                labels={'shortfall': 'Papers below expected', 'pair': ''},
                template=CHART_TEMPLATE,
                custom_data=['observed', 'expected'],
            )
            fig.update_traces(
                marker_color='#2171b5',
                hovertemplate='%{y}<br>%{customdata[0]} papers vs about '
                              '%{customdata[1]} expected<extra></extra>',
            )
            fig.update_layout(height=max(400, len(df_gaps) * 38))
            st.plotly_chart(fig, use_container_width=True)
            st.info(
                'Expected = a topic\'s total papers × how common a study '
                'design is across all topics ÷ all classified papers. It is '
                'the count you would see if every topic used designs in the '
                'same proportions as the field overall; the z-score is how far '
                'below that a pair falls, in standard deviations.\n\n'
                'To keep this to defensible gaps only, pairs where the design '
                'has already been used for the topic at least a few times are '
                'shown, so each is a design demonstrably applicable to that '
                'topic. That rules out the cases where a design is simply a '
                'poor fit, such as running a randomized trial on the health '
                'effects of a war or famine, where you cannot assign who is '
                'exposed.',
                icon=':material/info:',
            )
            download_csv_button(
                df_gaps[['topic', 'method', 'observed', 'expected',
                         'shortfall', 'z_score', 'topic_volume']],
                'methods_gaps.csv',
            )
        else:
            st.info(
                f'No clear methodological gaps with the current filters '
                f'({int(grand_total):,} classified papers). Method use is '
                'fairly even across topics, or the filters are narrow.',
                icon=':material/info:',
            )

    # ------------------------------------------------------------------
    # Method usage over time
    # ------------------------------------------------------------------
    section_header(
        'Method Usage Over Time',
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
                    color_discrete_map=TOPIC_COLORS_BY_LABEL,
                    labels={'year': 'Year', 'n': 'Papers', 'label': 'Topic'},
                    title=f'{METHOD_LABELS.get(selected_method, selected_method)} '
                          f'Usage by Topic',
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
                        color='topic', color_discrete_map=TOPIC_COLORS_BY_LABEL,
                    )
                    fig.update_layout(
                        height=max(350, len(df_growth) * 30),
                        showlegend=False,
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

    # ------------------------------------------------------------------
    # Country–Method Profile (interactive drill-down, last)
    # ------------------------------------------------------------------
    section_header(
        'Country–Method Profile',
        'Compare a country\'s method usage distribution against the '
        'global average.',
    )

    df_countries = query_df(
        f"""SELECT study_country AS country, COUNT(*) AS n
            FROM works w {base_where}
            AND study_country IS NOT NULL
            AND study_country NOT IN ('GLOBAL', 'UNKNOWN')
            AND method_type IS NOT NULL
            GROUP BY study_country ORDER BY n DESC LIMIT 30""",
        tuple(params),
    )

    if not df_countries.empty:
        selected_country = st.selectbox(
            'Select study country:',
            options=df_countries['country'].tolist(),
            format_func=iso2_to_country_name,
            key='method_country',
        )

        if selected_country:
            country_name = iso2_to_country_name(selected_country)
            df_country_methods = query_df(
                f"""SELECT method_type AS method, COUNT(*) AS n
                    FROM works w {base_where}
                    AND study_country = ? AND method_type IS NOT NULL
                    GROUP BY method_type""",
                tuple(params + [selected_country]),
            )

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

                colors = ['#0072B2' if v > 0 else '#D55E00'
                          for v in df_compare['divergence']]

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    y=df_compare['label'],
                    x=df_compare['divergence'],
                    orientation='h',
                    marker_color=colors,
                    hovertemplate=(
                        '%{y}<br>'
                        f'{country_name}: ' + '%{customdata[0]:.1f}%<br>'
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
                    xaxis_title='Divergence from Global Average (pp)',
                    title=f'{country_name} vs Global Method Usage',
                )
                st.plotly_chart(fig, use_container_width=True)

                st.caption(
                    f'Green = {country_name} uses this method more than '
                    f'the global average. Red = less than average. '
                    f'Based on {country_total:,} papers from {country_name} '
                    f'vs. {global_total:,} globally.'
                )
