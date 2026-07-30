"""
dashboard/pages/lens_c_topics.py

Lens C (Topic Trends): Are we researching what matters most?

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
    page_subtitle,
)
from dashboard.constants import (
    TOPIC_COLORS, TOPIC_COLORS_BY_LABEL, TOPIC_LABELS, NON_EMPIRICAL_METHODS,
    UNCATEGORIZED_TOPICS, QUAL_PALETTE, CHART_TEMPLATE, CHART_HEIGHT,
    CHART_HEIGHT_TALL, LMIC_REGIONS, LMIC_BURDEN_LABEL,
)
from dashboard.db import query_df, query_scalar, build_where_clause, table_exists


def page():
    st.title('Topic Trends')
    page_subtitle(
        'Which topics does global health research focus on most? Does that '
        'effort match the disease burden, and is the balance changing over '
        'time?'
    )

    if not check_data_ready(require_topics=True, require_gbd=True):
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

    has_gbd = table_exists('gbd_burden') and table_exists('topic_burden_map')

    # Track research intensity data across sections
    df_ri = None

    # ------------------------------------------------------------------
    # Summary indicators (answer the subtitle questions at a glance)
    # ------------------------------------------------------------------
    df_tvol = query_df(
        f"""SELECT w.publication_year AS year, w.topic_category AS cat,
                   COUNT(*) AS n
            FROM works w
            {base_where} AND w.topic_category IS NOT NULL
            GROUP BY 1, 2""",
        tuple(params),
    )
    if not df_tvol.empty:
        totals = df_tvol.groupby('cat')['n'].sum()
        overall = totals.sum()
        top_cat = totals.idxmax()
        top_share = totals.max() / overall * 100

        yr_tot = df_tvol.groupby('year')['n'].sum()
        df_tvol['share'] = df_tvol.apply(
            lambda r: r['n'] / yr_tot[r['year']] * 100, axis=1)
        slopes = {c: float(np.polyfit(g['year'], g['share'], 1)[0])
                  for c, g in df_tvol.groupby('cat')
                  if g['year'].nunique() >= 5}
        rising = max(slopes, key=slopes.get) if slopes else None
        falling = min(slopes, key=slopes.get) if slopes else None

        gap_cat, gap_note = None, None
        if has_gbd:
            _ph = ', '.join(['?'] * len(LMIC_REGIONS))
            gy = query_scalar(
                f"SELECT MAX(year) FROM gbd_burden WHERE measure = 'DALYs' "
                f"AND metric = 'Number' AND region IN ({_ph})",
                tuple(LMIC_REGIONS))
            df_b = query_df(
                f"""SELECT tbm.topic_category AS cat, SUM(g.val) AS v
                    FROM topic_burden_map tbm
                    JOIN gbd_burden g ON tbm.gbd_cause = g.cause
                    WHERE g.measure = 'DALYs' AND g.metric = 'Number'
                      AND g.sex = 'Both' AND g.age_group = 'All ages'
                      AND g.region IN ({_ph}) AND g.year = ?
                    GROUP BY 1""",
                tuple(list(LMIC_REGIONS) + [gy]))
            if not df_b.empty:
                df_b['bshare'] = df_b['v'] / df_b['v'].sum() * 100
                pub_sh = (totals / overall * 100).rename('pshare').reset_index()
                merged = df_b.merge(pub_sh, on='cat')
                merged['gap'] = merged['bshare'] - merged['pshare']
                row = merged.loc[merged['gap'].idxmax()]
                gap_cat = row['cat']
                gap_note = (f"{row['bshare']:.0f}% of burden, "
                            f"{row['pshare']:.0f}% of papers")

        # Allow long topic names to wrap in the metric value (default truncates).
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
        gc = st.columns(4)
        with gc[0]:
            st.metric('Most-researched topic',
                      TOPIC_LABELS.get(top_cat, top_cat),
                      help='The topic category with the largest share of '
                           'classified publications.')
            st.caption(f'{top_share:.0f}% of papers')
        with gc[1]:
            st.metric('Largest burden–research gap',
                      TOPIC_LABELS.get(gap_cat, gap_cat) if gap_cat else '—',
                      help='The topic whose share of low- and middle-income '
                           'disease burden most exceeds its share of '
                           'publications.')
            if gap_note:
                st.caption(gap_note)
        with gc[2]:
            st.metric('Fastest-rising share',
                      TOPIC_LABELS.get(rising, rising) if rising else '—',
                      help='The topic whose share of publications has grown '
                           'fastest over the study period.')
            if rising:
                st.caption(f'+{slopes[rising]:.1f} pp per year')
        with gc[3]:
            st.metric('Fastest-falling share',
                      TOPIC_LABELS.get(falling, falling) if falling else '—',
                      help='The topic whose share of publications has fallen '
                           'fastest over the study period.')
            if falling:
                st.caption(f'{slopes[falling]:.1f} pp per year')

    # ------------------------------------------------------------------
    # Scoping note: under the summary indicators, above the first section.
    # ------------------------------------------------------------------
    st.info(
        'These figures cover the **research corpus only**: commentary, '
        'editorials, and perspectives are excluded. "Share of publications" '
        'therefore means share of research articles, not of all content.',
        icon=':material/info:',
    )

    # ------------------------------------------------------------------
    # Share of publications by topic (opening view)
    # ------------------------------------------------------------------
    if not df_tvol.empty:
        section_header(
            'Share of Publications by Topic',
            'How the corpus divides across topic areas.',
        )
        rank = df_tvol.groupby('cat')['n'].sum()
        rank = (rank / rank.sum() * 100).rename('pct').reset_index()
        rank['label'] = rank['cat'].map(lambda c: TOPIC_LABELS.get(c, c))
        rank = rank.sort_values('pct')
        fig = px.bar(
            rank, y='label', x='pct', orientation='h',
            color='cat', color_discrete_map=TOPIC_COLORS,
            labels={'pct': 'Share of publications (%)', 'label': ''},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(height=max(400, len(rank) * 32), showlegend=False,
                          yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)

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
            color_discrete_map=TOPIC_COLORS_BY_LABEL,
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
            'Research Attention vs Disease Burden',
            'Each topic\'s share of published research alongside its share of '
            'low- and middle-income disease burden.',
        )

        # Burden benchmark = pooled low- and middle-income (World Bank) burden,
        # not global burden (which folds in the high-income NCD burden and would
        # understate NCDs in a corpus that is overwhelmingly about LMIC settings).
        # Placeholder list for the region IN (...) clause.
        _lmic_ph = ', '.join(['?'] * len(LMIC_REGIONS))

        # Which year to use for burden: latest year available for the LMIC
        # income aggregates (2023; the income-level extract is a single year).
        gbd_year = query_scalar(
            f"SELECT MAX(year) FROM gbd_burden WHERE measure = 'DALYs' "
            f"AND metric = 'Number' AND region IN ({_lmic_ph})",
            tuple(LMIC_REGIONS),
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
                      AND g.region IN ({_lmic_ph})
                    GROUP BY tbm.topic_category""",
                (gbd_year, *LMIC_REGIONS),
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
                      AND g.region IN ({_lmic_ph})
                    GROUP BY tbm.topic_category""",
                (gbd_year, *LMIC_REGIONS),
            )

            if (not df_pub_share.empty and not df_burden_dalys.empty
                    and not df_burden_deaths.empty):
                # Only topics with a GBD burden mapping can be compared.
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

                # Burden measure toggle (DALYs default; Deaths as a cross-check).
                measure = st.radio(
                    'Burden measure', ['DALYs', 'Deaths'],
                    horizontal=True, key='ri_measure',
                )
                bcol = 'burden_dalys' if measure == 'DALYs' else 'burden_deaths'

                dd = df_ri.copy()
                dd['pub_pct'] = dd['pub_share'] * 100
                dd['bur_pct'] = dd[bcol] * 100
                dd = dd.sort_values('bur_pct')

                PUB_C, BUR_C = '#E69F00', '#CC79A7'
                fig = go.Figure()
                for _, r in dd.iterrows():
                    fig.add_trace(go.Scatter(
                        x=[r['bur_pct'], r['pub_pct']],
                        y=[r['label'], r['label']],
                        mode='lines', line=dict(color='#cccccc', width=2),
                        showlegend=False, hoverinfo='skip',
                    ))
                fig.add_trace(go.Scatter(
                    x=dd['bur_pct'], y=dd['label'], mode='markers',
                    name=f'Share of LMIC disease burden ({measure})',
                    marker=dict(color=BUR_C, size=12),
                    hovertemplate='%{y}<br>Burden share: %{x:.1f}%<extra></extra>',
                ))
                fig.add_trace(go.Scatter(
                    x=dd['pub_pct'], y=dd['label'], mode='markers',
                    name='Share of publications',
                    marker=dict(color=PUB_C, size=12),
                    hovertemplate='%{y}<br>Publication share: %{x:.1f}%<extra></extra>',
                ))
                fig.update_layout(
                    template=CHART_TEMPLATE,
                    height=max(500, len(dd) * 40),
                    xaxis_title='Share (%)',
                    legend=dict(orientation='h', y=1.06, title_text=''),
                )
                st.plotly_chart(fig, use_container_width=True)

                st.info(
                    'For each topic, the two dots '
                    'show its share of published research (amber) and its share '
                    f'of low- and middle-income disease burden (purple; '
                    f'{measure}, GBD 2023). This shows where the literature '
                    'sits relative to burden. Only topics with a GBD burden '
                    'mapping are shown.\n\n'
                    'A few things to keep in mind:\n\n'
                    '- **Equal shares is an arbitrary benchmark.** Publications '
                    'and disease burden measure different things.\n'
                    '- **Publication counts proxy attention, not effort or '
                    'funding.** Some fields produce many papers per study (for '
                    'example trials and cohorts), others few, so a larger '
                    'publication share need not mean more research resources.',
                    icon=':material/info:',
                )

                download_csv_button(
                    df_ri[['cat', 'label', 'pub_share',
                           'burden_dalys', 'burden_deaths']],
                    'research_attention_vs_burden.csv',
                )

        # ------------------------------------------------------------------
        # Research intensity decomposition
        # ------------------------------------------------------------------
        if has_gbd and not df_volume.empty:
            section_header(
                'Publication Share vs Burden Share Over Time',
                'Select a topic to see how its share of publications and its '
                'share of disease burden each moved over time.',
            )

            # Build topic selector from available mappings
            mapped_topics = query_df(
                "SELECT DISTINCT topic_category FROM topic_burden_map "
                "ORDER BY topic_category"
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
                            shared_yaxes=True,
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
                                line=dict(color='#E69F00', width=2),
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
                                    line=dict(color='#CC79A7', width=2),
                                ),
                                row=1, col=2,
                            )

                        fig.update_layout(
                            template=CHART_TEMPLATE, height=400,
                        )
                        # Log y (fixed across both panels and every topic) so
                        # small-share topics' trends stay legible instead of
                        # being flattened against a large linear maximum.
                        fig.update_yaxes(
                            title_text='Share (%)', type='log',
                            range=[np.log10(0.4), np.log10(60)],
                            tickvals=[0.5, 1, 2, 5, 10, 20, 50],
                            ticktext=['0.5', '1', '2', '5', '10', '20', '50'],
                        )
                        fig.update_xaxes(range=[year_range[0], year_range[1]])
                        st.plotly_chart(fig, use_container_width=True)

                        # Interpretation
                        if not df_burden_trend.empty and len(df_pub_trend) >= 2:
                            pub_change = (df_pub_trend['pub_share'].iloc[-1]
                                          - df_pub_trend['pub_share'].iloc[0])
                            burden_change = (df_burden_trend['burden_share'].iloc[-1]
                                             - df_burden_trend['burden_share'].iloc[0])

                            if pub_change > 0 and burden_change <= 0:
                                interp = ('Publication share rose while burden '
                                          'share was flat or falling.')
                            elif pub_change < 0 and burden_change >= 0:
                                interp = ('Publication share fell while burden '
                                          'share was flat or rising.')
                            elif pub_change < 0 and burden_change < 0:
                                interp = ('Both publication share and burden '
                                          'share fell.')
                            else:
                                interp = ('Both publication share and burden '
                                          'share rose.')

                            st.info(
                                f'**What changed:** Publication share changed '
                                f'{pub_change:+.1f} pp, burden share changed '
                                f'{burden_change:+.1f} pp. {interp}',
                                icon=':material/analytics:',
                            )

                        st.info(
                            'The left panel is the '
                            'topic\'s share of publications over time; the '
                            'right panel is its share of disease burden (DALYs) '
                            'over time. Placing them side by side shows how the '
                            'two trends move. Any resemblance is '
                            'an ecological observation, not cause and effect: '
                            'the direction, if there is one at all, could run '
                            'either way. Both panels share a fixed log vertical '
                            'axis, so small shares stay legible and topics can '
                            'be compared directly. *The burden trend uses '
                            'global GBD '
                            'burden (1980–2023), the only series available year '
                            'by year; the low- and middle-income burden shown '
                            'earlier is a single-year (2023) snapshot.*',
                            icon=':material/info:',
                        )

    # ------------------------------------------------------------------
    # Yearly topic shares (shared by the two change charts below)
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Which topics are gaining or losing publication share
    # ------------------------------------------------------------------
    if not df_shares.empty:
        yrs = sorted(df_shares['year'].unique())
        n_end = min(3, max(1, len(yrs) // 3))
        e0, e1 = int(yrs[0]), int(yrs[n_end - 1])
        l0, l1 = int(yrs[-n_end]), int(yrs[-1])
        early_lbl = f'{e0}' if e0 == e1 else f'{e0}–{e1}'
        late_lbl = f'{l0}' if l0 == l1 else f'{l0}–{l1}'

        section_header(
            'Which Topics Are Gaining or Losing Share',
            f'Change in each topic\'s share of publications from its '
            f'{early_lbl} average to its {late_lbl} average.',
        )

        early = (df_shares[df_shares['year'].isin(yrs[:n_end])]
                 .groupby('cat')['share'].mean())
        late = (df_shares[df_shares['year'].isin(yrs[-n_end:])]
                .groupby('cat')['share'].mean())
        chg = (late - early).dropna().reset_index()
        chg.columns = ['cat', 'change']
        chg['label'] = chg['cat'].map(lambda c: TOPIC_LABELS.get(c, c))
        chg = chg.sort_values('change')

        fig = px.bar(
            chg, y='label', x='change', orientation='h',
            color='cat', color_discrete_map=TOPIC_COLORS,
            labels={'change': 'Change in publication share (percentage points)',
                    'label': ''},
            template=CHART_TEMPLATE,
        )
        fig.add_vline(x=0, line_width=1, line_color='#888')
        fig.update_layout(height=max(400, len(chg) * 32), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            'Positive = the topic is a larger share of research now than at '
            'the start of the period.'
        )

    # ------------------------------------------------------------------
    # COVID displacement analysis (kept last: tangential but interesting)
    # ------------------------------------------------------------------
    if not df_shares.empty:
        section_header(
            'COVID Displacement Analysis',
            'How did the COVID-19 pandemic reshape the global health research '
            'agenda? Pre-2020 trends are projected forward to estimate '
            'what would have happened without COVID.',
        )

        # Pre-COVID (2010-2019) vs COVID/post-COVID (2020+) share changes
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

        df_shift_sorted = df_shift.sort_values('change')

        fig = go.Figure()
        colors = ['#0072B2' if v > 0 else '#D55E00'
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
            'Pre-COVID trend (2010\u20132019) projected forward as a dashed line.'
        )

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

            with cols[idx % 2]:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=topic_data['year'], y=topic_data['share'],
                    mode='lines+markers', name='Actual',
                    line=dict(color=TOPIC_COLORS.get(cat, '#333'), width=2),
                ))
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
