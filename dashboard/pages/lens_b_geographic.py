"""
dashboard/pages/lens_b_geographic.py

Lens B (Geographic Power): Is local research leadership genuinely growing?

Analytical interactions:
- Corridor deep-dive (click flow matrix cell → topic/funding breakdown)
- Country profile (select country → parachute trend, topic profile)
- Parachute by topic (click → which countries are parachuted into)
- Regional trajectory comparison (small multiples per WHO region)
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.components import (
    check_data_ready, metric_row, section_header, download_csv_button,
    page_subtitle,
)
from dashboard.constants import (
    TOPIC_LABELS, TOPIC_COLORS, NON_EMPIRICAL_METHODS, UNCATEGORIZED_TOPICS,
    WHO_REGIONS, WHO_REGION_NAMES, WHO_REGION_COLORS,
    CHART_TEMPLATE, CHART_HEIGHT, CHART_HEIGHT_TALL, iso2_to_country_name,
)
from dashboard.db import query_df, query_scalar, build_where_clause


def page():
    st.title('Geographic Power')
    page_subtitle(
        'Where is global health research conducted, and who leads it: local '
        'researchers or external "parachute science"? Is that changing over time?'
    )

    if not check_data_ready(require_countries=True):
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
    # Shared per-paper authorship geography (single-study-country papers).
    # Reused by the summary band and the leadership/typology/region charts.
    # ------------------------------------------------------------------
    df_auth = query_df(
        f"""WITH pa AS (
                SELECT w.openalex_id,
                       w.publication_year AS year,
                       w.study_country AS study,
                       MAX(CASE WHEN a.position = 'first'
                           THEN a.institution_country END) AS first_c,
                       MAX(CASE WHEN a.position = 'last'
                           THEN a.institution_country END) AS last_c
                FROM works w
                JOIN authorships a ON w.openalex_id = a.openalex_id
                {base_where}
                  AND w.study_country IS NOT NULL
                  AND w.study_country != 'GLOBAL'
                  AND w.study_country NOT LIKE '%|%'
                  AND w.study_country != 'UNKNOWN'
                GROUP BY 1, 2, 3
            )
            SELECT * FROM pa
            WHERE first_c IS NOT NULL AND first_c != '' AND first_c != 'UNKNOWN'""",
        tuple(params),
    )

    # ------------------------------------------------------------------
    # Summary indicators of geographic power
    # ------------------------------------------------------------------
    if not df_auth.empty:
        _tot = len(df_auth)
        _local = (df_auth['first_c'] == df_auth['study']).mean() * 100
        _para = (
            (df_auth['first_c'] != df_auth['study'])
            & (df_auth['last_c'] != df_auth['study'])
        ).mean() * 100
        _fa = df_auth['first_c'].value_counts()
        _top_name = iso2_to_country_name(_fa.index[0])
        _top_share = _fa.iloc[0] / _tot * 100
        _n_study = df_auth['study'].nunique()
        gc = st.columns(4)
        gc[0].metric('Local-led rate', f'{_local:.0f}%',
                     help='Share of papers whose first author is affiliated '
                          'with the study country.')
        gc[1].metric('Parachute rate', f'{_para:.0f}%',
                     help='Share of papers with no local first or last author.')
        gc[2].metric('Top author country', f'{_top_name}, {_top_share:.0f}%',
                     help='Country holding the largest single share of '
                          'first-authorships.')
        gc[3].metric('Study countries', f'{_n_study:,}',
                     help='Distinct countries that are the subject of research '
                          'in the current filter.')
        st.info(
            'These indicators cover **single-study-country research papers**; '
            'studies spanning several countries are excluded. "Local" versus '
            '"parachute" is inferred from author affiliation relative to the '
            'study country. It is a proxy for research leadership, not a '
            'judgment of any individual collaboration.',
            icon=':material/info:',
        )

    # ------------------------------------------------------------------
    # Parachute rate over time
    # ------------------------------------------------------------------
    section_header(
        'Externally-led (parachute) research over time',
        'Share of single-country research where no author is affiliated with the '
        'study country.',
    )

    df_parachute = query_df(
        f"""WITH paper_authors AS (
                SELECT w.openalex_id, w.publication_year,
                       MAX(CASE WHEN a.institution_country = w.study_country
                           THEN 1 ELSE 0 END) AS has_local
                FROM works w
                JOIN authorships a ON w.openalex_id = a.openalex_id
                {base_where}
                AND w.study_country IS NOT NULL
                AND w.study_country NOT IN ('GLOBAL', 'UNKNOWN')
                AND w.study_country NOT LIKE '%|%'
                AND a.institution_country IS NOT NULL
                AND a.institution_country <> ''
                GROUP BY w.openalex_id, w.publication_year, w.study_country
            )
            SELECT publication_year AS year,
                   COUNT(*) AS total,
                   SUM(CASE WHEN has_local = 0 THEN 1 ELSE 0 END) AS parachute
            FROM paper_authors
            GROUP BY publication_year
            ORDER BY year""",
        tuple(params),
    )

    if not df_parachute.empty:
        df_parachute['rate'] = (
            df_parachute['parachute'] / df_parachute['total'] * 100
        ).round(1)

        # Key metric
        latest = df_parachute.iloc[-1]
        earliest = df_parachute.iloc[0]
        delta = latest['rate'] - earliest['rate']
        metric_row([
            ('Current Parachute Rate', f"{latest['rate']:.1f}%", None),
            ('Change Since {}'.format(int(earliest['year'])),
             f"{delta:+.1f} pp", None),
            ('Papers Analyzed', int(df_parachute['total'].sum()), None),
        ])

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_parachute['year'], y=df_parachute['rate'],
            mode='lines+markers', name='Parachute rate',
            line=dict(color='#D55E00', width=2),
        ))
        # Focused y-range so the rise-to-2016 and subsequent decline read
        # clearly; the rate itself moves only within a ~45-60% band.
        _lo = max(0, (df_parachute['rate'].min() // 5) * 5 - 5)
        _hi = min(100, (df_parachute['rate'].max() // 5) * 5 + 10)
        fig.update_layout(
            template=CHART_TEMPLATE, height=CHART_HEIGHT,
            yaxis_title='Parachute Rate (%)',
            xaxis_title='Year',
            yaxis=dict(range=[_lo, _hi]),
        )
        fig.update_xaxes(dtick=1)
        st.plotly_chart(fig, use_container_width=True)

    st.info(
        '**Parachute science** (also called *helicopter* or *parasitic* '
        'research) is research conducted in a country, usually a low- or '
        'middle-income one, by researchers based elsewhere, with little or no '
        'leadership from local scientists. Here it is measured, for single-country '
        'studies, as papers in which **no author at any position is affiliated '
        'with the study country** (matching the analysis in the paper). This is a '
        'proxy built from author affiliation, not a judgment of any individual '
        'collaboration.',
        icon=':material/info:',
    )

    # ------------------------------------------------------------------
    # C. Local leadership / collaboration / parachute, over time
    # ------------------------------------------------------------------
    section_header(
        'Local Leadership, Collaboration, or Parachute',
        'Every study-country paper split three ways by authorship, over time.',
    )

    if not df_auth.empty:
        def _kind(r):
            if r.first_c == r.study:
                return 'Local-led'
            if r.last_c == r.study:
                return 'Collaborative (local last author)'
            return 'Parachute (no local author)'

        d = df_auth.copy()
        d['kind'] = [_kind(r) for r in d.itertuples()]
        comp = d.groupby(['year', 'kind']).size().reset_index(name='n')
        comp['pct'] = comp['n'] / comp.groupby('year')['n'].transform('sum') * 100
        kind_order = ['Local-led', 'Collaborative (local last author)',
                      'Parachute (no local author)']
        kind_colors = {
            'Local-led': '#009E73',
            'Collaborative (local last author)': '#0072B2',
            'Parachute (no local author)': '#D55E00',
        }
        fig = px.area(
            comp, x='year', y='pct', color='kind',
            category_orders={'kind': kind_order},
            color_discrete_map=kind_colors,
            labels={'pct': 'Share of papers (%)', 'year': '', 'kind': ''},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=CHART_HEIGHT, yaxis_range=[0, 100],
            legend=dict(orientation='h', y=1.12, title_text=''),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            'Collaborative = the first author is external but the last (often '
            'senior) author is local. Parachute = no local first or last '
            'author. Separating these avoids counting genuine collaboration as '
            'extraction.'
        )

    # ------------------------------------------------------------------
    # D. Local leadership by region over time
    # ------------------------------------------------------------------
    section_header(
        'Local Leadership by Region Over Time',
        'Share of papers with a local first author, by the study country\'s '
        'WHO region.',
    )

    if not df_auth.empty:
        dr = df_auth.copy()
        dr['region'] = dr['study'].map(WHO_REGIONS).map(WHO_REGION_NAMES)
        dr = dr[dr['region'].notna()]
        dr['local'] = (dr['first_c'] == dr['study']).astype(int)
        reg = dr.groupby(['year', 'region']).agg(
            local=('local', 'mean'), n=('local', 'size')).reset_index()
        reg = reg[reg['n'] >= 15]  # suppress thin region-years
        reg['pct'] = reg['local'] * 100
        if not reg.empty:
            fig = px.line(
                reg, x='year', y='pct', color='region',
                color_discrete_map=WHO_REGION_COLORS, markers=True,
                labels={'pct': 'Local-led rate (%)', 'year': '',
                        'region': 'WHO region'},
                template=CHART_TEMPLATE,
            )
            fig.update_layout(
                height=CHART_HEIGHT, yaxis_range=[0, 100],
                legend=dict(font=dict(size=9), title_text=''),
            )
            st.plotly_chart(fig, use_container_width=True)
            st.caption(
                'Local-led rate = share of a region\'s study-country papers '
                'with a first author from the study country. Region-years with '
                'fewer than 15 papers are omitted.'
            )

    # ------------------------------------------------------------------
    # Author country → Study country flow matrix
    # ------------------------------------------------------------------
    section_header(
        'First-Author Country \u2192 Study Country Flow',
        'Each cell shows how many papers have a first author from one country '
        'studying another. Diagonal = local research.',
    )

    # Exclude multi-country studies (pipe-separated) so each cell maps
    # a single author country to a single study country, giving a clean
    # square matrix with a true diagonal.
    df_flow = query_df(
        f"""SELECT a.institution_country AS author_country,
                   w.study_country AS study_country,
                   COUNT(DISTINCT w.openalex_id) AS n
            FROM works w
            JOIN authorships a ON w.openalex_id = a.openalex_id
            {base_where}
            AND a.position = 'first'
            AND a.institution_country IS NOT NULL
            AND a.institution_country != ''
            AND w.study_country IS NOT NULL
            AND w.study_country != ''
            AND w.study_country != 'GLOBAL'
            AND w.study_country NOT LIKE '%|%'
            AND w.study_country != 'UNKNOWN'
            AND a.institution_country != 'UNKNOWN'
            GROUP BY a.institution_country, w.study_country""",
        tuple(params),
    )

    if not df_flow.empty:
        # Use a single country set for both axes so the matrix is square.
        all_countries = (
            df_flow.groupby('author_country')['n'].sum()
            .add(df_flow.groupby('study_country')['n'].sum(), fill_value=0)
        )
        # Remove any blank/empty entries before selecting top countries
        all_countries = all_countries[
            all_countries.index.map(lambda x: bool(x and x.strip()))
        ]
        top_countries = list(all_countries.nlargest(15).index)

        flow_filtered = df_flow[
            df_flow['author_country'].isin(top_countries) &
            df_flow['study_country'].isin(top_countries)
        ]

        if not flow_filtered.empty:
            flow_filtered = flow_filtered.copy()
            flow_filtered['author_name'] = flow_filtered['author_country'].apply(iso2_to_country_name)
            flow_filtered['study_name'] = flow_filtered['study_country'].apply(iso2_to_country_name)
            pivot = flow_filtered.pivot_table(
                index='author_name', columns='study_name',
                values='n', fill_value=0,
            )

            # Order both axes by total volume (author + study) descending so the
            # highest-output countries cluster top-left and the block structure
            # (who studies whom) reads coherently instead of alphabetically.
            order_names = [
                n for n in (iso2_to_country_name(c) for c in top_countries)
                if n and n.strip()
            ]
            pivot = pivot.reindex(index=order_names, columns=order_names,
                                  fill_value=0)

            # Saturate the color scale at the 95th-percentile OFF-DIAGONAL
            # (cross-country) flow so the parachute cells span the full gradient.
            # The large local-research diagonal is kept and simply reads as the
            # top of the scale; cell text still shows the true counts.
            _st = pivot.stack()
            _off = _st[[i != j for i, j in _st.index]]
            _off = _off[_off > 0]
            zmax = float(_off.quantile(0.95)) if not _off.empty else 1.0

            fig = px.imshow(
                pivot, text_auto=True,
                labels={'x': 'Study Country', 'y': 'First Author Country',
                        'color': 'Papers'},
                color_continuous_scale='YlOrRd',
                zmin=0, zmax=zmax,
                template=CHART_TEMPLATE,
                aspect='equal',
            )
            fig.update_layout(
                height=max(700, len(order_names) * 55),
                margin=dict(t=10),
            )
            st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Parachute rate by topic
    # ------------------------------------------------------------------
    section_header(
        'Parachute Rate by Topic',
        'Which research areas have the most external authorship?',
    )

    df_para_topic = query_df(
        f"""WITH paper_info AS (
                SELECT w.openalex_id, w.topic_category,
                       MAX(CASE WHEN a.institution_country = w.study_country
                           THEN 1 ELSE 0 END) AS has_local
                FROM works w
                JOIN authorships a ON w.openalex_id = a.openalex_id
                {base_where}
                AND w.study_country IS NOT NULL
                AND w.study_country NOT IN ('GLOBAL', 'UNKNOWN')
                AND w.study_country NOT LIKE '%|%'
                AND w.topic_category IS NOT NULL
                AND a.institution_country IS NOT NULL
                AND a.institution_country <> ''
                GROUP BY w.openalex_id, w.topic_category, w.study_country
            )
            SELECT topic_category AS cat,
                   COUNT(*) AS total,
                   SUM(CASE WHEN has_local = 0 THEN 1 ELSE 0 END) AS parachute
            FROM paper_info
            GROUP BY topic_category""",
        tuple(params),
    )

    if not df_para_topic.empty:
        df_para_topic['rate'] = (
            df_para_topic['parachute'] / df_para_topic['total'] * 100
        ).round(1)
        df_para_topic['label'] = df_para_topic['cat'].map(
            lambda c: TOPIC_LABELS.get(c, c)
        )
        median_rate = df_para_topic['rate'].median()

        fig = px.bar(
            df_para_topic.sort_values('rate', ascending=True),
            y='label', x='rate', orientation='h',
            labels={'rate': 'Parachute Rate (%)', 'label': ''},
            template=CHART_TEMPLATE,
            color='cat', color_discrete_map=TOPIC_COLORS,
        )
        fig.add_vline(
            x=median_rate, line_dash='dash', line_color='gray',
            annotation_text=f'Median: {median_rate:.1f}%',
        )
        fig.update_layout(
            height=max(400, len(df_para_topic) * 35),
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)
        download_csv_button(
            df_para_topic[['cat', 'label', 'total', 'parachute', 'rate']],
            'parachute_by_topic.csv',
        )

    # ------------------------------------------------------------------
    # Country profile drill-down (interactive, last)
    # ------------------------------------------------------------------
    section_header(
        'Country Profile',
        'Select a study country to see its research landscape.',
    )

    # Get top study countries for selector (exclude multi-country and UNKNOWN)
    df_countries = query_df(
        f"""SELECT study_country AS country, COUNT(*) AS n
            FROM works w {base_where}
            AND study_country IS NOT NULL
            AND study_country NOT IN ('GLOBAL', 'UNKNOWN')
            AND study_country NOT LIKE '%|%'
            GROUP BY study_country ORDER BY n DESC LIMIT 30""",
        tuple(params),
    )

    if not df_countries.empty:
        df_countries['name'] = df_countries['country'].apply(iso2_to_country_name)
        # Build a display→code mapping for the selector
        country_display = dict(zip(df_countries['name'], df_countries['country']))
        selected_display = st.selectbox(
            'Select study country:',
            options=df_countries['name'].tolist(),
            key='selected_country',
        )
        selected_country = country_display.get(selected_display, selected_display)

        if selected_country:
            col_a, col_b = st.columns(2)

            # Topic profile for this country
            with col_a:
                df_ctopic = query_df(
                    f"""SELECT topic_category AS cat, COUNT(*) AS n
                        FROM works w {base_where}
                        AND study_country = ? AND topic_category IS NOT NULL
                        GROUP BY topic_category ORDER BY n DESC""",
                    tuple(params + [selected_country]),
                )
                if not df_ctopic.empty:
                    df_ctopic['label'] = df_ctopic['cat'].map(
                        lambda c: TOPIC_LABELS.get(c, c)
                    )
                    fig = px.pie(
                        df_ctopic, values='n', names='label',
                        color='cat', color_discrete_map=TOPIC_COLORS,
                        title=f'Research Topics in {selected_display}',
                        template=CHART_TEMPLATE,
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)

            # External author countries
            with col_b:
                df_ext = query_df(
                    f"""SELECT a.institution_country AS country, COUNT(*) AS n
                        FROM works w
                        JOIN authorships a ON w.openalex_id = a.openalex_id
                        {base_where}
                        AND w.study_country = ?
                        AND a.position = 'first'
                        AND a.institution_country IS NOT NULL
                        AND a.institution_country != ''
                        AND a.institution_country != 'UNKNOWN'
                        AND a.institution_country != ?
                        GROUP BY a.institution_country
                        ORDER BY n DESC LIMIT 10""",
                    tuple(params + [selected_country, selected_country]),
                )
                if not df_ext.empty:
                    df_ext['name'] = df_ext['country'].apply(iso2_to_country_name)
                    fig = px.bar(
                        df_ext, y='name', x='n', orientation='h',
                        title='Top External Researcher Countries',
                        labels={'n': 'Papers', 'name': ''},
                        template=CHART_TEMPLATE,
                    )
                    fig.update_traces(marker_color='#E69F00')
                    fig.update_layout(
                        height=400,
                        yaxis={'categoryorder': 'total ascending'},
                    )
                    st.plotly_chart(fig, use_container_width=True)

