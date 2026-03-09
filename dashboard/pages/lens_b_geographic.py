"""
dashboard/pages/lens_b_geographic.py

Lens B — Geographic Power: Is local research leadership genuinely growing?

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
)
from dashboard.constants import (
    TOPIC_LABELS, NON_EMPIRICAL_METHODS, WHO_REGIONS, WHO_REGION_NAMES,
    CHART_TEMPLATE, CHART_HEIGHT, CHART_HEIGHT_TALL, iso2_to_country_name,
)
from dashboard.db import query_df, query_scalar, build_where_clause


def page():
    st.title('Geographic Power')
    st.caption(
        'Is local research leadership genuinely growing, or is '
        '"parachute science" structurally persistent?'
    )

    if not check_data_ready(require_countries=True):
        return

    year_range = st.session_state.get('year_range', (2010, 2024))
    topics = st.session_state.get('selected_topics', [])
    where, params = build_where_clause(year_range=year_range, topics=topics or None)

    # Exclude non-empirical publications (commentary/editorials) from analysis
    ne_placeholders = ', '.join(['?'] * len(NON_EMPIRICAL_METHODS))
    ne_clause = (f" AND (w.method_type IS NULL "
                 f"OR w.method_type NOT IN ({ne_placeholders}))")
    base_where = f"WHERE TRUE {where}{ne_clause}"
    params = params + list(NON_EMPIRICAL_METHODS)

    # ------------------------------------------------------------------
    # Page controls
    # ------------------------------------------------------------------
    col1, col2 = st.columns(2)
    with col1:
        parachute_def = st.radio(
            'Parachute science definition',
            ['No local first or last author (strict)',
             'No local first author only (relaxed)'],
            key='parachute_def',
        )
    strict = 'strict' in parachute_def

    # ------------------------------------------------------------------
    # Parachute science index over time
    # ------------------------------------------------------------------
    section_header(
        'Parachute Science Index Over Time',
        'Proportion of papers where the study country has no local '
        'first (and/or last) author.',
    )

    if strict:
        parachute_condition = """
            AND first_author_country != study_country
            AND last_author_country != study_country
        """
    else:
        parachute_condition = """
            AND first_author_country != study_country
        """

    df_parachute = query_df(
        f"""WITH paper_authors AS (
                SELECT w.openalex_id, w.publication_year, w.study_country,
                       MAX(CASE WHEN a.position = 'first'
                           THEN a.institution_country END) AS first_author_country,
                       MAX(CASE WHEN a.position = 'last'
                           THEN a.institution_country END) AS last_author_country
                FROM works w
                JOIN authorships a ON w.openalex_id = a.openalex_id
                {base_where}
                AND w.study_country IS NOT NULL
                AND w.study_country != 'GLOBAL'
                GROUP BY w.openalex_id, w.publication_year, w.study_country
            )
            SELECT publication_year AS year,
                   COUNT(*) AS total,
                   SUM(CASE WHEN first_author_country IS NOT NULL
                            AND last_author_country IS NOT NULL
                            {parachute_condition}
                       THEN 1 ELSE 0 END) AS parachute
            FROM paper_authors
            WHERE first_author_country IS NOT NULL
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
            line=dict(color='#d62728', width=2),
            fill='tozeroy', fillcolor='rgba(214, 39, 40, 0.1)',
        ))
        fig.update_layout(
            template=CHART_TEMPLATE, height=CHART_HEIGHT,
            yaxis_title='Parachute Rate (%)',
            xaxis_title='Year',
        )
        fig.update_xaxes(dtick=1)
        st.plotly_chart(fig, use_container_width=True)

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
            AND w.study_country IS NOT NULL
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

            # Ensure both axes have the same countries in the same order
            country_names = sorted(
                [iso2_to_country_name(c) for c in top_countries]
            )
            pivot = pivot.reindex(index=country_names, columns=country_names,
                                  fill_value=0)

            fig = px.imshow(
                pivot, text_auto=True,
                labels={'x': 'Study Country', 'y': 'First Author Country',
                        'color': 'Papers'},
                color_continuous_scale='Blues',
                template=CHART_TEMPLATE,
                aspect='equal',
            )
            fig.update_layout(
                height=max(700, len(country_names) * 55),
                margin=dict(t=10),
            )
            st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Country profile drill-down
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
                    fig.update_traces(marker_color='#ff7f0e')
                    fig.update_layout(
                        height=400,
                        yaxis={'categoryorder': 'total ascending'},
                    )
                    st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Parachute index by topic
    # ------------------------------------------------------------------
    section_header(
        'Parachute Rate by Topic',
        'Which research areas have the most external authorship?',
    )

    df_para_topic = query_df(
        f"""WITH paper_info AS (
                SELECT w.openalex_id, w.topic_category, w.study_country,
                       MAX(CASE WHEN a.position = 'first'
                           THEN a.institution_country END) AS first_country,
                       MAX(CASE WHEN a.position = 'last'
                           THEN a.institution_country END) AS last_country
                FROM works w
                JOIN authorships a ON w.openalex_id = a.openalex_id
                {base_where}
                AND w.study_country IS NOT NULL AND w.study_country != 'GLOBAL'
                AND w.topic_category IS NOT NULL
                GROUP BY w.openalex_id, w.topic_category, w.study_country
            )
            SELECT topic_category AS cat,
                   COUNT(*) AS total,
                   SUM(CASE WHEN first_country IS NOT NULL
                            AND first_country != study_country
                       THEN 1 ELSE 0 END) AS parachute
            FROM paper_info
            WHERE first_country IS NOT NULL
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
            color='rate', color_continuous_scale='RdYlGn_r',
        )
        fig.add_vline(
            x=median_rate, line_dash='dash', line_color='gray',
            annotation_text=f'Median: {median_rate:.1f}%',
        )
        fig.update_layout(
            height=max(400, len(df_para_topic) * 35),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)
        download_csv_button(
            df_para_topic[['cat', 'label', 'total', 'parachute', 'rate']],
            'parachute_by_topic.csv',
        )
