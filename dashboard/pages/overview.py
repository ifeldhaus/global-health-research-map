"""
dashboard/pages/overview.py

Overview page — corpus summary with key metrics and distribution charts.
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.components import (
    check_data_ready, metric_row, section_header, download_csv_button,
    pipeline_progress_card,
)
from dashboard.constants import (
    TOPIC_COLORS, TOPIC_LABELS, METHOD_LABELS, GENDER_COLORS,
    CHART_TEMPLATE, CHART_HEIGHT, iso2_to_country_name,
)
from dashboard.db import query_df, query_scalar, build_where_clause


def page():
    st.title('Overview')
    st.caption(
        'Corpus summary across ~100,000 global health research papers '
        '(2010\u20132024) from 10 core journals.'
    )

    if not check_data_ready():
        pipeline_progress_card()
        return

    year_range = st.session_state.get('year_range', (2010, 2024))
    topics = st.session_state.get('selected_topics', [])

    where, params = build_where_clause(year_range=year_range, topics=topics or None)
    base_where = f"WHERE TRUE {where}"

    # ------------------------------------------------------------------
    # Metrics row
    # ------------------------------------------------------------------
    total_papers = query_scalar(
        f"SELECT COUNT(*) FROM works w {base_where}", tuple(params)
    )
    unique_funders = query_scalar(
        f"""SELECT COUNT(DISTINCT f.canonical_name)
            FROM grants g
            JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
            JOIN works w ON g.openalex_id = w.openalex_id
            {base_where}""",
        tuple(params),
    )
    unique_institutions = query_scalar(
        f"""SELECT COUNT(DISTINCT a.institution_name)
            FROM authorships a
            JOIN works w ON a.openalex_id = w.openalex_id
            {base_where}
            AND a.institution_name IS NOT NULL""",
        tuple(params),
    )
    unique_countries = query_scalar(
        f"""SELECT COUNT(DISTINCT w.study_country)
            FROM works w
            {base_where}
            AND w.study_country IS NOT NULL""",
        tuple(params),
    )

    metric_row([
        ('Total Papers', total_papers or 0, None),
        ('Years', f"{year_range[0]}\u2013{year_range[1]}", None),
        ('Unique Funders', unique_funders or 0, None),
        ('Unique Institutions', unique_institutions or 0, None),
        ('Study Countries', unique_countries or 0, None),
    ])

    # ------------------------------------------------------------------
    # Publications by year
    # ------------------------------------------------------------------
    section_header('Publications by Year')

    df_year = query_df(
        f"""SELECT publication_year AS year, COUNT(*) AS n
            FROM works w {base_where}
            GROUP BY publication_year ORDER BY publication_year""",
        tuple(params),
    )

    if not df_year.empty:
        fig = px.bar(
            df_year, x='year', y='n',
            labels={'year': 'Publication Year', 'n': 'Papers'},
            template=CHART_TEMPLATE,
        )
        fig.update_traces(marker_color='#2171b5')
        fig.update_layout(height=CHART_HEIGHT, bargap=0.15)
        st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Topic distribution
    # ------------------------------------------------------------------
    section_header(
        'Topic Category Distribution',
        'Based on LLM-classified topic categories (A\u2013O).',
    )

    df_topic = query_df(
        f"""SELECT topic_category AS cat, COUNT(*) AS n
            FROM works w
            {base_where} AND classified_topic = TRUE
            GROUP BY topic_category ORDER BY n DESC""",
        tuple(params),
    )

    if not df_topic.empty:
        df_topic['label'] = df_topic['cat'].map(
            lambda c: TOPIC_LABELS.get(c, c)
        )
        df_topic['color'] = df_topic['cat'].map(TOPIC_COLORS)

        fig = px.bar(
            df_topic, y='label', x='n', orientation='h',
            labels={'n': 'Papers', 'label': ''},
            template=CHART_TEMPLATE,
            color='cat', color_discrete_map=TOPIC_COLORS,
        )
        fig.update_layout(
            height=max(400, len(df_topic) * 35),
            showlegend=False,
            yaxis={'categoryorder': 'total ascending'},
        )
        st.plotly_chart(fig, use_container_width=True)
        download_csv_button(df_topic[['cat', 'label', 'n']], 'topic_distribution.csv')

    # ------------------------------------------------------------------
    # Methods distribution
    # ------------------------------------------------------------------
    section_header(
        'Methods Distribution',
        'Study methodology types across the corpus.',
    )

    df_method = query_df(
        f"""SELECT method_type AS method, COUNT(*) AS n
            FROM works w
            {base_where} AND classified_method = TRUE
            GROUP BY method_type ORDER BY n DESC""",
        tuple(params),
    )

    if not df_method.empty:
        df_method['label'] = df_method['method'].map(
            lambda m: METHOD_LABELS.get(m, m)
        )

        fig = px.bar(
            df_method, y='label', x='n', orientation='h',
            labels={'n': 'Papers', 'label': ''},
            template=CHART_TEMPLATE,
        )
        fig.update_traces(marker_color='#2ca02c')
        fig.update_layout(
            height=max(400, len(df_method) * 35),
            yaxis={'categoryorder': 'total ascending'},
        )
        st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Top study countries
    # ------------------------------------------------------------------
    section_header('Top 20 Study Countries')

    df_country = query_df(
        f"""SELECT study_country AS country, COUNT(*) AS n
            FROM works w
            {base_where} AND study_country IS NOT NULL
              AND study_country != 'GLOBAL'
            GROUP BY study_country ORDER BY n DESC LIMIT 20""",
        tuple(params),
    )

    if not df_country.empty:
        df_country['name'] = df_country['country'].apply(iso2_to_country_name)

        fig = px.bar(
            df_country, y='name', x='n', orientation='h',
            labels={'n': 'Papers', 'name': ''},
            template=CHART_TEMPLATE,
        )
        fig.update_traces(marker_color='#ff7f0e')
        fig.update_layout(
            height=max(400, len(df_country) * 30),
            yaxis={'categoryorder': 'total ascending'},
        )
        st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Gender authorship
    # ------------------------------------------------------------------
    section_header(
        'Gender of First and Last Authors',
        'Probabilistic gender inference from author given names.',
    )

    df_gender = query_df(
        f"""SELECT
                gender_first AS gender, 'First Author' AS position, COUNT(*) AS n
            FROM works w
            {base_where} AND gender_first IS NOT NULL
            GROUP BY gender_first
            UNION ALL
            SELECT
                gender_last AS gender, 'Last Author' AS position, COUNT(*) AS n
            FROM works w
            {base_where} AND gender_last IS NOT NULL
            GROUP BY gender_last""",
        tuple(params + params),
    )

    if not df_gender.empty:
        # Parse "male|0.85" format: extract label and confidence
        df_gender['label'] = df_gender['gender'].str.split('|').str[0]
        df_gender['confidence'] = df_gender['gender'].str.split('|').str[1].astype(float)

        # Remove unknowns
        df_gender = df_gender[df_gender['label'] != 'unknown'].copy()

        if not df_gender.empty:
            fig = px.bar(
                df_gender, x='position', y='n', color='label',
                barmode='group',
                color_discrete_map=GENDER_COLORS,
                labels={'n': 'Papers', 'position': '', 'label': 'Gender'},
                template=CHART_TEMPLATE,
            )
            fig.update_layout(height=CHART_HEIGHT)
            st.plotly_chart(fig, use_container_width=True)

            # Methodology note
            conf = df_gender['confidence'].iloc[0]
            st.caption(
                f'**Methodology:** Gender is inferred probabilistically from '
                f'author given names using a name-to-gender database. Each '
                f'assignment carries a confidence score (currently {conf:.2f} '
                f'for this corpus), meaning the model estimates an {conf:.0%} '
                f'probability that the inferred gender is correct. Papers '
                f'where gender could not be determined are excluded from this '
                f'chart. This approach has known limitations for names that '
                f'are culturally ambiguous or gender-neutral.'
            )
