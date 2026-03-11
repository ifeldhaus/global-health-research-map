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
    UNCATEGORIZED_TOPICS, NON_EMPIRICAL_METHODS,
    CHART_TEMPLATE, CHART_HEIGHT, iso2_to_country_name,
)
from dashboard.db import query_df, query_scalar, build_where_clause


def page():
    st.title('Overview')
    st.caption(
        'Corpus summary across global health research papers '
        '(2010\u20132024) from 11 core journals.'
    )

    # ------------------------------------------------------------------
    # About this dashboard — always visible at top
    # ------------------------------------------------------------------
    with st.expander(':material/menu_book: **About This Dashboard: Data Sources & Methods**',
                     expanded=False):
        st.markdown(
            '### Data Sources\n\n'
            'This dashboard draws on a bibliometric corpus of global health '
            'research papers published between **2010 and 2024** in **11 core '
            'journals** selected for their prominence in global health '
            '(e.g., *The Lancet*, *BMJ Global Health*, *PLOS Medicine*). '
            'Metadata for each paper \u2014 title, abstract, authors, '
            'institutions, and funding acknowledgements \u2014 was retrieved '
            'from [OpenAlex](https://openalex.org/), an open bibliometric '
            'database.\n\n'
            '### Classification Methods\n\n'
            'Each paper with a usable abstract was enriched through four '
            'AI-assisted classification steps:\n\n'
            '1. **Topic classification** \u2014 Papers were classified into '
            '15 topic categories (A\u2013O) using a large language model '
            '(Claude) based on each paper\'s title and abstract. The '
            'taxonomy covers major global health research areas such as '
            'infectious diseases, non-communicable diseases, health '
            'systems, and environmental health.\n'
            '2. **Methods classification** \u2014 Each paper was assigned a '
            'study design type (e.g., cross-sectional, cohort, RCT, '
            'systematic review, qualitative) using the same LLM approach.\n'
            '3. **Study country extraction** \u2014 The country or countries '
            'where each study was conducted were identified from the '
            'abstract.\n'
            '4. **Gender inference** \u2014 The likely gender of first and last '
            'authors was inferred probabilistically from given names '
            'using a name-to-gender database. This approach has known '
            'limitations for culturally ambiguous names.\n\n'
            'Disease burden data from the '
            '[Global Burden of Disease (GBD)](https://www.healthdata.org/research-analysis/gbd) '
            'study is used in the Topic Trends lens to compare research '
            'attention against actual disease burden (DALYs and deaths).\n\n'
            '### Important Limitations\n\n'
            '- **~25% of papers lack usable abstracts** in OpenAlex and '
            'could not be topic- or method-classified. This missingness '
            'is systematic \u2014 concentrated in specific journals '
            '(see the [Data Completeness](/data-completeness) page for '
            'details). Findings from topic, methods, and geographic '
            'analyses should be interpreted with this in mind.\n'
            '- **Classification is AI-assisted**, not manually validated '
            'at scale. While the LLM performs well on clear-cut cases, '
            'some papers (especially editorials and cross-cutting '
            'commentary) may be mis-classified.\n'
            '- **Funder data depends on OpenAlex metadata**, which '
            'captures funding acknowledgements where publishers make '
            'them available. Unfunded rates may reflect incomplete '
            'metadata rather than true absence of funding.\n'
            '- **Gender inference** is probabilistic and binary, which '
            'does not capture the full spectrum of gender identity.\n\n'
            '### How to Use This Dashboard\n\n'
            'Use the **sidebar filters** to narrow by publication year, '
            'topic category, or funder type. The dashboard is organized '
            'into five analytical lenses:\n\n'
            '- **Funder Power** \u2014 Who funds global health research and '
            'how concentrated is funding?\n'
            '- **Geographic Power** \u2014 Where is research conducted and by '
            'whom? How prevalent is "parachute science"?\n'
            '- **Topic Trends** \u2014 Does research attention align with '
            'disease burden?\n'
            '- **Methods Gaps** \u2014 Which study designs are under-utilized '
            'for which topics?\n'
            '- **Institutions** \u2014 Who produces global health research '
            'and how concentrated is production?\n\n'
            'The **Data Completeness** page provides full transparency on '
            'data quality and missingness.'
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

    uc_placeholders = ', '.join(['?'] * len(UNCATEGORIZED_TOPICS))
    df_topic = query_df(
        f"""SELECT topic_category AS cat, COUNT(*) AS n
            FROM works w
            {base_where} AND classified_topic = TRUE
            AND topic_category NOT IN ({uc_placeholders})
            GROUP BY topic_category ORDER BY n DESC""",
        tuple(params) + UNCATEGORIZED_TOPICS,
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

    ne_placeholders = ', '.join(['?'] * len(NON_EMPIRICAL_METHODS))
    df_method = query_df(
        f"""SELECT method_type AS method, COUNT(*) AS n
            FROM works w
            {base_where} AND classified_method = TRUE
            AND method_type NOT IN ({ne_placeholders})
            GROUP BY method_type ORDER BY n DESC""",
        tuple(params) + NON_EMPIRICAL_METHODS,
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
              AND study_country NOT IN ('GLOBAL', 'UNKNOWN')
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
            fig.update_traces(marker_line_width=0)
            fig.update_layout(
                height=CHART_HEIGHT,
                bargap=0.25,
                bargroupgap=0.1,
            )
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
