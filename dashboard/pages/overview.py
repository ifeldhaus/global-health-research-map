"""
dashboard/pages/overview.py

Overview page: corpus summary with key metrics and distribution charts.
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.components import (
    check_data_ready, metric_row, section_header, download_csv_button,
    pipeline_progress_card, page_subtitle,
)
from dashboard.constants import (
    TOPIC_COLORS, TOPIC_LABELS, METHOD_LABELS, GENDER_COLORS,
    UNCATEGORIZED_TOPICS, NON_EMPIRICAL_METHODS, JOURNAL_NAMES,
    CHART_TEMPLATE, CHART_HEIGHT, iso2_to_country_name, country_color_map,
    institution_label, WHO_REGIONS, WHO_REGION_NAMES, WHO_REGION_COLORS,
)
from dashboard.db import (
    query_df, query_scalar, build_where_clause, shared_institution_names,
)


def page():
    st.title('Overview')
    page_subtitle(
        'The corpus behind the analysis: global health research papers from '
        '11 core journals, 2010 through July 2026 (2026 is a partial year).'
    )

    # ------------------------------------------------------------------
    # About this dashboard: always visible at top
    # ------------------------------------------------------------------
    with st.expander(':material/menu_book: **About This Dashboard: Data Sources & Methods**',
                     expanded=False):
        st.markdown(
            '### Data Sources\n\n'
            'This dashboard draws on a bibliometric corpus of global health '
            'research papers published between **2010 and July 2026** (2026 is a partial year) in **11 core '
            'journals** selected for their prominence in global health:\n\n'
            '- *Lancet Global Health*\n'
            '- *BMJ Global Health*\n'
            '- *PLOS Global Public Health*\n'
            '- *Bulletin of the World Health Organization*\n'
            '- *Health Policy and Planning*\n'
            '- *Globalization and Health*\n'
            '- *Global Public Health*\n'
            '- *Tropical Medicine & International Health*\n'
            '- *Journal of Global Health*\n'
            '- *Global Health Science and Practice*\n'
            '- *Annals of Global Health*\n\n'
            'Metadata for each paper (title, abstract, authors, '
            'institutions, and funding acknowledgements) was retrieved '
            'from [OpenAlex](https://openalex.org/), an open bibliometric '
            'database.\n\n'
            '### Classification Methods\n\n'
            'Each paper with a usable abstract was processed through four '
            'AI-assisted classification steps, using the large language model '
            '**Claude Opus 4.8 (Anthropic)**:\n\n'
            '1. **Topic classification**: Papers were classified into '
            '15 topic categories (A\u2013O) based on each paper\'s title and '
            'abstract, with an optional **secondary category** where a second '
            'topic is a co-equal focus. The taxonomy covers major global '
            'health research areas such as infectious diseases, '
            'non-communicable diseases, health systems, and environmental '
            'health.\n'
            '2. **Methods classification**: Each paper was assigned a '
            'study design type (e.g., cross-sectional, cohort, RCT, '
            'systematic review, qualitative) using the same LLM approach.\n'
            '3. **Study country extraction**: The country or countries '
            'where each study was conducted were identified from the '
            'abstract.\n'
            '4. **Gender inference**: The likely gender of first and last '
            'authors was inferred probabilistically from given names '
            'using the **[Genderize.io](https://genderize.io) API**, which '
            'estimates gender from a large name-to-gender frequency database. '
            'This approach has known limitations for culturally ambiguous '
            'names.\n\n'
            'The full taxonomy, classification prompts, and validation are '
            'detailed in the accompanying paper’s Methods section '
            '([link forthcoming](#)).\n\n'
            'Disease burden data from the '
            '[Global Burden of Disease (GBD)](https://www.healthdata.org/research-analysis/gbd) '
            'study is used in the Topic Trends lens to compare research '
            'attention against actual disease burden (DALYs and deaths).\n\n'
            '### Important Limitations\n\n'
            '- **13% of papers (4,516 of 33,964) lack usable abstracts** in OpenAlex and '
            'could not be topic- or method-classified. This missingness '
            'is systematic, concentrated in specific journals '
            '(see the [Data Completeness](/data-completeness) page for '
            'details). Findings from topic, methods, and geographic '
            'analyses should be interpreted with this in mind.\n'
            '- **Classification is AI-assisted**, validated against 345 blind '
            'hand-labeled papers (Cohen’s κ: topic 0.66, methods 0.67, '
            'country 0.93, substantial to almost-perfect agreement). '
            'Agreement is strong but imperfect; some papers, especially '
            'editorials and cross-cutting commentary, may be mis-classified.\n'
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
            '- **Funder Power**: Who funds global health research and '
            'how concentrated is funding?\n'
            '- **Geographic Power**: Where is research conducted and by '
            'whom? How prevalent is "parachute science"?\n'
            '- **Topic Trends**: Does research attention align with '
            'disease burden?\n'
            '- **Methods Gaps**: Which study designs are under-utilized '
            'for which topics?\n'
            '- **Institutions**: Who produces global health research '
            'and how concentrated is production?\n\n'
            'The **Data Completeness** page provides full transparency on '
            'data quality and missingness.'
        )

    if not check_data_ready():
        pipeline_progress_card()
        return

    year_range = st.session_state.get('year_range', (2010, 2025))
    topics = st.session_state.get('selected_topics', [])

    where, params = build_where_clause(year_range=year_range, topics=topics or None)
    base_where = f"WHERE TRUE {where}"

    # ------------------------------------------------------------------
    # Metrics row
    # ------------------------------------------------------------------
    total_papers = query_scalar(
        f"SELECT COUNT(*) FROM works w {base_where}", tuple(params)
    )
    # Count funders on the research corpus only (excluding commentary /
    # non-empirical and uncategorized-topic papers) so this matches the
    # Funder Power page, whose funder analysis is scoped the same way.
    _uc_ph = ', '.join(['?'] * len(UNCATEGORIZED_TOPICS))
    _ne_ph = ', '.join(['?'] * len(NON_EMPIRICAL_METHODS))
    unique_funders = query_scalar(
        f"""SELECT COUNT(DISTINCT f.canonical_name)
            FROM grants g
            JOIN funders f ON REPLACE(g.funder_id, 'https://openalex.org/', '') = f.openalex_id
            JOIN works w ON g.openalex_id = w.openalex_id
            {base_where}
            AND (w.topic_category IS NULL OR w.topic_category NOT IN ({_uc_ph}))
            AND (w.method_type IS NULL OR w.method_type NOT IN ({_ne_ph}))""",
        tuple(params + list(UNCATEGORIZED_TOPICS) + list(NON_EMPIRICAL_METHODS)),
    )
    # Institutions are counted the same way as on the Institutions lens:
    # distinct entities (OpenAlex ID) that led a paper (first or last author)
    # in the research corpus (commentary and uncategorized topics excluded).
    inst_where = (
        f"{base_where}"
        f" AND (w.topic_category IS NULL OR w.topic_category NOT IN ({_uc_ph}))"
        f" AND (w.method_type IS NULL OR w.method_type NOT IN ({_ne_ph}))"
    )
    inst_params = params + list(UNCATEGORIZED_TOPICS) + list(NON_EMPIRICAL_METHODS)
    unique_institutions = query_scalar(
        f"""SELECT COUNT(DISTINCT a.institution_id)
            FROM authorships a
            JOIN works w ON a.openalex_id = w.openalex_id
            {inst_where}
            AND a.institution_name IS NOT NULL
            AND a.institution_name != ''
            AND a.position IN ('first', 'last')""",
        tuple(inst_params),
    )
    # study_country can hold multi-country strings ("US|IN"); count distinct
    # individual countries, not distinct raw strings, and drop sentinels.
    unique_countries = query_scalar(
        f"""SELECT COUNT(DISTINCT country) FROM (
                SELECT TRIM(UNNEST(string_split(w.study_country, '|'))) AS country
                FROM works w
                {base_where}
                AND w.study_country IS NOT NULL
            ) t
            WHERE country NOT IN ('GLOBAL', 'UNKNOWN') AND country <> ''""",
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
    section_header(
        'Publications by Year',
        'Annual output, split by journal to show how the corpus is composed.',
    )

    df_year = query_df(
        f"""SELECT publication_year AS year, journal_issn AS issn,
                   COUNT(*) AS n
            FROM works w {base_where}
            GROUP BY publication_year, journal_issn
            ORDER BY publication_year""",
        tuple(params),
    )

    if not df_year.empty:
        df_year['journal'] = df_year['issn'].map(
            lambda i: JOURNAL_NAMES.get(i, 'Other'))
        journal_order = (df_year.groupby('journal')['n'].sum()
                         .sort_values(ascending=False).index.tolist())
        fig = px.bar(
            df_year, x='year', y='n', color='journal',
            category_orders={'journal': journal_order},
            color_discrete_sequence=px.colors.qualitative.Safe,
            labels={'year': 'Publication Year', 'n': 'Papers',
                    'journal': 'Journal'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=CHART_HEIGHT, bargap=0.15,
            legend=dict(font=dict(size=8), title_text=''),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            'Each bar is one year’s output, stacked by journal. Part of the '
            'rise reflects new journals entering the field (for example '
            'PLOS Global Public Health from 2021), not only growth in '
            'existing ones.'
        )

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
        fig.update_traces(marker_color='#009E73')
        fig.update_layout(
            height=max(400, len(df_method) * 35),
            yaxis={'categoryorder': 'total ascending'},
        )
        st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Top study countries
    # ------------------------------------------------------------------
    section_header('Top 20 Study Countries')

    # Split multi-country strings so each study counts toward every country
    # it covers, and each bar is a single country.
    df_country = query_df(
        f"""SELECT country, COUNT(*) AS n FROM (
                SELECT TRIM(UNNEST(string_split(w.study_country, '|'))) AS country
                FROM works w
                {base_where} AND w.study_country IS NOT NULL
            ) t
            WHERE country NOT IN ('GLOBAL', 'UNKNOWN') AND country <> ''
            GROUP BY country ORDER BY n DESC LIMIT 20""",
        tuple(params),
    )

    if not df_country.empty:
        df_country['name'] = df_country['country'].apply(iso2_to_country_name)
        df_country['region'] = (
            df_country['country'].map(WHO_REGIONS).map(WHO_REGION_NAMES)
            .fillna('Other')
        )

        fig = px.bar(
            df_country, y='name', x='n', orientation='h', color='region',
            color_discrete_map=WHO_REGION_COLORS,
            labels={'n': 'Papers', 'name': '', 'region': 'WHO region'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=max(400, len(df_country) * 30),
            yaxis={'categoryorder': 'total ascending'},
            legend=dict(orientation='h', yanchor='bottom', y=1.02,
                        xanchor='left', x=0, title_text=''),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ------------------------------------------------------------------
    # Top producing institutions
    # ------------------------------------------------------------------
    section_header(
        'Top 15 Producing Institutions',
        'Institutions with the most first- or last-author papers. '
        'The Institutions lens explores production in depth.',
    )

    df_inst_ov = query_df(
        f"""SELECT ANY_VALUE(a.institution_name) AS institution,
                   ANY_VALUE(a.institution_country) AS country,
                   COUNT(DISTINCT a.openalex_id) AS n
            FROM authorships a
            JOIN works w ON a.openalex_id = w.openalex_id
            {inst_where}
            AND a.institution_name IS NOT NULL
            AND a.institution_name != ''
            AND a.position IN ('first', 'last')
            GROUP BY a.institution_id
            ORDER BY n DESC LIMIT 15""",
        tuple(inst_params),
    )

    if not df_inst_ov.empty:
        _shared = shared_institution_names()
        df_inst_ov['institution'] = [
            institution_label(n, c, _shared)
            for n, c in zip(df_inst_ov['institution'], df_inst_ov['country'])
        ]
        df_inst_ov['country_name'] = df_inst_ov['country'].apply(
            iso2_to_country_name)
        fig = px.bar(
            df_inst_ov, y='institution', x='n', orientation='h',
            color='country_name',
            color_discrete_map=country_color_map(
                df_inst_ov['country_name'].unique()),
            labels={'n': 'Papers', 'institution': '', 'country_name': 'Country'},
            template=CHART_TEMPLATE,
        )
        fig.update_layout(
            height=max(400, len(df_inst_ov) * 30),
            yaxis={'categoryorder': 'total ascending'},
            legend=dict(font=dict(size=9)),
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
            # Count-weighted mean confidence across all inferred authors
            mean_conf = (
                (df_gender['n'] * df_gender['confidence']).sum()
                / df_gender['n'].sum()
            )
            # Collapse the per-confidence rows into one bar per gender ×
            # author position (grouping by the raw "label|conf" string would
            # otherwise stack many thin segments inside each bar).
            df_bars = (
                df_gender.groupby(['position', 'label'], as_index=False)['n']
                .sum()
            )
            fig = px.bar(
                df_bars, x='position', y='n', color='label',
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
            conf = mean_conf
            st.caption(
                f'**Methodology:** Gender is inferred probabilistically from '
                f'author given names using the [Genderize.io](https://genderize.io) '
                f'API (a name-to-gender frequency service). Each '
                f'assignment carries a confidence score (currently {conf:.2f} '
                f'for this corpus), i.e. an estimated {conf:.0%} average '
                f'probability that an inferred gender is correct. Papers '
                f'where gender could not be determined are excluded from this '
                f'chart. This approach has known limitations for names that '
                f'are culturally ambiguous or gender-neutral.'
            )
