"""
dashboard/pages/data_completeness.py

Data completeness and missingness transparency page.  Shows readers
where abstract data is missing, why, and how it affects the analysis.
"""

import plotly.express as px
import streamlit as st

from dashboard.components import (
    check_data_ready,
    download_csv_button,
    metric_row,
    section_header,
)
from dashboard.constants import (
    CHART_HEIGHT,
    CHART_MARGIN,
    CHART_TEMPLATE,
    COMPLETENESS_COLORS,
    JOURNAL_NAMES,
)
from dashboard.db import build_where_clause, query_df, query_scalar


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

# Boilerplate patterns must stay in sync with pipeline/02_topic_classify.py
_BOILERPLATE_CASE = """
    CASE
        WHEN abstract IS NULL OR TRIM(abstract) = ''
            THEN 'no_abstract'
        WHEN LENGTH(TRIM(abstract)) <= 50
            THEN 'insufficient_abstract'
        WHEN abstract LIKE 'Annals of Global Health is a peer-reviewed%'
          OR abstract LIKE 'Welcome to Annals of Global Health%'
            THEN 'boilerplate_abstract'
        ELSE 'classifiable'
    END
"""

_STATUS_ORDER = ['classifiable', 'boilerplate_abstract',
                 'insufficient_abstract', 'no_abstract']

_STATUS_LABELS = {
    'classifiable': 'Usable abstract',
    'no_abstract': 'No abstract',
    'insufficient_abstract': 'Insufficient abstract',
    'boilerplate_abstract': 'Boilerplate text',
}


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

def page():
    st.title('Data Completeness')
    st.caption(
        'Abstract availability in the OpenAlex source data and its '
        'implications for topic and methods classification.'
    )

    if not check_data_ready():
        return

    # -- Sidebar filters (year range only) --------------------------------
    year_range = st.session_state.get('year_range', (2010, 2024))
    where, params = build_where_clause(year_range=year_range)
    base_where = f"WHERE TRUE {where}"

    # =====================================================================
    # 1. Summary metrics
    # =====================================================================

    total = query_scalar(
        f"SELECT COUNT(*) FROM works w {base_where}",
        tuple(params),
    )

    classifiable = query_scalar(
        f"""SELECT COUNT(*) FROM works w
            {base_where}
            AND abstract IS NOT NULL
            AND LENGTH(TRIM(abstract)) > 50
            AND abstract NOT LIKE 'Annals of Global Health is a peer-reviewed%%'
            AND abstract NOT LIKE 'Welcome to Annals of Global Health%%'""",
        tuple(params),
    )

    missing = total - classifiable

    topic_classified = query_scalar(
        f"""SELECT COUNT(*) FROM works w
            {base_where} AND classified_topic = TRUE
            AND topic_category != 'Z'""",
        tuple(params),
    )

    pct_missing = (missing / total * 100) if total else 0
    pct_classifiable = (classifiable / total * 100) if total else 0
    pct_topic = (topic_classified / classifiable * 100) if classifiable else 0

    metric_row([
        ('Total papers', total, None),
        ('Usable abstract', classifiable, f"{pct_classifiable:.0f}% of total"),
        ('Missing abstract', missing, f"{pct_missing:.0f}% of total"),
        ('Topic classified', topic_classified, f"{pct_topic:.0f}% of usable"),
    ])

    # =====================================================================
    # 2. Abstract availability by journal
    # =====================================================================

    section_header(
        'Abstract availability by journal',
        'Missingness is concentrated in specific journals due to OpenAlex '
        'metadata coverage, not random sampling.',
    )

    df_journal = query_df(
        f"""SELECT
                journal_issn,
                {_BOILERPLATE_CASE} AS status,
                COUNT(*) AS n
            FROM works w
            {base_where}
            GROUP BY journal_issn, status
            ORDER BY journal_issn, status""",
        tuple(params),
    )

    if not df_journal.empty:
        # Map ISSN to journal name
        df_journal['journal'] = df_journal['journal_issn'].map(
            lambda x: JOURNAL_NAMES.get(x, x)
        )
        df_journal['status_label'] = df_journal['status'].map(_STATUS_LABELS)

        # Sort journals by missingness rate (highest at top)
        journal_totals = df_journal.groupby('journal')['n'].sum()
        journal_classifiable = (
            df_journal[df_journal['status'] == 'classifiable']
            .groupby('journal')['n'].sum()
        )
        journal_miss_rate = 1 - (journal_classifiable / journal_totals).fillna(0)
        journal_order = journal_miss_rate.sort_values(ascending=True).index.tolist()

        color_map = {_STATUS_LABELS[k]: v for k, v in COMPLETENESS_COLORS.items()}

        fig_journal = px.bar(
            df_journal,
            y='journal',
            x='n',
            color='status_label',
            orientation='h',
            template=CHART_TEMPLATE,
            color_discrete_map=color_map,
            category_orders={
                'journal': journal_order,
                'status_label': [_STATUS_LABELS[s] for s in _STATUS_ORDER],
            },
            labels={'n': 'Papers', 'journal': '', 'status_label': 'Abstract status'},
        )
        fig_journal.update_layout(
            height=max(400, len(journal_order) * 45),
            margin=CHART_MARGIN,
            legend=dict(orientation='h', yanchor='bottom', y=1.02,
                        xanchor='left', x=0),
            barmode='stack',
        )
        st.plotly_chart(fig_journal, use_container_width=True)
        download_csv_button(df_journal[['journal', 'status', 'n']],
                            'abstract_availability_by_journal.csv')

    # =====================================================================
    # 3. Abstract availability by year
    # =====================================================================

    section_header(
        'Abstract availability by year',
        'Temporal pattern of missingness across the study period.',
    )

    df_year = query_df(
        f"""SELECT
                publication_year AS year,
                {_BOILERPLATE_CASE} AS status,
                COUNT(*) AS n
            FROM works w
            {base_where}
            GROUP BY publication_year, status
            ORDER BY publication_year, status""",
        tuple(params),
    )

    if not df_year.empty:
        df_year['status_label'] = df_year['status'].map(_STATUS_LABELS)

        color_map = {_STATUS_LABELS[k]: v for k, v in COMPLETENESS_COLORS.items()}

        fig_year = px.bar(
            df_year,
            x='year',
            y='n',
            color='status_label',
            template=CHART_TEMPLATE,
            color_discrete_map=color_map,
            category_orders={
                'status_label': [_STATUS_LABELS[s] for s in _STATUS_ORDER],
            },
            labels={'n': 'Papers', 'year': 'Publication year',
                    'status_label': 'Abstract status'},
        )
        fig_year.update_layout(
            height=CHART_HEIGHT,
            margin=CHART_MARGIN,
            legend=dict(orientation='h', yanchor='bottom', y=1.02,
                        xanchor='left', x=0),
            barmode='stack',
        )
        st.plotly_chart(fig_year, use_container_width=True)
        download_csv_button(df_year[['year', 'status', 'n']],
                            'abstract_availability_by_year.csv')

    # =====================================================================
    # 4. Classification coverage (of classifiable papers)
    # =====================================================================

    section_header(
        'Classification coverage',
        'Enrichment completion rates for papers with usable abstracts.',
    )

    df_enrich = query_df(
        f"""SELECT
                SUM(CASE WHEN classified_topic AND topic_category != 'Z'
                    THEN 1 ELSE 0 END) AS topic,
                SUM(CASE WHEN classified_method THEN 1 ELSE 0 END) AS method,
                SUM(CASE WHEN classified_country THEN 1 ELSE 0 END) AS country,
                SUM(CASE WHEN gender_first IS NOT NULL
                    THEN 1 ELSE 0 END) AS gender,
                COUNT(*) AS total
            FROM works w
            {base_where}
            AND abstract IS NOT NULL
            AND LENGTH(TRIM(abstract)) > 50
            AND abstract NOT LIKE 'Annals of Global Health is a peer-reviewed%%'
            AND abstract NOT LIKE 'Welcome to Annals of Global Health%%'""",
        tuple(params),
    )

    if not df_enrich.empty and df_enrich.iloc[0]['total'] > 0:
        row = df_enrich.iloc[0]
        enrichments = {
            'Topic classification': int(row['topic']),
            'Methods classification': int(row['method']),
            'Study country': int(row['country']),
            'Gender inference': int(row['gender']),
        }
        enrich_total = int(row['total'])

        for label, done in enrichments.items():
            pct = done / enrich_total * 100
            st.progress(
                pct / 100,
                text=f"{label}: {done:,} / {enrich_total:,} ({pct:.0f}%)",
            )

    # =====================================================================
    # 5. Methodological note
    # =====================================================================

    section_header('Methodological note')

    st.info(
        "**Systematic missingness in abstract data.** "
        "Of the papers in this corpus, 25% lack usable abstracts in OpenAlex "
        "and therefore could not be classified by topic or method. "
        "This missingness is not random: it is concentrated in "
        "*Lancet Global Health* (52%), *Annals of Global Health* (55%), "
        "and *Globalization and Health* (68%), likely due to publisher "
        "metadata policies and OpenAlex ingestion coverage. "
        "Findings from the topic, methods, and geographic analyses should "
        "be interpreted with awareness that these three journals are "
        "underrepresented in the classified subset. "
        "All papers remain in the corpus and are included in "
        "publication volume, authorship, funder, and institution analyses "
        "that do not depend on abstract-derived classifications.",
        icon=':material/info:',
    )
