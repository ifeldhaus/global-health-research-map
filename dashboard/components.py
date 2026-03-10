"""
dashboard/components.py

Reusable UI components for the Streamlit dashboard.
"""

import pandas as pd
import streamlit as st

from dashboard.db import get_pipeline_status


def empty_state(message: str, icon: str = 'info',
                help_text: str | None = None):
    """Render a styled empty-state placeholder."""
    icons = {
        'info': ':material/info:',
        'warning': ':material/warning:',
        'data': ':material/database:',
        'chart': ':material/bar_chart:',
    }
    st.info(f"{icons.get(icon, '')} {message}", icon=icons.get(icon))
    if help_text:
        st.caption(help_text)


def pipeline_progress_card():
    """Show which pipeline stages have data — useful before corpus is loaded."""
    status = get_pipeline_status()
    if not status:
        empty_state(
            'Database not found. Run `uv run python pipeline/00_setup_db.py` to initialize.',
            icon='warning',
        )
        return

    st.subheader('Pipeline Progress')

    total = status.get('total_works', 0)

    stages = [
        ('Corpus Pull', status.get('works', 0), 'papers loaded'),
        ('Authorships', status.get('authorships', 0), 'author records'),
        ('Grants', status.get('grants', 0), 'grant records'),
        ('Funders', status.get('funders', 0), 'canonical funders'),
        ('GBD Burden', status.get('gbd_burden', 0), 'burden data rows'),
    ]

    for label, count, unit in stages:
        if count > 0:
            st.write(f"- :white_check_mark: **{label}**: {count:,} {unit}")
        else:
            st.write(f"- :hourglass_flowing_sand: **{label}**: awaiting data")

    if total > 0:
        st.divider()
        st.subheader('Enrichment Progress')
        enrichments = [
            ('Topic Classification', status.get('topic_classified', 0)),
            ('Methods Classification', status.get('method_classified', 0)),
            ('Study Country', status.get('country_classified', 0)),
            ('Gender Inference', status.get('gender_inferred', 0)),
        ]
        for label, done in enrichments:
            pct = (done / total * 100) if total > 0 else 0
            st.progress(pct / 100, text=f"{label}: {done:,}/{total:,} ({pct:.0f}%)")


def metric_row(metrics: list[tuple[str, str | int | float, str | None]],
               delta_color: str = 'normal'):
    """Render a row of st.metric cards.

    Each metric is (label, value, delta_or_none).
    delta_color: 'normal' (green/red arrows), 'off' (plain gray text).
    """
    cols = st.columns(len(metrics))
    for col, (label, value, delta) in zip(cols, metrics):
        display_val = f"{value:,}" if isinstance(value, (int, float)) else value
        col.metric(label, display_val, delta, delta_color=delta_color)


def section_header(title: str, description: str | None = None):
    """Render a consistent section header with optional description."""
    st.markdown('---')
    st.subheader(title)
    if description:
        st.caption(description)


def download_csv_button(df: pd.DataFrame, filename: str,
                        label: str = 'Download CSV'):
    """Add a download button for the underlying data."""
    csv_data = df.to_csv(index=False)
    st.download_button(
        label=label,
        data=csv_data,
        file_name=filename,
        mime='text/csv',
    )


def check_data_ready(min_works: int = 1,
                     require_topics: bool = False,
                     require_methods: bool = False,
                     require_countries: bool = False,
                     require_gender: bool = False,
                     require_gbd: bool = False) -> bool:
    """Check if enough data is available for a page. Shows appropriate messages.

    Returns True if the minimum data requirements are met.
    """
    from dashboard.db import db_exists, table_exists

    if not db_exists():
        empty_state(
            'Database not found.',
            icon='warning',
            help_text='Run `uv run python pipeline/00_setup_db.py` to initialize the database.',
        )
        return False

    if not table_exists('works'):
        st.title('Welcome')
        st.markdown(
            'The database is initialized but empty. '
            'Run the corpus pull to start loading papers.'
        )
        pipeline_progress_card()
        return False

    status = get_pipeline_status()
    total = status.get('total_works', 0)

    if total < min_works:
        st.info(f'Only {total:,} papers loaded so far. '
                f'Some visualizations may be limited.')

    # Show banners for missing enrichments but don't block
    warnings = []
    if require_topics and status.get('topic_classified', 0) == 0:
        warnings.append('Topic classification has not run yet.')
    if require_methods and status.get('method_classified', 0) == 0:
        warnings.append('Methods classification has not run yet.')
    if require_countries and status.get('country_classified', 0) == 0:
        warnings.append('Study country extraction has not run yet.')
    if require_gender and status.get('gender_inferred', 0) == 0:
        warnings.append('Gender inference has not run yet.')
    if require_gbd and status.get('gbd_burden', 0) == 0:
        warnings.append(
            'GBD burden data not loaded. '
            'Run `uv run python pipeline/07_gbd_burden.py` to enable '
            'burden-adjusted analysis.'
        )

    for w in warnings:
        st.warning(w, icon=':material/warning:')

    return True
