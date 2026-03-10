"""
dashboard/app.py

Streamlit dashboard entry point.

Usage:
    uv run streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

# Ensure the project root is on sys.path so 'dashboard' is importable.
# This is needed because `streamlit run dashboard/app.py` doesn't
# automatically add the project root to the Python path.
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import streamlit as st

from dashboard.constants import TOPIC_LABELS, FUNDER_CATEGORY_COLORS
from dashboard.db import db_exists, get_pipeline_status

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title='Global Health Research Map',
    page_icon=':earth_americas:',
    layout='wide',
    initial_sidebar_state='expanded',
)

# ---------------------------------------------------------------------------
# Import pages
# ---------------------------------------------------------------------------

from dashboard.pages.overview import page as overview_page           # noqa: E402
from dashboard.pages.lens_a_funder import page as lens_a_page        # noqa: E402
from dashboard.pages.lens_b_geographic import page as lens_b_page    # noqa: E402
from dashboard.pages.lens_c_topics import page as lens_c_page        # noqa: E402
from dashboard.pages.lens_d_methods import page as lens_d_page       # noqa: E402
from dashboard.pages.institutions import page as institutions_page   # noqa: E402
from dashboard.pages.data_completeness import page as data_completeness_page  # noqa: E402

# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------

pages = {
    'Overview': [
        st.Page(overview_page, title='Overview', icon=':material/dashboard:',
                url_path='overview', default=True),
    ],
    'Research Lenses': [
        st.Page(lens_a_page, title='Funder Power', icon=':material/payments:',
                url_path='funder-power'),
        st.Page(lens_b_page, title='Geographic Power', icon=':material/public:',
                url_path='geographic-power'),
        st.Page(lens_c_page, title='Topic Trends', icon=':material/trending_up:',
                url_path='topic-trends'),
        st.Page(lens_d_page, title='Methods Gaps', icon=':material/science:',
                url_path='methods-gaps'),
        st.Page(institutions_page, title='Institutions', icon=':material/school:',
                url_path='institutions'),
    ],
    'Data Quality': [
        st.Page(data_completeness_page, title='Data Completeness',
                icon=':material/fact_check:', url_path='data-completeness'),
    ],
}

pg = st.navigation(pages)

# ---------------------------------------------------------------------------
# Sidebar: global filters
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title(':earth_americas: Global Health Research Map')
    st.caption('Mapping power, priority, and practice in global health research')

    st.divider()

    # Year range filter
    st.subheader('Filters')
    year_range = st.slider(
        'Publication years',
        min_value=2010,
        max_value=2024,
        value=(2010, 2024),
        key='year_range',
    )

    # Topic category filter
    topic_options = {k: v for k, v in sorted(TOPIC_LABELS.items())}
    selected_topics = st.multiselect(
        'Topic categories',
        options=list(topic_options.keys()),
        format_func=lambda x: topic_options.get(x, x),
        key='selected_topics',
        placeholder='All topics',
    )

    # Funder category filter
    funder_cats = list(FUNDER_CATEGORY_COLORS.keys())
    selected_funder_cats = st.multiselect(
        'Funder categories',
        options=funder_cats,
        key='selected_funder_cats',
        placeholder='All funder types',
    )

    # Pipeline status at bottom of sidebar
    st.divider()
    if db_exists():
        status = get_pipeline_status()
        total = status.get('total_works', 0)
        if total > 0:
            classified = status.get('topic_classified', 0)
            pct = (classified / total * 100) if total > 0 else 0
            st.caption(f':white_check_mark: {total:,} papers loaded')
            if pct < 100:
                st.caption(f':hourglass_flowing_sand: {pct:.0f}% enriched')
            else:
                st.caption(':white_check_mark: All enrichments complete')
        else:
            st.caption(':hourglass_flowing_sand: Awaiting corpus pull')
    else:
        st.caption(':warning: Database not initialized')

# ---------------------------------------------------------------------------
# Run selected page
# ---------------------------------------------------------------------------

pg.run()
