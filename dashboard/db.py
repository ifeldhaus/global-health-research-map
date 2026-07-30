"""
dashboard/db.py

DuckDB connection manager and cached query helpers for the Streamlit dashboard.
All queries use read-only connections to avoid contention with pipeline writes.
"""

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

_DATA_DIR = Path(__file__).parent.parent / 'data'
_FULL_DB = _DATA_DIR / 'global_health.duckdb'
_SLIM_DB = _DATA_DIR / 'global_health_slim.duckdb'

# Use the full database locally when present; fall back to the slim database
# (the only one committed to the repo) for the public deployment.
DB_PATH = _FULL_DB if _FULL_DB.exists() else _SLIM_DB


def db_exists() -> bool:
    """Check if the DuckDB file exists on disk."""
    return DB_PATH.exists()


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a read-only DuckDB connection."""
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(ttl=300)
def query_df(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame, cached for 5 minutes."""
    con = get_connection()
    try:
        if params:
            return con.execute(sql, list(params)).fetchdf()
        return con.execute(sql).fetchdf()
    finally:
        con.close()


@st.cache_data(ttl=300)
def query_scalar(sql: str, params: tuple | None = None):
    """Execute SQL and return a single scalar value, cached for 5 minutes."""
    con = get_connection()
    try:
        if params:
            result = con.execute(sql, list(params)).fetchone()
        else:
            result = con.execute(sql).fetchone()
        return result[0] if result else None
    finally:
        con.close()


@st.cache_data(ttl=300)
def shared_institution_names() -> frozenset:
    """Display names shared by more than one OpenAlex institution ID.

    These are umbrella names (e.g. "Ministry of Health") that OpenAlex assigns
    to distinct national entities. Callers disambiguate them by appending the
    country so each is treated as its own institution.
    """
    con = get_connection()
    try:
        rows = con.execute(
            """SELECT institution_name FROM authorships
               WHERE institution_name IS NOT NULL AND institution_name <> ''
               GROUP BY institution_name
               HAVING COUNT(DISTINCT institution_id) > 1"""
        ).fetchall()
        return frozenset(r[0] for r in rows)
    finally:
        con.close()


def table_exists(name: str) -> bool:
    """Check if a table exists and has at least one row."""
    try:
        con = get_connection()
        count = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        con.close()
        return count > 0
    except Exception:
        return False


@st.cache_data(ttl=60)
def get_pipeline_status() -> dict:
    """Return a status dict showing which pipeline stages have data."""
    status = {}
    try:
        con = get_connection()

        status['works'] = con.execute(
            "SELECT COUNT(*) FROM works"
        ).fetchone()[0]

        status['authorships'] = con.execute(
            "SELECT COUNT(*) FROM authorships"
        ).fetchone()[0]

        status['grants'] = con.execute(
            "SELECT COUNT(*) FROM grants"
        ).fetchone()[0]

        status['funders'] = con.execute(
            "SELECT COUNT(*) FROM funders"
        ).fetchone()[0]

        status['gbd_burden'] = con.execute(
            "SELECT COUNT(*) FROM gbd_burden"
        ).fetchone()[0]

        # Enrichment progress
        if status['works'] > 0:
            enrichment = con.execute("""
                SELECT
                    SUM(CASE WHEN classified_topic THEN 1 ELSE 0 END) AS topics,
                    SUM(CASE WHEN classified_method THEN 1 ELSE 0 END) AS methods,
                    SUM(CASE WHEN classified_country THEN 1 ELSE 0 END) AS countries,
                    SUM(CASE WHEN gender_first IS NOT NULL THEN 1 ELSE 0 END) AS gender,
                    COUNT(*) AS total
                FROM works
            """).fetchdf().iloc[0]
            status['topic_classified'] = int(enrichment['topics'])
            status['method_classified'] = int(enrichment['methods'])
            status['country_classified'] = int(enrichment['countries'])
            status['gender_inferred'] = int(enrichment['gender'])
            status['total_works'] = int(enrichment['total'])
        else:
            status['topic_classified'] = 0
            status['method_classified'] = 0
            status['country_classified'] = 0
            status['gender_inferred'] = 0
            status['total_works'] = 0

        con.close()
    except Exception:
        pass

    return status


def build_where_clause(year_range: tuple[int, int] | None = None,
                       topics: list[str] | None = None,
                       funder_categories: list[str] | None = None,
                       table_alias: str = 'w') -> tuple[str, list]:
    """Build a parameterized WHERE clause from sidebar filter values.

    Returns (clause_string, params_list). The clause_string starts with
    'AND' so it can be appended to an existing WHERE TRUE.
    """
    clauses = []
    params = []

    if year_range:
        clauses.append(f"{table_alias}.publication_year BETWEEN ? AND ?")
        params.extend(year_range)

    if topics:
        placeholders = ', '.join(['?'] * len(topics))
        clauses.append(f"{table_alias}.topic_category IN ({placeholders})")
        params.extend(topics)

    # Funder category filter requires a join; handled separately per query
    # We just store the values for the caller to use
    if funder_categories:
        pass  # Handled by individual queries that join to funders

    clause_str = ''
    if clauses:
        clause_str = ' AND ' + ' AND '.join(clauses)

    return clause_str, params
