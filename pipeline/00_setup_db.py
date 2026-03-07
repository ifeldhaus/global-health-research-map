"""
pipeline/00_setup_db.py

Initializes the DuckDB schema. Run once before any other pipeline script.
Safe to re-run --- all CREATE statements use IF NOT EXISTS.

Usage:
    uv run python pipeline/00_setup_db.py
"""

import duckdb

DB = 'data/global_health.duckdb'

con = duckdb.connect(DB)

# ---------------------------------------------------------------------------
# Core papers table
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TABLE IF NOT EXISTS works (
        openalex_id       VARCHAR PRIMARY KEY,
        title             VARCHAR,
        abstract          VARCHAR,
        publication_year  INTEGER,
        journal_issn      VARCHAR,
        cited_by_count    INTEGER,

        -- enrichment columns: NULL until pipeline fills them
        topic_category    VARCHAR,
        topic_subtopic    VARCHAR,
        topic_confidence  VARCHAR,
        method_type       VARCHAR,
        method_confidence VARCHAR,
        study_country      VARCHAR,
        country_confidence VARCHAR,
        gender_first       VARCHAR,
        gender_last       VARCHAR,

        -- completion flags for resumability
        classified_topic   BOOLEAN DEFAULT FALSE,
        classified_method  BOOLEAN DEFAULT FALSE,
        classified_country BOOLEAN DEFAULT FALSE
    )
""")

# ---------------------------------------------------------------------------
# Author-paper pairs
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TABLE IF NOT EXISTS authorships (
        openalex_id          VARCHAR,
        author_id            VARCHAR,
        author_name          VARCHAR,
        position             VARCHAR,   -- 'first', 'middle', 'last'
        institution_id       VARCHAR,
        institution_name     VARCHAR,
        institution_country  VARCHAR,
        PRIMARY KEY (openalex_id, author_id, position)
    )
""")

# ---------------------------------------------------------------------------
# Raw grant/funder strings as returned by OpenAlex.
# Separate from the funders lookup table below --- this is the raw join table.
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TABLE IF NOT EXISTS grants (
        openalex_id      VARCHAR,
        funder_id        VARCHAR,
        funder_name_raw  VARCHAR,
        award_id         VARCHAR,
        PRIMARY KEY (openalex_id, funder_id, award_id)
    )
""")

# ---------------------------------------------------------------------------
# Normalized funder lookup table.
# Raw funder strings from grants are resolved to canonical entries here
# by pipeline/04_funder_normalize.py.
# Seeded from data/funders_canonical.csv.
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TABLE IF NOT EXISTS funders (
        canonical_name   VARCHAR PRIMARY KEY,
        funder_category  VARCHAR,   -- 'Government', 'Philanthropic', 'Multilateral', etc.
        funder_country   VARCHAR,
        openalex_id      VARCHAR,
        aliases          VARCHAR    -- pipe-separated alias strings
    )
""")

# ---------------------------------------------------------------------------
# WHO Global Burden of Disease data
# Loaded from data/gbd/ CSVs by a one-off notebook before analysis
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TABLE IF NOT EXISTS gbd_burden (
        cause        VARCHAR,
        region       VARCHAR,
        year         INTEGER,
        metric       VARCHAR,   -- 'DALYs', 'Deaths', 'YLDs', 'YLLs'
        sex          VARCHAR,
        age_group    VARCHAR,
        val          DOUBLE,
        upper        DOUBLE,
        lower        DOUBLE,
        PRIMARY KEY (cause, region, year, metric, sex, age_group)
    )
""")

# ---------------------------------------------------------------------------
# Manual mapping: research topic category -> GBD cause category
# Populated by hand in data/taxonomy/topic_burden_map.csv, loaded once
# ---------------------------------------------------------------------------
con.execute("""
    CREATE TABLE IF NOT EXISTS topic_burden_map (
        topic_category  VARCHAR PRIMARY KEY,
        topic_name      VARCHAR,
        gbd_cause       VARCHAR,
        notes           VARCHAR
    )
""")

# ---------------------------------------------------------------------------
# Indexes on the columns most commonly filtered/joined on
# ---------------------------------------------------------------------------
con.execute("CREATE INDEX IF NOT EXISTS idx_year    ON works(publication_year)")
con.execute("CREATE INDEX IF NOT EXISTS idx_topic   ON works(topic_category)")
con.execute("CREATE INDEX IF NOT EXISTS idx_journal ON works(journal_issn)")
con.execute("CREATE INDEX IF NOT EXISTS idx_method  ON works(method_type)")

con.close()
print('Schema initialized.')
