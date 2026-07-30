"""Build a compact, deploy-sized DuckDB from the full analysis database.

The dashboard never reads full abstract text; it needs only an 80-character
prefix (for the Data Completeness page's presence / length / boilerplate
checks). Dropping the two abstract-text columns and rewriting every table also
reclaims DuckDB's unused free space, taking the file from ~1.9 GB to ~22 MB,
which hosts on Hugging Face Spaces' free tier.

    uv run python deploy/build_slim_db.py
"""
import os
import duckdb

SRC = 'data/global_health.duckdb'
DST = 'data/global_health_slim.duckdb'


def main():
    if not os.path.exists(SRC):
        raise SystemExit(f'Source database not found: {SRC}')
    if os.path.exists(DST):
        os.remove(DST)
    con = duckdb.connect(DST)
    con.execute(f"ATTACH '{SRC}' AS src (READ_ONLY)")
    tables = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog='src' AND table_schema='main'").fetchall()]
    cols = [r[1] for r in con.execute(
        "PRAGMA table_info('src.works')").fetchall()]
    sel = []
    for c in cols:
        if c == 'abstract':
            # Keep only the prefix: preserves NULL/empty, the <=50-char length
            # test, and the two Annals boilerplate prefixes.
            sel.append('LEFT(abstract, 80) AS abstract')
        elif c == 'abstract_orig':
            continue  # unused by the dashboard
        else:
            sel.append(c)
    con.execute(f"CREATE TABLE works AS SELECT {', '.join(sel)} FROM src.works")
    for t in tables:
        if t == 'works':
            continue
        con.execute(f'CREATE TABLE "{t}" AS SELECT * FROM src."{t}"')
    con.execute('DETACH src')
    con.close()
    print(f'Wrote {DST} ({os.path.getsize(DST) / 1e6:.1f} MB)')


if __name__ == '__main__':
    main()
