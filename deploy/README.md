# Deploying the dashboard to Streamlit Community Cloud

The dashboard reads a DuckDB database. The full analysis DB is ~1.9 GB (mostly
abstract text the dashboard never uses); `build_slim_db.py` produces a ~22 MB
compact version that is committed to the repo and used by the public deploy.
`dashboard/db.py` uses the full DB when it is present locally and falls back to
the slim DB otherwise, so the deployed app reads the slim copy automatically.

## Prerequisites
- A GitHub account (the repo is already on GitHub).
- The slim DB committed at `data/global_health_slim.duckdb` (see below).

## Rebuild the slim database (only when the data changes)
From `data/global_health.duckdb`:
```bash
uv run python deploy/build_slim_db.py
git add -f data/global_health_slim.duckdb
git commit -m "Rebuild slim database"
git push
```
The `-f` is required because `*.duckdb` is gitignored; a `.gitignore` exception
re-includes the slim file specifically.

## Deploy
1. Go to https://share.streamlit.io and sign in with GitHub.
2. **New app** -> repository `ifeldhaus/global-health-research-map`,
   branch `master`, main file path `dashboard/app.py`.
3. Set a custom subdomain (e.g. `global-health-research-map`), so the app
   serves at `https://global-health-research-map.streamlit.app`.
4. **Deploy.** Community Cloud installs from `requirements.txt` and runs the
   app. The first build takes a few minutes.
5. Add the resulting URL to the top of the project README and to the paper's
   data-and-code availability statement.

Redeploys are automatic: every push to `master` rebuilds the app. Rebuild and
commit the slim DB (above) only when the underlying data changes.
