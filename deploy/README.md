# Deploying the dashboard to Hugging Face Spaces

The dashboard reads a DuckDB database. The full analysis DB is ~1.9 GB (mostly
abstract text the dashboard never uses); `build_slim_db.py` produces a ~22 MB
compact version that runs on the free tier.

## Prerequisites
- A Hugging Face account, logged in.
- `git` and `git-lfs` (`brew install git-lfs && git lfs install`).

## Steps
1. Build the slim database (from `data/global_health.duckdb`):
   ```bash
   uv run python deploy/build_slim_db.py
   ```
2. On huggingface.co: **New Space** -> name `global-health-research-map`,
   SDK **Streamlit**, hardware **CPU basic (free)**, visibility **Public**.
3. Clone the new Space, populate it from this repo, and push:
   ```bash
   git clone https://huggingface.co/spaces/<your-username>/global-health-research-map ghrm-space
   deploy/build_space.sh ghrm-space
   cd ghrm-space
   git lfs track "*.duckdb"
   git add -A
   git commit -m "Deploy dashboard"
   git push
   ```
4. It builds and serves at
   `https://<your-username>-global-health-research-map.hf.space`. Add that link
   to the top of the project README.

To redeploy after changes: re-run step 1 (if data changed) and the
`build_space.sh` + `git push` in step 3.
