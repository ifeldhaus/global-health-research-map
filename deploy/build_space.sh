#!/usr/bin/env bash
# Assemble a Hugging Face Space directory from this repo.
#   deploy/build_space.sh <target_dir>
# <target_dir> is normally a freshly cloned, empty HF Space repo.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:?usage: deploy/build_space.sh <target_dir>}"

if [ ! -f "$ROOT/data/global_health_slim.duckdb" ]; then
  echo "Slim DB missing. Build it first:" >&2
  echo "  uv run python deploy/build_slim_db.py" >&2
  exit 1
fi

mkdir -p "$OUT/data"
cp -r "$ROOT/dashboard" "$OUT/"
find "$OUT/dashboard" -name __pycache__ -type d -prune -exec rm -rf {} +
cp "$ROOT/requirements.txt" "$OUT/"
cp "$ROOT/deploy/hf_space/README.md" "$OUT/README.md"
cp "$ROOT/data/global_health_slim.duckdb" "$OUT/data/global_health.duckdb"
echo "Space assembled at: $OUT"
