#!/bin/zsh
set -euo pipefail
cd "$(dirname "$0")"

if [ "${CANNARADAR_RUN_CANONICAL_INGEST:-0}" = "1" ]; then
  echo "Running canonical ingest."
  PYTHONPATH="$PWD" python3 jobs/ingest_sources.py
else
  echo "Skipping canonical ingest (set CANNARADAR_RUN_CANONICAL_INGEST=1 to enable)."
fi

./run_v4.sh
