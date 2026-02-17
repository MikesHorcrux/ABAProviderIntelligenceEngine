#!/bin/zsh
set -euo pipefail
cd "$(dirname "$0")"
PYTHONPATH="$PWD" python3 jobs/ingest_sources.py
./run_v4.sh
