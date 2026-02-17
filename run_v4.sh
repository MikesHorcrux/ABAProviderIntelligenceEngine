#!/bin/zsh
set -euo pipefail
cd "$(dirname "$0")"
./crawler_v2.py --mode full
./enrich.py
./postprocess_v4.py
./brief.py
