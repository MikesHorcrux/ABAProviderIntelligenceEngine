#!/bin/zsh
set -euo pipefail
cd "$(dirname "$0")"
./crawler_v2.py --mode full
./enrich.py
./brief.py
