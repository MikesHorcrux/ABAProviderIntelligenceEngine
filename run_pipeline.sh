#!/bin/zsh
set -euo pipefail
cd "$(dirname "$0")"
./crawler.py
./brief.py
