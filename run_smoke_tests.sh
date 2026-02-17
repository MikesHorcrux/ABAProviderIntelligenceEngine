#!/bin/zsh
set -euo pipefail
cd "$(dirname "$0")"
python3 tests/smoke_v1.py
