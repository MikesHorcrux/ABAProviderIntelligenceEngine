#!/bin/zsh
set -euo pipefail
cd "$(dirname "$0")"
PYTHONPATH="$PWD" python3.11 tests/test_fetch_config.py
PYTHONPATH="$PWD" python3.11 tests/test_fetch_dispatch.py
PYTHONPATH="$PWD" python3.11 tests/test_parse_stage.py
PYTHONPATH="$PWD" python3.11 tests/test_resolve_stage.py
PYTHONPATH="$PWD" python3.11 tests/smoke_v1.py
