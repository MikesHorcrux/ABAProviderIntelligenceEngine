#!/bin/zsh
set -euo pipefail
cd "$(dirname "$0")"

echo "[1/4] Canonical ingest + pipeline"
./run_v1_features.sh

echo "[2/4] Change report"
python3 jobs/export_changes.py

echo "[3/4] Smoke tests"
./run_smoke_tests.sh

echo "[4/4] Acceptance summary"
python3 - <<'PY'
from pathlib import Path
root = Path('.')
required = [
    root/'out'/'outreach_dispensary_100.csv',
    root/'out'/'excluded_non_dispensary.csv',
    root/'out'/'v4_quality_report.txt',
]
missing = [str(p) for p in required if not p.exists()]
if missing:
    print('Missing required outputs:')
    for m in missing:
        print('-', m)
    raise SystemExit(1)
print('V1 acceptance runner complete. Required outputs present.')
PY
