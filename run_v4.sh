#!/bin/zsh
set -euo pipefail

BASE="$(cd "$(dirname "$0")" && pwd)"
cd "$BASE"

OUT_DIR="$BASE/out"
STATE_DIR="$BASE/data/state"
LOCK_FILE="$STATE_DIR/run_v4.lock"
RUN_ID="$(date +%Y%m%d-%H%M%S)"
MODE="${CANNARADAR_CRAWL_MODE:-full}"
CRAWLER_CONFIG="${CANNARADAR_CRAWLER_CONFIG:-$BASE/crawler_config.json}"

mkdir -p "$OUT_DIR" "$STATE_DIR"

cleanup() {
  if [ -f "$LOCK_FILE" ]; then
    rm -f "$LOCK_FILE"
  fi
}
trap 'cleanup' EXIT

# Runtime lock to prevent overlapping runs.
if ! command -v flock >/dev/null 2>&1; then
  if [ -f "$LOCK_FILE" ]; then
    lock_age="$(python3 - <<PY
import os, time
print(int(time.time() - os.path.getmtime('$LOCK_FILE')))
PY
)"
    if [ "$lock_age" -lt 14400 ]; then
      echo "Another run is in progress. Exiting." >&2
      exit 1
    fi
    rm -f "$LOCK_FILE"
  fi
  : > "$LOCK_FILE"
else
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "Another run is in progress. Exiting." >&2
    exit 1
  fi
fi

echo "Run v4 start: ${RUN_ID}"

seed_file="$(python3 - <<PY
import json, os
from pathlib import Path

cfg_path = Path(os.environ.get('CANNARADAR_CRAWLER_CONFIG', 'crawler_config.json'))
try:
    cfg = json.loads(cfg_path.read_text())
except Exception:
    cfg = {}

candidates = [
    os.environ.get('CANNARADAR_SEED_FILE', ''),
    cfg.get('seedFile', ''),
    'seeds.csv',
]

resolved = []
for raw in candidates:
    if not raw:
        continue
    p = Path(raw)
    if not p.is_absolute():
        p = (cfg_path.parent / p).resolve()
    if str(p) not in resolved:
        resolved.append(str(p))
    if p.exists():
        print(str(p))
        raise SystemExit

if not resolved:
    print('')
else:
    print(resolved[-1])
PY
)"
if [ -z "$seed_file" ] || [ ! -f "$seed_file" ]; then
  echo "No valid seed file resolved for crawler. Checked from config: $CRAWLER_CONFIG" >&2
  exit 1
fi

./crawler_v2.py --mode "$MODE" --config "$CRAWLER_CONFIG"
raw_count="$(python3 - <<PY
import csv
from pathlib import Path
path = Path('$OUT_DIR/raw_leads.csv')
if not path.exists():
    print(0)
    raise SystemExit
rows = list(csv.DictReader(path.open()))
print(len(rows))
PY
)"

if [ "$raw_count" -eq 0 ]; then
  echo "Empty crawl output. Aborting." >&2
  exit 1
fi

./enrich.py
python3 - <<PY
import pathlib, csv
path = pathlib.Path('$OUT_DIR/enriched_leads.csv')
if not path.exists():
    raise SystemExit(0)
print(f"enriched_rows={sum(1 for _ in csv.DictReader(path.open()))}")
PY

./postprocess_v4.py
./jobs/export_changes.py --run-id "$RUN_ID"
./brief.py

python3 - <<PY
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import subprocess

base = Path('$BASE')
out = base / 'out'

def read_csv(path):
    if not path.exists():
        return []
    with path.open(newline='') as f:
        return list(csv.DictReader(f))

def sha(path):
    if not path.exists():
        return None
    return hashlib.sha1(path.read_bytes()).hexdigest()

run_payload = {
    'run_id': '$RUN_ID',
    'started_at_utc': datetime.now(timezone.utc).isoformat(),
    'crawl_mode': '$MODE',
    'crawler_config': '$CRAWLER_CONFIG',
    'seed_file': '$seed_file',
    'git': {
        'branch': subprocess.getoutput('git rev-parse --abbrev-ref HEAD'),
        'head': subprocess.getoutput('git rev-parse HEAD'),
        'dirty': bool(subprocess.getoutput('git status --porcelain')),
    },
    'counts': {
        'raw': len(read_csv(out / 'raw_leads.csv')),
        'enriched': len(read_csv(out / 'enriched_leads.csv')),
        'outreach': len(read_csv(out / 'outreach_dispensary_100.csv')),
        'excluded': len(read_csv(out / 'excluded_non_dispensary.csv')),
        'segmented': len(read_csv(out / 'v4_all_segmented.csv')),
    },
    'config_sha': sha(Path('$CRAWLER_CONFIG')),
}
manifest = base / 'data' / 'state' / 'last_run_manifest.json'
manifest.write_text(json.dumps(run_payload, indent=2))
print(json.dumps(run_payload, indent=2))
PY

python3 - <<PY
import csv
from pathlib import Path

out = Path('$OUT_DIR')
path = out / 'outreach_dispensary_100.csv'
with path.open(newline='') as f:
    rows = list(csv.DictReader(f))
bad = [r for r in rows if (r.get('segment') or '').strip().lower() != 'dispensary']
if bad:
    raise SystemExit(f'Segment guardrail failed: {len(bad)} rows not marked dispensary in outreach')
print(f'Segment guardrail passed: {len(rows)} dispensary rows')
PY
