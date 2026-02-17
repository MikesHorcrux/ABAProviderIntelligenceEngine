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

if [ "${CANNARADAR_RUN_CANONICAL_INGEST:-0}" = "1" ]; then
  PYTHONPATH="$BASE" python3 jobs/ingest_sources.py
fi

seed_file="$(python3 - <<PY
import json, os
from pathlib import Path

cfg_path = Path(os.environ.get('CANNARADAR_CRAWLER_CONFIG', '$CRAWLER_CONFIG'))
try:
    cfg = json.loads(Path(cfg_path).read_text())
except Exception:
    cfg = {}

for candidate in [
    os.environ.get('CANNARADAR_SEED_FILE', ''),
    cfg.get('seedFile', ''),
    'seeds.csv',
]:
    if not candidate:
        continue
    p = Path(candidate)
    if not p.is_absolute():
        p = (cfg_path.parent / p).resolve()
    if p.exists():
        print(str(p))
        raise SystemExit

print('')
PY
)"

if [ -z "$seed_file" ] || [ ! -f "$seed_file" ]; then
  echo "Seed file not found: ${seed_file}" >&2
  exit 1
fi

if [ -n "${CANNARADAR_MAX_SEEDS:-}" ]; then
  python3 cannaradar_cli.py crawl:run --seeds "$seed_file" --max "$CANNARADAR_MAX_SEEDS" --export-tier A --export-limit 200
else
  python3 cannaradar_cli.py crawl:run --seeds "$seed_file" --export-tier A --export-limit 200
fi
python3 jobs/export_changes.py --run-id "$RUN_ID"

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
    'pipeline_mode': '$MODE',
    'crawler_config': '$CRAWLER_CONFIG',
    'seed_file': '$seed_file',
    'started_at_utc': datetime.now(timezone.utc).isoformat(),
    'git': {
        'branch': subprocess.getoutput('git rev-parse --abbrev-ref HEAD'),
        'head': subprocess.getoutput('git rev-parse HEAD'),
    },
    'counts': {
        'raw': len(read_csv(out / 'outreach_dispensary_100.csv')),
        'excluded': len(read_csv(out / 'excluded_non_dispensary.csv')),
        'research': len(read_csv(out / 'research_queue.csv')),
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
path = Path('$OUT_DIR') / 'outreach_dispensary_100.csv'
rows = list(csv.DictReader(path.open(newline='')))
bad = [r for r in rows if (r.get('segment') or '').lower() != 'dispensary' and r.get('segment') is not None]
if bad:
  raise SystemExit(f'Segment guardrail failed: {len(bad)} rows not marked as dispensary')
print(f'Segment guardrail passed for {len(rows)} rows')
PY
