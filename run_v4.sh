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
CRAWL_MODE="${CANNARADAR_CRAWL_MODE:-balanced}"
CRAWL_DISCOVERY_LIMIT="${CANNARADAR_DISCOVERY_LIMIT:-}"
CRAWL_MONITOR_LIMIT="${CANNARADAR_MONITOR_LIMIT:-}"
CRAWL_STALE_DAYS="${CANNARADAR_MONITOR_STALE_DAYS:-}"
CRAWL_GROWTH_MAX_PAGES="${CANNARADAR_GROWTH_MAX_PAGES_PER_DOMAIN:-}"
CRAWL_GROWTH_MAX_TOTAL="${CANNARADAR_GROWTH_MAX_TOTAL_PAGES:-}"
CRAWL_GROWTH_MAX_DEPTH="${CANNARADAR_GROWTH_MAX_DEPTH:-}"
CRAWL_MONITOR_MAX_PAGES="${CANNARADAR_MONITOR_MAX_PAGES_PER_DOMAIN:-}"
CRAWL_MONITOR_MAX_TOTAL="${CANNARADAR_MONITOR_MAX_TOTAL_PAGES:-}"
CRAWL_MONITOR_MAX_DEPTH="${CANNARADAR_MONITOR_MAX_DEPTH:-}"
CRAWL_SEED_FAILURE_STREAK_LIMIT="${CANNARADAR_SEED_FAILURE_STREAK_LIMIT:-}"
CRAWL_SEED_BACKOFF_HOURS="${CANNARADAR_SEED_BACKOFF_HOURS:-}"
CRAWL_WEEKLY_NEW_LEAD_TARGET="${CANNARADAR_WEEKLY_NEW_LEAD_TARGET:-}"
CRAWL_GROWTH_WINDOW_DAYS="${CANNARADAR_GROWTH_WINDOW_DAYS:-}"
CRAWL_GOVERNOR_SWITCH="${CANNARADAR_ENFORCE_GROWTH_GOVERNOR:-}"
CRAWL_REQUIRE_FETCH_SUCCESS_GATE="${CANNARADAR_REQUIRE_FETCH_SUCCESS_GATE:-}"

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
  crawl_args=(--max "$CANNARADAR_MAX_SEEDS")
else
  crawl_args=()
fi
crawl_args+=(--crawl-mode "$CRAWL_MODE")
if [ -n "$CRAWL_DISCOVERY_LIMIT" ]; then
  crawl_args+=(--discovery-limit "$CRAWL_DISCOVERY_LIMIT")
fi
if [ -n "$CRAWL_MONITOR_LIMIT" ]; then
  crawl_args+=(--monitor-limit "$CRAWL_MONITOR_LIMIT")
fi
if [ -n "$CRAWL_STALE_DAYS" ]; then
  crawl_args+=(--stale-days "$CRAWL_STALE_DAYS")
fi
if [ -n "$CRAWL_GROWTH_MAX_PAGES" ]; then
  crawl_args+=(--growth-max-pages "$CRAWL_GROWTH_MAX_PAGES")
fi
if [ -n "$CRAWL_GROWTH_MAX_TOTAL" ]; then
  crawl_args+=(--growth-max-total "$CRAWL_GROWTH_MAX_TOTAL")
fi
if [ -n "$CRAWL_GROWTH_MAX_DEPTH" ]; then
  crawl_args+=(--growth-max-depth "$CRAWL_GROWTH_MAX_DEPTH")
fi
if [ -n "$CRAWL_MONITOR_MAX_PAGES" ]; then
  crawl_args+=(--monitor-max-pages "$CRAWL_MONITOR_MAX_PAGES")
fi
if [ -n "$CRAWL_MONITOR_MAX_TOTAL" ]; then
  crawl_args+=(--monitor-max-total "$CRAWL_MONITOR_MAX_TOTAL")
fi
if [ -n "$CRAWL_MONITOR_MAX_DEPTH" ]; then
  crawl_args+=(--monitor-max-depth "$CRAWL_MONITOR_MAX_DEPTH")
fi
if [ -n "$CRAWL_SEED_FAILURE_STREAK_LIMIT" ]; then
  export CANNARADAR_SEED_FAILURE_STREAK_LIMIT="$CRAWL_SEED_FAILURE_STREAK_LIMIT"
fi
if [ -n "$CRAWL_SEED_BACKOFF_HOURS" ]; then
  export CANNARADAR_SEED_BACKOFF_HOURS="$CRAWL_SEED_BACKOFF_HOURS"
fi
if [ -n "$CRAWL_WEEKLY_NEW_LEAD_TARGET" ]; then
  export CANNARADAR_WEEKLY_NEW_LEAD_TARGET="$CRAWL_WEEKLY_NEW_LEAD_TARGET"
fi
if [ -n "$CRAWL_GROWTH_WINDOW_DAYS" ]; then
  export CANNARADAR_GROWTH_WINDOW_DAYS="$CRAWL_GROWTH_WINDOW_DAYS"
fi
if [ -n "$CRAWL_GOVERNOR_SWITCH" ]; then
  export CANNARADAR_ENFORCE_GROWTH_GOVERNOR="$CRAWL_GOVERNOR_SWITCH"
fi
if [ -n "$CRAWL_REQUIRE_FETCH_SUCCESS_GATE" ]; then
  export CANNARADAR_REQUIRE_FETCH_SUCCESS_GATE="$CRAWL_REQUIRE_FETCH_SUCCESS_GATE"
fi

python3 cannaradar_cli.py crawl:run --seeds "$seed_file" "${crawl_args[@]}" --export-tier A --export-limit 200
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

def latest(prefix):
    candidates = sorted(out.glob(f'{prefix}*.csv'))
    return candidates[-1] if candidates else None

def merge_governor(source, destination):
    governor = source.get("growth_governor") or {}
    destination["growth_governor"] = governor
    for key in ["governor"]:
        if key in source and key not in destination:
            destination[key] = source[key]

run_payload = {
    'run_id': '$RUN_ID',
    'pipeline_mode': '$CRAWL_MODE',
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
        'new_leads': len(read_csv(latest('new_leads_since_run_'))),
        'watchlist': len(read_csv(latest('buying_signal_watchlist_'))),
    },
    'config_sha': sha(Path('$CRAWLER_CONFIG')),
}

pipeline_manifest = {}
manifest = base / 'data' / 'state' / 'last_run_manifest.json'
if manifest.exists():
    try:
        pipeline_manifest = json.loads(manifest.read_text())
    except Exception:
        pipeline_manifest = {}
    if isinstance(pipeline_manifest, dict):
        merge_governor(pipeline_manifest, run_payload)

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
