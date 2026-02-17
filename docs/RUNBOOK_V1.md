# CannaRadar V1.5 Runbook

## Purpose
Operational playbook for the v1.5 production pipeline.

## Prereqs
- Python 3.10+
- Repository at `/Users/horcrux/Development/CannaRadar`
- SQLite database writable in `data/`

## Core commands

- Full production run: `./run_v4.sh`
- Canonical ingest only: `PYTHONPATH=$PWD python3 jobs/ingest_sources.py`
- Run pipeline stage only:
  - `python3 cannaradar_cli.py crawl:run --seeds seeds.csv`
  - `python3 cannaradar_cli.py enrich:run --since "2026-02-17T00:00:00"`
  - `python3 cannaradar_cli.py score:run`
  - `python3 cannaradar_cli.py export:outreach --tier A --limit 200`
  - `python3 cannaradar_cli.py export:research --limit 200`
  - `python3 cannaradar_cli.py quality:report`
- Change report: `python3 jobs/export_changes.py --run-id "$(date +%Y%m%d-%H%M%S)"`
- Log outreach outcome:
  `python3 jobs/log_outreach_event.py --website curaleaf.com --channel email --outcome replied --notes "left voicemail"`

## Outputs
- `out/outreach_ready_<YYYYMMDD-HHMMSS>.csv`
- `out/outreach_dispensary_100.csv`
- `out/excluded_non_dispensary.csv`
- `out/research_queue.csv`
- `out/merge_suggestions_<YYYYMMDD-HHMMSS>.csv`
- `out/v4_quality_report.txt`
- `out/changes_<YYYYMMDD-HHMMSS>.csv` and `.txt`
- `out/quality_report.json`
- `data/state/last_run_manifest.json`
- `data/state/last_change_metrics.json`

## Schema migrations

Canonical schema is versioned in:
- SQLite `PRAGMA user_version` (currently `5`)
- `schema_migrations(schema_version, migration_name, schema_checksum, applied_at)`

Validation performed in `jobs/ingest_sources.py`:
- required table existence
- required column existence
- `user_version` exact match
- migration checksum match
- required index presence checks

`jobs/export_changes.py` change report behavior:
- Uses a single normalized timestamp key (`YYYYMMDD-HHMMSS`) for output filenames.
- Tracks version separately as `run_version` in `data/state/last_change_metrics.json`.

Rollback:
1. Stop scheduled writers touching `data/cannaradar_v1.db`.
2. Backup current DB:
   `cp data/cannaradar_v1.db data/cannaradar_v1.db.$(date +%F_%H%M%S).bak`
3. Restore latest known-good backup:
   `cp <known-good>.db data/cannaradar_v1.db`
4. Re-run:
   `PYTHONPATH=$PWD python3 jobs/ingest_sources.py`
5. Re-run:
   `./run_v4.sh`

## Troubleshooting

- **Segment guardrail fails in run log**
  - Usually indicates parsing changes; verify that outreach export contains only dispensary rows in the `segment` column.
- **Schema check failure**
  - Re-run `PYTHONPATH=$PWD python3 jobs/ingest_sources.py` and review migration guidance.
- **No crawl output**
  - Confirm seed file path from config/env.
  - Confirm denylist is not over-restrictive.
- **No enrichment output**
  - Validate crawl produced `outreach` data in `out/outreach_ready_*.csv` and re-run `enrich:run`.

## Recovery sequence
1. Stop writers and take manual DB backup.
2. Re-run ingest and pipeline.
3. Rebuild change metrics by running `jobs/export_changes.py`.
4. Validate smoke checks before re-enabling schedules.
