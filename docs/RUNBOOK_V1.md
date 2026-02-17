# CannaRadar V1 Runbook

## Purpose
Operational guide for setup, run, debug, and recovery of the V1 pipeline.

## Prereqs
- Python 3.10+
- Repo checked out locally

## Core commands
- Full V1 flow: `./run_v1_features.sh`
- Crawl + enrich + postprocess: `./run_v4.sh`
- Canonical ingest only: `PYTHONPATH=$PWD python3 jobs/ingest_sources.py`
- Change report: `python3 jobs/export_changes.py --run-id "$(date +%Y%m%d-%H%M%S)"` (outputs `out/changes_<run-id>.csv`)
- Live run manifest: `data/state/last_run_manifest.json`
- Change metrics: `data/state/last_change_metrics.json`
- Log verification event:
  `python3 jobs/log_outreach_event.py --website curaleaf.com --channel email --outcome replied --notes "left voicemail"`

## Key outputs
- `out/outreach_dispensary_100.csv`
- `out/excluded_non_dispensary.csv`
- `out/v4_quality_report.txt`
- `out/changes_<run-id>.csv` and `out/changes_<run-id>.txt`
- `out/morning_brief.txt`
- `data/state/last_run_manifest.json`
- `data/state/last_change_metrics.json`

## Schema migrations

- Canonical DB schema is versioned with SQLite `user_version` (`PRAGMA user_version`) and migration metadata stored in `schema_migrations`.
- On each ingest bootstrap (`jobs/ingest_sources.py`), the process verifies:
  - expected required tables/columns exist,
  - `schema_version` matches repository schema,
  - `schema_checksum` matches expected schema fingerprint.
- If this check fails, stop pipeline and fix schema drift before ingesting new rows.

Rollback guidance (schema drift):
1. Stop scheduled jobs touching `data/cannaradar_v1.db`.
2. Backup the failing database:
   `cp data/cannaradar_v1.db data/cannaradar_v1.db.$(date +%F_%H%M%S).bak`
3. Restore last known-good backup:
   `cp <known-good>.db data/cannaradar_v1.db`
4. Re-run canonical ingest:
   `PYTHONPATH=$PWD python3 jobs/ingest_sources.py`
5. Re-run `./run_v4.sh` and smoke checks.

## Troubleshooting
1. Missing exports
   - Ensure upstream input exists (`out/raw_leads.csv` or `out/enriched_leads.csv`)
   - Re-run `./run_v4.sh`
2. Schema check fails
   - Run `PYTHONPATH=$PWD python3 jobs/ingest_sources.py` and verify output includes expected schema version/metadata.
   - If mismatch persists, see [Schema migrations](#schema-migrations).
3. No canonical DB
   - Run `PYTHONPATH=$PWD python3 jobs/ingest_sources.py`
4. Event logging fails to resolve location
   - Provide `--location-pk` directly, or `--name` + `--state`
5. Empty change report
   - First run initializes baseline; run export again after a new snapshot

## Recovery
1. Rebuild canonical schema by rerunning ingest.
2. Regenerate V4 outputs.
3. Regenerate change report.
4. Verify smoke tests pass.

## Known V1 limitations
- Owner extraction still noisy on some sites.
- Segment classification is rule-based (not ML).
- Coverage quality depends on seeds quality.
