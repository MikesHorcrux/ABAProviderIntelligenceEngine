# CannaRadar v1.5 Runbook

## Purpose

This runbook is the operator source of truth for running, validating, and recovering the v1.5 pipeline.

## Prerequisites

- Python 3.10+
- SQLite write access under `data/`
- `crawler_config.json` present and valid JSON
- Seed source available (`--seeds` or environment override)

## Standard environment variables

- `CANNARADAR_CRAWLER_CONFIG`: path to crawler config.
- `CANNARADAR_SEED_FILE`: alternate seed file.
- `CANNARADAR_DENYLIST`: comma-separated domains to skip.
- `CANNARADAR_MAX_SEEDS`: optional run-time seed cap.
- `CANNARADAR_RUN_CANONICAL_INGEST`: set `1` to run migration/bootstrap before crawl in `run_v4.sh`.

## Run modes

### Full run (recommended)

```bash
./run_v4.sh
```

What it performs:

- lock acquisition
- optional canonical ingest if `CANNARADAR_RUN_CANONICAL_INGEST=1`
- crawl discovery/fetch/parse/enrich/score/export
- changelog diff generation
- run manifest write
- segment guardrail check

### Stage-only runs

```bash
python3 cannaradar_cli.py crawl:run --seeds seeds.csv
python3 cannaradar_cli.py enrich:run --since "2026-02-17T00:00:00"
python3 cannaradar_cli.py score:run
python3 cannaradar_cli.py export:outreach --tier A --limit 200
python3 cannaradar_cli.py export:research --limit 200
python3 cannaradar_cli.py quality:report
```

### Maintenance runs

```bash
PYTHONPATH=$PWD python3 jobs/ingest_sources.py
python3 jobs/export_changes.py --run-id "$(date +%Y%m%d-%H%M%S)"
python3 jobs/log_outreach_event.py --website curaleaf.com --channel email --outcome replied --notes "Left voicemail"
```

## Expected file outputs

- `out/outreach_ready_<YYYYMMDD-HHMMSS>.csv`
- `out/outreach_dispensary_100.csv`
- `out/excluded_non_dispensary.csv`
- `out/merge_suggestions_<YYYYMMDD-HHMMSS>.csv`
- `out/research_queue.csv`
- `out/v4_quality_report.txt`
- `out/quality_report.json`
- `out/changes_<YYYYMMDD-HHMMSS>.csv`
- `out/changes_<YYYYMMDD-HHMMSS>.txt`
- `data/state/last_run_manifest.json`
- `data/state/last_change_metrics.json`

## Pre-flight checks

- Ensure seed file exists and has `name,website,state,market` header.
- Verify `CANNARADAR_DENYLIST` does not include required domains.
- Confirm robots access by checking fetch logs for blocked URLs.

## Post-run checks

- Confirm `out/outreach_dispensary_100.csv` exists and has header `segment`.
- Confirm all rows in this file are `segment == dispensary`.
- Confirm `out/research_queue.csv` is not empty when discovery footprint is expected.
- Confirm `out/v4_quality_report.txt` and `out/quality_report.json` are generated.
- Confirm manifest and metrics files exist and include run identifiers.

## Schema checks and rollout safety

`jobs/ingest_sources.py` enforces:

- correct DB user version (`user_version`)
- exact required tables and columns
- required index set
- `schema_migrations` metadata check for current version
- checksum match for migration integrity

If bootstrap fails:

1. Stop all scheduled runs.
2. Backup current DB:
   `cp data/cannaradar_v1.db data/cannaradar_v1.db.<timestamp>.bak`
3. Restore previous known-good DB.
4. Re-run `PYTHONPATH=$PWD python3 jobs/ingest_sources.py`.
5. Re-run the target pipeline command.

## Recovery playbook

### No crawl output

- Re-run with smaller max page settings.
- Check seed URLs are live and not denied.
- Confirm DNS/SSL/network availability.

### Segment purity alert

- Inspect segment rule inputs in `pipeline/stages/export.py`.
- Verify extraction is correctly mapping `website` and `canonical_name`.
- Re-run `python3 cannaradar_cli.py export:outreach --tier A --limit 200` after fixes.

### Empty score output

- Ensure crawl generated successful results (`status_code == 200`) and evidence rows exist.
- Confirm `lead_scores` rows are being written during `score:run`.

### Merge-suggestion noise spike

- Inspect `out/merge_suggestions_<timestamp>.csv`.
- Spot-check `reason`, `canonical_location_pk`, and `candidate_location_pk`.
- Tighten normalization inputs before rerunning.

## AI agent / automation checklist

For deterministic changes:

- touch one stage at a time
- keep behavior scoped to that stage
- update relevant tests under `tests/`
- record rationale in commit message
- mention rollback steps in runbook if behavior changes externally visible

## Acceptance check (manual)

- Dispensary export is populated.
- Segment guardrail is passing.
- Diff artifacts are generated with normalized keys.
- Quality report includes non-empty freshness buckets and menu provider list.
- Latest manifest references executed seed and config paths.
