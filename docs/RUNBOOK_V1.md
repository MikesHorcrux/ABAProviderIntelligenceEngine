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

### Seed hygiene cleanup

Run before crawl runs when `seeds.csv` has drift/duplicates:

```bash
python3 tools/seed_hygiene.py
```

Outputs:

- `out/seeds_clean.csv` (rows with missing website removed, website/domain normalized, deduped by normalized website)
- `out/seed_hygiene_report.json` (counts and paths)

### Discovery ranking signals

Run after seed hygiene (or directly against `seeds.csv`) to produce a ranked discovery input using simple metadata signals:

```bash
python3 tools/discovery_rank_signals.py
```

Behavior:

- Reads `out/seeds_clean.csv` when present, otherwise `seeds.csv`.
- Scores each row using `has_state`, `has_market`, `known_mso_name_match`, and `website_quality` (HTTPS + non-placeholder domain).

Outputs:

- `out/discovery_ranked.csv` (input rows + `rank_score` + `rank_reasons`, sorted by `rank_score` descending)
- `out/discovery_rank_report.json` (score distribution and summary stats)

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

## Discovery troubleshooting quick-loop (high signal)

Use this loop when discovery quality drops (irrelevant results, duplicates, weak fit).

1) Capture baseline before changes

```bash
python3 cannaradar_cli.py quality:report
ls -1t out/research_queue.csv out/outreach_dispensary_100.csv out/quality_report.json 2>/dev/null
```

2) Validate discovery seed quality

```bash
head -n 20 discoveries.csv
python3 - <<'PY'
import csv
from collections import Counter
rows=list(csv.DictReader(open('discoveries.csv', newline='', encoding='utf-8')))
print('rows',len(rows))
print('missing_website',sum(1 for r in rows if not (r.get('website') or '').strip()))
print('duplicate_websites',len(rows)-len({(r.get('website') or '').strip().lower() for r in rows if (r.get('website') or '').strip()}))
print('states_top5',Counter((r.get('state') or '').strip() for r in rows).most_common(5))
PY
```

3) Run constrained discovery test

```bash
CANNARADAR_MAX_SEEDS=50 ./run_v4.sh
```

4) Check for noisy outputs

```bash
python3 - <<'PY'
import csv
from collections import Counter
rows=list(csv.DictReader(open('out/research_queue.csv', newline='', encoding='utf-8')))
print('research_rows',len(rows))
print('top_domains',Counter((r.get('website') or '').split('/')[0] for r in rows).most_common(10))
PY
```

5) If noise is high, tighten inputs (in order)

- Remove weak/empty seeds from `discoveries.csv`.
- Add known low-signal domains to `CANNARADAR_DENYLIST`.
- Reduce discovery breadth with `CANNARADAR_MAX_SEEDS` for validation runs.
- Re-run and compare `out/quality_report.json` before/after.

6) Rollback if quality regresses

```bash
cp data/cannaradar_v1.db data/cannaradar_v1.db.rollback.$(date +%Y%m%d-%H%M%S)
# restore previous known-good backup if needed
```

Success criteria for discovery fix runs:

- Fewer irrelevant entries in `out/research_queue.csv`
- Stable/clean dispensary list in `out/outreach_dispensary_100.csv`
- No spike in merge noise (`out/merge_suggestions_<timestamp>.csv`)
