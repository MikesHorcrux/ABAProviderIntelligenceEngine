# CannaRadar V4 / V1.5 Pipeline Notes

This stack is now aligned to a production-oriented, evidence-driven lead pipeline:

- Crawl only allowed public pages (robots-aware)
- Preserve source provenance on key fields
- Resolve duplicates deterministically
- Score and rank leads for outreach actionability
- Export clean dispensary-only outreach lists and research queues

## Primary pipeline

1. `python3 cannaradar_cli.py crawl:run --seeds seeds.csv`  
   Runs discovery → fetch → parse/extract → enrichment → scoring → exports.
2. `python3 cannaradar_cli.py enrich:run --since <ISO_TIMESTAMP>`  
   Re-run enrichment only for recent crawl results.
3. `python3 cannaradar_cli.py score:run`  
   Recompute lead scoring and feature vectors.
4. `python3 cannaradar_cli.py export:outreach --tier A --limit 200`
5. `python3 cannaradar_cli.py export:research --limit 200`
6. `python3 cannaradar_cli.py quality:report`
7. `python3 jobs/export_changes.py --run-id <YYYYMMDD-HHMMSS>`
8. `PYTHONPATH=$PWD python3 jobs/ingest_sources.py` for canonical ingest/migrations

## Outputs

- `out/outreach_ready_<YYYYMMDD-HHMMSS>.csv` (new production-ready outreach format)
- `out/outreach_dispensary_100.csv` (legacy compatibility alias)
- `out/excluded_non_dispensary.csv`
- `out/research_queue.csv`
- `out/merge_suggestions_<YYYYMMDD-HHMMSS>.csv`
- `out/v4_quality_report.txt` + `out/quality_report.json`
- `out/changes_<YYYYMMDD-HHMMSS>.csv` + `out/changes_<YYYYMMDD-HHMMSS>.txt`
- `data/state/last_run_manifest.json`
- `data/state/last_change_metrics.json`

## Runbook entrypoint

Use `./run_v4.sh` for lock-safe scheduled execution.  
It will:

- Optionally run canonical ingest (`CANNARADAR_RUN_CANONICAL_INGEST=1`)
- Run a full `crawl:run`
- Write change diff artifacts
- Write run manifest

## Important env vars

- `CANNARADAR_CRAWLER_CONFIG` — path to crawler config
- `CANNARADAR_SEED_FILE` — alternate seed list
- `CANNARADAR_DENYLIST` — comma-separated denylist domains
- `CANNARADAR_MAX_SEEDS` — optional max seeds for one run
- `CANNARADAR_RUN_CANONICAL_INGEST=1` — run bootstrap ingest first

## Segment and scoring behavior

- Segment filtering is rule-based and conservative (dispensary-first).
- Scoring features include:
  - buyer/contact role signals
  - role inbox
  - direct email
  - menu provider detections
  - multi-location signals
  - enterprise/chain risk signals

## Compliance

- No LinkedIn crawling in the current pipeline flow.
- Respect for robots and per-domain minimum intervals is enforced in fetch.
- PII is intentionally constrained to business emails and phone/store contact handles with evidence.

### Change report key format

- `jobs/export_changes.py` outputs are keyed by a single timestamp (`YYYYMMDD-HHMMSS`).
- `--run-id` values are normalized to that timestamp format before generating filenames.
- `run_version` is tracked separately in `data/state/last_change_metrics.json`.

Example:

```bash
python3 jobs/export_changes.py --run-id 2026-02-17-093000
```
