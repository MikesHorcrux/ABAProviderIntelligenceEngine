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
- Change report: `python3 jobs/export_changes.py`
- Log verification event:
  `python3 jobs/log_outreach_event.py --website curaleaf.com --channel email --outcome replied --notes "left voicemail"`

## Key outputs
- `out/outreach_dispensary_100.csv`
- `out/excluded_non_dispensary.csv`
- `out/v4_quality_report.txt`
- `out/changes_*.csv` and `out/changes_*.txt`

## Troubleshooting
1. Missing exports
   - Ensure upstream input exists (`out/raw_leads.csv` or `out/enriched_leads.csv`)
   - Re-run `./run_v4.sh`
2. No canonical DB
   - Run `PYTHONPATH=$PWD python3 jobs/ingest_sources.py`
3. Event logging fails to resolve location
   - Provide `--location-pk` directly, or `--name` + `--state`
4. Empty change report
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
