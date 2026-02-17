# Leads Engine V4 (Local, Zero-Budget)

Purpose: continuously generate dispensary-focused outreach leads every few hours, while excluding non-dispensary entities from the active outreach list.

## Pipeline

1. `crawler_v2.py` — discovers + crawls seed domains
2. `enrich.py` — pulls extra contact/owner signals from common pages
3. `postprocess_v4.py` — segments rows (`dispensary` vs non-dispensary), adds `segment_confidence` and `segment_reason`
4. `brief.py` — creates `out/morning_brief.txt`
5. `jobs/export_changes.py` — computes run-to-run diffs and writes `out/changes_<run-id>.csv`
6. `run_v4.sh` — writes `data/state/last_run_manifest.json` and enforces run locks

Run all:

```bash
cd /Users/horcrux/Development/CannaRadar
./run_v4.sh
```

## Key Outputs

- `out/outreach_dispensary_100.csv` ← primary outreach file
- `out/excluded_non_dispensary.csv` ← excluded rows (brands/distributors/unknown)
- `out/v4_all_segmented.csv` ← full segmented data
- `out/v4_quality_report.txt` ← quality summary
- `out/morning_brief.txt` ← daily brief
- `out/changes_<run-id>.csv` + `.txt` ← run diff outputs
- `data/state/last_run_manifest.json` ← run telemetry snapshot
- `data/state/last_change_metrics.json` ← run-to-run change metrics

## Notes

- This is intentionally broad capture + strict segmentation.
- `outreach_dispensary_100.csv` should be used for outbound.
- Segmenting uses `postprocess_segment_rules.json` and emits `segment_confidence` + `segment_reason`.
- Owner fields are evidence-assisted and still require periodic spot checks.

## Scheduling

Current cron should point to `run_v4.sh` every 4h.

Optional runtime env:

- `CANNARADAR_CRAWL_MODE=incremental` for lighter interval runs
- `CANNARADAR_RUN_CANONICAL_INGEST=1` to run canonical ingest via `run_v1_features.sh`
- `CANNARADAR_CRAWLER_CONFIG=...` for alternate crawler config
