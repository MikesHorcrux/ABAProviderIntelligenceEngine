# Leads Engine V4 (Local, Zero-Budget)

Purpose: continuously generate dispensary-focused outreach leads every few hours, while excluding non-dispensary entities from the active outreach list.

## Pipeline

1. `crawler_v2.py` — discovers + crawls seed domains
2. `enrich.py` — pulls extra contact/owner signals from common pages
3. `postprocess_v4.py` — segments rows (`dispensary` vs non-dispensary), cleans owner noise, exports outreach list
4. `brief.py` — creates `morning_brief.txt`

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

## Notes

- This is intentionally broad capture + strict segmentation.
- `outreach_dispensary_100.csv` should be used for outbound.
- Owner fields are evidence-assisted and still require periodic spot checks.

## Scheduling

Current cron should point to `run_v4.sh` every 4h.
