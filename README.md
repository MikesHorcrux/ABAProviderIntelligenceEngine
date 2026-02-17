# CannaRadar

CannaRadar is a local-first dispensary intelligence system for founder-led outbound.

It is designed to continuously build and refresh a **dispensary-only** lead pipeline (no distributor/brand mixing), with evidence-backed fields and scheduled runs.

## Why this exists

Most lead tools are generic. Cannabis GTM needs:
- segment purity (dispensary storefronts vs brands/distributors)
- provenance (where each field came from)
- repeatable refreshes (not one-off scraping)
- outreach-ready outputs every day

CannaRadar is built as that engine.

---

## Current capabilities (V1 in progress)

- Crawl and enrich dispensary candidates from seed sources
- Segment rows into dispensary vs non-dispensary
- Export primary outreach list + excluded list
- Generate quality report and morning brief
- Canonical schema and source adapter scaffolding
- Scheduled pipeline execution support

---

## Project structure

- `crawler_v2.py` — queue/depth crawler and base extraction
- `enrich.py` — follow-up extraction on high-value pages
- `postprocess_v4.py` — segmentation, cleanup, scoring adjustments, exports
- `brief.py` — morning brief generation
- `run_v4.sh` — active pipeline runner
- `run_v1_features.sh` — canonical ingest + pipeline

### Canonical model and ingestion
- `db/schema.sql` — organizations, licenses, locations, contact_points, evidence, outreach_events
- `adapters/base.py` — adapter interface
- `adapters/seeds_adapter.py` — seed adapter implementation
- `jobs/ingest_sources.py` — initial canonical ingest job

### Docs
- `CANNARADAR_FULL_BUILD_SPEC.md` — full build blueprint and continuity plan
- `docs/RUNBOOK_V1.md` — setup/run/debug/recovery operations runbook
- `CANNARADAR_V2_IDEAS.md` — post-V1 roadmap
- `README_V4.md` — prior v4 notes

---

## Quick start

```bash
cd CannaRadar
./run_v4.sh
```

To include canonical ingest before pipeline:

```bash
./run_v1_features.sh
```

Run smoke checks:

```bash
./run_smoke_tests.sh
```

---

## Outputs

Generated under `out/`:

- `outreach_dispensary_100.csv` — primary outreach file (dispensary-only)
- `excluded_non_dispensary.csv` — filtered out rows
- `v4_all_segmented.csv` — full segmented output
- `v4_quality_report.txt` — quality metrics snapshot
- `morning_brief.txt` — concise daily brief

---

## Operating principles

1. **Dispensary-only outbound**
   - No mixing with distributors/brands in outreach exports.

2. **Evidence over guesses**
   - Prioritize source URL + extracted context.

3. **Automation over heroics**
   - Build for unattended scheduled runs.

4. **Iteration over perfection**
   - Keep outputs usable while improving architecture and quality.

---

## V1 completion criteria

- Adapter framework expanded for multi-source ingestion
- Canonical model fully integrated into export flow
- Dedupe/resolution baseline hardened
- Verification loop ingestion path active
- Change/diff reporting added
- Runbook + smoke checks complete

---

## Status

CannaRadar is actively under development with a working pipeline and ongoing hardening.

If you are taking over from another session, start with:
- `CANNARADAR_FULL_BUILD_SPEC.md`
- `db/schema.sql`
- `run_v1_features.sh`

Then verify outputs in `out/`.
