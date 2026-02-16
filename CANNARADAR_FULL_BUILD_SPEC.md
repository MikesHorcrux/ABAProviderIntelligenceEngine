# CannaRadar Full Build Spec (V1 Execution Blueprint)

Last updated: 2026-02-16
Owner: Mike + Luna
Status: Active build

---

## 0) What CannaRadar is

CannaRadar is a local, always-on dispensary intelligence system.

It is **not** just a crawler.

It is a pipeline that:
1. Ingests source records (starting from reliable business/license-like records)
2. Normalizes entities (org/location/contact)
3. Crawls and enriches first-party sites politely
4. Separates dispensary leads from non-dispensary supply-chain entities
5. Scores and exports actionable outreach files
6. Tracks evidence + confidence so data is usable and improvable over time

Primary goal: produce consistent, high-quality dispensary lead outputs for outreach without paid APIs.

---

## 1) Why we are building this

### Business reason
You need a repeatable pipeline of qualified dispensary leads for your inventory/sales offer. Manual hunting is too slow and inconsistent.

### Technical reason
A single script can produce rows, but not trustworthy operations data. We need identity + provenance + segmentation + confidence + refresh loops.

### Strategic reason
If this system compounds over time (with verification from outreach), it becomes a proprietary GTM asset.

---

## 2) Scope for V1

V1 is feature-complete when these are done and wired:

1) Source adapter framework (plugin model)
2) Canonical schema for org/location/license/contact/evidence
3) Crawl + enrich pipeline
4) Hard segmentation gate (dispensary only for outreach)
5) Dedupe/entity resolution baseline
6) Confidence + fit scoring baseline
7) Exports (primary outreach + excluded + quality report + brief)
8) Scheduling/automation
9) Basic verification loop table + ingestion path
10) Runbook docs

V1 is **not** “perfect data.”
V1 is a robust, unattended, segment-clean system.

---

## 3) Current project files and what they do

### Core pipeline files
- `crawler_v2.py`
  - Crawls seed domains with queue/depth/limits
  - Extracts base contact/role hints
  - Writes intermediate lead outputs

- `enrich.py`
  - Hits extra likely pages (`/about`, `/team`, `/contact`, etc.)
  - Improves contact/owner signals
  - Writes enriched output

- `postprocess_v4.py`
  - Applies segment rules
  - Cleans owner noise
  - Splits outreach vs excluded outputs
  - Generates quality report data

- `brief.py`
  - Builds short brief from latest output

### Runners
- `run_v4.sh`
  - Current active pipeline runner
  - Executes crawl -> enrich -> segment/postprocess -> brief

- `run_v1_features.sh`
  - Includes canonical ingest step + v4 pipeline

### Canonical/modeling work
- `db/schema.sql`
  - Canonical tables: organizations, licenses, locations, contact_points, evidence, outreach_events

- `jobs/ingest_sources.py`
  - Ingests source rows into canonical DB model

### Adapter framework (in progress)
- `adapters/base.py`
  - Standard adapter interface
- `adapters/seeds_adapter.py`
  - Seed CSV adapter implementation

### Outputs
- `out/outreach_dispensary_100.csv` (primary)
- `out/excluded_non_dispensary.csv`
- `out/v4_quality_report.txt`
- `out/morning_brief.txt`
- `out/v4_all_segmented.csv`

---

## 4) Architecture (V1)

### Stage A: Ingestion
Input sources (initial):
- Seed CSV and other free/public data sources (adapter-based)

Output:
- Canonical entities in `data/cannaradar_v1.db`

### Stage B: Crawl
- Domain-level polite crawl
- In-domain links with depth and cap
- Retry + timeout + crawl delay

Output:
- raw lead/contact signals

### Stage C: Enrichment
- Targeted page pass for contact and buyer-role clues

Output:
- enriched signals with source URL/snippet

### Stage D: Segmentation + Quality
- Hard classify segment
- Only dispensary rows in outreach export
- Non-dispensary routed to excluded file

Output:
- clean outreach feed + diagnostics

### Stage E: Delivery
- Morning brief
- Scheduled cron runs
- Optional update notifications

---

## 5) Data model (V1 canonical)

### organizations
Represents parent business identity.

Key fields:
- org_pk
- legal_name
- dba_name
- state

### licenses
Represents license-like record and status source.

Key fields:
- license_pk
- org_pk
- state
- license_id
- license_type
- status
- source_url
- retrieved_at
- fingerprint

### locations
Represents storefront-level node.

Key fields:
- location_pk
- org_pk
- canonical_name
- address/city/state/zip
- website_domain
- phone
- fit_score
- last_crawled_at

### contact_points
All outreach channels with confidence.

Key fields:
- contact_pk
- location_pk
- type (email/phone/form/website)
- value
- confidence
- source_url
- first_seen_at / last_seen_at

### evidence
Provenance for extracted fields.

Key fields:
- evidence_pk
- entity_type/entity_pk
- field_name/field_value
- source_url
- snippet
- captured_at

### outreach_events
Verification loop input.

Key fields:
- event_pk
- location_pk
- channel
- outcome
- notes
- created_at

---

## 6) Segment policy (non-negotiable)

CannaRadar must not mix supply-chain segments in outreach.

Rules:
- `segment=dispensary` -> allowed in `outreach_dispensary_100.csv`
- non-dispensary/unknown -> routed to `excluded_non_dispensary.csv`

This policy prevents distributor/brand contamination in storefront outreach.

---

## 7) Scoring and confidence

### Fit score (lead priority)
Based on available proxy signals:
- storefront relevance
- contact completeness
- crawl signal quality
- segment match

### Confidence score (field reliability)
Based on evidence source and extraction quality:
- official site page > inferred patterns
- verified outreach outcomes should increase confidence
- stale/unverified data should decay over time (phase extension)

---

## 8) Automation and operations

Current jobs:
- `leads:crawl-4h` (active)
- `leads:morning-brief` (active)
- overnight updates can be enabled/disabled as needed

Operational mode:
- unattended scheduled runs
- outputs refreshed every cycle
- user checks exports/briefs when needed

---

## 9) Remaining V1 build items (exact)

1) Complete adapter plugin scaffolding for multi-source ingestion
2) Wire canonical DB into downstream scoring/export flow (not just side ingest)
3) Improve dedupe/entity resolution baseline
4) Add verification event ingestion utility
   - e.g. `jobs/log_outreach_event.py`
   - outcomes: bounced, replied, confirmed, no-fit
5) Add change/diff report utility
   - e.g. `jobs/export_changes.py`
6) Finalize runbook docs
   - setup, run, debug, recovery, known limitations
7) Add smoke tests
   - DB init, ingest sanity, export schema checks

---

## 10) What to run from fresh machine

Assumptions:
- Python 3 installed
- project at `~/.openclaw/workspace/leads_engine`

### Basic run
```bash
cd ~/.openclaw/workspace/leads_engine
./run_v4.sh
```

### Canonical ingest + pipeline
```bash
cd ~/.openclaw/workspace/leads_engine
./run_v1_features.sh
```

### Check outputs
- `out/outreach_dispensary_100.csv`
- `out/v4_quality_report.txt`
- `out/morning_brief.txt`

---

## 11) Quality gates for V1 acceptance

V1 should be considered complete when:

1) Pipeline runs unattended on schedule
2) Segment split is consistently enforced
3) Canonical ingest + downstream exports are integrated
4) Verification event path exists and updates data
5) Change report exists
6) Runbook and smoke checks exist

---

## 12) Known constraints (current)

- No paid enrichment APIs
- Some sites block/limit crawling
- Owner extraction quality remains the noisiest field
- Coverage growth depends on source expansion quality

---

## 13) Post-V1 direction

V2 ideas are stored in:
- `CANNARADAR_V2_IDEAS.md`

V2 focuses on intent signals, personalization, freshness/decay, outcome learning, and quality dashboards.

---

## 14) If Luna is unavailable during updates

Use this order:
1) inspect schema in `db/schema.sql`
2) run `run_v1_features.sh`
3) verify outputs in `out/`
4) inspect cron jobs with `openclaw cron list`
5) resume from remaining V1 checklist section above

This document is the full continuity blueprint.
