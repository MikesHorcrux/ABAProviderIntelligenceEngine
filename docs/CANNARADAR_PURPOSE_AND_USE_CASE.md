# CannaRadar: Purpose, Use Case, and Strategic Value

## Executive Summary

CannaRadar is a local-first, evidence-backed lead intelligence system built to support founder-led outbound in cannabis.

Its core job is simple:
- continuously discover and refresh dispensary opportunities,
- keep outreach lists segment-pure (dispensary-only for this motion),
- and produce actionable daily outputs for sales execution.

CannaRadar V1 is designed to be practical and operational now, not perfect later.

---

## Why CannaRadar Exists

Most generic lead tools do not fit cannabis GTM realities:

1. Segments get mixed
   - Dispensaries, brands, distributors, and manufacturers are blended together.
   - This causes bad targeting and lower conversion.

2. Data provenance is weak
   - You cannot reliably trace where a contact claim came from.
   - Outreach confidence drops when evidence is unclear.

3. Lists go stale quickly
   - Cannabis operators change sites, contacts, and structures often.
   - One-time list pulls decay fast.

4. Founder time is scarce
   - Manual sourcing and cleanup burn critical build/sales time.

CannaRadar solves this by making lead generation a repeatable system instead of a one-off spreadsheet project.

---

## Project Purpose

The purpose of CannaRadar is to create a repeatable pipeline that transforms raw web/source signals into daily, outreach-ready dispensary leads.

At a high level, it must:
- ingest candidate entities from source adapters,
- crawl and enrich contact/owner signals,
- normalize and score entities,
- enforce segment cleanliness,
- and output a ranked outreach list with quality and change reporting.

---

## Primary Use Case (V1)

### User
Founder/operator running outbound (Mike + team).

### Goal
Generate a consistent stream of dispensary leads that are usable today for outreach execution.

### Workflow
1. Run pipeline on schedule.
2. Review dispensary outreach export.
3. Use quality and diff reports to focus effort.
4. Log outreach outcomes back into system.
5. Repeat with refreshed data.

### Success Criteria
- Reliable daily lead output.
- Clean dispensary-only targeting.
- Clear signal on what changed since last run.
- Tight loop between outreach outcomes and list quality.

---

## What V1 Delivers

V1 focuses on operational completeness:

1. Canonical data model and ingest path
   - organizations, licenses, locations, contact points, evidence, outreach events.

2. Multi-stage pipeline
   - crawl -> enrich -> postprocess -> exports.

3. Segment enforcement
   - dispensary-only outreach output with non-dispensary exclusion file.

4. Canonical integration into exports
   - canonical records are included in downstream output flow.

5. Dedupe/resolution baseline
   - domain/name/state-based dedupe for cleaner records.

6. Verification event ingestion
   - outreach outcomes logged back into canonical model.

7. Change/diff reporting
   - run-over-run change visibility.

8. Runbook and smoke tests
   - repeatable operations and baseline health checks.

---

## Outputs and How They’re Used

### `out/outreach_dispensary_100.csv`
Primary execution list.
- Used directly for outbound tasking.
- Ranked and filtered for dispensary targeting.

### `out/excluded_non_dispensary.csv`
Guardrail artifact.
- Shows what was excluded.
- Validates segment purity.

### `out/v4_quality_report.txt`
Quality snapshot.
- Tracks contact/owner signal quality.
- Flags practical data health.

### `out/changes_*.csv` + `out/changes_*.txt`
Change intelligence.
- Tells the team what is new/removed/modified.
- Prevents reprocessing unchanged rows unnecessarily.

### `out/morning_brief.txt`
Quick operational summary.
- Supports fast review cadence.

---

## Why the Business Needs This

CannaRadar gives the company leverage in three areas:

1. Throughput
   - More leads generated with less manual effort.

2. Precision
   - Better target quality via segmentation and evidence.

3. Learning speed
   - Feedback loop from outreach outcomes into data quality.

This is foundational for scaling outbound without scaling chaos.

---

## What “Good” Looks Like Operationally

A good CannaRadar day means:
- pipeline runs unattended,
- export is clean and usable,
- team can immediately act on top leads,
- outcome events are captured,
- and tomorrow’s run is better informed.

---

## Risks / Constraints (Current)

1. Source quality limits output quality.
2. Some sites block or degrade scraping.
3. Owner extraction remains noisy in edge cases.
4. Rule-based segmentation can require iterative tuning.

These are manageable and expected in V1.

---

## Strategic Positioning

CannaRadar is not just a crawler.
It is the lead intelligence operating layer for cannabis outbound.

In V1, it establishes the execution backbone.
In later versions, it can expand into stronger intent scoring, personalization, and outcome-driven optimization.

---

## Bottom Line

CannaRadar exists to make dispensary outbound:
- repeatable,
- segment-clean,
- evidence-backed,
- and fast enough for founder-paced execution.

That is why we need it, and that is what V1 is built to deliver.
