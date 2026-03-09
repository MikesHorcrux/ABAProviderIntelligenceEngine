# Provider Intelligence Engine

Evidence-backed provider intelligence for New Jersey ASD/ADHD diagnostic pathways.

## What This Is

This repository runs a local-first crawl and enrichment pipeline that finds providers and practices, verifies whether they appear to diagnose autism and/or ADHD, classifies likely prescribing authority by credential and New Jersey rules, and exports:

- canonical provider records
- evidence bundles
- client-facing provider profiles
- sales-facing outreach briefs for outreach-ready records
- review queue records for anything uncertain

The product goal is provider intelligence, not raw lead generation. If a critical claim is not source-backed, the system should queue the record for review instead of exporting it as fact.

## What It Produces

Runs write outputs under `out/provider_intel/`:

- `provider_records_<run_id>.csv`
- `provider_records_<run_id>.json`
- `sales_report_<run_id>.csv`
- `review_queue_<run_id>.csv`
- `profiles/<record_id>/profile.md`
- `profiles/<record_id>/profile.pdf`
- `outreach/<record_id>/sales_brief.md`
- `outreach/<record_id>/sales_brief.pdf`
- `evidence/<record_id>.json`

State and resumability data live under `data/state/agent_runs/`. The primary SQLite database is `data/provider_intel_v1.db`.

## Install

Requirements:

- macOS or Linux
- Python `3.11`
- `pip`
- Playwright Chromium for JS-heavy sites and PDF rendering

Setup:

```bash
git clone https://github.com/MikesHorcrux/CannaRadar.git
cd CannaRadar
python3.11 -m venv .venv
source .venv/bin/activate
python3.11 -m pip install -r requirements.txt
python3.11 -m playwright install chromium
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
```

If `doctor` returns `ok: true`, the runtime is ready.

## Quick Start

Run the full NJ pilot flow:

```bash
python3.11 provider_intel_cli.py sync --json --max 50 --limit 100
python3.11 provider_intel_cli.py status --json
```

Re-export the latest approved records:

```bash
python3.11 provider_intel_cli.py export --json --limit 100
```

Search the local state:

```bash
python3.11 provider_intel_cli.py search --json "cassia"
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py search --json --preset review-queue
```

Inspect raw SQLite state:

```bash
python3.11 provider_intel_cli.py sql --json --query "SELECT provider_name_snapshot, record_confidence FROM provider_practice_records ORDER BY record_confidence DESC LIMIT 20"
```

Resume an interrupted run:

```bash
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py sync --json --resume latest
```

## Bounded Live Test

Use the reusable two-source live test pack in [seed_packs/examples/cassia_live_test.json](/Users/horcrux/Development/CannaRadar/seed_packs/examples/cassia_live_test.json):

```bash
python3.11 provider_intel_cli.py sync --json --seeds seed_packs/examples/cassia_live_test.json --max 2 --limit 10
```

That pack is meant for validating the end-to-end pipeline against one live NJ provider and one live public-profile verification source without running the whole statewide pack.

## Pipeline

Stage order:

1. `seed_ingest`
2. `crawl`
3. `extract`
4. `resolve`
5. `score`
6. `qa`
7. `export`

Key rules:

- exported critical fields must have evidence
- official or first-party evidence should win over secondary directories
- low-confidence or contradictory records go to `review_queue`
- `record_confidence` proves factual quality
- `outreach_fit_score` ranks approved records for sales use

## Current Scope

- geography: New Jersey
- conditions: autism and ADHD diagnostic capability
- prescribing logic: New Jersey rules only
- canonical export unit: one provider-practice-state affiliation record

## Current Limitations

The engine is real and working end to end, but it still needs crawl hygiene and parser hardening on noisy domains. In live runs today, the main quality issues are:

- noisy side-page discovery on some directory domains
- imperfect phone extraction on some public profile pages
- evidence snippets that sometimes include title or style noise
- review-queue junk records from over-broad provider name matching on certain templates

Use bounded runs and inspect outputs before scaling.

## Docs

- [docs/README.md](/Users/horcrux/Development/CannaRadar/docs/README.md)
- [docs/AGENT_OPS_PLAYBOOK.md](/Users/horcrux/Development/CannaRadar/docs/AGENT_OPS_PLAYBOOK.md)
- [docs/RUNBOOK_V1.md](/Users/horcrux/Development/CannaRadar/docs/RUNBOOK_V1.md)
- [README_AI_AGENTS.md](/Users/horcrux/Development/CannaRadar/README_AI_AGENTS.md)
- [SKILL.md](/Users/horcrux/Development/CannaRadar/SKILL.md)

## Legacy Note

`cannaradar_cli.py` remains only as a redirect shim to `provider_intel_cli.py`.
