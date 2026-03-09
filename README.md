# Provider Intelligence Engine

> New Jersey-first provider intelligence for ASD/ADHD diagnosis and prescribing capability.

This repository runs a local-first, agent-operable crawl pipeline that finds and evaluates providers and practices that may assess or diagnose autism and ADHD. The system is built for evidence-backed provider intelligence, not lead generation.

## Current Surface

- Canonical CLI: `provider_intel_cli.py`
- Runtime: Python 3.11
- Persistence: SQLite in `data/provider_intel_v1.db`
- State: checkpoint/control files under `data/state/agent_runs/`
- Outputs: `out/provider_intel/`

## Pipeline

1. `seed_ingest`
2. `crawl`
3. `extract`
4. `resolve`
5. `score`
6. `qa`
7. `export`

The pipeline prefers evidence-backed records over volume. Critical fields must be source-backed or the record is routed to review.

## Canonical Commands

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 25 --limit 100
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py export --json --limit 100
```

Resume a run:

```bash
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py sync --json --resume latest
```

Useful diagnostics:

```bash
python3.11 provider_intel_cli.py search --json --preset failed-domains
python3.11 provider_intel_cli.py search --json --preset blocked-domains
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py search --json --preset review-queue
python3.11 provider_intel_cli.py search --json --preset contradictions
```

## Outputs

- `out/provider_intel/provider_records_<run_id>.csv`
- `out/provider_intel/provider_records_<run_id>.json`
- `out/provider_intel/sales_report_<run_id>.csv`
- `out/provider_intel/review_queue_<run_id>.csv`
- `out/provider_intel/profiles/<record_id>/profile.md`
- `out/provider_intel/profiles/<record_id>/profile.pdf`
- `out/provider_intel/evidence/<record_id>.json`
- `out/provider_intel/outreach/<record_id>/sales_brief.md`
- `out/provider_intel/outreach/<record_id>/sales_brief.pdf`

## Scope

- Pilot geography is New Jersey.
- Canonical export unit is one provider-practice-state affiliation record.
- Browser-capable crawling remains available for JS-heavy sources.
- Low-confidence and practice-only records are queued for review instead of exported as truth.

## Docs

- [Internal docs index](./docs/README.md)
- [Agent ops playbook](./docs/AGENT_OPS_PLAYBOOK.md)
- [Runbook](./docs/RUNBOOK_V1.md)

## Legacy Note

The legacy CLI shim remains only as a redirect to `provider_intel_cli.py`.
