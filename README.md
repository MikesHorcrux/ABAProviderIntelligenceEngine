# ABA Provider Intelligence Engine

Last verified against commit `0c5e92b`.

Evidence-backed provider intelligence for New Jersey autism and ADHD diagnostic pathways. The runtime crawls live provider sources, extracts explicit diagnostic and licensing signals, scores confidence, queues uncertain records for review, and exports profiles plus outreach-ready sales briefs.

## Who It Is For

- Developers extending the crawler, extractors, or export pipeline.
- Operators running bounded live crawls, resuming failed jobs, and diagnosing noisy domains.
- Non-technical stakeholders who need vetted provider intelligence instead of raw lead lists.

## What It Gives You

- A canonical SQLite-backed record set in `data/provider_intel_v1.db`.
- Evidence-linked provider exports under `out/provider_intel/`.
- Review-queue outputs for records that should not ship as truth yet.
- Sales-facing briefs for records that pass both factual QA and outreach readiness.

The runtime is evidence-first. If a critical claim is not source-backed, QA blocks export in `pipeline/stages/qa.py`.

## 5-Minute Quickstart

```bash
git clone https://github.com/MikesHorcrux/CannaRadar.git
cd CannaRadar
python3.11 -m venv .venv
source .venv/bin/activate
python3.11 -m pip install -r requirements.txt
python3.11 -m playwright install chromium
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 10 --limit 25
python3.11 provider_intel_cli.py status --json
```

What to expect:

- `init` creates `crawler_config.json`, `fetch_policies.json`, `data/provider_intel_v1.db`, and `data/state/agent_runs/`.
- `doctor` validates Python, config, writable paths, schema metadata, and Crawlee/Playwright availability.
- `sync` runs `seed_ingest -> crawl -> extract -> resolve -> score -> qa -> export`.
- `status` summarizes run state, DB counts, and the latest export artifacts.

## Key Commands

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 50 --limit 100
python3.11 provider_intel_cli.py sync --json --resume latest
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py search --json --preset review-queue
python3.11 provider_intel_cli.py control --json --run-id latest show
python3.11 provider_intel_cli.py export --json --limit 100
```

## Primary Outputs

Runs write to `out/provider_intel/`:

- `provider_records_<run_id>.csv`
- `provider_records_<run_id>.json`
- `review_queue_<run_id>.csv`
- `sales_report_<run_id>.csv`
- `profiles/<record_id>/profile.md`
- `profiles/<record_id>/profile.pdf`
- `outreach/<record_id>/sales_brief.md`
- `outreach/<record_id>/sales_brief.pdf`
- `evidence/<record_id>.json`

## Current Scope And Boundaries

- Geography: New Jersey only.
- Conditions: ASD and ADHD diagnostic capability.
- Prescribing classification: New Jersey rules in `reference/prescriber_rules/nj.json`.
- Canonical export unit: one provider-practice-state affiliation record.
- PDF generation: currently a minimal fallback PDF writer in `pipeline/stages/export.py`; Playwright is used for crawling, not current PDF rendering.

## Documentation Map

- [`docs/README.md`](docs/README.md): full documentation index by audience.
- [`docs/architecture.md`](docs/architecture.md): system overview, module boundaries, and architecture diagrams.
- [`docs/data-model.md`](docs/data-model.md): entities, tables, schemas, and ER diagrams.
- [`docs/runtime-and-pipeline.md`](docs/runtime-and-pipeline.md): stage-by-stage execution, checkpoints, and failure handling.
- [`docs/cli-reference.md`](docs/cli-reference.md): command and flag reference with examples.
- [`docs/operations.md`](docs/operations.md): setup, monitoring, incident response, and recovery.
- [`docs/security-and-safety.md`](docs/security-and-safety.md): evidence gates, secrets model, and data handling boundaries.
- [`docs/testing-and-quality.md`](docs/testing-and-quality.md): test coverage, gaps, and release checks.
- [`docs/faq.md`](docs/faq.md): short answers for operators, developers, and stakeholders.
- [`docs/adr/`](docs/adr/): architecture decisions and tradeoffs.

## Fast Validation Pack

Use `seed_packs/examples/cassia_live_test.json` for a bounded live test:

```bash
python3.11 provider_intel_cli.py sync --json --seeds seed_packs/examples/cassia_live_test.json --max 2 --limit 10
```

## Legacy Note

`cannaradar_cli.py` is intentionally retired and exits immediately with a redirect to `provider_intel_cli.py`.
