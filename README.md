# ABAProviderIntelligenceEngine

Last verified against commit `0c5e92b`.

Evidence-backed provider intelligence for New Jersey autism and ADHD diagnostic pathways. The runtime crawls live provider sources, extracts explicit diagnostic and licensing signals, scores confidence, queues uncertain records for review, and exports profiles plus outreach-ready sales briefs. A tenant-scoped local agent control plane can orchestrate the same deterministic runtime for isolated client or operator workspaces.

## Release Status

This repository is source-available public code under the
[`ABAProviderIntelligenceEngine Public Source License 1.0`](LICENSE).
Everyone except the excluded entity named in the license is free to use,
modify, and redistribute it under that license. Third-party dependencies keep
their own licenses; see [`NOTICE.md`](NOTICE.md).

Additional public-release notes:

- `tests/fixtures/provider_intel/` contains synthetic fixtures, not copied site captures.
- Generated crawl data, SQLite DBs, and export artifacts are intentionally kept out of git.
- Proxy credentials and other operator secrets are not part of the public repo.
- Contributor expectations are documented in [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Purpose

ABAProviderIntelligenceEngine exists to turn noisy public provider websites,
licensing pages, and directory pages into an evidence-backed local dataset that
an operator can trust.

The intended outcome is not "more leads at any cost." The intended outcome is:

- provider-practice records with source-backed ASD / ADHD diagnostic signals
- explicit licensing and prescribing evidence where available
- a review queue for uncertain or contradictory records
- exportable profiles and outreach briefs only after QA passes

In practice, this means the repository is built for research loops where an
agent or operator can crawl, extract, score, inspect, resume, and export
without weakening the evidence gate.

## Primary Use Cases

- Build a New Jersey provider intelligence dataset for ASD and ADHD diagnostic pathways.
- Run bounded live crawls against known seed packs and resume interrupted runs.
- Separate approved provider truth from practice-only signals and uncertain records.
- Generate review queues for manual follow-up instead of forcing weak records into export.
- Produce outreach-ready exports only for records that clear factual QA and outreach thresholds.
- Give an AI agent a repeatable local workflow for end-to-end research operations.

## Who It Is For

- Developers extending the crawler, extractors, or export pipeline.
- Operators running bounded live crawls, resuming failed jobs, and diagnosing noisy domains.
- Operators or client teams who need isolated per-workspace runtime roots via `--tenant`.
- Non-technical stakeholders who need vetted provider intelligence instead of raw lead lists.

## What It Gives You

- A canonical SQLite-backed record set in `data/provider_intel_v1.db` by default, or `storage/tenants/<tenant_id>/data/provider_intel_v1.db` when `--tenant` is used.
- Evidence-linked provider exports under `out/provider_intel/` by default, or tenant-scoped outputs under `storage/tenants/<tenant_id>/out/provider_intel/`.
- Review-queue outputs for records that should not ship as truth yet.
- Sales-facing briefs for records that pass both factual QA and outreach readiness.
- A separate tenant-scoped agent memory store at `storage/tenants/<tenant_id>/memory/agent_memory_v1.db` when the `agent` command surface is used.

The runtime is evidence-first. If a critical claim is not source-backed, QA blocks export in `pipeline/stages/qa.py`.

## 5-Minute Quickstart

```bash
git clone https://github.com/MikesHorcrux/ABAProviderIntelligenceEngine.git
cd ABAProviderIntelligenceEngine
python3.11 -m venv .venv
source .venv/bin/activate
python3.11 -m pip install -r requirements.txt
python3.11 -m playwright install chromium
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 10 --limit 25
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py --json --tenant demo agent status
```

If you want a shorter human-facing wrapper from the repo root, use `./ae`. It
forwards canonical commands unchanged and adds ergonomic agent aliases such as
`./ae run --tenant demo "Run a bounded provider-intel loop"`.
Add `--trace` if you want live observable agent activity on stderr while the
session runs.

What to expect:

- `init` creates `crawler_config.json`, `fetch_policies.json`, `data/provider_intel_v1.db`, and `data/state/agent_runs/` by default.
- `init` with `--tenant <id>` creates an isolated runtime under `storage/tenants/<id>/`.
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
python3.11 provider_intel_cli.py --json --tenant acme agent run --goal "Find NJ providers worth outbound this week"
python3.11 provider_intel_cli.py --json --tenant acme agent status
./ae init --json
./ae run --json --tenant acme "Find NJ providers worth outbound this week"
./ae session-status --json --tenant acme
```

## Full Agentic Research Loop

If you want an AI agent to operate this repository end to end, give it the repo
root, the seed scope, the run limits, and the output you expect. The agent
should use the canonical CLI or the tenant-scoped `agent` command surface and
keep the evidence-first safety rules intact. The agent layer orchestrates the
existing deterministic runtime; it is not a separate truth-writing pipeline
stage.

Minimum handoff:

- Workspace: the repository root with Python 3.11 and Playwright installed
- Scope: which geography, conditions, and seed pack to run
- Bounds: `--max`, `--limit`, and whether the run should start fresh or resume
- Goal: review queue triage, outreach-ready export generation, blocked-domain diagnosis, or a bounded validation run
- Required outputs: status summary, control summary, review queue findings, export artifacts, or code changes
- Constraints: never invent unsupported clinical or licensing claims; keep fixtures synthetic; do not commit generated data or secrets

Recommended command loop for agents:

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 10 --limit 25
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py search --json --preset review-queue
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py control --json --run-id latest show
python3.11 provider_intel_cli.py sync --json --resume latest
python3.11 provider_intel_cli.py export --json --limit 100
```

How the loop should be used:

- Start bounded, not broad. Small runs make blocked domains, bad seeds, and noisy extraction easier to diagnose.
- Use `--crawl-mode refresh` when you want the same stage order with smaller fetch budgets from the `monitor*` config settings.
- Treat `review_queue` as a normal output lane, not a failure.
- Use `control --run-id latest show` to inspect the current run before changing config or code.
- Resume with `sync --resume latest` when a run was interrupted or when you want to continue from checkpoints.
- Export only after QA has approved records; outreach readiness is downstream of truth.

Example agent handoff prompt:

```text
Use ABAProviderIntelligenceEngine from the repo root. Run the canonical CLI for a bounded live research loop with seed_packs/examples/cassia_live_test.json, max 2, limit 10. Start with init and doctor, then run sync, status, review-queue search, outreach-ready search, and control show for the latest run. If the run is interrupted, resume latest instead of starting over. Summarize blocked domains, uncertain records, approved exports, and the next operator action. Do not weaken QA, do not fabricate unsupported ASD/ADHD or licensing claims, and do not commit generated data.
```

## Primary Outputs

Runs write to `out/provider_intel/`:

When `--tenant <id>` is used, the same export tree is created under
`storage/tenants/<id>/out/provider_intel/`.

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

## For AI Agents

Start with [`AGENTS.md`](AGENTS.md) and [`README_AI_AGENTS.md`](README_AI_AGENTS.md).
Agents must preserve the evidence-first runtime behavior and must not describe
this repository inaccurately. The correct public description is source-available
public code with an excluded-entity restriction.

## Fast Validation Pack

Use `seed_packs/examples/cassia_live_test.json` for a bounded live test:

```bash
python3.11 provider_intel_cli.py sync --json --seeds seed_packs/examples/cassia_live_test.json --max 2 --limit 10
```

## Compatibility Note

Use `provider_intel_cli.py` or the repo-local `./ae` wrapper. Older retired
entrypoints are not part of the supported public interface.
