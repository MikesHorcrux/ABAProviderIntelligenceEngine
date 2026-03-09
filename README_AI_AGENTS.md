# AI Agent Reference: Provider Intelligence

This repository contains the active provider-intelligence runtime. Agents should treat it as an evidence-first crawler and export system for New Jersey ASD/ADHD provider intelligence.

## What The Runtime Does

- crawls seed sources and discovered provider pages
- extracts diagnostic and operational signals
- resolves provider-practice records
- scores factual confidence and outreach fit separately
- runs QA and contradiction handling
- exports provider profiles, evidence bundles, review queue rows, and sales briefs

## Canonical Commands

- `python3.11 provider_intel_cli.py init --json`
- `python3.11 provider_intel_cli.py doctor --json`
- `python3.11 provider_intel_cli.py sync --json --max 50 --limit 100`
- `python3.11 provider_intel_cli.py status --json`
- `python3.11 provider_intel_cli.py search --json --preset outreach-ready`
- `python3.11 provider_intel_cli.py export --json --limit 100`
- `python3.11 provider_intel_cli.py control --json --run-id latest show`

## Read First

1. [README.md](/Users/horcrux/Development/CannaRadar/README.md)
2. [docs/RUNBOOK_V1.md](/Users/horcrux/Development/CannaRadar/docs/RUNBOOK_V1.md)
3. [docs/AGENT_OPS_PLAYBOOK.md](/Users/horcrux/Development/CannaRadar/docs/AGENT_OPS_PLAYBOOK.md)
4. [SKILL.md](/Users/horcrux/Development/CannaRadar/SKILL.md)

## Agent Rules

- Never claim ASD/ADHD diagnosis, license status, or prescribing capability without evidence.
- Prefer bounded live runs before broad seed-pack runs.
- Use `review_queue` as a normal safety output, not a failure.
- Keep `record_confidence` and `outreach_fit_score` conceptually separate.
- If live output is noisy, tighten crawl controls or extraction rules before scaling.

## Fast Live Validation

Use the reusable example pack:

- `python3.11 provider_intel_cli.py sync --json --seeds seed_packs/examples/cassia_live_test.json --max 2 --limit 10`
