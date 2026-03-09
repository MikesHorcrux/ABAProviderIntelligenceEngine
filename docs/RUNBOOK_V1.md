# Provider Intel Runbook

## Purpose

Use this runbook to install, initialize, run, validate, and recover the New Jersey provider-intelligence pipeline.

## Install

From repo root:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python3.11 -m pip install -r requirements.txt
python3.11 -m playwright install chromium
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
```

Expected outcome:

- `doctor` returns `ok: true`
- `data/provider_intel_v1.db` exists
- `crawler_config.json` exists
- `fetch_policies.json` exists
- `data/state/agent_runs/` exists

## Core Commands

Initialize and validate:

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
```

Run a bounded sync:

```bash
python3.11 provider_intel_cli.py sync --json --max 25 --limit 100
```

Inspect runtime state:

```bash
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py search --json --preset review-queue
```

Re-export current approved records:

```bash
python3.11 provider_intel_cli.py export --json --limit 100
```

Resume an interrupted run:

```bash
python3.11 provider_intel_cli.py sync --json --resume latest
```

## Recommended Operating Sequence

1. Run `init`.
2. Run `doctor`.
3. Start with a bounded sync, not the full seed inventory.
4. Inspect `status`, `review-queue`, and `outreach-ready`.
5. Open the generated provider profiles and evidence bundles.
6. Only then scale the seed pack or page limits.

## Example Live Test

Use the reusable example seed pack in [seed_packs/examples/cassia_live_test.json](/Users/horcrux/Development/CannaRadar/seed_packs/examples/cassia_live_test.json):

```bash
python3.11 provider_intel_cli.py sync --json --seeds seed_packs/examples/cassia_live_test.json --max 2 --limit 10
```

This is the fastest way to validate end-to-end crawling, extraction, QA, export, and outreach outputs against live pages.

## Diagnostics

Preset searches:

```bash
python3.11 provider_intel_cli.py search --json --preset failed-domains
python3.11 provider_intel_cli.py search --json --preset blocked-domains
python3.11 provider_intel_cli.py search --json --preset low-confidence-records
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py search --json --preset review-queue
python3.11 provider_intel_cli.py search --json --preset contradictions
```

Direct SQL:

```bash
python3.11 provider_intel_cli.py sql --json --query "SELECT provider_name_snapshot, record_confidence, outreach_fit_score, export_status FROM provider_practice_records ORDER BY updated_at DESC LIMIT 20"
```

## Expected Outputs

Primary outputs:

- `out/provider_intel/provider_records_<run_id>.csv`
- `out/provider_intel/provider_records_<run_id>.json`
- `out/provider_intel/sales_report_<run_id>.csv`
- `out/provider_intel/review_queue_<run_id>.csv`
- `out/provider_intel/profiles/<record_id>/profile.md`
- `out/provider_intel/profiles/<record_id>/profile.pdf`
- `out/provider_intel/outreach/<record_id>/sales_brief.md`
- `out/provider_intel/outreach/<record_id>/sales_brief.pdf`
- `out/provider_intel/evidence/<record_id>.json`

State and recovery outputs:

- `data/state/last_run_manifest.json`
- `data/state/agent_runs/run_<id>.json`
- `data/state/agent_runs/control_<id>.json`

## Pre-Run Checks

- Python `3.11` is active
- dependencies installed from `requirements.txt`
- Playwright Chromium installed
- `doctor --json` returns `ok: true`
- chosen seed pack exists and is readable
- `reference/prescriber_rules/nj.json` exists
- `data/` and `out/provider_intel/` are writable

## Post-Run Checks

- approved exports exist when evidence thresholds are met
- `sales_report_<run_id>.csv` exists when at least one approved record is outreach-ready
- `review_queue_<run_id>.csv` exists when there are unresolved or low-confidence records
- `status --json` counts align with run summary
- evidence bundles exist for exported records

## Recovery

If a run is interrupted:

```bash
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py sync --json --resume latest
```

If a domain is noisy:

```bash
python3.11 provider_intel_cli.py control --json --run-id latest suppress-prefix --domain example.com --prefix /blog/ --reason low_value_path
python3.11 provider_intel_cli.py control --json --run-id latest cap-domain --domain example.com --max-pages 2 --reason bounded_retry
python3.11 provider_intel_cli.py control --json --run-id latest stop-domain --domain example.com --reason verification_noise
```

If schema/bootstrap drift appears:

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
```

## Current Cautions

The runtime is working, but live crawl quality still depends on domain controls and extraction tuning. Pay particular attention to:

- directory domains that create noisy side-page discovery
- public profile pages with malformed phone extraction
- weak evidence snippets from CSS or title text
- false-positive clinician names from generic text fragments
