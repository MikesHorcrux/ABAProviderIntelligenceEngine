# Provider Intel Runbook

## Purpose

Run, validate, and recover the provider-intelligence pipeline for the New Jersey ASD/ADHD pilot.

## Prerequisites

- Python 3.11+
- `pip install -r requirements.txt`
- `playwright install chromium`
- SQLite write access under `data/`
- `crawler_config.json`
- `seed_packs/nj/seed_pack.json`
- `reference/prescriber_rules/nj.json`

## Canonical Workflow

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 50 --limit 100
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py export --json --limit 100
```

## Resume Workflow

```bash
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py sync --json --resume latest
```

## Diagnostics

```bash
python3.11 provider_intel_cli.py search --json --preset failed-domains
python3.11 provider_intel_cli.py search --json --preset blocked-domains
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py search --json --preset review-queue
python3.11 provider_intel_cli.py search --json --preset contradictions
python3.11 provider_intel_cli.py sql --json --query "SELECT seed_domain, last_status_code FROM seed_telemetry ORDER BY updated_at DESC LIMIT 20"
```

## Expected Outputs

- `out/provider_intel/provider_records_<run_id>.csv`
- `out/provider_intel/provider_records_<run_id>.json`
- `out/provider_intel/sales_report_<run_id>.csv`
- `out/provider_intel/review_queue_<run_id>.csv`
- `out/provider_intel/profiles/`
- `out/provider_intel/evidence/`
- `out/provider_intel/outreach/`
- `data/state/last_run_manifest.json`
- `data/state/agent_runs/run_<id>.json`
- `data/state/agent_runs/control_<id>.json`

## Pre-Run Checks

- `doctor --json` returns `ok: true`
- seed pack exists and is readable
- prescriber rules exist and are readable
- output and state directories are writable
- browser dependencies are installed for JS-heavy sources

## Post-Run Checks

- approved exports exist when confidence/evidence thresholds are met
- sales report exists when at least one approved record is outreach-ready
- review queue exists when records are uncertain or missing clinician/license proof
- checkpoint summary matches crawl/extract/resolve totals
- blocked domains and contradiction counts are visible in `status`

## Recovery

- Resume interrupted runs with `--resume latest`
- Use `control` commands to quarantine or stop noisy domains instead of restarting
- Re-run `init` if schema checksum or migration metadata drifts
- Prefer bounded reruns against a reduced seed subset when validating extraction changes
