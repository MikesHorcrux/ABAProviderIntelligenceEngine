# Provider Intel Agent Ops Playbook

This is the concise operating contract for agents working in this repository.

## Repo Goal

Produce evidence-backed provider intelligence for New Jersey ASD/ADHD diagnosis and prescribing capability, plus outreach-ready exports for approved records.

## Canonical Flow

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 50 --limit 100
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py export --json --limit 100
```

Resume:

```bash
python3.11 provider_intel_cli.py sync --json --resume latest
```

## Query Presets

- `failed-domains`
- `blocked-domains`
- `low-confidence-records`
- `outreach-ready`
- `review-queue`
- `contradictions`

## Outputs To Inspect

- `provider_records_<run_id>.csv`
- `sales_report_<run_id>.csv`
- `review_queue_<run_id>.csv`
- `profiles/<record_id>/profile.md`
- `outreach/<record_id>/sales_brief.md`
- `evidence/<record_id>.json`

## Runtime Controls

```bash
python3.11 provider_intel_cli.py control --json --run-id latest show
python3.11 provider_intel_cli.py control --json --run-id latest quarantine-seed --domain bad.example --reason malformed_seed
python3.11 provider_intel_cli.py control --json --run-id latest suppress-prefix --domain noisy.example --prefix /blog/ --reason low_value_path
python3.11 provider_intel_cli.py control --json --run-id latest cap-domain --domain noisy.example --max-pages 2 --reason bounded_retry
python3.11 provider_intel_cli.py control --json --run-id latest stop-domain --domain noisy.example --reason verification_noise
```

## Operating Rules

- Critical fields must be source-backed before export.
- Unknown and unclear are acceptable values.
- Prefer official or first-party evidence over directory evidence.
- Do not weaken QA just to increase export volume.
- `record_confidence` is a truth-quality score, not a sales-priority score.
- `outreach_fit_score` ranks approved records for outbound use.
- Only records with `outreach_ready=1` should feed sales briefs or cold outreach.
- Low-confidence, contradictory, or unmatched-license records belong in `review_queue`.

## Live-Run Heuristics

- Start with bounded runs.
- Use the example live pack in [seed_packs/examples/cassia_live_test.json](/Users/horcrux/Development/CannaRadar/seed_packs/examples/cassia_live_test.json) for fast end-to-end validation.
- If a seed starts generating 404 storms or irrelevant taxonomy pages, treat it as URL-filter debt.
- If a public profile source is useful for verification but noisy for discovery, constrain it with controls instead of removing it outright.
- Check `status`, `outreach-ready`, and `review-queue` after every bounded run.

## Validation

- `PYTHONPATH=$PWD python3.11 tests/test_agent_cli.py`
- `PYTHONPATH=$PWD python3.11 tests/test_run_state.py`
- `PYTHONPATH=$PWD python3.11 tests/test_fetch_config.py`
- `PYTHONPATH=$PWD python3.11 tests/test_lead_research.py`
- `PYTHONPATH=$PWD python3.11 tests/test_fetch_dispatch.py`
- `PYTHONPATH=$PWD python3.11 tests/test_parse_stage.py`
- `PYTHONPATH=$PWD python3.11 tests/test_resolve_stage.py`
