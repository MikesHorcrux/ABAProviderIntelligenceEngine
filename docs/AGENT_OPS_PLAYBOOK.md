# Provider Intel Agent Ops Playbook

This is the canonical operator contract for running the provider-intelligence pipeline.

## Command Flow

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --max 50 --limit 100
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py export --json --limit 100
```

Resume flow:

```bash
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py sync --json --resume latest
```

## Stages

1. `seed_ingest`
2. `crawl`
3. `extract`
4. `resolve`
5. `score`
6. `qa`
7. `export`

## Query Presets

- `failed-domains`
- `blocked-domains`
- `low-confidence-records`
- `outreach-ready`
- `review-queue`
- `contradictions`

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
- Unknown or unclear is acceptable; unsupported certainty is not.
- Official or first-party evidence should win over secondary directories.
- Low-confidence or contradictory records go to `review_queue`.
- Outbound sales briefs should be generated only from `outreach_ready=1` approved records.
- Bounded live interventions are preferred over restarting long crawls.
