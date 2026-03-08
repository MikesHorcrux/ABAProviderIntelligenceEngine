# CannaRadar Agent Ops Playbook

This playbook is the canonical agent-facing contract for running CannaRadar reliably.

## Command Flow

Recommended automation loop:

```bash
python3.11 cannaradar_cli.py init --json
python3.11 cannaradar_cli.py doctor --json
python3.11 cannaradar_cli.py sync --json --crawl-mode growth --max 50
python3.11 cannaradar_cli.py status --json
python3.11 cannaradar_cli.py export --json --kind all
```

Recovery loop after interruption:

```bash
python3.11 cannaradar_cli.py status --json
python3.11 cannaradar_cli.py sync --json --resume latest
```

## Canonical Commands

- `init`: create config, fetch policy, DB schema, output/state dirs.
- `doctor`: preflight checks for config, runtime, writable paths, DB schema, Crawlee, Playwright.
- `sync`: checkpointed batch crawl with resumable stage boundaries.
- `tail`: repeated `sync` loop for monitoring workflows.
- `status`: latest manifest, checkpoint, DB summary, recent failures.
- `control`: bounded runtime interventions for active or resumable runs.
- `search`: local query surface with text search and curated presets.
- `sql`: read-only `SELECT` / `WITH` access to local SQLite state.
- `export`: outreach/research/new/signal/quality outputs.
- `sync` includes a post-score `research` stage that writes lead research summaries and `out/agent_research_queue.csv`.

Legacy compatibility commands remain available:

- `crawl:run`
- `enrich:run`
- `score:run`
- `export:outreach`
- `export:research`
- `export:new`
- `export:signals`
- `quality:report`
- `schema:check`

## JSON Contract

All canonical commands support `--json` and emit:

```json
{
  "schema_version": "cli.v1",
  "command": "doctor",
  "ok": true,
  "message": "doctor completed",
  "data": {}
}
```

All canonical commands also support `--plain` for line-oriented operator output, but `--json` is the stable automation contract.

Errors use:

```json
{
  "schema_version": "cli.v1",
  "command": "sql",
  "ok": false,
  "error": {
    "code": "data_validation_error",
    "message": "SQL command must start with SELECT or WITH.",
    "details": {}
  }
}
```

Schema docs live under:

- `docs/schemas/cli/v1/envelope.json`
- `docs/schemas/cli/v1/init.json`
- `docs/schemas/cli/v1/doctor.json`
- `docs/schemas/cli/v1/sync.json`
- `docs/schemas/cli/v1/tail.json`
- `docs/schemas/cli/v1/status.json`
- `docs/schemas/cli/v1/control.json`
- `docs/schemas/cli/v1/search.json`
- `docs/schemas/cli/v1/sql.json`
- `docs/schemas/cli/v1/export.json`

## Resume Model

`sync` checkpoints stage boundaries in `data/state/agent_runs/` by default.

Stages:

1. `discovery`
2. `fetch`
3. `enrich`
4. `score`
5. `research`
6. `export`

Resume semantics:

- completed stages are skipped
- the current interrupted stage is rerun
- fetch/enrich/score/export are treated as idempotent stage boundaries
- the checkpoint file records the recovery pointer and latest stage details

## Curated Query Shortcuts

`search --preset failed-domains`
- seeds with zero success pages or non-completed latest runs

`search --preset blocked-domains`
- domains ending with `401/403/429/503`

`search --preset stale-records`
- locations missing recent crawls

`search --preset low-confidence-leads`
- lower-scored local leads for triage

`search --preset research-needed`
- leads that still have open agent-research gaps

## Runtime Control Shortcuts

Inspect live controls:

```bash
python3.11 cannaradar_cli.py control --json --run-id latest show
```

Quarantine a bad seed for the active run:

```bash
python3.11 cannaradar_cli.py control --json --run-id latest quarantine-seed --domain bad.example --reason "malformed seed"
```

Suppress a noisy path prefix while the run is active:

```bash
python3.11 cannaradar_cli.py control --json --run-id latest suppress-prefix --domain noisy.example --prefix /blog/ --reason "404 churn"
```

Lower a domain page cap mid-run:

```bash
python3.11 cannaradar_cli.py control --json --run-id latest cap-domain --domain noisy.example --max-pages 2 --reason "agent throttle"
```

## Exit Codes

- `0`: success
- `2`: usage error
- `10`: config error
- `11`: auth error
- `12`: network error
- `13`: data validation error
- `14`: storage error
- `15`: resume state error
- `16`: runtime error
- `17`: command failed

## Triage Shortcuts

Inspect latest state:

```bash
python3.11 cannaradar_cli.py status --json
```

Inspect failed seeds:

```bash
python3.11 cannaradar_cli.py search --json --preset failed-domains
```

Inspect blocked seeds:

```bash
python3.11 cannaradar_cli.py search --json --preset blocked-domains
```

Query local state directly:

```bash
python3.11 cannaradar_cli.py sql --json --query "SELECT seed_domain, last_status_code FROM seed_telemetry ORDER BY updated_at DESC LIMIT 20"
```

Inspect agent research output only:

```bash
python3.11 cannaradar_cli.py export --json --kind agent-research
```
