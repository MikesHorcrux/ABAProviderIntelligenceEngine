# CLI Reference

Last verified against commit `0c5e92b`.

The canonical CLI entrypoint is `provider_intel_cli.py`. The retired `cannaradar_cli.py` only exits with a redirect message.

## Global Flags

| Flag | Meaning | Notes |
| --- | --- | --- |
| `--db` | Alternate SQLite path | Defaults to `data/provider_intel_v1.db` |
| `--db-timeout-ms` | Declared SQLite timeout | Accepted by `cli/app.py`, currently not consumed by `pipeline/db.py` |
| `--config` | Alternate `crawler_config.json` | Also sets `PROVIDER_INTEL_CONFIG` and `CANNARADAR_CRAWLER_CONFIG` for the process |
| `--json` | Emit strict JSON envelope | Uses schema `provider_intel.cli.v1` |
| `--plain` | Emit plain-text output | Default |

## Command Reference

### `init`

Create config, fetch-policy file, DB schema, and state directories.

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py init --json --db /tmp/provider_intel.db --config /tmp/crawler_config.json
```

Flags:

- `--checkpoint-dir`

### `doctor`

Run environment and schema diagnostics.

```bash
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py doctor --json --config ./crawler_config.json
```

Checks performed in `cli/doctor.py`:

- Python version
- Config path and JSON load
- Seed pack presence
- Prescriber rule pack presence
- Writable DB/output/state directories
- DB open and schema validation
- Crawlee import
- Playwright import
- Disk space

Flags:

- `--checkpoint-dir`

### `sync`

Run the full pipeline with checkpointing.

```bash
python3.11 provider_intel_cli.py sync --json --max 10 --limit 25
python3.11 provider_intel_cli.py sync --json --seeds seed_packs/examples/cassia_live_test.json --max 2 --limit 10
python3.11 provider_intel_cli.py sync --json --resume latest
```

Flags:

| Flag | Meaning | Actual behavior |
| --- | --- | --- |
| `--seeds` | Seed pack path | Active |
| `--max` | Seed limit | Active |
| `--crawl-mode` | `full` or `refresh` | Stored in checkpoint metadata; current pipeline does not branch on it |
| `--limit` | Export limit | Active, passed to export |
| `--crawlee-headless` | `on` or `off` | Stored in sync options; current fetch layer still reads headless mode from config/env |
| `--run-id` | Explicit run id | Active |
| `--resume` | Resume a checkpoint by id or `latest` | Active |
| `--checkpoint-dir` | Alternate checkpoint directory | Active |

### `tail`

Loop `sync` on an interval for continuous operation.

```bash
python3.11 provider_intel_cli.py tail --json --interval-seconds 600 --iterations 3 --max 5 --limit 25
```

Additional flags:

- `--interval-seconds`
- `--iterations`

### `status`

Summarize current DB counts, last manifest, checkpoint state, run control state, and output snapshots.

```bash
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py status --json --run-id 20260309-202814+0000
```

Flags:

- `--run-id`
- `--checkpoint-dir`

### `search`

Search provider records by name/practice, or run built-in diagnostic presets.

```bash
python3.11 provider_intel_cli.py search --json "cassia"
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py search --json --preset contradictions --limit 50
```

Presets from `cli/query.py`:

- `failed-domains`
- `blocked-domains`
- `low-confidence-records`
- `review-queue`
- `contradictions`
- `outreach-ready`

Flags:

- positional `query`
- `--preset`
- `--limit`

### `control`

Inspect or apply bounded runtime controls for a run.

Show current state:

```bash
python3.11 provider_intel_cli.py control --json --run-id latest show
```

Apply controls:

```bash
python3.11 provider_intel_cli.py control --json --run-id latest quarantine-seed --domain noisy.example --reason blocked_seed
python3.11 provider_intel_cli.py control --json --run-id latest suppress-prefix --domain noisy.example --prefix /blog/ --reason low_value_path
python3.11 provider_intel_cli.py control --json --run-id latest cap-domain --domain noisy.example --max-pages 2 --reason bounded_retry
python3.11 provider_intel_cli.py control --json --run-id latest stop-domain --domain noisy.example --reason verification_noise
python3.11 provider_intel_cli.py control --json --run-id latest clear-domain --domain noisy.example --reason reset
```

Supported control actions:

- `show`
- `quarantine-seed`
- `suppress-prefix`
- `cap-domain`
- `stop-domain`
- `clear-domain`

### `sql`

Execute read-only SQL against the SQLite DB.

```bash
python3.11 provider_intel_cli.py sql --json --query "SELECT provider_name_snapshot, record_confidence FROM provider_practice_records ORDER BY record_confidence DESC LIMIT 20"
```

Rules from `cli/query.py`:

- Query must start with `SELECT` or `WITH`
- Write verbs like `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER` are rejected
- Final result is wrapped in an outer `LIMIT`

Flags:

- positional `query`
- `--query`
- `--limit`

### `export`

Re-export currently approved records without re-running crawl/extract.

```bash
python3.11 provider_intel_cli.py export --json --limit 100
```

Flags:

- `--limit`

## JSON Envelope

All `--json` responses use the envelope in `cli/output.py`:

Successful command:

```json
{
  "schema_version": "provider_intel.cli.v1",
  "command": "status",
  "ok": true,
  "message": "status completed",
  "data": {}
}
```

Error response:

```json
{
  "schema_version": "provider_intel.cli.v1",
  "command": "sql",
  "ok": false,
  "error": {
    "code": "data_validation_error",
    "message": "SQL command must start with SELECT or WITH.",
    "details": {}
  }
}
```

## Practical Recipes

### First bounded run

```bash
python3.11 provider_intel_cli.py init --json
python3.11 provider_intel_cli.py doctor --json
python3.11 provider_intel_cli.py sync --json --seeds seed_packs/examples/cassia_live_test.json --max 2 --limit 10
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py search --json --preset review-queue
```

### Resume after a failure

```bash
python3.11 provider_intel_cli.py status --json
python3.11 provider_intel_cli.py sync --json --resume latest
```

### Inspect export-ready records

```bash
python3.11 provider_intel_cli.py search --json --preset outreach-ready
python3.11 provider_intel_cli.py sql --json --query "SELECT provider_name_snapshot, practice_name_snapshot, outreach_fit_score FROM provider_practice_records WHERE outreach_ready=1 ORDER BY outreach_fit_score DESC"
```

### Investigate noisy domains

```bash
python3.11 provider_intel_cli.py search --json --preset failed-domains
python3.11 provider_intel_cli.py search --json --preset blocked-domains
python3.11 provider_intel_cli.py control --json --run-id latest show
```

## Troubleshooting By Command

| Command | Symptom | What to check |
| --- | --- | --- |
| `init` | Config rewritten unexpectedly | Existing config did not look like provider-intel config; `cli/doctor.py` rewrites outdated configs |
| `doctor` | `db_schema` fails | Re-run `init`; check `db/schema.sql` checksum metadata |
| `sync` | `0` exported records | Inspect `review-queue`, `contradictions`, and critical evidence availability |
| `sync` | Browser-heavy domains still fail | Check `fetch_policies.json`, Playwright install, and blocked-domain preset |
| `status` | Missing output snapshot | Export may have produced no approved records yet |
| `search` | Empty preset results | The DB may genuinely contain no rows for that condition |
| `sql` | Rejected query | Ensure it starts with `SELECT` or `WITH` and contains no write verb |
| `export` | Empty sales report | No approved records currently have `outreach_ready=1` |

## Exit Codes

Exit codes come from `cli/errors.py`:

- `0` success
- `2` usage error
- `10` config error
- `11` auth error
- `12` network error
- `13` data validation error
- `14` storage error
- `15` resume state error
- `16` runtime error
- `17` command failed
