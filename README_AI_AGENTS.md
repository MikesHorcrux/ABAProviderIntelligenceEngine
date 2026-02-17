# AI Agent Reference: CannaRadar v1.5

This document describes how an AI agent should modify and reason about the system.

## Scope

Use this when proposing code changes.

Focus areas:

- Parser and extraction behavior (`pipeline/stages/parse.py`)
- Fetch behavior and politeness (`pipeline/stages/fetch.py`, `pipeline/config.py`)
- Resolution logic (`pipeline/stages/resolve.py`)
- Scoring model and features (`pipeline/stages/score.py`)
- Enrichment steps (`pipeline/stages/enrich.py`)
- Export contracts (`pipeline/stages/export.py`, `jobs/export_changes.py`)

## Safe edit principles

- One stage per patch whenever possible.
- Keep SQL migrations additive if possible.
- Preserve existing output column contracts unless a migration is included.
- Add tests for changed behavior in `tests/`.
- Preserve soft-delete patterns and `deleted_at=''` semantics unless schema-wide change is intentional.

## Required reasoning before edits

- Identify the contract that changes (input schema, output schema, score semantics, crawl policy).
- Confirm whether any output file contract changes (`out/*` column names or run-id format).
- Verify how the change affects manifest/change-report schema and `run_v4.sh`.

## Stage-specific editing notes

### Fetching and crawling

- Do not modify robot or denylist behavior without documenting legal scope.
- If adding concurrency, validate DB write path for lock contention.
- Keep per-domain delay and cache semantics explicit in `pipeline/stages/fetch.py`.

### Parsing

- Keep extraction regexes minimal and anchored to avoid false positives.
- Maintain role extraction in a way that does not inject non-business PII.
- Add/extend tests in `tests/test_parse_stage.py` for each regex class changed.

### Resolution

- `resolve_and_upsert_locations` must stay deterministic for identical seeds/pages.
- Merge suggestions should only mark probable collisions and should not auto-merge automatically.
- Keep confidence semantics explicit in `entity_resolutions.reason`.

### Scoring

- Keep score ranges within [0,100].
- Update `FEATURE_KEYS` together with any new feature.
- Ensure new features are inserted into `scoring_features` and covered by quality expectations.

### Exports

- Never weaken segment purity for `outreach_dispensary_100.csv`.
- Keep `outreach_ready_<timestamp>.csv` column contract stable.
- For change-report keys, keep the single timestamp format only: `YYYYMMDD-HHMMSS`.

### Migrations

- Any schema change requires:
  - `db/schema.sql` update
  - `jobs/ingest_sources.py` checks alignment if required columns/indexes changed
  - smoke test expectations update if needed
- Do not weaken migration checks without explicit product approval.

## Testing expectations

When changes touch parsing:

- add/extend `tests/test_parse_stage.py`

When changes touch resolution:

- add/extend `tests/test_resolve_stage.py`

For broader stability:

- run `./run_smoke_tests.sh` after validation in a clean environment.

## How to reason about outcomes

All lead quality comes from evidence:

- parse/parse-derived facts with source URL
- enrichment inferences with low confidence and explicit evidence note
- scoring features with per-feature persistence

If there is no evidence row for a critical claim, classify it as inference and keep confidence low.

## Rollback habits

Before major edits, record in commit message:

- behavior changed
- why changed
- fallback plan if score/segment behavior regresses

Use existing rollback playbook in `docs/RUNBOOK_V1.md` for DB-related incidents.
