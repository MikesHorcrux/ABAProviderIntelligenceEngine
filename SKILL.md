# CannaRadar Repo Skill (Local)

Use this file as the repo-local AI editing playbook.

## Scope

Apply when modifying CannaRadar code in this repository.

Primary focus areas:
- Parser: `pipeline/stages/parse.py`
- Fetch/politeness: `pipeline/stages/fetch.py`, `pipeline/config.py`
- Resolution: `pipeline/stages/resolve.py`
- Scoring: `pipeline/stages/score.py`
- Exports/contracts: `pipeline/stages/export.py`, `jobs/export_changes.py`
- Schema/migrations: `db/schema.sql`, `jobs/ingest_sources.py`

## Guardrails

- Keep edits stage-local when possible.
- Preserve output contracts unless a migration/compat update is included.
- Keep segment purity for `outreach_dispensary_100.csv`.
- Keep score range in `[0,100]`.
- Do not auto-merge entities; suggestions only.
- Keep change report run-id format: `YYYYMMDD-HHMMSS`.

## Required checks before PR

Run from repo root:

- `PYTHONPATH=$PWD python3 tests/test_parse_stage.py`
- `PYTHONPATH=$PWD python3 tests/test_resolve_stage.py`
- `PYTHONPATH=$PWD python3 tests/smoke_v1.py`

If pipeline behavior changes, also run:

- `CANNARADAR_MAX_SEEDS=1 ./run_v4.sh`

## If schema changes

Required in same PR:
- Update `db/schema.sql`
- Validate `jobs/ingest_sources.py` compatibility checks
- Update smoke test expectations if needed

## Commit message expectation

Include:
- what behavior changed
- why it changed
- rollback hint if external behavior/output changed

## References

- AI guide: `README_AI_AGENTS.md`
- Operator runbook: `docs/RUNBOOK_V1.md`
