# Summary

This change fixes GitHub issue #30 by correcting the provider export identity contract. The approved provider export was populating `provider_id` with the provider-practice `record_id`, which corrupted the canonical provider identity in both CSV and JSON outputs. The export now emits:

- `record_id`: the provider-practice-state affiliation record identifier
- `provider_id`: the canonical provider identifier from `provider_practice_records.provider_id`

The change also adds regression coverage and updates documentation so the distinction is explicit.

# Root Cause

The bug was not in naming alone. It was a semantic mismatch in the export stage:

- `provider_practice_records` stores both `record_id` and `provider_id`
- the export query selected `record_id` but did not select `provider_id`
- the serializer then wrote `row["record_id"]` into the `provider_id` field

That meant downstream consumers of `provider_records_<run_id>.csv` and `provider_records_<run_id>.json` received a record-scoped identifier where a canonical provider identifier was promised. Because `record_id` is derived from `provider_id + practice_id + location_id + state`, the exported value looked stable enough to avoid obvious breakage while still being semantically wrong.

# Exact Files Changed

1. `pipeline/stages/export.py`

- Updated the approved-record export query to select `pr.provider_id` in addition to `pr.record_id`.
- Added `record_id` to the exported provider row shape.
- Corrected `provider_id` to serialize from the canonical `pr.provider_id` column instead of `record_id`.

2. `tests/test_lead_research.py`

- Added regression assertions proving:
  - export rows include `record_id`
  - export rows include canonical `provider_id`
  - `provider_id != record_id`
  - both CSV and JSON exports preserve the correct identity values

3. `docs/cli-reference.md`

- Documented that the export emits `record_id` and canonical `provider_id` as distinct fields.
- Explicitly states that `provider_id` must not be substituted with `record_id`.

4. `docs/runtime-and-pipeline.md`

- Updated the export-stage behavior description to state that provider exports include both `record_id` and canonical `provider_id`.

# Behavior Before

For approved exports:

- CSV rows exposed a `provider_id` column
- JSON rows exposed a `provider_id` field
- both were populated from `record_id`
- there was no explicit exported `record_id` field in the provider export payload

Practical impact:

- canonical provider identity was lost in the primary provider export artifacts
- any downstream consumer using `provider_id` for joins, dedupe, or identity tracking would actually be operating on record-scoped IDs
- the bug was easy to miss because record IDs are deterministic and superficially similar in purpose

# Behavior After

For approved exports:

- CSV rows include both `record_id` and `provider_id`
- JSON rows include both `record_id` and `provider_id`
- `provider_id` is sourced from the canonical provider row
- `record_id` remains available for record-level joins and artifact correlation

Result:

- canonical provider identity is preserved correctly
- record-level export identity remains explicit instead of being overloaded into the provider field
- the export contract now matches the repository data model

# Test Coverage

Targeted verification completed:

1. `PYTHONPATH=. python3 tests/test_lead_research.py`
   - Passed
   - Confirms export generation still works
   - Confirms CSV and JSON provider exports emit `record_id == "rec_1"` and `provider_id == "prov_1"`
   - Confirms `provider_id != record_id`

Broader relevant verification attempted after targeted pass:

1. `PYTHONPATH=. python3 tests/test_agent_cli.py`
   - Failed in this environment
   - Failure was at the existing `init`/`doctor` expectation (`payload["data"]["doctor"]["ok"] is True`)
   - This is environment-sensitive and not caused by the export identity change

2. `PYTHONPATH=. python3 tests/test_agent_orchestrator.py`
   - Failed in this environment
   - Import error: missing `crawlee`
   - This is a local dependency/setup issue, not a regression introduced by this change

Notes:

- `pytest` was not available in the shell environment
- `python` was not on PATH; verification used `python3`
- the relevant targeted regression is covered and passing

# Risk Assessment

Overall risk: low

Why low:

- the code change is localized to export serialization
- no resolver, scorer, QA, or DB schema logic changed
- the fix aligns the export with already-existing canonical DB semantics
- the test covers the exact regression path in both CSV and JSON outputs

Potential compatibility consideration:

- provider export consumers will now see an additional `record_id` column/field
- this is additive, but any strict schema consumer that rejects new columns should be checked

Why that additive change is justified:

- it preserves the record-level identifier that had previously been smuggled incorrectly into `provider_id`
- it makes the two identities explicit instead of forcing downstream consumers to choose between correctness and utility

# Reviewer Checklist

1. Verify `pipeline/stages/export.py` now selects and serializes `pr.provider_id` directly.
2. Verify the provider export payload includes both `record_id` and `provider_id`.
3. Confirm the regression test fails if `provider_id` is reverted to `record_id`.
4. Confirm CSV and JSON export contracts are consistent with each other.
5. Confirm the doc updates match the implemented export behavior.
6. Consider whether any downstream consumer depends on an exact provider-export column list and, if so, validate the additive `record_id` field.

# Issue Mapping

- Fixes GitHub issue #30
- Scope limited to the export identity contract and its documentation/tests
