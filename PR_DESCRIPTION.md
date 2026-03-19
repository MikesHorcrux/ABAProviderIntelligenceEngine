# Fix issue #26: keep review-only material out of standard lead-intelligence exports

## Summary

This change closes the trust-boundary gap in the export stage.

Standard outward-facing lead-intelligence dossiers now only generate from `provider_practice_records` rows that are already `export_status='approved'`.

Review-only material such as `missing_provider`, `practice_only_signal`, queued rows, and other non-exportable account signals no longer flows into the standard `lead_intelligence_<run_id>.csv/.json` index or the `dossiers/<run_id>/.../lead_intelligence.*` artifacts.

To preserve clearly intentional internal-review behavior, the prior review-only account aggregation has been retained in a separate internal lane:

- `internal_review_accounts_<run_id>.csv`
- `internal_review_accounts_<run_id>.json`
- `internal_review/<run_id>/.../internal_review_summary.{md,pdf,json}`

These preserved review artifacts are explicitly named as internal review outputs and no longer look like standard export artifacts.

## Root Cause

The export stage had two different trust models in the same function:

- The provider export, evidence bundle, and outreach brief path correctly gated on `pr.export_status='approved'`.
- The lead-intelligence dossier path did not reuse that same gate. It rebuilt dossier candidates from `pr.review_status IN ('ready', 'queued')` and then synthesized additional `review_only` practice/account rows directly from `review_queue` + `extracted_records`.

That meant review-lane material could bypass the normal export gate and still produce outward-facing artifacts with standard lead-intelligence naming:

- `lead_intelligence_<run_id>.csv`
- `lead_intelligence_<run_id>.json`
- `dossiers/<run_id>/.../lead_intelligence.md`
- `dossiers/<run_id>/.../lead_intelligence.pdf`

In practice, queued `missing_provider` / `practice_only_signal` accounts could look like approved sales-intelligence output even though they had never crossed QA/export approval.

## Exact Files Changed

### `/private/tmp/aba-fix-26/pipeline/stages/export.py`

- Split dossier generation into two lanes.
- Added `_approved_dossier_candidates(...)` so the standard dossier path only reads approved/exportable records.
- Stopped attaching review-lane rows and review notes to standard lead-intelligence dossiers.
- Preserved review-only aggregation in a separate internal-review path.
- Added `_internal_review_summary_markdown(...)` for clearly internal account summaries.
- Added new report keys:
  - `internal_review_csv`
  - `internal_review_json`
  - `internal_review_dir`
  - `internal_review_count`

### `/private/tmp/aba-fix-26/tests/test_lead_research.py`

- Replaced the old expectation that review-only pages collapse into a standard dossier.
- Added regression coverage proving review-only pages now land in internal-review outputs instead.
- Added mixed-lane coverage proving approved dossiers ignore queued/review-only account material even when both exist in the same export run.

### `/private/tmp/aba-fix-26/agent_runtime/orchestrator.py`

- Added the new internal-review artifact keys to export snapshot collection so operators can still discover those outputs through the agent runtime.

### `/private/tmp/aba-fix-26/docs/runtime-and-pipeline.md`

- Made the export-stage contract explicit:
  - standard lead-intelligence dossiers are approved/exportable only
  - preserved review-only aggregation is written to distinct internal-review outputs

### `/private/tmp/aba-fix-26/docs/testing-and-quality.md`

- Added quality-gate language for the trust boundary between approved dossiers and internal-review account summaries.

### `/private/tmp/aba-fix-26/README.md`

- Updated the output inventory to include the lead-intelligence export files and the new internal-review account summary files.

## Behavior Before

- Standard provider CSV/JSON exports were approved-only.
- Sales briefs were approved + `outreach_ready` only.
- Lead-intelligence dossiers were not approved-only.
- The export stage pulled dossier candidates from rows in review states (`ready` / `queued`).
- The export stage also synthesized additional review-only account rows from `review_queue` and `extracted_records`.
- Review-only accounts could therefore produce normal-looking `lead_intelligence` CSV/JSON entries and `lead_intelligence.md/pdf` dossier artifacts.
- Approved dossiers could also absorb review-lane notes/material because dossier grouping merged in review records by record/source.

## Behavior After

- Standard provider CSV/JSON exports remain approved-only.
- Sales briefs remain approved + `outreach_ready` only.
- Standard lead-intelligence dossiers now also follow the same export gate and only generate from approved/exportable rows.
- Approved dossiers no longer merge queued/review-only review notes into outward-facing lead-intelligence artifacts.
- Review-only account aggregation still exists, but it is emitted as explicitly internal-review output with separate filenames and directories.
- Internal-review summaries are not named or structured like standard lead-intelligence exports.

## Test Coverage

Targeted tests run first:

- `PYTHONPATH=$PWD python3 tests/test_lead_research.py`

Broader relevant tests run after targeted success:

- `PYTHONPATH=$PWD python3 tests/test_resolve_stage.py`

New/updated regression coverage:

- Standard approved export still generates outward-facing provider/sales/dossier outputs.
- Review-only pages no longer create standard `lead_intelligence` dossier rows.
- Review-only pages still produce preserved internal-review summaries.
- Approved dossiers ignore review-only/queued account material even when both are present in the same run.

Tests attempted but blocked by environment:

- `PYTHONPATH=$PWD python3 tests/test_agent_orchestrator.py`
- `PYTHONPATH=$PWD python3 tests/test_run_state.py`

Both currently fail in this workspace because the runtime imports `crawlee`, which is not installed here:

- `ModuleNotFoundError: No module named 'crawlee'`

## Risk Assessment

Overall risk is moderate and localized to export artifact generation.

Low-risk areas:

- Provider CSV/JSON export gating was already approved-only and was not loosened.
- Sales brief gating was already correct and was not changed.
- The new regression tests directly cover the issue’s trust boundary.

Primary risk areas:

- Any downstream consumer that implicitly expected review-only accounts inside `lead_intelligence_*` outputs will now see those rows removed from the standard dossier lane.
- Consumers that want the old review-only aggregation must switch to the new internal-review artifact names/paths.
- Internal-review grouping logic is preserved but renamed/re-scoped; operator workflows that inspect artifact names may need to use the new internal-review outputs.

Why this risk is acceptable:

- The old behavior violated the repository’s documented export contract.
- The new behavior aligns all outward-facing export lanes behind the same QA/export gate.
- Review-only information is preserved instead of discarded.

## Reviewer Checklist

- Confirm `pipeline/stages/export.py` only uses approved/exportable rows for the standard `lead_intelligence_*` and `dossiers/.../lead_intelligence.*` path.
- Confirm queued/review-only `missing_provider` / `practice_only_signal` material no longer appears in outward-facing dossier markdown/CSV/JSON.
- Confirm review-only aggregation still exists in the new internal-review outputs and is clearly named as internal-only.
- Confirm the mixed approved + queued regression test captures the original bypass risk.
- Confirm docs now state that standard dossiers are approved-only and internal-review summaries are separate.
- Confirm the new report keys are acceptable for callers that inspect export output metadata.
