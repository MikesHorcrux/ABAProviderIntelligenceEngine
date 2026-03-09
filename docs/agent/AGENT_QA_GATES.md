# Agent QA Gates (Scaffold)

## Objective

Define minimum quality thresholds for agent-generated outputs before they are treated as ready for downstream use.

## Threshold Fields

Configured under `qa_thresholds`:

- `min_sources` (integer)
- `min_signals` (integer)
- `min_contact_coverage_pct` (float, percent)

## Metrics Inputs

Evaluated against:

- `source_count`
- `signal_count`
- `contact_coverage_pct`

## Evaluation Semantics

Current implementation (`agent_runtime.qa.evaluate_qa_gates`) is strict minimum checking:

- fail if `source_count < min_sources`
- fail if `signal_count < min_signals`
- fail if `contact_coverage_pct < min_contact_coverage_pct`

Return payload:

- `passed` boolean
- `failures` tuple of failure codes
- echoed `thresholds` and `metrics`

Failure codes:

- `sources_below_min`
- `signals_below_min`
- `contact_coverage_below_min`

## Recommended Starting Values

- `min_sources`: 2
- `min_signals`: 3
- `min_contact_coverage_pct`: 50.0

These defaults are intentionally conservative for scaffold validation and should be tuned after real provider outputs are measured.

## Non-Goals in Scaffold

- no confidence weighting
- no source de-duplication logic
- no role-specific threshold profiles
