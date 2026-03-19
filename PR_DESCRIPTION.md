## Summary

This change fixes GitHub issue #31 by making live run-control persistence concurrency-safe for the current file-based architecture.

Before this patch, runtime crawl code and operator control code could each load `control_<run_id>.json`, mutate different parts of the document, and write the entire file back. During a live crawl, that meant a later runtime flush could overwrite newer operator-issued controls or erase trust-critical intervention entries and operator-entered reasons that were written moments earlier.

The fix introduces serialized per-run control mutations and moves the live crawl paths onto locked read-modify-write helpers that always begin from the latest on-disk state. Runtime writers now update runtime-owned fields from the latest file image instead of replaying a stale snapshot over the full document. Operator/agent control writes continue to mutate control-owned fields, and intervention appends are preserved across competing writes.

## Root Cause

The root cause was stale whole-file persistence against a shared JSON coordination file:

- `pipeline/fetch_backends/crawlee_backend.py` repeatedly performed `load_run_control(...)`, mutated runtime data, and then called `save_run_control(...)`.
- Operator control paths also mutated the same JSON file.
- `save_run_control(...)` replaced the whole file atomically, but it did not merge against a newer on-disk version.
- Atomic replace prevented torn writes, but it did not prevent logical lost updates.

That meant the code was safe against partial-file corruption, but unsafe against concurrent writers carrying stale snapshots.

## Exact Files Changed

- `pipeline/run_control.py`
  Added per-run lock files and a `mutate_run_control(...)` helper that:
  reads the latest control file under lock, applies a mutation callback, and writes the updated document back before releasing the lock.
  `update_agent_controls(...)`, `update_runtime_controls(...)`, and `finalize_run_control(...)` now use this path.
  `ensure_run_control(...)` now creates the initial state through the same locked mutation path.
  `save_run_control(...)` is still available, but live mutation paths no longer rely on stale load/mutate/save cycles.

- `pipeline/fetch_backends/crawlee_backend.py`
  Replaced live runtime/operator control writes with `update_runtime_controls(...)` and `update_agent_controls(...)`.
  This covers:
  periodic runtime flushes,
  intervention persistence,
  auto-suppress prefix actions,
  automatic control record updates,
  invalid-seed intervention recording.

- `tests/test_run_state.py`
  Added a regression test that starts competing runtime and operator threads against the same run control file and verifies:
  operator controls survive,
  operator-entered reasons survive,
  runtime counters survive,
  intervention history keeps both entries in order.

- `docs/runtime-and-pipeline.md`
  Documented the live run-control persistence contract so future work does not regress to stale whole-file saves.

- `docs/data-model.md`
  Clarified that `control_<id>.json` is a live coordination file whose mutations must be serialized and merged from the latest on-disk state.

## Behavior Before

- Runtime crawl code could flush counters or interventions using a stale in-memory snapshot of `control_<run_id>.json`.
- If an operator issued a control action in the middle of a crawl, a later runtime write could overwrite:
  `agent_controls.domains.*`,
  operator-entered quarantine reasons,
  suppression overrides,
  stop requests,
  intervention history entries written after the runtime snapshot was loaded.
- The file write was atomic, but atomic file replacement alone did not prevent lost updates.

## Behavior After

- All supported live control mutations are serialized with a per-run lock file.
- Runtime persistence re-reads the latest file state while holding that lock, mutates only the runtime-owned fields, and writes the result back.
- Operator/agent control mutations do the same for control-owned fields.
- Intervention history is appended under the same serialization path, so concurrent runtime/operator writes no longer drop newer entries.
- Trust-critical operator fields are preserved during live crawls, including:
  `agent_controls`,
  intervention history,
  operator-entered reasons/notes such as quarantine reasons and manual intervention reasons.

## Test Coverage

Targeted tests run first:

- `PYTHONPATH=. ./.venv/bin/python tests/test_run_state.py`
- `PYTHONPATH=. ./.venv/bin/python tests/test_fetch_dispatch.py`

Broader relevant tests run after targeted tests passed:

- `PYTHONPATH=. ./.venv/bin/python tests/test_agent_cli.py`

Additional regression added:

- `test_competing_runtime_and_operator_updates_preserve_operator_controls_and_history`
  Simulates competing runtime/operator writes and asserts that:
  operator controls are not clobbered,
  operator-entered reason text is not lost,
  runtime counters still persist,
  both intervention entries remain present.

Additional note:

- `PYTHONPATH=. ./.venv/bin/python tests/test_cli_contracts.py` currently fails in this branch with `AttributeError: module 'cli.app' has no attribute 'execute_agent_run'`.
  That failure is pre-existing and unrelated to this issue fix.

## Risk Assessment

### Low-risk aspects

- The fix stays within the existing JSON control-file architecture.
- No fake locking was added; the lock now guards the exact critical section that was losing updates.
- Runtime/operator behavior is preserved semantically, but updates are now serialized against the latest state.

### Main risks

- The locking uses `fcntl`, so the behavior is POSIX-oriented and assumes the repository’s current local runtime model.
- A future caller that bypasses the mutation helpers and reintroduces stale `load -> mutate -> save` patterns could reintroduce lost updates.
- The lock serializes writes, so extremely high-frequency control mutations could wait briefly behind each other. Given the current local operator/crawl model, that tradeoff is appropriate.

### Why this is still the right minimum fix

- The project already uses a local file-backed coordination model.
- Replacing the control plane with SQLite or another shared store would be a much larger architectural change.
- This patch closes the correctness gap in the current architecture without pretending atomic rename alone is enough.

## Reviewer Checklist

- Verify `pipeline/run_control.py` now serializes all supported live mutations under a per-run lock.
- Verify runtime persistence in `pipeline/fetch_backends/crawlee_backend.py` no longer performs stale whole-file saves.
- Verify operator-owned fields in `agent_controls` are preserved when runtime flushes happen concurrently.
- Verify intervention history keeps both runtime and operator entries under competing writes.
- Verify docs now describe the supported run-control mutation contract.
- Verify the targeted tests and broader relevant test still pass locally.
- Decide whether a future cleanup should further de-emphasize direct `save_run_control(...)` usage for non-setup code.
