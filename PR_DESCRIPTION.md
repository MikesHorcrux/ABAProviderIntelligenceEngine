## Summary

This change fixes issue #33 by enforcing tenant ownership whenever the agent layer accepts a `session_id` from the caller.

Before this patch, agent session lookup was effectively global inside the shared session store API. `agent status --session-id`, `agent resume --session-id`, and any `agent run --session-id ...` path trusted the supplied session id without verifying that the session belonged to the active `--tenant`. That meant a caller could resume or inspect another tenant's session if both sessions were reachable through the same underlying agent memory database.

The fix introduces a tenant-aware session lookup helper in the session store and routes all session-id-based agent entry points through that helper. The contract is also documented explicitly, and regression tests now cover cross-tenant mismatches at the storage, orchestrator, and CLI resume layers.

## Root Cause

The root cause was a missing authorization boundary at the session lookup layer:

- `SessionStore.get_session(session_id)` only filtered on `session_id`.
- `AgentOrchestrator.status()` used that global lookup whenever a session id was supplied.
- `AgentOrchestrator._ensure_session()` used that global lookup whenever `run(..., session_id=...)` was used to continue a session.
- `cli/agent.py::execute_agent_resume()` loaded the stored session goal with the same global lookup before resuming the session.

The runtime already isolates tenant state by default through tenant-specific runtime paths, but the session ownership rule was not enforced at the storage API boundary. That left the code vulnerable if multiple tenants shared one backing agent-memory database in tests, custom runtime overrides, or future storage refactors.

## Exact Files Changed

- `agent_runtime/memory.py`
  Added a centralized tenant-aware session lookup helper:
  - `get_session_for_tenant(session_id, tenant_id)`
  - internal `_get_session(..., tenant_id=None)` shared by global and tenant-aware lookups
  Also made the tenant mismatch error explicit: `Agent session not found for tenant <tenant>: <session_id>`.

- `agent_runtime/orchestrator.py`
  Updated:
  - `status(session_id=...)` to require tenant ownership
  - `_ensure_session(..., session_id=...)` to require tenant ownership
  This covers both status inspection and any resume/continue path that flows through `run(..., session_id=...)`.

- `cli/agent.py`
  Updated `execute_agent_resume()` to fetch the stored session goal through the tenant-aware lookup before calling `orchestrator.run(...)`.

- `tests/test_agent_memory.py`
  Added regression coverage proving the session store rejects cross-tenant session lookup.

- `tests/test_agent_orchestrator.py`
  Added regression coverage proving the orchestrator rejects a session id owned by another tenant for:
  - `status(session_id=...)`
  - `run(..., session_id=...)`

- `tests/test_cli_contracts.py`
  Fixed the existing `agent run` contract monkeypatch to target `cli.agent.execute_agent_run`, which matches the current dispatcher layout.
  Added regression coverage proving `execute_agent_resume()` rejects a session id owned by another tenant before resuming.

- `docs/cli-reference.md`
  Documented the CLI contract explicitly:
  - `agent run --session-id`
  - `agent status --session-id`
  - `agent resume --session-id`
  all require that the session belong to the same `--tenant`.

## Behavior Before

- `agent status --session-id <id>` trusted `<id>` and returned that session even if it belonged to another tenant, as long as the session existed in the backing agent memory DB.
- `agent resume --session-id <id>` trusted `<id>`, loaded its stored goal, and resumed that session without verifying the active tenant.
- `agent run --session-id <id> --goal ...` reused the stored session without verifying tenant ownership.
- The tenant/session ownership requirement was implicit and path-based, not enforced by the session lookup contract itself.

## Behavior After

- Any session-id-based agent lookup now enforces tenant ownership.
- If a caller supplies a session id that does not belong to the active tenant, the request fails with:
  - `Agent session not found for tenant <tenant_id>: <session_id>`
- `agent status --session-id`, `agent resume --session-id`, and any orchestrator `run(..., session_id=...)` continuation path now share the same tenant-aware ownership check.
- The CLI docs explicitly state the ownership contract.

## Test Coverage

Targeted tests run first:

- `PYTHONPATH=$PWD ./.venv/bin/python tests/test_agent_memory.py`
- `PYTHONPATH=$PWD ./.venv/bin/python tests/test_agent_orchestrator.py`

Broader relevant tests run after targeted pass:

- `PYTHONPATH=$PWD ./.venv/bin/python tests/test_cli_contracts.py`
- `PYTHONPATH=$PWD ./.venv/bin/python tests/test_ae_cli.py`

Coverage added by this patch:

- Storage layer regression:
  - cross-tenant `SessionStore.get_session_for_tenant(...)` rejection
- Orchestrator layer regression:
  - cross-tenant rejection for `status(session_id=...)`
  - cross-tenant rejection for `run(..., session_id=...)`
- CLI resume regression:
  - cross-tenant rejection in `execute_agent_resume()`

## Risk Assessment

Overall risk is low.

Why risk is low:

- The change is narrow and localized to session lookup behavior.
- The new enforcement only affects code paths that already rely on a caller-supplied `session_id`.
- The default happy path for correct same-tenant session ids is unchanged.
- Existing non-session-id flows, including `agent status` without a session id, still behave the same.

Primary compatibility consideration:

- Any existing workflow that incorrectly reused a session id across tenants will now fail fast instead of silently crossing tenant boundaries. That is an intentional security/ownership correction.

Secondary considerations:

- `MemoryStore` tables remain non-tenant-scoped in this patch; this change is specifically about enforcing tenant ownership of session ids.
- The fix does not change runtime path isolation or checkpoint behavior.

## Reviewer Checklist

- Verify `SessionStore.get_session_for_tenant()` is the only new ownership helper needed and that the error semantics are acceptable.
- Verify `AgentOrchestrator.status()` and `_ensure_session()` now consistently enforce tenant ownership for supplied session ids.
- Verify `cli/agent.py::execute_agent_resume()` cannot read another tenant’s stored goal anymore.
- Verify the new regression tests model the shared-DB scenario that exposed the bug class.
- Verify the docs language in `docs/cli-reference.md` matches the intended public CLI contract.
- Verify there are no other session-id entry points outside these paths that should also be routed through the tenant-aware lookup helper.
