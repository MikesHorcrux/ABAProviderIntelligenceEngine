# Testing And Quality

Last verified against commit `0c5e92b`.

## Test Strategy

The current suite is mostly contract and fixture driven. It proves the core pipeline shape without pretending to fully validate live web behavior.

## Coverage Map

| Area | Files | What is covered |
| --- | --- | --- |
| CLI contracts | `tests/test_agent_cli.py` | `init`, `search`, `status`, `sql`, `export` JSON/plain behavior |
| Tenant runtime paths | `tests/test_runtime_context.py` | default path compatibility, tenant path derivation, tenant CLI bootstrap |
| Run-state and resume | `tests/test_run_state.py` | checkpoint creation, failed-stage resume, run-control finalization |
| Config loading | `tests/test_fetch_config.py` | default config values and env overrides |
| Agent memory | `tests/test_agent_memory.py` | sessions, turns, tool events, run memory, domain tactics, client profiles |
| Agent policy | `tests/test_agent_policy.py` | bounded tool allowlist, reason requirement, control validation |
| OpenAI adapter | `tests/test_openai_adapter.py` | Responses request shape, tool-call parsing, transient retry behavior |
| Agent orchestration | `tests/test_agent_orchestrator.py` | full operator loop, tenant isolation, failed-sync recovery and resume |
| CLI schema contracts | `tests/test_cli_contracts.py` | `status`, `agent run`, and `agent status` schema compatibility |
| Fetch core behavior | `tests/test_fetch_dispatch.py` | domain policies, block detection, telemetry recording |
| Optional local fetch integration | `tests/test_fetch_integration.py` | local HTTP server with HTTP and browser modes, gated by env |
| Extraction | `tests/test_parse_stage.py` | practice page parsing, blocked board handling, board enrichment parsing, hospital/university fixtures |
| Resolution | `tests/test_resolve_stage.py` | dedupe order, practice-only review routing, board enrichment, multi-provider practice handling |
| Score + QA + export | `tests/test_lead_research.py` | prescriber rule application, approval, provider outputs, sales report output |

## What Is Covered Well

- Schema and bootstrap expectations
- Checkpoint/resume behavior
- Evidence-backed scoring and QA blocking
- Deterministic extraction against frozen HTML fixtures
- Export artifact creation
- Domain-policy parsing and fetch telemetry

## What Is Not Fully Covered

- Real live-source stability across current NJ domains
- Browser worker subprocess behavior across all platforms
- Layout fidelity of generated PDFs
- High-volume crawl performance
- End-to-end statewide pilot quality metrics
- Every config field in `pipeline/config.py`

Important nuance:

- `tests/test_fetch_integration.py` is opt-in and only runs when `PROVIDER_INTEL_RUN_FETCH_INTEGRATION=1`.
- `pipeline/quality.py` is not part of the active provider-intel pipeline and is not a release gate.

## Recommended Test Commands

Core suite:

```bash
PYTHONPATH=$PWD python tests/test_agent_cli.py
PYTHONPATH=$PWD python tests/test_runtime_context.py
PYTHONPATH=$PWD python tests/test_run_state.py
PYTHONPATH=$PWD python tests/test_fetch_config.py
PYTHONPATH=$PWD python tests/test_agent_memory.py
PYTHONPATH=$PWD python tests/test_agent_policy.py
PYTHONPATH=$PWD python tests/test_openai_adapter.py
PYTHONPATH=$PWD python tests/test_agent_orchestrator.py
PYTHONPATH=$PWD python tests/test_cli_contracts.py
PYTHONPATH=$PWD python tests/test_fetch_dispatch.py
PYTHONPATH=$PWD python tests/test_parse_stage.py
PYTHONPATH=$PWD python tests/test_resolve_stage.py
PYTHONPATH=$PWD python tests/test_lead_research.py
```

Optional local integration:

```bash
PROVIDER_INTEL_RUN_FETCH_INTEGRATION=1 PYTHONPATH=$PWD python tests/test_fetch_integration.py
```

CLI sanity checks:

```bash
PYTHONPATH=$PWD python provider_intel_cli.py doctor --json
PYTHONPATH=$PWD python provider_intel_cli.py status --json
PYTHONPATH=$PWD python provider_intel_cli.py search --json --preset outreach-ready
PYTHONPATH=$PWD python provider_intel_cli.py --json --tenant demo agent status
```

## Quality Gates In Code

The runtime already encodes some release-quality rules:

- schema checksum must match `db/schema.sql`
- export only includes `export_status='approved'`
- outreach artifacts only generate when `outreach_ready=1`
- critical fields must have evidence
- contradictions lower confidence
- read-only SQL is enforced for operator queries

## Release Readiness Checklist

- `init` and `doctor` pass on a clean machine
- core test suite passes
- optional fetch integration passes on at least one supported environment
- bounded live run completes without runtime exceptions
- tenant-scoped bootstrap and `agent status` work on a clean workspace
- approved record outputs contain evidence-backed critical fields
- review queue is non-empty when source ambiguity exists
- sales report only contains approved outreach-ready rows
- docs still match CLI flags, tenant path behavior, and stage behavior

## Recommended Next Quality Improvements

- Add golden-file assertions for exported Markdown and PDF artifacts
- Add fixture coverage for more NJ official source patterns
- Add a small end-to-end acceptance test with a fixed crawl-result fixture set
- Remove or isolate legacy dead code such as `pipeline/quality.py`
