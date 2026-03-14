# Architecture

Last verified against commit `0c5e92b`.

## System Overview

The runtime is a local-first provider intelligence engine for New Jersey ASD/ADHD provider discovery and verification. The primary entrypoint is `provider_intel_cli.py`, which dispatches into `cli/app.py`. The main execution path then flows through:

- `cli/sync.py` for orchestration and resumability
- `cli/agent.py` and `agent_runtime/` for tenant-scoped agent orchestration, memory, and tool use
- `pipeline/pipeline.py` for stage wiring
- `pipeline/fetch_backends/crawlee_backend.py` for HTTP and browser crawl execution
- `pipeline/stages/*.py` for extract, resolve, score, QA, and export
- `jobs/ingest_sources.py` and `db/schema.sql` for schema bootstrap and reference-rule loading

The architecture is intentionally conservative:

- SQLite is the canonical runtime store.
- Local JSON files define seeds, prescriber rules, crawl config, and domain policies.
- Tenant isolation is path-based: each tenant can run inside its own runtime root with separate DB, state, outputs, and agent memory.
- The provider agent control plane is optional and sits above the deterministic runtime.
- Critical claims are only exported if evidence exists in `field_evidence`.
- Uncertain or contradictory records are routed to `review_queue`.

```mermaid
flowchart LR
  CLI["CLI\nprovider_intel_cli.py"] --> APP["Command router\ncli/app.py"]
  APP --> INIT["Init + doctor\ncli/doctor.py"]
  APP --> SYNC["Sync + tail + export\ncli/sync.py"]
  APP --> QUERY["Status / search / sql\ncli/query.py"]
  APP --> CONTROL["Run controls\ncli/control.py"]
  APP --> AGENT["Tenant agent\ncli/agent.py"]

  SYNC --> RUNNER["PipelineRunner\npipeline/pipeline.py"]
  AGENT --> AR["Agent runtime\nagent_runtime/*"]
  AR --> RUNNER
  AR --> MEM[("Agent memory\nagent_memory_v1.db")]
  RUNNER --> SEEDS["Seed ingest\nseed_packs/*.json\nreference/prescriber_rules/*.json"]
  RUNNER --> FETCH["Fetch\npipeline/fetch_backends/crawlee_backend.py"]
  RUNNER --> EXTRACT["Extract\npipeline/stages/extract.py"]
  RUNNER --> RESOLVE["Resolve\npipeline/stages/resolve.py"]
  RUNNER --> SCORE["Score\npipeline/stages/score.py"]
  RUNNER --> QA["QA\npipeline/stages/qa.py"]
  RUNNER --> EXPORT["Export\npipeline/stages/export.py"]

  FETCH --> DB[("SQLite\nprovider_intel_v1.db")]
  EXTRACT --> DB
  RESOLVE --> DB
  SCORE --> DB
  QA --> DB
  EXPORT --> OUT["Filesystem outputs\nout/provider_intel/"]
  SYNC --> STATE["Run checkpoints\n data/state/agent_runs/"]
  CONTROL --> STATE
  AR --> STATE
```

## Component Responsibilities

| Component | Files | Responsibility |
| --- | --- | --- |
| CLI shell | `provider_intel_cli.py`, `cli/app.py` | Parse commands, enforce Python 3.11+, emit plain or JSON payloads |
| Bootstrap | `cli/doctor.py`, `jobs/ingest_sources.py`, `db/schema.sql` | Create config/policy files, initialize schema, validate environment and schema metadata |
| Agent control plane | `cli/agent.py`, `agent_runtime/*` | Run tenant-scoped agent sessions, persist agent memory, call bounded tools, and synthesize operator-facing summaries |
| Run orchestration | `cli/sync.py`, `pipeline/pipeline.py`, `pipeline/run_state.py` | Execute stages in order, checkpoint progress, support resume |
| Runtime controls | `cli/control.py`, `pipeline/run_control.py` | Show/apply bounded domain controls and persist interventions |
| Fetch layer | `pipeline/stages/fetch.py`, `pipeline/fetch_backends/crawlee_backend.py`, `pipeline/fetch_backends/browser_worker.py`, `pipeline/fetch_backends/domain_policy.py` | Seed crawl jobs, enforce per-domain policies, escalate to browser when blocked |
| Extraction layer | `pipeline/stages/parse.py`, `pipeline/stages/extract.py` | Convert HTML into deterministic extracted records plus evidence snippets |
| Entity resolution | `pipeline/stages/resolve.py` | Build providers, practices, licenses, provider-practice records, and review-only items |
| Scoring | `pipeline/stages/score.py` | Apply NJ prescriber rules, calculate field confidence, record confidence, and outreach fit |
| QA gate | `pipeline/stages/qa.py` | Block records missing critical evidence, record contradictions, mark outreach readiness |
| Export | `pipeline/stages/export.py` | Produce CSV/JSON/profile/evidence/review/outreach artifacts |
| Observability | `pipeline/observability.py` | Structured JSON logs and in-memory counters |

## Runtime Execution Flow

The `sync` command in `cli/sync.py` executes a fixed stage order from `pipeline/run_state.py`:

1. `seed_ingest`
2. `crawl`
3. `extract`
4. `resolve`
5. `score`
6. `qa`
7. `export`

Each completed stage writes its own result payload into the run-state JSON. On failure, the current stage is marked `failed`, `last_error` is written, and the run can later resume from the next incomplete stage.

```mermaid
flowchart TB
  subgraph Inputs
    CFG["crawler_config.json"]
    POL["fetch_policies.json"]
    SEED["seed_packs/nj/seed_pack.json"]
    RULES["reference/prescriber_rules/nj.json"]
    METROS["reference/metros/nj.json"]
  end

  subgraph Runtime
    RS["run_<id>.json\nprovider_intel.run_state.v1"]
    RC["control_<id>.json\nrun_control.v1"]
    DB["provider_intel_v1.db"]
    MEM["agent_memory_v1.db"]
  end

  subgraph Outputs
    MAN["last_run_manifest.json"]
    OUT["out/provider_intel/*"]
  end

  CFG --> Runtime
  POL --> Runtime
  SEED --> Runtime
  RULES --> Runtime
  METROS --> Runtime
  Runtime --> MAN
  Runtime --> OUT
```

## Tooling Boundaries

What is inside the runtime:

- Python command and stage orchestration
- SQLite persistence
- Tenant-scoped runtime roots under `storage/tenants/<tenant_id>/` when `--tenant` is used
- Crawlee HTTP crawling
- Optional Playwright-backed browser crawling
- Markdown and fallback PDF export generation
- Optional agent orchestration, memory, and tool traces

What is outside the runtime:

- No remote queue or scheduler
- No secrets manager
- No external API dependency for scoring or QA
- No current HTML-to-PDF renderer beyond the fallback PDF writer
- No multi-state prescribing rules beyond New Jersey

Optional external dependency:

- The agent control plane can call the OpenAI Responses API through a provider adapter, but that does not change the deterministic pipeline’s truth rules or storage contracts.

## Notes On Accuracy And Current Gaps

- The documented "agentic research loop" is now available as a local tenant-scoped control plane above the deterministic pipeline, not as an additional truth-writing pipeline stage.
- `--crawl-mode refresh` changes fetch breadth by using `monitorMaxPagesPerDomain`, `monitorMaxTotalPages`, and `monitorMaxDepth`, but it keeps the same stage order and still performs a fresh-run table reset in `seed_ingest`.
- `--crawlee-headless on|off` now overrides the effective browser headless setting for the current sync run.
- `--db-timeout-ms` now sets SQLite connection timeout and `PRAGMA busy_timeout` for writable and read-only CLI connections.
- `pipeline/quality.py` is legacy code from the prior product surface and is not part of the active provider-intel execution path.
