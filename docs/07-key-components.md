# 07 Key Components

This document explains the major components by responsibility, callers, callees, state access, and runtime role.

## `cli/app.py`

Responsibility:

- define the canonical command contract
- normalize command aliases
- dispatch to command handlers
- enforce Python 3.11+
- emit stable JSON/plain output envelopes

Called by:

- `cannaradar_cli.py`

Calls:

- `cli/doctor.py:run_doctor`
- `cli/sync.py:execute_sync`
- `cli/sync.py:execute_tail`
- `cli/query.py:run_status`
- `cli/query.py:run_search`
- `cli/query.py:run_sql`
- `cli/control.py`
- `pipeline/pipeline.py:PipelineRunner` for legacy stage aliases

Reads/writes:

- environment variable `CANNARADAR_CRAWLER_CONFIG`

Runtime role:

- startup controller

## `cli/sync.py:execute_sync`

Responsibility:

- materialize a run id
- create or load a checkpoint
- call stages in order
- mark stage started/completed/failed
- finalize run-control state
- build the final run summary

Called by:

- `cli/app.py`

Calls:

- `pipeline/run_state.py`
- `pipeline/run_control.py`
- `pipeline/pipeline.py:PipelineRunner`

Reads/writes:

- `data/state/agent_runs/run_<id>.json`
- `data/state/agent_runs/control_<id>.json`

Runtime role:

- batch run orchestrator

## `pipeline/pipeline.py:PipelineRunner`

Responsibility:

- implement the domain pipeline stages
- decide discovery vs monitoring seed mix
- apply growth governor and reliability/net-new gates
- load results for enrich
- write run manifests and daily growth summaries

Called by:

- `cli/sync.py`
- legacy direct commands in `cli/app.py`
- `run_v4.sh` indirectly through CLI

Calls:

- `pipeline/stages/discovery.py`
- `pipeline/stages/fetch.py`
- `pipeline/stages/parse.py`
- `pipeline/stages/resolve.py`
- `pipeline/stages/enrich.py`
- `pipeline/stages/score.py`
- `pipeline/stages/research.py`
- `pipeline/stages/export.py`
- `pipeline/quality.py`
- `pipeline/db.py`

Reads/writes:

- SQLite
- `out/`
- `data/state/last_run_manifest.json`
- `out/daily_growth_summary.json`

Runtime role:

- core business orchestrator

## `pipeline/fetch_backends/common.py:SeedRunRecorder`

Responsibility:

- create `crawl_jobs`
- append `crawl_results`
- maintain per-seed counters
- upsert `seed_telemetry`
- emit `FetchResult` objects

Called by:

- `pipeline/fetch_backends/crawlee_backend.py`

Calls:

- SQLite only

Reads/writes:

- `crawl_jobs`
- `crawl_results`
- `seed_telemetry`

Runtime role:

- fetch persistence adapter

Why it matters:

- it commits incrementally, which is the fetch durability backbone

## `pipeline/fetch_backends/crawlee_backend.py:SeedCrawlState`

Responsibility:

- hold per-seed runtime state
- manage URL acceptance and link queueing
- poll live control overrides
- persist runtime counters
- detect stop/quarantine/escalation conditions
- implement self-healing heuristics

Called by:

- `pipeline/fetch_backends/crawlee_backend.py:run_fetch`

Calls:

- `pipeline/run_control.py`
- `pipeline/fetch_backends/common.py`
- Crawlee request handlers

Reads/writes:

- run-control JSON
- DB through `SeedRunRecorder`

Runtime role:

- live crawl state machine

This is one of the highest-value files in the repo for operational behavior changes.

## `pipeline/fetch_backends/crawlee_backend.py:run_fetch`

Responsibility:

- validate seed domains
- load domain policies
- derive per-seed crawl limits
- create recorder/state objects
- choose HTTP-only vs browser-first vs HTTP-then-browser behavior
- contain seed-level failures
- finalize seed statuses

Called by:

- `pipeline/pipeline.py:PipelineRunner.run_fetch`

Calls:

- `SeedCrawlState`
- `_run_http_crawl`
- `_run_browser_crawl_dispatch`
- `_handle_seed_crawl_exception`

Reads/writes:

- DB
- run-control JSON

Runtime role:

- fetch engine entrypoint

## `pipeline/fetch_backends/browser_worker.py`

Responsibility:

- execute browser crawl in an isolated subprocess
- protect the main agent process from Playwright instability
- return browser results as JSON for replay by the parent

Called by:

- `pipeline/fetch_backends/crawlee_backend.py:_run_browser_worker_subprocess`

Calls:

- `PlaywrightCrawler`
- `RequestQueue`

Reads/writes:

- worker payload JSON
- worker result JSON

Runtime role:

- short-lived worker process

## `pipeline/stages/parse.py:parse_page`

Responsibility:

- strip HTML to text
- detect emails and phones
- extract named contacts from simple regex patterns
- detect social URLs
- detect menu providers
- detect schema.org local business hints
- return `ParsedPage`

Called by:

- `pipeline/pipeline.py:PipelineRunner.run_enrich`

Calls:

- regex-based helpers only

Reads/writes:

- pure transformation; no persistence

Runtime role:

- transformation step inside enrich

## `pipeline/stages/resolve.py:resolve_and_upsert_locations`

Responsibility:

- map parsed seed/page signals to an existing or new canonical location
- match by domain, then phone, then name+state
- create merge suggestions on collision

Called by:

- `pipeline/pipeline.py:PipelineRunner.run_enrich`

Calls:

- SQLite

Reads/writes:

- `organizations`
- `companies`
- `locations`
- `domains`
- `entity_resolutions`

Runtime role:

- canonicalization step inside enrich

## `pipeline/stages/enrich.py:run_waterfall_enrichment`

Responsibility:

- add a light heuristic enrichment pass after parse/resolve
- infer a candidate email from contact name + domain when direct email is absent
- update verification and location timestamps

Called by:

- `pipeline/pipeline.py:PipelineRunner.run_enrich`

Calls:

- SQLite

Reads/writes:

- `contacts`
- `evidence`
- `locations`

Runtime role:

- final enrichment step inside enrich

## `pipeline/stages/score.py:run_score`

Responsibility:

- compute heuristic lead score and tier
- persist feature vector for explainability

Called by:

- `pipeline/pipeline.py:PipelineRunner.run_score`

Calls:

- `score_location`

Reads/writes:

- reads `locations`, `contacts`, `contact_points`, `evidence`
- writes `lead_scores`, `scoring_features`
- updates `locations.fit_score`

Runtime role:

- ranking subsystem

## `pipeline/stages/research.py:build_lead_research_briefs`

Responsibility:

- turn scored leads into operator/agent follow-up briefs
- identify research gaps
- derive target roles
- propose public site paths to inspect
- produce a recommended next action and summary

Called by:

- `pipeline/stages/research.py:run_lead_research`
- `pipeline/stages/export.py:export_agent_research_queue`
- `pipeline/stages/export.py:export_lead_intelligence_dossier`

Calls:

- multiple DB query helpers in the same module

Reads/writes:

- reads locations, lead_scores, contacts, contact_points, evidence
- writes via `_upsert_research_evidence` when run as a stage

Runtime role:

- post-score enhancement subsystem

## `pipeline/stages/export.py`

Responsibility:

- shape canonical DB state into downstream CSV contracts
- apply segment filtering
- produce legacy compatibility outputs
- create new-leads and buyer-signal views
- generate agent research export
- generate lead-intelligence index files
- generate per-lead package scaffolds and agent handoff artifacts

Called by:

- `pipeline/pipeline.py:PipelineRunner.run_export`

Calls:

- `pipeline/stages/research.py:build_lead_research_briefs`
- `pipeline/quality.py:run_quality_report`

Reads/writes:

- reads canonical DB tables
- writes `out/*.csv`, `out/*.json`, and markdown package files under `out/lead_intelligence/leads/`

Runtime role:

- output adapter layer

## `pipeline/run_state.py`

Responsibility:

- create and store resumable run checkpoints
- track stage completion state
- expose recovery pointer semantics

Called by:

- `cli/sync.py`
- `cli/query.py`

Reads/writes:

- `data/state/agent_runs/run_<id>.json`

Runtime role:

- run lifecycle metadata store

## `pipeline/run_control.py`

Responsibility:

- store live domain runtime counters
- store manual and automatic interventions
- provide a bounded control surface for agents

Called by:

- `cli/control.py`
- `cli/query.py`
- `pipeline/fetch_backends/crawlee_backend.py`
- `cli/sync.py`

Reads/writes:

- `data/state/agent_runs/control_<id>.json`

Runtime role:

- live run state store and control plane

## `cli/query.py`

Responsibility:

- provide human/agent inspection surface over local state
- status snapshot
- safe read-only SQL
- curated search presets

Called by:

- `cli/app.py`

Calls:

- SQLite read-only mode
- run-state and run-control loaders

Reads/writes:

- reads DB
- reads manifest
- reads outputs snapshot metadata

Runtime role:

- inspection/query surface

## `jobs/ingest_sources.py`

Responsibility:

- apply/validate schema
- ingest adapter rows into canonical DB

Called by:

- `cli/doctor.py`
- `run_v4.sh`
- humans/operators

Calls:

- `adapters/registry.py:build_adapters`

Reads/writes:

- `db/schema.sql`
- canonical DB

Runtime role:

- bootstrap and maintenance path

Assumption:

Because only the seeds adapter is enabled, this path is currently more of a schema/bootstrap guardrail than a rich ingestion subsystem.
