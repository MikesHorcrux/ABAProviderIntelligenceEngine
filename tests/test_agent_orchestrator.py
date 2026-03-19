#!/usr/bin/env python3.11
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from agent_runtime.config import AgentConfig
from agent_runtime.memory import MemoryStore, SessionStore
from agent_runtime.models import ModelAdapter, ModelResponse, ToolCall, ToolDefinition
from agent_runtime.orchestrator import AgentOrchestrator
from cli.errors import ConfigError
from pipeline.utils import normalize_domain, utcnow_iso
from runtime_context import build_tenant_context


class FakeModelAdapter(ModelAdapter):
    provider_name = "fake"

    def __init__(self, scripts: dict[str, list[ModelResponse]]):
        self.scripts = {key: list(value) for key, value in scripts.items()}

    def generate(self, *, agent_name: str, instructions: str, messages, tools, model: str, previous_response_id: str | None = None) -> ModelResponse:  # noqa: ANN001
        del instructions, messages, tools, model, previous_response_id
        queue = self.scripts.get(agent_name) or []
        if not queue:
            return ModelResponse(text=f"{agent_name} idle")
        return queue.pop(0)


class FakeToolRegistry:
    def __init__(self, *, tenant_id: str, tenant_root: Path, session_store: SessionStore, memory_store: MemoryStore, fail_sync_once: bool = False):
        self.tenant_id = tenant_id
        self.tenant_root = tenant_root
        self.session_store = session_store
        self.memory_store = memory_store
        self.fail_sync_once = fail_sync_once
        self.sync_attempts = 0

    def definitions(self) -> list[ToolDefinition]:
        return [
            ToolDefinition(name="doctor", description="doctor", parameters={"type": "object", "properties": {"reason": {"type": "string"}}, "required": ["reason"]}),
            ToolDefinition(name="sync", description="sync", parameters={"type": "object", "properties": {"reason": {"type": "string"}}, "required": ["reason"]}),
            ToolDefinition(name="resume", description="resume", parameters={"type": "object", "properties": {"reason": {"type": "string"}, "resume": {"type": "string"}}, "required": ["reason", "resume"]}),
            ToolDefinition(name="status", description="status", parameters={"type": "object", "properties": {"reason": {"type": "string"}}, "required": ["reason"]}),
            ToolDefinition(name="search", description="search", parameters={"type": "object", "properties": {"reason": {"type": "string"}, "preset": {"type": "string"}}, "required": ["reason"]}),
            ToolDefinition(name="control_apply", description="control", parameters={"type": "object", "properties": {"reason": {"type": "string"}, "action": {"type": "string"}, "domain": {"type": "string"}}, "required": ["reason", "action", "domain"]}),
            ToolDefinition(name="export", description="export", parameters={"type": "object", "properties": {"reason": {"type": "string"}}, "required": ["reason"]}),
        ]

    def invoke(self, *, session_id: str, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        started_at = utcnow_iso()
        data = self._data(tool_name, arguments)
        status = "failed" if data.get("ok") is False and "error" in data else "completed"
        event = self.session_store.record_tool_event(
            session_id=session_id,
            tenant_id=self.tenant_id,
            tool_name=tool_name,
            reason=str(arguments.get("reason") or ""),
            input_payload=arguments,
            output_payload=data,
            status=status,
            started_at=started_at,
            completed_at=utcnow_iso(),
        )
        if tool_name in {"sync", "resume"} and status == "completed":
            self.memory_store.record_run_memory(
                run_id=str(data["data"]["run_id"]),
                session_id=session_id,
                summary=dict(data["data"]["summary"]),
                report=dict(data["data"]["report"]),
            )
        if tool_name == "control_apply" and status == "completed":
            self.memory_store.upsert_domain_tactic(
                domain=normalize_domain(str(arguments.get("domain") or "")),
                tactic={"action": arguments.get("action"), "reason": arguments.get("reason")},
                last_confirmed_source_url="",
                last_confirmed_at=utcnow_iso(),
                decay_at="2026-03-28T00:00:00Z",
            )
        return {
            "ok": status == "completed",
            "tenant_id": self.tenant_id,
            "session_id": session_id,
            "tool_name": tool_name,
            "started_at": started_at,
            "completed_at": utcnow_iso(),
            "trace_event_id": event["event_id"],
            "data": data.get("data", {}),
            **({"error": data["error"]} if "error" in data else {}),
        }

    def _data(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        exports_root = self.tenant_root / "out" / "provider_intel"
        run_id = f"{self.tenant_id}-run-{self.sync_attempts + 1}"
        if tool_name == "doctor":
            return {"data": {"ok": True, "summary": {"failed": 0}}}
        if tool_name == "sync":
            self.sync_attempts += 1
            if self.fail_sync_once and self.sync_attempts == 1:
                return {"ok": False, "error": {"code": "runtime_error", "message": "synthetic sync failure", "details": {}}}
            return {
                "data": {
                    "run_id": run_id,
                    "summary": {"approved": 2, "queued": 1},
                    "report": {
                        "records_csv": str(exports_root / f"provider_records_{run_id}.csv"),
                        "review_queue_csv": str(exports_root / f"review_queue_{run_id}.csv"),
                        "sales_report_csv": str(exports_root / f"sales_report_{run_id}.csv"),
                    },
                }
            }
        if tool_name == "resume":
            return {
                "data": {
                    "run_id": f"{self.tenant_id}-resume-1",
                    "summary": {"approved": 3, "queued": 0},
                    "report": {
                        "records_csv": str(exports_root / f"provider_records_{self.tenant_id}-resume-1.csv"),
                        "review_queue_csv": str(exports_root / f"review_queue_{self.tenant_id}-resume-1.csv"),
                        "sales_report_csv": str(exports_root / f"sales_report_{self.tenant_id}-resume-1.csv"),
                    },
                }
            }
        if tool_name == "status":
            return {
                "data": {
                    "counts": {"review_queue": 1, "contradictions": 1, "records": 3},
                    "outputs": {"records_csv": {"path": str(exports_root / "latest.csv")}},
                }
            }
        if tool_name == "search":
            preset = arguments.get("preset")
            rows = [{"seed_domain": "blocked.example"}] if preset == "blocked-domains" else [{"review_type": "record_review"}]
            if preset == "contradictions":
                rows = [{"field_name": "license_status"}]
            return {"data": {"preset": preset, "row_count": len(rows), "rows": rows}}
        if tool_name == "control_apply":
            return {"data": {"domains": {str(arguments["domain"]): {"stop_requested": True}}}}
        if tool_name == "export":
            return {
                "data": {
                    "records_csv": str(exports_root / "provider_records_latest.csv"),
                    "records_json": str(exports_root / "provider_records_latest.json"),
                    "review_queue_csv": str(exports_root / "review_queue_latest.csv"),
                    "sales_report_csv": str(exports_root / "sales_report_latest.csv"),
                    "profiles_dir": str(exports_root / "profiles"),
                    "evidence_dir": str(exports_root / "evidence"),
                    "outreach_dir": str(exports_root / "outreach"),
                }
            }
        raise AssertionError(f"Unhandled tool: {tool_name}")


def _build_orchestrator(*, tenant_id: str, tenant_root_base: Path, fail_sync_once: bool = False) -> tuple[AgentOrchestrator, Any]:
    context = build_tenant_context(tenant_id=tenant_id, tenant_root_base=tenant_root_base)
    session_store = SessionStore(context.runtime_paths.agent_memory_db_path)
    memory_store = MemoryStore(context.runtime_paths.agent_memory_db_path)
    memory_store.upsert_client_profile(client_id="default", client_name="Default", profile={"geo": "NJ"})
    model = FakeModelAdapter(
        {
            "RunOpsAgent": [
                ModelResponse(
                    text="Running operator loop.",
                    tool_calls=[
                        ToolCall("call_doctor", "doctor", {"reason": "Validate tenant runtime"}),
                        ToolCall("call_sync", "sync", {"reason": "Run a bounded sync"}),
                        ToolCall("call_status", "status", {"reason": "Inspect run state"}),
                        ToolCall("call_review", "search", {"reason": "Inspect review queue", "preset": "review-queue"}),
                        ToolCall("call_contra", "search", {"reason": "Inspect contradictions", "preset": "contradictions"}),
                        ToolCall("call_blocked", "search", {"reason": "Inspect blocked domains", "preset": "blocked-domains"}),
                        ToolCall("call_control", "control_apply", {"reason": "Stop a blocked domain", "action": "stop-domain", "domain": "blocked.example"}),
                        ToolCall("call_export", "export", {"reason": "Collect current exports"}),
                    ],
                ),
                ModelResponse(text="RunOpsAgent complete."),
            ],
            "ReviewAgent": [ModelResponse(text="Review queue and contradiction counts still require follow-up.")],
            "ClientBriefAgent": [ModelResponse(text="Artifacts are ready for operator review.")],
            "SupervisorAgent": [ModelResponse(text="Agent session completed with evidence-first outputs.")],
        }
    )
    registry = FakeToolRegistry(
        tenant_id=tenant_id,
        tenant_root=context.runtime_paths.tenant_root or tenant_root_base,
        session_store=session_store,
        memory_store=memory_store,
        fail_sync_once=fail_sync_once,
    )
    orchestrator = AgentOrchestrator(
        config=AgentConfig(provider="fake", model="fake-model"),
        model_adapter=model,
        session_store=session_store,
        memory_store=memory_store,
        tool_registry=registry,
    )
    return orchestrator, context


def test_agent_orchestrator_runs_full_operator_loop_and_records_memory() -> None:
    with tempfile.TemporaryDirectory() as td:
        orchestrator, context = _build_orchestrator(tenant_id="tenant-a", tenant_root_base=Path(td))
        result = orchestrator.run("Find NJ providers worth outbound this week", context)
        assert result["tenant_id"] == "tenant-a"
        assert "sync" in result["tools_used"]
        assert result["run_ids"] == ["tenant-a-run-1"]
        assert result["memory_updates"]["domain_tactics"] == ["blocked.example"]
        assert all(str(Path(item["path"])).startswith(str(context.runtime_paths.tenant_root)) for item in result["exports"])
        status = orchestrator.status(result["session_id"], tenant_id="tenant-a")
        assert status["session"]["status"] == "completed"
        assert len(status["tool_events"]) >= 8


def test_agent_orchestrator_isolates_tenants() -> None:
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        orchestrator_a, context_a = _build_orchestrator(tenant_id="tenant-a", tenant_root_base=base)
        orchestrator_b, context_b = _build_orchestrator(tenant_id="tenant-b", tenant_root_base=base)
        result_a = orchestrator_a.run("Goal A", context_a)
        result_b = orchestrator_b.run("Goal B", context_b)
        assert context_a.runtime_paths.agent_memory_db_path != context_b.runtime_paths.agent_memory_db_path
        assert result_a["run_ids"] == ["tenant-a-run-1"]
        assert result_b["run_ids"] == ["tenant-b-run-1"]
        assert str(context_a.runtime_paths.tenant_root) in result_a["exports"][0]["path"]
        assert str(context_b.runtime_paths.tenant_root) in result_b["exports"][0]["path"]


def test_agent_orchestrator_can_resume_after_failed_sync() -> None:
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        orchestrator, context = _build_orchestrator(tenant_id="tenant-c", tenant_root_base=base, fail_sync_once=True)
        first = orchestrator.run("Recover a failed run", context)
        assert any("Tool failure: sync" in item for item in first["unresolved_risks"])

        orchestrator.model_adapter = FakeModelAdapter(
            {
                "RunOpsAgent": [
                    ModelResponse(
                        text="Resuming prior work.",
                        tool_calls=[
                            ToolCall("call_resume", "resume", {"reason": "Resume latest failed run", "resume": "latest"}),
                            ToolCall("call_status", "status", {"reason": "Check resumed state"}),
                        ],
                    ),
                    ModelResponse(text="Resume complete."),
                ],
                "ReviewAgent": [ModelResponse(text="No major review blockers remain.")],
                "ClientBriefAgent": [ModelResponse(text="Resumed artifacts are ready.")],
                "SupervisorAgent": [ModelResponse(text="Session recovered successfully.")],
            }
        )
        second = orchestrator.run("Recover a failed run", context, session_id=first["session_id"])
        assert "tenant-c-resume-1" in second["run_ids"]
        status = orchestrator.status(first["session_id"], tenant_id="tenant-c")
        tool_names = [event["tool_name"] for event in status["tool_events"]]
        assert "sync" in tool_names
        assert "resume" in tool_names


def test_agent_orchestrator_snapshot_collects_nested_sync_report_exports() -> None:
    with tempfile.TemporaryDirectory() as td:
        orchestrator, context = _build_orchestrator(tenant_id="tenant-d", tenant_root_base=Path(td))
        result = orchestrator.run("Capture sync exports", context)
        export_pairs = {(item["tool"], item["key"]) for item in result["exports"]}
        assert ("sync", "records_csv") in export_pairs
        assert ("sync", "review_queue_csv") in export_pairs
        assert ("sync", "sales_report_csv") in export_pairs


def test_agent_orchestrator_emits_trace_events() -> None:
    with tempfile.TemporaryDirectory() as td:
        events: list[dict[str, Any]] = []
        context = build_tenant_context(tenant_id="tenant-trace", tenant_root_base=Path(td))
        session_store = SessionStore(context.runtime_paths.agent_memory_db_path)
        memory_store = MemoryStore(context.runtime_paths.agent_memory_db_path)
        model = FakeModelAdapter(
            {
                "RunOpsAgent": [
                    ModelResponse(
                        text="Checking runtime.",
                        tool_calls=[ToolCall("call_status", "status", {"reason": "Inspect counts"})],
                    ),
                    ModelResponse(text="RunOpsAgent complete."),
                ],
                "ReviewAgent": [ModelResponse(text="Review summary.")],
                "ClientBriefAgent": [ModelResponse(text="Client summary.")],
                "SupervisorAgent": [ModelResponse(text="Supervisor summary.")],
            }
        )
        registry = FakeToolRegistry(
            tenant_id="tenant-trace",
            tenant_root=context.runtime_paths.tenant_root or Path(td),
            session_store=session_store,
            memory_store=memory_store,
        )
        orchestrator = AgentOrchestrator(
            config=AgentConfig(provider="fake", model="fake-model"),
            model_adapter=model,
            session_store=session_store,
            memory_store=memory_store,
            tool_registry=registry,
            trace_hook=events.append,
        )
        result = orchestrator.run("Inspect trace behavior", context)

        assert result["session_id"]
        event_types = [event["type"] for event in events]
        assert "session_started" in event_types
        assert "tool_call_requested" in event_types
        assert "tool_call_completed" in event_types
        assert "session_completed" in event_types


def test_agent_orchestrator_rejects_cross_tenant_session_ids_for_run_and_status() -> None:
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        shared_db_path = base / "shared" / "agent_memory.db"
        context_a = build_tenant_context(tenant_id="tenant-a", tenant_root_base=base, db_path=shared_db_path)
        context_b = build_tenant_context(tenant_id="tenant-b", tenant_root_base=base, db_path=shared_db_path)
        session_store = SessionStore(shared_db_path)
        memory_store = MemoryStore(shared_db_path)

        orchestrator_a = AgentOrchestrator(
            config=AgentConfig(provider="fake", model="fake-model"),
            model_adapter=FakeModelAdapter(
                {
                    "RunOpsAgent": [ModelResponse(text="RunOpsAgent complete.")],
                    "ReviewAgent": [ModelResponse(text="Review summary.")],
                    "ClientBriefAgent": [ModelResponse(text="Client summary.")],
                    "SupervisorAgent": [ModelResponse(text="Supervisor summary.")],
                }
            ),
            session_store=session_store,
            memory_store=memory_store,
            tool_registry=FakeToolRegistry(
                tenant_id="tenant-a",
                tenant_root=context_a.runtime_paths.tenant_root or base,
                session_store=session_store,
                memory_store=memory_store,
            ),
        )
        orchestrator_b = AgentOrchestrator(
            config=AgentConfig(provider="fake", model="fake-model"),
            model_adapter=FakeModelAdapter(
                {
                    "RunOpsAgent": [ModelResponse(text="RunOpsAgent complete.")],
                    "ReviewAgent": [ModelResponse(text="Review summary.")],
                    "ClientBriefAgent": [ModelResponse(text="Client summary.")],
                    "SupervisorAgent": [ModelResponse(text="Supervisor summary.")],
                }
            ),
            session_store=session_store,
            memory_store=memory_store,
            tool_registry=FakeToolRegistry(
                tenant_id="tenant-b",
                tenant_root=context_b.runtime_paths.tenant_root or base,
                session_store=session_store,
                memory_store=memory_store,
            ),
        )

        result_a = orchestrator_a.run("Tenant A goal", context_a)
        session_id = str(result_a["session_id"])

        try:
            orchestrator_b.status(session_id, tenant_id="tenant-b")
        except ConfigError as exc:
            assert str(exc) == f"Agent session not found for tenant tenant-b: {session_id}"
        else:
            raise AssertionError("Expected status lookup to reject a session owned by another tenant.")

        try:
            orchestrator_b.run("Tenant B goal", context_b, session_id=session_id)
        except ConfigError as exc:
            assert str(exc) == f"Agent session not found for tenant tenant-b: {session_id}"
        else:
            raise AssertionError("Expected run(session_id=...) to reject a session owned by another tenant.")


def main() -> None:
    test_agent_orchestrator_runs_full_operator_loop_and_records_memory()
    test_agent_orchestrator_isolates_tenants()
    test_agent_orchestrator_can_resume_after_failed_sync()
    test_agent_orchestrator_snapshot_collects_nested_sync_report_exports()
    test_agent_orchestrator_emits_trace_events()
    test_agent_orchestrator_rejects_cross_tenant_session_ids_for_run_and_status()
    print("test_agent_orchestrator: ok")


if __name__ == "__main__":
    main()
