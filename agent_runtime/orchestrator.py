from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from pipeline.utils import utcnow_iso
from runtime_context import TenantContext

from agent_runtime.config import AgentConfig
from agent_runtime.memory import MemoryStore, SessionStore
from agent_runtime.models import ModelAdapter, ModelMessage
from agent_runtime.tools import ToolRegistry


class AgentOrchestrator:
    def __init__(
        self,
        *,
        config: AgentConfig,
        model_adapter: ModelAdapter,
        session_store: SessionStore,
        memory_store: MemoryStore,
        tool_registry: ToolRegistry,
        trace_hook: Callable[[dict[str, Any]], None] | None = None,
    ):
        self.config = config
        self.model_adapter = model_adapter
        self.session_store = session_store
        self.memory_store = memory_store
        self.tool_registry = tool_registry
        self.trace_hook = trace_hook

    def run(self, goal: str, tenant_context: TenantContext, session_id: str | None = None) -> dict[str, Any]:
        session = self._ensure_session(goal=goal, tenant_id=str(tenant_context.tenant_id or ""), session_id=session_id)
        self._emit_trace(
            {
                "type": "session_started",
                "tenant_id": tenant_context.tenant_id,
                "session_id": session["session_id"],
                "goal": goal,
            }
        )
        self.session_store.append_turn(session["session_id"], role="user", agent_name="SupervisorAgent", content=goal, metadata={"at": utcnow_iso()})
        before_events = len(self.session_store.list_tool_events(session["session_id"]))
        try:
            run_ops_summary = self._run_ops_agent(goal=goal, session_id=session["session_id"])
            snapshot = self._build_snapshot(session["session_id"], new_event_offset=before_events)
            review_summary = self._run_summary_agent(
                agent_name="ReviewAgent",
                instructions=self._review_agent_instructions(),
                context=self._review_context(goal=goal, snapshot=snapshot),
                session_id=session["session_id"],
            )
            client_brief = self._run_summary_agent(
                agent_name="ClientBriefAgent",
                instructions=self._client_brief_instructions(),
                context=self._client_brief_context(goal=goal, snapshot=snapshot),
                session_id=session["session_id"],
            )
            supervisor_summary = self._run_summary_agent(
                agent_name="SupervisorAgent",
                instructions=self._supervisor_agent_instructions(),
                context=self._supervisor_context(goal=goal, run_ops_summary=run_ops_summary, review_summary=review_summary, client_brief=client_brief, snapshot=snapshot),
                session_id=session["session_id"],
            )
            unresolved_risks = self._derive_unresolved_risks(snapshot)
            next_actions = self._derive_next_actions(snapshot)
            summary = {
                "run_ops_summary": run_ops_summary,
                "review_summary": review_summary,
                "client_brief": client_brief,
                "supervisor_summary": supervisor_summary,
            }
            latest_run_id = snapshot["run_ids"][-1] if snapshot["run_ids"] else ""
            updated_session = self.session_store.update_session(
                session["session_id"],
                status="completed",
                last_run_id=latest_run_id,
                summary=summary,
                unresolved_risks=unresolved_risks,
                recommended_next_actions=next_actions,
            )
            result = {
                "tenant_id": tenant_context.tenant_id,
                "session_id": updated_session["session_id"],
                "goal": goal,
                "tools_used": snapshot["tools_used"],
                "run_ids": snapshot["run_ids"],
                "exports": snapshot["exports"],
                "unresolved_risks": unresolved_risks,
                "recommended_next_actions": next_actions,
                "memory_updates": snapshot["memory_updates"],
                "summaries": summary,
            }
            self._emit_trace(
                {
                    "type": "session_completed",
                    "tenant_id": tenant_context.tenant_id,
                    "session_id": updated_session["session_id"],
                    "run_ids": snapshot["run_ids"],
                    "tools_used": snapshot["tools_used"],
                }
            )
            return result
        except Exception as exc:
            self.session_store.update_session(session["session_id"], status="failed")
            self._emit_trace(
                {
                    "type": "session_failed",
                    "tenant_id": tenant_context.tenant_id,
                    "session_id": session["session_id"],
                    "error": str(exc),
                }
            )
            raise

    def status(self, session_id: str | None, *, tenant_id: str) -> dict[str, Any]:
        session = self.session_store.get_session(session_id) if session_id else self.session_store.latest_session(tenant_id)
        if not session:
            return {
                "tenant_id": tenant_id,
                "session": {},
                "turns": [],
                "tool_events": [],
                "run_memory": [],
                "domain_tactics": [],
                "client_profiles": [],
            }
        return {
            "tenant_id": tenant_id,
            "session": session,
            "turns": self.session_store.list_turns(session["session_id"], limit=50, tail=True),
            "tool_events": self.session_store.list_tool_events(session["session_id"], limit=50, tail=True),
            "run_memory": self.memory_store.list_run_memory(limit=20),
            "domain_tactics": self.memory_store.list_domain_tactics(limit=20),
            "client_profiles": self.memory_store.list_client_profiles(limit=20),
        }

    def _ensure_session(self, *, goal: str, tenant_id: str, session_id: str | None) -> dict[str, Any]:
        if session_id:
            session = self.session_store.get_session(session_id)
            return session
        return self.session_store.create_session(
            tenant_id=tenant_id,
            goal=goal,
            model_provider=self.model_adapter.provider_name,
            model_name=self.config.model,
        )

    def _run_ops_agent(self, *, goal: str, session_id: str) -> str:
        memory_context = self._memory_context()
        user_context = "\n\n".join(
            [
                f"Operator goal:\n{goal}",
                f"Known memory:\n{memory_context}",
                "Prefer bounded runs, evidence-first reasoning, and explicit operator-safe conclusions.",
            ]
        )
        messages = [ModelMessage(role="user", content=user_context)]
        last_text = ""
        previous_response_id: str | None = None
        for _ in range(self.config.max_turns):
            response = self.model_adapter.generate(
                agent_name="RunOpsAgent",
                instructions=self._run_ops_instructions(),
                messages=messages,
                tools=self.tool_registry.definitions(),
                model=self.config.model,
                previous_response_id=previous_response_id,
            )
            if response.response_id:
                previous_response_id = response.response_id
            if response.text:
                last_text = response.text
                self.session_store.append_turn(session_id, role="assistant", agent_name="RunOpsAgent", content=response.text)
                self._emit_trace(
                    {
                        "type": "agent_message",
                        "agent_name": "RunOpsAgent",
                        "session_id": session_id,
                        "text": response.text,
                    }
                )
            if not response.tool_calls:
                break
            next_messages: list[ModelMessage] = []
            if response.text and not response.response_id:
                next_messages.append(ModelMessage(role="assistant", content=response.text))
            for call in response.tool_calls:
                self._emit_trace(
                    {
                        "type": "tool_call_requested",
                        "session_id": session_id,
                        "tool_name": call.name,
                        "arguments": call.arguments,
                    }
                )
                result = self.tool_registry.invoke(session_id=session_id, tool_name=call.name, arguments=call.arguments)
                self._emit_trace(
                    {
                        "type": "tool_call_completed",
                        "session_id": session_id,
                        "tool_name": call.name,
                        "ok": result.get("ok", False),
                        "summary": self._summarize_tool_result(result),
                    }
                )
                next_messages.append(
                    ModelMessage(
                        role="tool",
                        type="function_call_output",
                        call_id=call.call_id or f"{call.name}-call",
                        content=json.dumps(result, default=str),
                    )
                )
            messages = next_messages
        else:
            last_text = (last_text + "\n" if last_text else "") + "RunOpsAgent reached the configured max turn limit."
        return last_text

    def _run_summary_agent(self, *, agent_name: str, instructions: str, context: str, session_id: str) -> str:
        response = self.model_adapter.generate(
            agent_name=agent_name,
            instructions=instructions,
            messages=[ModelMessage(role="user", content=context)],
            tools=[],
            model=self.config.model,
            previous_response_id=None,
        )
        text = response.text.strip() or f"{agent_name} produced no summary."
        self.session_store.append_turn(session_id, role="assistant", agent_name=agent_name, content=text)
        self._emit_trace(
            {
                "type": "agent_message",
                "agent_name": agent_name,
                "session_id": session_id,
                "text": text,
            }
        )
        return text

    def _build_snapshot(self, session_id: str, *, new_event_offset: int) -> dict[str, Any]:
        events = self.session_store.list_tool_events(session_id, limit=200)
        new_events = events[new_event_offset:]
        tools_used = [event["tool_name"] for event in new_events]
        run_ids: list[str] = []
        exports: list[dict[str, Any]] = []
        domains_updated: list[str] = []
        export_keys = (
            "records_csv",
            "records_json",
            "review_queue_csv",
            "sales_report_csv",
            "profiles_dir",
            "evidence_dir",
            "outreach_dir",
            "dossiers_dir",
            "dossiers_csv",
            "dossiers_json",
            "internal_review_dir",
            "internal_review_csv",
            "internal_review_json",
        )
        for event in new_events:
            output_payload = dict(event.get("output") or {})
            data = dict(output_payload.get("data") or output_payload)
            run_id = data.get("run_id")
            if isinstance(run_id, str) and run_id and run_id not in run_ids:
                run_ids.append(run_id)
            artifact_sources = [data]
            nested_report = data.get("report")
            if isinstance(nested_report, dict):
                artifact_sources.append(nested_report)
            for source in artifact_sources:
                for key in export_keys:
                    if key in source:
                        export_item = {"tool": event["tool_name"], "key": key, "path": source[key]}
                        if export_item not in exports:
                            exports.append(export_item)
            if event["tool_name"] == "control_apply":
                domain = str((event.get("input") or {}).get("domain") or "").strip()
                if domain and domain not in domains_updated:
                    domains_updated.append(domain)
        return {
            "events": new_events,
            "tools_used": tools_used,
            "run_ids": run_ids,
            "exports": exports,
            "memory_updates": {
                "run_memory": run_ids,
                "domain_tactics": domains_updated,
                "client_profile_used": self.config.default_client_id,
            },
            "latest_status": self._latest_success_data(new_events, "status"),
            "latest_search_review_queue": self._latest_search_data(new_events, "review-queue"),
            "latest_search_contradictions": self._latest_search_data(new_events, "contradictions"),
            "latest_search_blocked_domains": self._latest_search_data(new_events, "blocked-domains"),
        }

    @staticmethod
    def _latest_success_data(events: list[dict[str, Any]], tool_name: str) -> dict[str, Any]:
        for event in reversed(events):
            if event["tool_name"] != tool_name or event["status"] != "completed":
                continue
            output_payload = dict(event.get("output") or {})
            return dict(output_payload.get("data") or output_payload)
        return {}

    @staticmethod
    def _latest_search_data(events: list[dict[str, Any]], preset: str) -> dict[str, Any]:
        for event in reversed(events):
            if event["tool_name"] != "search" or event["status"] != "completed":
                continue
            output_payload = dict(event.get("output") or {})
            data = dict(output_payload.get("data") or output_payload)
            if data.get("preset") == preset:
                return data
        return {}

    def _memory_context(self) -> str:
        client_profile = self.memory_store.get_client_profile(self.config.default_client_id)
        tactics = self.memory_store.list_domain_tactics(limit=10)
        runs = self.memory_store.list_run_memory(limit=5)
        parts = [
            f"Client profile: {json.dumps(client_profile, default=str, sort_keys=True) if client_profile else '{}'}",
            f"Recent domain tactics: {json.dumps(tactics, default=str, sort_keys=True)}",
            f"Recent runs: {json.dumps(runs, default=str, sort_keys=True)}",
        ]
        return "\n".join(parts)

    def _emit_trace(self, event: dict[str, Any]) -> None:
        if self.trace_hook is not None:
            self.trace_hook(event)

    @staticmethod
    def _summarize_tool_result(result: dict[str, Any]) -> str:
        data = dict(result.get("data") or {})
        payload = dict(data.get("data") or data)
        if not result.get("ok", False):
            error = dict(data.get("error") or result.get("error") or {})
            return str(error.get("message") or "tool failed")
        if "run_id" in payload:
            return f"run_id={payload['run_id']}"
        if "row_count" in payload:
            return f"row_count={payload['row_count']}"
        counts = payload.get("counts")
        if isinstance(counts, dict):
            records = counts.get("records")
            review_queue = counts.get("review_queue")
            contradictions = counts.get("contradictions")
            parts = []
            if records is not None:
                parts.append(f"records={records}")
            if review_queue is not None:
                parts.append(f"review_queue={review_queue}")
            if contradictions is not None:
                parts.append(f"contradictions={contradictions}")
            if parts:
                return ", ".join(parts)
        export_keys = [key for key in ("records_csv", "records_json", "review_queue_csv", "sales_report_csv") if key in payload]
        if export_keys:
            return "exports=" + ",".join(export_keys)
        if "ok" in payload:
            return f"ok={payload['ok']}"
        return "completed"

    @staticmethod
    def _run_ops_instructions() -> str:
        return (
            "You are RunOpsAgent for an evidence-first provider intelligence engine. "
            "Use tools to inspect runtime state, run or resume bounded sync loops, diagnose blocked domains, "
            "triage review lanes, inspect exported artifacts, and export approved artifacts. "
            "Treat sync as a strictly bounded validation loop: refresh mode only, 2 or 3 seeds at a time, and small export limits. "
            "If a run yields weak or empty output, inspect the run state and exported artifacts before trying another bounded sync. "
            "Never claim provider truth without the deterministic runtime."
        )

    @staticmethod
    def _review_agent_instructions() -> str:
        return (
            "You are ReviewAgent. Explain unresolved review-queue, contradiction, and QA risks from the provided structured data. "
            "Do not invent evidence or override the deterministic runtime."
        )

    @staticmethod
    def _client_brief_instructions() -> str:
        return (
            "You are ClientBriefAgent. Produce a concise operator-facing summary of what happened, what artifacts exist, and what follow-up matters. "
            "Be factual and evidence-first."
        )

    @staticmethod
    def _supervisor_agent_instructions() -> str:
        return (
            "You are SupervisorAgent. Synthesize run operations, review analysis, and client briefing into a short trusted summary. "
            "Prefer direct statements and explicit next actions."
        )

    def _review_context(self, *, goal: str, snapshot: dict[str, Any]) -> str:
        return json.dumps(
            {
                "goal": goal,
                "status": snapshot.get("latest_status", {}),
                "review_queue": snapshot.get("latest_search_review_queue", {}),
                "contradictions": snapshot.get("latest_search_contradictions", {}),
                "blocked_domains": snapshot.get("latest_search_blocked_domains", {}),
            },
            indent=2,
            default=str,
        )

    def _client_brief_context(self, *, goal: str, snapshot: dict[str, Any]) -> str:
        return json.dumps(
            {
                "goal": goal,
                "exports": snapshot.get("exports", []),
                "run_ids": snapshot.get("run_ids", []),
                "status": snapshot.get("latest_status", {}),
                "memory_updates": snapshot.get("memory_updates", {}),
            },
            indent=2,
            default=str,
        )

    def _supervisor_context(
        self,
        *,
        goal: str,
        run_ops_summary: str,
        review_summary: str,
        client_brief: str,
        snapshot: dict[str, Any],
    ) -> str:
        return json.dumps(
            {
                "goal": goal,
                "run_ops_summary": run_ops_summary,
                "review_summary": review_summary,
                "client_brief": client_brief,
                "events": snapshot.get("events", []),
                "exports": snapshot.get("exports", []),
            },
            indent=2,
            default=str,
        )

    @staticmethod
    def _derive_unresolved_risks(snapshot: dict[str, Any]) -> list[str]:
        risks: list[str] = []
        status = dict(snapshot.get("latest_status") or {})
        counts = dict(status.get("counts") or {})
        if int(counts.get("review_queue", 0) or 0) > 0:
            risks.append(f"{counts['review_queue']} review-queue items still need operator attention.")
        if int(counts.get("contradictions", 0) or 0) > 0:
            risks.append(f"{counts['contradictions']} contradictions remain unresolved.")
        blocked = dict(snapshot.get("latest_search_blocked_domains") or {})
        if int(blocked.get("row_count", 0) or 0) > 0:
            risks.append(f"{blocked['row_count']} blocked or throttled domains may require control actions.")
        for event in snapshot.get("events", []):
            if event["status"] == "failed":
                risks.append(f"Tool failure: {event['tool_name']} - {((event.get('output') or {}).get('error') or {}).get('message', 'unknown error')}")
        return risks

    @staticmethod
    def _derive_next_actions(snapshot: dict[str, Any]) -> list[str]:
        actions: list[str] = []
        blocked = dict(snapshot.get("latest_search_blocked_domains") or {})
        if int(blocked.get("row_count", 0) or 0) > 0:
            actions.append("Inspect blocked domains and apply bounded run controls where justified.")
        review_queue = dict(snapshot.get("latest_search_review_queue") or {})
        if int(review_queue.get("row_count", 0) or 0) > 0:
            actions.append("Review queued records before treating them as provider truth.")
        if snapshot.get("exports"):
            actions.append("Use the latest exported artifacts for operator follow-up and client-facing reporting.")
        if not actions:
            actions.append("No immediate corrective action detected; continue monitoring bounded runs.")
        return actions
