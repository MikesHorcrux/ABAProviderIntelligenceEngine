from __future__ import annotations

from copy import deepcopy
import json
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

from cli.control import run_control_apply, run_control_show
from cli.doctor import run_doctor
from cli.errors import classify_exception
from cli.query import run_search, run_sql, run_status
from cli.sync import execute_export, execute_init, execute_sync
from pipeline.utils import normalize_domain, utcnow_iso
from runtime_context import TenantContext

from agent_runtime.memory import MemoryStore, SessionStore
from agent_runtime.models import ToolDefinition
from agent_runtime.policy import PolicyEngine


class ToolRegistry:
    def __init__(
        self,
        *,
        tenant_context: TenantContext,
        session_store: SessionStore,
        memory_store: MemoryStore,
        policy_engine: PolicyEngine,
        db_timeout_ms: int = 30000,
    ):
        self.tenant_context = tenant_context
        self.session_store = session_store
        self.memory_store = memory_store
        self.policy_engine = policy_engine
        self.db_timeout_ms = db_timeout_ms

    def definitions(self) -> list[ToolDefinition]:
        reason_prop = {
            "type": "string",
            "description": "Why this tool call is needed for the current operator workflow.",
        }
        return [
            ToolDefinition("doctor", "Validate the tenant runtime and environment.", self._schema({"reason": reason_prop}, required=["reason"])),
            ToolDefinition(
                "sync",
                "Run a bounded refresh provider-intel pipeline for this tenant.",
                self._schema(
                    {
                        "reason": reason_prop,
                        "seeds": {"type": "string"},
                        "max": {"type": "integer", "minimum": 1},
                        "crawl_mode": {"type": "string", "enum": ["full", "refresh"]},
                        "limit": {"type": "integer", "minimum": 1},
                        "run_id": {"type": "string"},
                        "crawlee_headless": {"type": "string", "enum": ["on", "off"]},
                    },
                    required=["reason"],
                ),
            ),
            ToolDefinition(
                "resume",
                "Resume a previously checkpointed provider-intel run for this tenant.",
                self._schema(
                    {
                        "reason": reason_prop,
                        "resume": {"type": "string", "description": "Run id to resume or `latest`."},
                        "limit": {"type": "integer", "minimum": 1},
                        "crawlee_headless": {"type": "string", "enum": ["on", "off"]},
                    },
                    required=["reason", "resume"],
                ),
            ),
            ToolDefinition(
                "status",
                "Inspect run state, counts, outputs, and control state for this tenant.",
                self._schema(
                    {
                        "reason": reason_prop,
                        "run_id": {"type": "string"},
                    },
                    required=["reason"],
                ),
            ),
            ToolDefinition(
                "search",
                "Search local provider-intel state or run a diagnostic preset.",
                self._schema(
                    {
                        "reason": reason_prop,
                        "query": {"type": "string"},
                        "preset": {
                            "type": "string",
                            "enum": [
                                "failed-domains",
                                "blocked-domains",
                                "low-confidence-records",
                                "review-queue",
                                "contradictions",
                                "outreach-ready",
                            ],
                        },
                        "limit": {"type": "integer", "minimum": 1},
                    },
                    required=["reason"],
                ),
            ),
            ToolDefinition(
                "control_show",
                "Show the current run control state for this tenant.",
                self._schema(
                    {
                        "reason": reason_prop,
                        "run_id": {"type": "string"},
                    },
                    required=["reason"],
                ),
            ),
            ToolDefinition(
                "control_apply",
                "Apply a bounded run control action for this tenant.",
                self._schema(
                    {
                        "reason": reason_prop,
                        "run_id": {"type": "string"},
                        "action": {
                            "type": "string",
                            "enum": [
                                "quarantine-seed",
                                "suppress-prefix",
                                "cap-domain",
                                "stop-domain",
                                "clear-domain",
                            ],
                        },
                        "domain": {"type": "string"},
                        "prefix": {"type": "string"},
                        "max_pages": {"type": "integer", "minimum": 1},
                    },
                    required=["reason", "action", "domain"],
                ),
            ),
            ToolDefinition(
                "export",
                "Re-export approved provider-intel records for this tenant.",
                self._schema(
                    {
                        "reason": reason_prop,
                        "limit": {"type": "integer", "minimum": 1},
                    },
                    required=["reason"],
                ),
            ),
            ToolDefinition(
                "sql",
                "Run a read-only SQL query against the tenant runtime DB.",
                self._schema(
                    {
                        "reason": reason_prop,
                        "query": {"type": "string"},
                        "limit": {"type": "integer", "minimum": 1},
                    },
                    required=["reason", "query"],
                ),
            ),
        ]

    @staticmethod
    def _schema(properties: dict[str, Any], *, required: list[str]) -> dict[str, Any]:
        normalized_properties = {
            key: ToolRegistry._normalize_property(value, nullable=key not in required)
            for key, value in properties.items()
        }
        return {
            "type": "object",
            "properties": normalized_properties,
            "required": list(normalized_properties.keys()),
            "additionalProperties": False,
        }

    @staticmethod
    def _normalize_property(schema: dict[str, Any], *, nullable: bool) -> dict[str, Any]:
        normalized = deepcopy(schema)
        prop_type = normalized.get("type")
        if prop_type == "object":
            nested_properties = dict(normalized.get("properties") or {})
            nested_required = list(normalized.get("required") or [])
            normalized["properties"] = {
                key: ToolRegistry._normalize_property(value, nullable=key not in nested_required)
                for key, value in nested_properties.items()
            }
            normalized["required"] = list(normalized["properties"].keys())
            normalized["additionalProperties"] = False
        elif prop_type == "array" and isinstance(normalized.get("items"), dict):
            normalized["items"] = ToolRegistry._normalize_property(dict(normalized["items"]), nullable=False)

        if nullable:
            normalized = ToolRegistry._make_nullable(normalized)
        return normalized

    @staticmethod
    def _make_nullable(schema: dict[str, Any]) -> dict[str, Any]:
        prop_type = schema.get("type")
        if isinstance(prop_type, list):
            schema["type"] = list(prop_type) if "null" in prop_type else [*prop_type, "null"]
        elif isinstance(prop_type, str):
            schema["type"] = [prop_type, "null"]
        if "enum" in schema:
            enum_values = list(schema.get("enum") or [])
            if None not in enum_values:
                enum_values.append(None)
            schema["enum"] = enum_values
        return schema

    def invoke(self, *, session_id: str, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.policy_engine.validate(tool_name, arguments)
        reason = str(arguments.get("reason") or "").strip()
        started_at = utcnow_iso()
        try:
            with self._runtime_env():
                output = self._execute(tool_name, arguments)
            status = "completed"
        except Exception as exc:
            cli_error = classify_exception(exc)
            output = {
                "ok": False,
                "error": {
                    "code": cli_error.code,
                    "message": cli_error.message,
                    "details": cli_error.details,
                },
            }
            status = "failed"
        completed_at = utcnow_iso()
        event = self.session_store.record_tool_event(
            session_id=session_id,
            tenant_id=str(self.tenant_context.tenant_id or ""),
            tool_name=tool_name,
            reason=reason,
            input_payload=arguments,
            output_payload=output,
            status=status,
            started_at=started_at,
            completed_at=completed_at,
        )
        self._update_memory_from_tool(session_id=session_id, tool_name=tool_name, arguments=arguments, output=output, status=status)
        return {
            "ok": status == "completed",
            "tenant_id": self.tenant_context.tenant_id,
            "session_id": session_id,
            "tool_name": tool_name,
            "started_at": started_at,
            "completed_at": completed_at,
            "trace_event_id": event["event_id"],
            "data": output,
        }

    def _execute(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        paths = self.tenant_context.runtime_paths
        if tool_name == "doctor":
            return run_doctor(
                db_path=str(paths.db_path),
                config_path=str(paths.config_path),
                run_state_dir=str(paths.checkpoint_dir),
                db_timeout_ms=self.db_timeout_ms,
                runtime_paths=paths,
            )
        if tool_name == "sync":
            args = SimpleNamespace(
                db=str(paths.db_path),
                seeds=arguments.get("seeds", "seed_packs/nj/seed_pack.json"),
                max=arguments.get("max", 2),
                crawl_mode=arguments.get("crawl_mode", "refresh"),
                limit=arguments.get("limit", 15),
                crawlee_headless=arguments.get("crawlee_headless"),
                run_id=arguments.get("run_id"),
                resume=None,
                checkpoint_dir=str(paths.checkpoint_dir),
                config=str(paths.config_path),
                db_timeout_ms=self.db_timeout_ms,
                runtime_paths=paths,
            )
            return execute_sync(args)
        if tool_name == "resume":
            args = SimpleNamespace(
                db=str(paths.db_path),
                seeds="seed_packs/nj/seed_pack.json",
                max=None,
                crawl_mode="full",
                limit=arguments.get("limit", 15),
                crawlee_headless=arguments.get("crawlee_headless"),
                run_id=None,
                resume=arguments.get("resume"),
                checkpoint_dir=str(paths.checkpoint_dir),
                config=str(paths.config_path),
                db_timeout_ms=self.db_timeout_ms,
                runtime_paths=paths,
            )
            return execute_sync(args)
        if tool_name == "status":
            return run_status(
                db_path=str(paths.db_path),
                run_id=arguments.get("run_id"),
                run_state_dir=str(paths.checkpoint_dir),
                db_timeout_ms=self.db_timeout_ms,
                runtime_paths=paths,
            )
        if tool_name == "search":
            return run_search(
                db_path=str(paths.db_path),
                query=arguments.get("query"),
                preset=arguments.get("preset"),
                limit=int(arguments.get("limit", 20) or 20),
                db_timeout_ms=self.db_timeout_ms,
            )
        if tool_name == "control_show":
            return run_control_show(run_id=arguments.get("run_id", "latest"), run_state_dir=str(paths.checkpoint_dir))
        if tool_name == "control_apply":
            value = None
            action = str(arguments.get("action") or "")
            if action == "suppress-prefix":
                value = arguments.get("prefix")
            elif action == "cap-domain":
                value = int(arguments.get("max_pages") or 0)
            return run_control_apply(
                run_id=arguments.get("run_id", "latest"),
                run_state_dir=str(paths.checkpoint_dir),
                action=action,
                domain=str(arguments.get("domain") or ""),
                value=value,
                reason=str(arguments.get("reason") or action),
            )
        if tool_name == "export":
            args = SimpleNamespace(
                db=str(paths.db_path),
                limit=int(arguments.get("limit", 100) or 100),
                db_timeout_ms=self.db_timeout_ms,
                runtime_paths=paths,
            )
            return execute_export(args)
        if tool_name == "sql":
            return run_sql(
                db_path=str(paths.db_path),
                query=str(arguments.get("query") or ""),
                limit=int(arguments.get("limit", 200) or 200),
                db_timeout_ms=self.db_timeout_ms,
            )
        raise RuntimeError(f"Unsupported tool: {tool_name}")

    def _update_memory_from_tool(
        self,
        *,
        session_id: str,
        tool_name: str,
        arguments: dict[str, Any],
        output: dict[str, Any],
        status: str,
    ) -> None:
        if status != "completed":
            return
        data = dict(output.get("data") or output)
        if tool_name in {"sync", "resume"} and data.get("run_id"):
            self.memory_store.record_run_memory(
                run_id=str(data["run_id"]),
                session_id=session_id,
                summary=dict(data.get("summary") or {}),
                report=dict(data.get("report") or {}),
            )
        if tool_name == "control_apply":
            domain = normalize_domain(str(arguments.get("domain") or ""))
            if domain:
                self.memory_store.upsert_domain_tactic(
                    domain=domain,
                    tactic={
                        "action": arguments.get("action"),
                        "reason": arguments.get("reason"),
                        "value": arguments.get("prefix") or arguments.get("max_pages"),
                    },
                    last_confirmed_source_url="",
                    last_confirmed_at=utcnow_iso(),
                    decay_at=self._future_iso(days=14),
                )

    @staticmethod
    def _future_iso(*, days: int) -> str:
        return (datetime.now(timezone.utc) + timedelta(days=days)).replace(microsecond=0).isoformat()

    @contextmanager
    def _runtime_env(self):
        paths = self.tenant_context.runtime_paths
        updates = {
            "PROVIDER_INTEL_CONFIG": str(paths.config_path),
            "PROVIDER_INTEL_CRAWLER_CONFIG": str(paths.config_path),
        }
        previous = {key: os.environ.get(key) for key in updates}
        try:
            for key, value in updates.items():
                os.environ[key] = value
            if not paths.db_path.exists() or not paths.config_path.exists():
                execute_init(
                    SimpleNamespace(
                        db=str(paths.db_path),
                        config=str(paths.config_path),
                        checkpoint_dir=str(paths.checkpoint_dir),
                        db_timeout_ms=self.db_timeout_ms,
                        runtime_paths=paths,
                    )
                )
            yield
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
