from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from uuid import uuid4

from cli.errors import ConfigError
from pipeline.utils import utcnow_iso


def _connect(path: str | Path) -> sqlite3.Connection:
    db_path = Path(path).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS agent_sessions (
          session_id TEXT PRIMARY KEY NOT NULL,
          tenant_id TEXT NOT NULL DEFAULT '',
          goal TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT 'running',
          model_provider TEXT NOT NULL DEFAULT '',
          model_name TEXT NOT NULL DEFAULT '',
          last_run_id TEXT NOT NULL DEFAULT '',
          summary_json TEXT NOT NULL DEFAULT '{}',
          unresolved_risks_json TEXT NOT NULL DEFAULT '[]',
          recommended_next_actions_json TEXT NOT NULL DEFAULT '[]',
          created_at TEXT NOT NULL DEFAULT '',
          updated_at TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_agent_sessions_tenant_updated ON agent_sessions(tenant_id, updated_at DESC);

        CREATE TABLE IF NOT EXISTS agent_turns (
          turn_id TEXT PRIMARY KEY NOT NULL,
          session_id TEXT NOT NULL,
          role TEXT NOT NULL DEFAULT '',
          agent_name TEXT NOT NULL DEFAULT '',
          content TEXT NOT NULL DEFAULT '',
          metadata_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL DEFAULT '',
          FOREIGN KEY (session_id) REFERENCES agent_sessions(session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_agent_turns_session_created ON agent_turns(session_id, created_at ASC);

        CREATE TABLE IF NOT EXISTS agent_tool_events (
          event_id TEXT PRIMARY KEY NOT NULL,
          session_id TEXT NOT NULL,
          turn_id TEXT NOT NULL DEFAULT '',
          tenant_id TEXT NOT NULL DEFAULT '',
          tool_name TEXT NOT NULL DEFAULT '',
          reason TEXT NOT NULL DEFAULT '',
          input_json TEXT NOT NULL DEFAULT '{}',
          output_json TEXT NOT NULL DEFAULT '{}',
          status TEXT NOT NULL DEFAULT 'completed',
          started_at TEXT NOT NULL DEFAULT '',
          completed_at TEXT NOT NULL DEFAULT '',
          FOREIGN KEY (session_id) REFERENCES agent_sessions(session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_agent_tool_events_session_started ON agent_tool_events(session_id, started_at ASC);

        CREATE TABLE IF NOT EXISTS run_memory (
          run_id TEXT PRIMARY KEY NOT NULL,
          session_id TEXT NOT NULL DEFAULT '',
          summary_json TEXT NOT NULL DEFAULT '{}',
          report_json TEXT NOT NULL DEFAULT '{}',
          updated_at TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS domain_tactics (
          domain TEXT PRIMARY KEY NOT NULL,
          tactic_json TEXT NOT NULL DEFAULT '{}',
          last_confirmed_source_url TEXT NOT NULL DEFAULT '',
          last_confirmed_at TEXT NOT NULL DEFAULT '',
          decay_at TEXT NOT NULL DEFAULT '',
          updated_at TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS client_profiles (
          client_id TEXT PRIMARY KEY NOT NULL,
          client_name TEXT NOT NULL DEFAULT '',
          profile_json TEXT NOT NULL DEFAULT '{}',
          updated_at TEXT NOT NULL DEFAULT ''
        );
        """
    )
    return con


class SessionStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()
        _connect(self.db_path).close()

    def create_session(self, *, tenant_id: str, goal: str, model_provider: str, model_name: str, session_id: str | None = None) -> dict[str, Any]:
        now = utcnow_iso()
        resolved_session_id = session_id or f"sess_{uuid4().hex[:16]}"
        con = _connect(self.db_path)
        con.execute(
            """
            INSERT OR REPLACE INTO agent_sessions
            (session_id, tenant_id, goal, status, model_provider, model_name, created_at, updated_at)
            VALUES (?, ?, ?, 'running', ?, ?, ?, ?)
            """,
            (resolved_session_id, tenant_id, goal, model_provider, model_name, now, now),
        )
        con.commit()
        con.close()
        return self.get_session(resolved_session_id)

    def get_session(self, session_id: str) -> dict[str, Any]:
        con = _connect(self.db_path)
        row = con.execute("SELECT * FROM agent_sessions WHERE session_id=?", (session_id,)).fetchone()
        con.close()
        if not row:
            raise ConfigError(f"Agent session not found: {session_id}")
        return self._decode_session(dict(row))

    def latest_session(self, tenant_id: str) -> dict[str, Any] | None:
        con = _connect(self.db_path)
        row = con.execute(
            "SELECT * FROM agent_sessions WHERE tenant_id=? ORDER BY updated_at DESC LIMIT 1",
            (tenant_id,),
        ).fetchone()
        con.close()
        return self._decode_session(dict(row)) if row else None

    def update_session(
        self,
        session_id: str,
        *,
        status: str | None = None,
        last_run_id: str | None = None,
        summary: dict[str, Any] | None = None,
        unresolved_risks: list[str] | None = None,
        recommended_next_actions: list[str] | None = None,
    ) -> dict[str, Any]:
        existing = self.get_session(session_id)
        payload = {
            "status": status or existing["status"],
            "last_run_id": last_run_id if last_run_id is not None else existing.get("last_run_id", ""),
            "summary_json": json.dumps(summary if summary is not None else existing.get("summary", {}), sort_keys=True),
            "unresolved_risks_json": json.dumps(unresolved_risks if unresolved_risks is not None else existing.get("unresolved_risks", [])),
            "recommended_next_actions_json": json.dumps(
                recommended_next_actions if recommended_next_actions is not None else existing.get("recommended_next_actions", [])
            ),
            "updated_at": utcnow_iso(),
        }
        con = _connect(self.db_path)
        con.execute(
            """
            UPDATE agent_sessions
            SET status=?, last_run_id=?, summary_json=?, unresolved_risks_json=?, recommended_next_actions_json=?, updated_at=?
            WHERE session_id=?
            """,
            (
                payload["status"],
                payload["last_run_id"],
                payload["summary_json"],
                payload["unresolved_risks_json"],
                payload["recommended_next_actions_json"],
                payload["updated_at"],
                session_id,
            ),
        )
        con.commit()
        con.close()
        return self.get_session(session_id)

    def append_turn(self, session_id: str, *, role: str, agent_name: str, content: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        now = utcnow_iso()
        turn = {
            "turn_id": f"turn_{uuid4().hex[:16]}",
            "session_id": session_id,
            "role": role,
            "agent_name": agent_name,
            "content": content,
            "metadata_json": json.dumps(metadata or {}, sort_keys=True),
            "created_at": now,
        }
        con = _connect(self.db_path)
        con.execute(
            """
            INSERT INTO agent_turns(turn_id, session_id, role, agent_name, content, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                turn["turn_id"],
                turn["session_id"],
                turn["role"],
                turn["agent_name"],
                turn["content"],
                turn["metadata_json"],
                turn["created_at"],
            ),
        )
        con.execute("UPDATE agent_sessions SET updated_at=? WHERE session_id=?", (now, session_id))
        con.commit()
        con.close()
        return self._decode_turn(turn)

    def list_turns(self, session_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        con = _connect(self.db_path)
        rows = con.execute(
            "SELECT * FROM agent_turns WHERE session_id=? ORDER BY created_at ASC LIMIT ?",
            (session_id, limit),
        ).fetchall()
        con.close()
        return [self._decode_turn(dict(row)) for row in rows]

    def record_tool_event(
        self,
        *,
        session_id: str,
        tenant_id: str,
        tool_name: str,
        reason: str,
        input_payload: dict[str, Any],
        output_payload: dict[str, Any],
        status: str,
        turn_id: str = "",
        started_at: str | None = None,
        completed_at: str | None = None,
    ) -> dict[str, Any]:
        started = started_at or utcnow_iso()
        completed = completed_at or utcnow_iso()
        event = {
            "event_id": f"tool_{uuid4().hex[:16]}",
            "session_id": session_id,
            "turn_id": turn_id,
            "tenant_id": tenant_id,
            "tool_name": tool_name,
            "reason": reason,
            "input_json": json.dumps(input_payload, sort_keys=True, default=str),
            "output_json": json.dumps(output_payload, sort_keys=True, default=str),
            "status": status,
            "started_at": started,
            "completed_at": completed,
        }
        con = _connect(self.db_path)
        con.execute(
            """
            INSERT INTO agent_tool_events
            (event_id, session_id, turn_id, tenant_id, tool_name, reason, input_json, output_json, status, started_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_id"],
                event["session_id"],
                event["turn_id"],
                event["tenant_id"],
                event["tool_name"],
                event["reason"],
                event["input_json"],
                event["output_json"],
                event["status"],
                event["started_at"],
                event["completed_at"],
            ),
        )
        con.execute("UPDATE agent_sessions SET updated_at=? WHERE session_id=?", (completed, session_id))
        con.commit()
        con.close()
        return self._decode_tool_event(event)

    def list_tool_events(self, session_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        con = _connect(self.db_path)
        rows = con.execute(
            "SELECT * FROM agent_tool_events WHERE session_id=? ORDER BY started_at ASC LIMIT ?",
            (session_id, limit),
        ).fetchall()
        con.close()
        return [self._decode_tool_event(dict(row)) for row in rows]

    @staticmethod
    def _decode_session(payload: dict[str, Any]) -> dict[str, Any]:
        payload["summary"] = json.loads(payload.pop("summary_json", "{}") or "{}")
        payload["unresolved_risks"] = json.loads(payload.pop("unresolved_risks_json", "[]") or "[]")
        payload["recommended_next_actions"] = json.loads(payload.pop("recommended_next_actions_json", "[]") or "[]")
        return payload

    @staticmethod
    def _decode_turn(payload: dict[str, Any]) -> dict[str, Any]:
        payload["metadata"] = json.loads(payload.pop("metadata_json", "{}") or "{}")
        return payload

    @staticmethod
    def _decode_tool_event(payload: dict[str, Any]) -> dict[str, Any]:
        payload["input"] = json.loads(payload.pop("input_json", "{}") or "{}")
        payload["output"] = json.loads(payload.pop("output_json", "{}") or "{}")
        return payload


class MemoryStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()
        _connect(self.db_path).close()

    def record_run_memory(self, *, run_id: str, session_id: str, summary: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
        now = utcnow_iso()
        con = _connect(self.db_path)
        con.execute(
            """
            INSERT OR REPLACE INTO run_memory(run_id, session_id, summary_json, report_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (run_id, session_id, json.dumps(summary, sort_keys=True), json.dumps(report, sort_keys=True), now),
        )
        con.commit()
        con.close()
        return self.get_run_memory(run_id)

    def get_run_memory(self, run_id: str) -> dict[str, Any]:
        con = _connect(self.db_path)
        row = con.execute("SELECT * FROM run_memory WHERE run_id=?", (run_id,)).fetchone()
        con.close()
        if not row:
            raise ConfigError(f"Run memory not found: {run_id}")
        payload = dict(row)
        payload["summary"] = json.loads(payload.pop("summary_json", "{}") or "{}")
        payload["report"] = json.loads(payload.pop("report_json", "{}") or "{}")
        return payload

    def list_run_memory(self, *, limit: int = 20) -> list[dict[str, Any]]:
        con = _connect(self.db_path)
        rows = con.execute("SELECT * FROM run_memory ORDER BY updated_at DESC LIMIT ?", (limit,)).fetchall()
        con.close()
        return [self.get_run_memory(str(row["run_id"])) for row in rows]

    def upsert_domain_tactic(
        self,
        *,
        domain: str,
        tactic: dict[str, Any],
        last_confirmed_source_url: str,
        last_confirmed_at: str,
        decay_at: str,
    ) -> dict[str, Any]:
        now = utcnow_iso()
        con = _connect(self.db_path)
        con.execute(
            """
            INSERT OR REPLACE INTO domain_tactics(domain, tactic_json, last_confirmed_source_url, last_confirmed_at, decay_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                domain,
                json.dumps(tactic, sort_keys=True),
                last_confirmed_source_url,
                last_confirmed_at,
                decay_at,
                now,
            ),
        )
        con.commit()
        con.close()
        return self.get_domain_tactic(domain)

    def get_domain_tactic(self, domain: str) -> dict[str, Any]:
        con = _connect(self.db_path)
        row = con.execute("SELECT * FROM domain_tactics WHERE domain=?", (domain,)).fetchone()
        con.close()
        if not row:
            raise ConfigError(f"Domain tactic not found: {domain}")
        payload = dict(row)
        payload["tactic"] = json.loads(payload.pop("tactic_json", "{}") or "{}")
        return payload

    def list_domain_tactics(self, *, limit: int = 50) -> list[dict[str, Any]]:
        con = _connect(self.db_path)
        rows = con.execute("SELECT * FROM domain_tactics ORDER BY updated_at DESC LIMIT ?", (limit,)).fetchall()
        con.close()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["tactic"] = json.loads(payload.pop("tactic_json", "{}") or "{}")
            out.append(payload)
        return out

    def upsert_client_profile(self, *, client_id: str, client_name: str, profile: dict[str, Any]) -> dict[str, Any]:
        now = utcnow_iso()
        con = _connect(self.db_path)
        con.execute(
            """
            INSERT OR REPLACE INTO client_profiles(client_id, client_name, profile_json, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (client_id, client_name, json.dumps(profile, sort_keys=True), now),
        )
        con.commit()
        con.close()
        return self.get_client_profile(client_id)

    def get_client_profile(self, client_id: str) -> dict[str, Any] | None:
        con = _connect(self.db_path)
        row = con.execute("SELECT * FROM client_profiles WHERE client_id=?", (client_id,)).fetchone()
        con.close()
        if not row:
            return None
        payload = dict(row)
        payload["profile"] = json.loads(payload.pop("profile_json", "{}") or "{}")
        return payload

    def list_client_profiles(self, *, limit: int = 20) -> list[dict[str, Any]]:
        con = _connect(self.db_path)
        rows = con.execute("SELECT * FROM client_profiles ORDER BY updated_at DESC LIMIT ?", (limit,)).fetchall()
        con.close()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["profile"] = json.loads(payload.pop("profile_json", "{}") or "{}")
            out.append(payload)
        return out
