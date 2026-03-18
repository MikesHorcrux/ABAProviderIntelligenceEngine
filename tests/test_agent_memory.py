#!/usr/bin/env python3.11
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from agent_runtime.memory import MemoryStore, SessionStore


def test_session_store_and_memory_store_round_trip() -> None:
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "agent_memory.db"
        sessions = SessionStore(db_path)
        memory = MemoryStore(db_path)

        session = sessions.create_session(
            tenant_id="tenant-a",
            goal="Find outreach-ready NJ providers",
            model_provider="fake",
            model_name="fake-model",
        )
        assert session["tenant_id"] == "tenant-a"

        turn = sessions.append_turn(session["session_id"], role="user", agent_name="SupervisorAgent", content="Goal")
        assert turn["role"] == "user"

        event = sessions.record_tool_event(
            session_id=session["session_id"],
            tenant_id="tenant-a",
            tool_name="status",
            reason="Inspect counts",
            input_payload={"reason": "Inspect counts"},
            output_payload={"data": {"counts": {"records": 3}}},
            status="completed",
            turn_id=turn["turn_id"],
        )
        assert event["tool_name"] == "status"

        sessions.update_session(
            session["session_id"],
            status="completed",
            last_run_id="run-123",
            summary={"supervisor_summary": "done"},
            unresolved_risks=["needs review"],
            recommended_next_actions=["inspect review queue"],
        )
        updated = sessions.get_session(session["session_id"])
        assert updated["last_run_id"] == "run-123"
        assert updated["unresolved_risks"] == ["needs review"]

        run_memory = memory.record_run_memory(
            run_id="run-123",
            session_id=session["session_id"],
            summary={"approved": 2},
            report={"records_csv": "/tmp/records.csv"},
        )
        assert run_memory["summary"]["approved"] == 2

        tactic = memory.upsert_domain_tactic(
            domain="example.com",
            tactic={"action": "cap-domain", "reason": "bounded_retry"},
            last_confirmed_source_url="https://example.com/providers",
            last_confirmed_at="2026-03-14T00:00:00Z",
            decay_at="2026-03-28T00:00:00Z",
        )
        assert tactic["tactic"]["action"] == "cap-domain"

        profile = memory.upsert_client_profile(
            client_id="default",
            client_name="Default Client",
            profile={"specialty": "ASD", "geography": "NJ"},
        )
        assert profile["profile"]["geography"] == "NJ"

        assert len(sessions.list_turns(session["session_id"])) == 1
        assert len(sessions.list_tool_events(session["session_id"])) == 1
        assert len(memory.list_run_memory()) == 1
        assert len(memory.list_domain_tactics()) == 1
        assert len(memory.list_client_profiles()) == 1


def test_session_store_tail_queries_return_latest_rows_in_chronological_order() -> None:
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "agent_memory.db"
        sessions = SessionStore(db_path)
        session = sessions.create_session(
            tenant_id="tenant-a",
            goal="Tail status should show latest activity",
            model_provider="fake",
            model_name="fake-model",
        )

        turn_ids: list[str] = []
        for idx in range(60):
            turn = sessions.append_turn(
                session["session_id"],
                role="assistant",
                agent_name="SupervisorAgent",
                content=f"turn-{idx}",
                metadata={"idx": idx},
            )
            turn_ids.append(turn["turn_id"])
            sessions.record_tool_event(
                session_id=session["session_id"],
                tenant_id="tenant-a",
                tool_name="status",
                reason=f"event-{idx}",
                input_payload={"idx": idx},
                output_payload={"data": {"idx": idx}},
                status="completed",
                started_at=f"2026-03-18T00:00:{idx:02d}Z",
                completed_at=f"2026-03-18T00:00:{idx:02d}Z",
            )

        con = sqlite3.connect(db_path)
        for idx, turn_id in enumerate(turn_ids):
            con.execute(
                "UPDATE agent_turns SET created_at=? WHERE turn_id=?",
                (f"2026-03-18T00:01:{idx:02d}Z", turn_id),
            )
        con.commit()
        con.close()

        latest_turns = sessions.list_turns(session["session_id"], limit=50, tail=True)
        latest_events = sessions.list_tool_events(session["session_id"], limit=50, tail=True)

        assert len(latest_turns) == 50
        assert latest_turns[0]["content"] == "turn-10"
        assert latest_turns[-1]["content"] == "turn-59"

        assert len(latest_events) == 50
        assert latest_events[0]["reason"] == "event-10"
        assert latest_events[-1]["reason"] == "event-59"


def main() -> None:
    test_session_store_and_memory_store_round_trip()
    test_session_store_tail_queries_return_latest_rows_in_chronological_order()
    print("test_agent_memory: ok")


if __name__ == "__main__":
    main()
