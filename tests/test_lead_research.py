#!/usr/bin/env python3.11
from __future__ import annotations

import csv
import sqlite3
import tempfile
from pathlib import Path

from pipeline.config import CrawlConfig
from pipeline.stages.export import export_agent_research_queue
from pipeline.stages.research import run_lead_research


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "db" / "schema.sql"


def _connect_with_schema(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    return con


def _seed_location(con: sqlite3.Connection) -> None:
    now = "2026-03-08T12:00:00"
    con.execute(
        """
        INSERT OR REPLACE INTO organizations
        (org_pk, legal_name, dba_name, state, created_at, updated_at, last_seen_at, deleted_at)
        VALUES ('org_green', 'Green Leaf LLC', 'Green Leaf', 'CA', ?, ?, ?, '')
        """,
        (now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO locations
        (location_pk, org_pk, canonical_name, address_1, city, state, zip, website_domain, phone, fit_score, last_crawled_at, created_at, updated_at, last_seen_at, deleted_at)
        VALUES ('loc_green', 'org_green', 'Green Leaf', '', 'Los Angeles', 'CA', '', 'greenleaf.example', '', 78, ?, ?, ?, ?, '')
        """,
        (now, now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO lead_scores
        (score_pk, location_pk, score_total, tier, run_id, created_at, as_of, deleted_at)
        VALUES ('score_green', 'loc_green', 78, 'A', 'run-1', ?, ?, '')
        """,
        (now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO contacts
        (contact_pk, location_pk, full_name, role, email, phone, source_kind, confidence, verification_status, created_at, updated_at, last_seen_at, deleted_at)
        VALUES ('contact_green', 'loc_green', 'Jamie Doe', 'Community Lead', '', '', 'first_party_parse', 0.7, 'unverified', ?, ?, ?, '')
        """,
        (now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO contact_points
        (contact_pk, location_pk, type, value, confidence, source_url, first_seen_at, last_seen_at, created_at, updated_at, deleted_at)
        VALUES
        ('cp_email', 'loc_green', 'email', 'hello@greenleaf.example', 0.8, 'https://greenleaf.example/contact', ?, ?, ?, ?, ''),
        ('cp_phone', 'loc_green', 'phone', '(415) 555-0102', 0.8, 'https://greenleaf.example/contact', ?, ?, ?, ?, '')
        """,
        (now, now, now, now, now, now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO evidence
        (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
        VALUES
        ('ev_menu', 'location', 'loc_green', 'menu_provider', 'dutchie', 'https://greenleaf.example/menu', 'menu detected', ?, ''),
        ('ev_social', 'location', 'loc_green', 'social_url', 'https://www.instagram.com/greenleaf', 'https://www.instagram.com/greenleaf', 'social signal', ?, ''),
        ('ev_contact', 'location', 'loc_green', 'contact', 'Jamie Doe (Community Lead)', 'https://greenleaf.example/team', 'team page', ?, '')
        """,
        (now, now, now),
    )
    con.commit()


def test_run_lead_research_persists_briefs_and_exports_queue() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "research.db"
        out_dir = root / "out"
        con = _connect_with_schema(db_path)
        _seed_location(con)

        result = run_lead_research(
            con,
            cfg=CrawlConfig(),
            run_id="research-run",
            since="2026-03-01T00:00:00",
            limit=10,
            min_score=48,
        )

        assert result["enabled"] is True
        assert result["researched_locations"] == 1
        assert result["enhanced_locations"] == 1

        status = con.execute(
            """
            SELECT field_value
            FROM evidence
            WHERE entity_pk='loc_green'
              AND field_name='agent_research_status'
            ORDER BY captured_at DESC
            LIMIT 1
            """
        ).fetchone()
        assert status is not None
        assert status["field_value"] in {"contactable", "enhanced"}

        summary = con.execute(
            """
            SELECT field_value
            FROM evidence
            WHERE entity_pk='loc_green'
              AND field_name='agent_research_summary'
            ORDER BY captured_at DESC
            LIMIT 1
            """
        ).fetchone()
        assert summary is not None
        assert "lead scored 78" in (summary["field_value"] or "")

        exported = export_agent_research_queue(
            con,
            out_dir,
            cfg=CrawlConfig(),
            since="2026-03-01T00:00:00",
            limit=10,
            min_score=48,
            run_id="research-run",
        )
        con.close()

        with Path(exported).open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 1
        assert rows[0]["company_name"] == "Green Leaf"
        assert rows[0]["research_status"] in {"contactable", "enhanced"}
        assert "Inventory Manager" in rows[0]["target_roles"]
        assert "/leadership" in rows[0]["suggested_paths"]


def main() -> None:
    test_run_lead_research_persists_briefs_and_exports_queue()
    print("test_lead_research: ok")


if __name__ == "__main__":
    main()
