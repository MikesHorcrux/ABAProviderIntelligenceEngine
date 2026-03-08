#!/usr/bin/env python3.11
from __future__ import annotations

import csv
import json
import sqlite3
import tempfile
from pathlib import Path

from pipeline.config import CrawlConfig
from pipeline.stages.export import export_agent_research_queue, export_lead_intelligence_dossier
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


def _seed_intelligence_network(con: sqlite3.Connection) -> None:
    now = "2026-03-08T12:00:00"
    con.execute(
        """
        INSERT OR REPLACE INTO organizations
        (org_pk, legal_name, dba_name, state, created_at, updated_at, last_seen_at, deleted_at)
        VALUES ('org_evergreen', 'Evergreen Cannabis Collective LLC', 'Evergreen Cannabis Collective', 'CO', ?, ?, ?, '')
        """,
        (now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO locations
        (location_pk, org_pk, canonical_name, address_1, city, state, zip, website_domain, phone, fit_score, last_crawled_at, created_at, updated_at, last_seen_at, deleted_at)
        VALUES
        ('loc_evergreen_den', 'org_evergreen', 'Evergreen Cannabis Collective', '', 'Denver', 'CO', '', 'evergreencc.example', '(303) 555-0101', 86, ?, ?, ?, ?, ''),
        ('loc_evergreen_lakewood', 'org_evergreen', 'Evergreen Cannabis Collective - Lakewood', '', 'Lakewood', 'CO', '', 'evergreencc.example', '(303) 555-0102', 80, ?, ?, ?, ?, '')
        """,
        (now, now, now, now, now, now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO lead_scores
        (score_pk, location_pk, score_total, tier, run_id, created_at, as_of, deleted_at)
        VALUES
        ('score_evergreen_den', 'loc_evergreen_den', 86, 'A', 'intel-run', ?, ?, ''),
        ('score_evergreen_lakewood', 'loc_evergreen_lakewood', 80, 'B', 'intel-run', ?, ?, '')
        """,
        (now, now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO contacts
        (contact_pk, location_pk, full_name, role, email, phone, source_kind, confidence, verification_status, created_at, updated_at, last_seen_at, deleted_at)
        VALUES
        ('contact_owner', 'loc_evergreen_den', 'Jason Alvarez', 'Founder & CEO', 'jason@evergreencc.example', '(303) 555-0201', 'first_party_parse', 0.95, 'verified', ?, ?, ?, ''),
        ('contact_ops', 'loc_evergreen_den', 'Rachel Kim', 'Director of Operations', 'rachel@evergreencc.example', '(303) 555-0202', 'first_party_parse', 0.93, 'verified', ?, ?, ?, ''),
        ('contact_compliance', 'loc_evergreen_lakewood', 'Daniel Ortiz', 'Compliance Manager', 'daniel@evergreencc.example', '(303) 555-0203', 'first_party_parse', 0.91, 'verified', ?, ?, ?, '')
        """,
        (now, now, now, now, now, now, now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO contact_points
        (contact_pk, location_pk, type, value, confidence, source_url, first_seen_at, last_seen_at, created_at, updated_at, deleted_at)
        VALUES
        ('cp_evergreen_phone_den', 'loc_evergreen_den', 'phone', '(303) 555-0101', 0.9, 'https://evergreencc.example/contact', ?, ?, ?, ?, ''),
        ('cp_evergreen_email_den', 'loc_evergreen_den', 'email', 'hello@evergreencc.example', 0.9, 'https://evergreencc.example/contact', ?, ?, ?, ?, '')
        """,
        (now, now, now, now, now, now, now, now),
    )
    con.execute(
        """
        INSERT OR REPLACE INTO evidence
        (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
        VALUES
        ('ev_evergreen_menu', 'location', 'loc_evergreen_den', 'menu_provider', 'dutchie', 'https://evergreencc.example/menu', 'menu detected', ?, ''),
        ('ev_evergreen_social', 'location', 'loc_evergreen_den', 'social_url', 'https://www.instagram.com/evergreencc', 'https://www.instagram.com/evergreencc', 'social signal', ?, ''),
        ('ev_evergreen_compliance', 'location', 'loc_evergreen_den', 'compliance_system', 'METRC', 'https://evergreencc.example/compliance', 'compliance note', ?, ''),
        ('ev_evergreen_rating', 'location', 'loc_evergreen_den', 'google_rating', '4.6', 'https://maps.example/evergreencc', 'maps signal', ?, ''),
        ('ev_evergreen_revenue', 'location', 'loc_evergreen_den', 'revenue_estimate', '$21M', 'https://evergreencc.example/about', 'revenue estimate', ?, '')
        """,
        (now, now, now, now, now),
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


def test_export_lead_intelligence_dossier_builds_index_and_profiles() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "intelligence.db"
        out_dir = root / "out"
        con = _connect_with_schema(db_path)
        _seed_intelligence_network(con)

        report = export_lead_intelligence_dossier(
            con,
            out_dir,
            cfg=CrawlConfig(),
            tier="A",
            limit=10,
            run_id="intel-run",
        )
        con.close()

        assert report["company_count"] == 1
        assert report["package_count"] == 1
        assert report["row_count"] == 3

        index_path = Path(str(report["index_csv"]))
        table_path = Path(str(report["table_md"]))
        manifest_path = Path(str(report["manifest_json"]))
        assert index_path.exists()
        assert table_path.exists()
        assert manifest_path.exists()

        with index_path.open(newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 3
        assert {row["contact_role"] for row in rows} == {"Owner", "Operations", "Compliance"}
        assert all(row["locations"] == "2" for row in rows)
        assert all(row["pos_system"] == "dutchie" for row in rows)
        assert all(row["compliance_system"] == "METRC" for row in rows)
        assert all(row["lead_package"] for row in rows)
        assert all(row["report"] for row in rows)
        assert all(row["agent_prompt"] for row in rows)

        profile_path = index_path.parent / rows[0]["profile"]
        strategy_path = index_path.parent / rows[0]["company_strategy"]
        package_path = index_path.parent / rows[0]["lead_package"]
        lead_summary_path = index_path.parent / rows[0]["lead_summary"]
        report_path = index_path.parent / rows[0]["report"]
        agent_brief_path = index_path.parent / rows[0]["agent_brief"]
        agent_prompt_path = index_path.parent / rows[0]["agent_prompt"]
        agent_packet_path = index_path.parent / rows[0]["agent_packet"]
        assert profile_path.exists()
        assert strategy_path.exists()
        assert package_path.exists()
        assert lead_summary_path.exists()
        assert report_path.exists()
        assert agent_brief_path.exists()
        assert agent_prompt_path.exists()
        assert agent_packet_path.exists()
        assert "## Overview" in profile_path.read_text(encoding="utf-8")
        assert "Evergreen Cannabis Collective" in profile_path.read_text(encoding="utf-8")
        assert "## Decision Network" in strategy_path.read_text(encoding="utf-8")
        assert "Jason Alvarez" in strategy_path.read_text(encoding="utf-8")
        assert "[company-strategy.md]" in table_path.read_text(encoding="utf-8")
        assert "[report.md]" in table_path.read_text(encoding="utf-8")
        assert "## Recommended Sequence" in (package_path / "outreach_sequence.md").read_text(encoding="utf-8")
        assert "Agent Research Brief" in agent_brief_path.read_text(encoding="utf-8")
        assert "You are the research agent" in agent_prompt_path.read_text(encoding="utf-8")
        assert "Lead Intelligence Dossier" in report_path.read_text(encoding="utf-8")

        summary_payload = json.loads(lead_summary_path.read_text(encoding="utf-8"))
        assert summary_payload["budget_band"] == "$25k-$100k"
        assert summary_payload["revenue_est"] == "$21M"
        assert summary_payload["location_count"] == 2

        manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest_payload["package_count"] == 1
        assert manifest_payload["packages"][0]["lead_id"] == "DISP001"


def main() -> None:
    test_run_lead_research_persists_briefs_and_exports_queue()
    test_export_lead_intelligence_dossier_builds_index_and_profiles()
    print("test_lead_research: ok")


if __name__ == "__main__":
    main()
