#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
from pathlib import Path

from pipeline.stages.discovery import DiscoverySeed
from pipeline.stages.parse import ParsedPage
from pipeline.stages.resolve import resolve_and_upsert_locations


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "db" / "schema.sql"


def test_resolve_is_deterministic_for_same_seed():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA_PATH.read_text())

    seed = DiscoverySeed(name="Green Leaf", website="https://greenleaf.com", state="CA", market="CA")
    parsed = ParsedPage(
        url="https://greenleaf.com",
        html="<html/>",
        text="Green Leaf",
        emails=[],
        phones=[],
        contact_people=[],
        social_urls=[],
        schema_local_business={},
        menu_providers=[],
        links=[],
    )
    first = resolve_and_upsert_locations(con, seed, [parsed])
    second = resolve_and_upsert_locations(con, seed, [parsed])
    assert first.location_pk == second.location_pk
    count = con.execute("SELECT COUNT(*) AS c FROM locations WHERE canonical_name='Green Leaf'").fetchone()["c"]
    assert count == 1
    con.close()


def main() -> None:
    test_resolve_is_deterministic_for_same_seed()
    print("test_resolve_stage: ok")


if __name__ == "__main__":
    main()
