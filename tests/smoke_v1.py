#!/usr/bin/env python3
from __future__ import annotations

import csv
import sqlite3
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / 'db/schema.sql'


def assert_true(cond: bool, msg: str):
    if not cond:
        raise AssertionError(msg)


def test_schema_tables():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / 'test.db'
        con = sqlite3.connect(db)
        con.executescript(SCHEMA.read_text())
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        required = {'organizations', 'licenses', 'locations', 'contact_points', 'evidence', 'outreach_events'}
        assert_true(required.issubset(tables), f'missing tables: {required - tables}')
        con.close()


def test_export_schema_contract():
    required_cols = ['dispensary','segment','website','state','market','owner_name','owner_role','email','phone','source_url','score','checked_at']
    out_file = ROOT / 'out' / 'outreach_dispensary_100.csv'
    if not out_file.exists():
        return
    with out_file.open(newline='') as f:
        reader = csv.DictReader(f)
        assert_true(reader.fieldnames is not None, 'missing headers in outreach_dispensary_100.csv')
        for c in required_cols:
            assert_true(c in reader.fieldnames, f'missing export column: {c}')


def main():
    test_schema_tables()
    test_export_schema_contract()
    print('smoke_v1: ok')


if __name__ == '__main__':
    main()
