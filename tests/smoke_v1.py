#!/usr/bin/env python3
from __future__ import annotations

import json
import hashlib
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
        required = {'organizations', 'licenses', 'locations', 'contact_points', 'evidence', 'outreach_events', 'schema_migrations'}
        assert_true(required.issubset(tables), f'missing tables: {required - tables}')
        con.close()


def test_schema_migration_metadata():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / 'test.db'
        con = sqlite3.connect(db)
        con.executescript(SCHEMA.read_text())
        user_version = int(con.execute('PRAGMA user_version').fetchone()[0])
        assert_true(user_version == 5, f'expected schema user_version=5, got {user_version}')
        row = con.execute('SELECT schema_checksum FROM schema_migrations WHERE schema_version=?', (5,)).fetchone()
        checksum = hashlib.sha256(SCHEMA.read_text().encode('utf-8')).hexdigest()
        assert_true(
            row is None or row[0] == checksum,
            f'schema_migrations checksum mismatch: expected {checksum}, got {row[0] if row else "MISSING"}',
        )
        con.close()


def test_foreign_key_enforcement():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / 'test.db'
        con = sqlite3.connect(db)
        con.execute('PRAGMA foreign_keys = ON')
        con.executescript(SCHEMA.read_text())
        now = '2026-01-01T00:00:00'
        try:
            con.execute(
                '''INSERT INTO outreach_events(event_pk, location_pk, channel, outcome, notes, created_at)
                   VALUES (?,?,?,?,?,?)''',
                ('event-1', 'missing-location', 'email', 'replied', 'smoke test', now),
            )
            raise AssertionError('Expected FK enforcement on outreach_events.location_pk')
        except sqlite3.IntegrityError:
            pass

        # Sanity check schema-level constraints are active for valid inserts too.
        con.execute(
            '''INSERT OR REPLACE INTO organizations(org_pk, legal_name, dba_name, state, created_at, updated_at)
               VALUES (?,?,?,?,?,?)''',
            ('org-1', 'Acme Wellness', 'Acme', 'CA', now, now),
        )
        con.execute(
            '''INSERT OR REPLACE INTO locations(location_pk, org_pk, canonical_name, address_1, city, state, zip, website_domain, phone, fit_score, last_crawled_at, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?, ?, ?)''',
            ('loc-1', 'org-1', 'Acme Wellness', '', '', 'CA', '', 'example.com', '', 10, now, now, now),
        )
        con.commit()
        con.close()


def test_export_schema_contract():
    required_cols = ['dispensary', 'segment', 'website', 'state', 'market', 'owner_name', 'owner_role', 'email', 'phone', 'source_url', 'score', 'checked_at', 'segment_confidence', 'segment_reason']
    out_file = ROOT / 'out' / 'outreach_dispensary_100.csv'
    assert_true(out_file.exists(), f'missing required output: {out_file}')
    with out_file.open(newline='') as f:
        reader = csv.DictReader(f)
        assert_true(reader.fieldnames is not None, 'missing headers in outreach_dispensary_100.csv')
        for c in required_cols:
            assert_true(c in reader.fieldnames, f'missing export column: {c}')
        rows = list(reader)
        assert_true(
            all((r.get('segment') or '').strip().lower() == 'dispensary' for r in rows),
            'outreach_dispensary_100.csv contains non-dispensary rows'
        )


def test_change_metrics_and_manifests():
    metrics = ROOT / 'data' / 'state' / 'last_change_metrics.json'
    manifest = ROOT / 'data' / 'state' / 'last_run_manifest.json'
    for path in (metrics, manifest):
        assert_true(path.exists(), f'missing observability artifact: {path}')
    data = json.loads(metrics.read_text())
    for key in ('run_id', 'current_count', 'previous_count', 'added', 'removed', 'modified', 'generated_at'):
        assert_true(key in data, f'missing metric key: {key}')
    manifest_payload = json.loads(manifest.read_text())
    for key in ('run_id', 'counts', 'crawler_config', 'seed_file'):
        assert_true(key in manifest_payload, f'missing manifest key: {key}')


def test_excluded_output_guard():
    excluded = ROOT / 'out' / 'excluded_non_dispensary.csv'
    assert_true(excluded.exists(), f'missing required output: {excluded}')
    with excluded.open(newline='') as f:
        reader = csv.DictReader(f)
        assert_true(reader.fieldnames is not None, 'missing headers in excluded_non_dispensary.csv')
        rows = list(reader)
        assert_true(any(r.get('segment') == 'non-dispensary' for r in rows) if rows else True,
                    'excluded output should include segment marker')


def main():
    test_schema_tables()
    test_schema_migration_metadata()
    test_export_schema_contract()
    test_change_metrics_and_manifests()
    test_excluded_output_guard()
    test_foreign_key_enforcement()
    print('smoke_v1: ok')


if __name__ == '__main__':
    main()
