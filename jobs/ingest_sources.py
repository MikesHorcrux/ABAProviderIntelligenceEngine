#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from adapters.registry import build_adapters
from adapters.base import LicenseRow

BASE = Path(__file__).resolve().parents[1]
DB = BASE / 'data/cannaradar_v1.db'
SCHEMA = BASE / 'db/schema.sql'
SCHEMA_TEXT = SCHEMA.read_text()
SCHEMA_VERSION = 4
SCHEMA_MIGRATION_NAME = 'v1.5.0'
SCHEMA_CHECKSUM = hashlib.sha256(SCHEMA_TEXT.encode('utf-8')).hexdigest()

REQUIRED_TABLE_COLUMNS = {
    'organizations': {'org_pk', 'legal_name', 'dba_name', 'state', 'created_at', 'updated_at'},
    'licenses': {'license_pk', 'org_pk', 'state', 'license_id', 'license_type', 'status', 'source_url', 'retrieved_at', 'fingerprint'},
    'locations': {'location_pk', 'org_pk', 'canonical_name', 'address_1', 'city', 'state', 'zip', 'website_domain', 'phone', 'fit_score', 'last_crawled_at', 'created_at', 'updated_at'},
    'contact_points': {'contact_pk', 'location_pk', 'type', 'value', 'confidence', 'source_url', 'first_seen_at', 'last_seen_at'},
    'evidence': {'evidence_pk', 'entity_type', 'entity_pk', 'field_name', 'field_value', 'source_url', 'snippet', 'captured_at'},
    'outreach_events': {'event_pk', 'location_pk', 'channel', 'outcome', 'notes', 'created_at'},
    'schema_migrations': {'schema_version', 'migration_name', 'schema_checksum', 'applied_at'},
}


def make_pk(prefix: str, parts: list[str]) -> str:
    s = '|'.join((p or '').strip().lower() for p in parts)
    h = hashlib.sha1(s.encode('utf-8', errors='ignore')).hexdigest()[:16]
    return f"{prefix}_{h}"


def normalized_domain(url_or_domain: str) -> str:
    v = (url_or_domain or '').strip()
    if not v:
        return ''
    if '://' not in v:
        v = f'https://{v}'
    try:
        host = (urlparse(v).netloc or '').lower()
        if host.startswith('www.'):
            host = host[4:]
        return host
    except Exception:
        return ''


def assert_schema_layout(con: sqlite3.Connection):
    for table, required_columns in REQUIRED_TABLE_COLUMNS.items():
        cols = {r[1] for r in con.execute(f'PRAGMA table_info({table})').fetchall()}
        missing = required_columns - cols
        if missing:
            raise SystemExit(f'Schema drift detected for {table}. Missing columns: {", ".join(sorted(missing))}')


def assert_schema_migration(con: sqlite3.Connection):
    current_version = int((con.execute('PRAGMA user_version').fetchone() or [0])[0])
    if current_version != SCHEMA_VERSION:
        raise SystemExit(
            f'Schema version mismatch for canonical DB. Expected {SCHEMA_VERSION}, found {current_version}. '
            'Run jobs/ingest_sources.py against a supported schema DB backup/restore path and re-run.'
        )

    row = con.execute(
        'SELECT migration_name, schema_checksum FROM schema_migrations WHERE schema_version=?',
        (SCHEMA_VERSION,),
    ).fetchone()

    if not row:
        con.execute(
            'INSERT INTO schema_migrations (schema_version, migration_name, schema_checksum, applied_at) VALUES (?,?,?,?)',
            (SCHEMA_VERSION, SCHEMA_MIGRATION_NAME, SCHEMA_CHECKSUM, datetime.now().isoformat(timespec='seconds')),
        )
        return

    if row[1] != SCHEMA_CHECKSUM:
        raise SystemExit(
            f'Schema checksum mismatch for version {SCHEMA_VERSION}. '
            f'Expected {SCHEMA_CHECKSUM}, found {row[1]}. '
            'Use a known-good DB backup or rebuild schema from seed source before continuing.'
        )


def init_db(con: sqlite3.Connection):
    con.executescript(SCHEMA_TEXT)
    assert_schema_layout(con)
    assert_schema_migration(con)
    con.commit()


def upsert_row(con: sqlite3.Connection, r: LicenseRow, now: str):
    domain = normalized_domain(r.website)
    org_pk = make_pk('org', [r.legal_name or r.dba_name, r.state])
    # Dedupe baseline: location key uses domain + state first, then name fallback.
    loc_pk = make_pk('loc', [domain or r.website, r.state, r.dba_name or r.legal_name])
    lic_pk = make_pk('lic', [r.state, r.license_id or domain or r.website, r.legal_name or r.dba_name])

    con.execute('''INSERT OR REPLACE INTO organizations (org_pk, legal_name, dba_name, state, created_at, updated_at)
                   VALUES (?,?,?,?,COALESCE((SELECT created_at FROM organizations WHERE org_pk=?),?),?)''',
                (org_pk, r.legal_name, r.dba_name, r.state, org_pk, now, now))

    con.execute('''INSERT OR REPLACE INTO licenses (license_pk, org_pk, state, license_id, license_type, status, source_url, retrieved_at, fingerprint)
                   VALUES (?,?,?,?,?,?,?,?,?)''',
                (lic_pk, org_pk, r.state, r.license_id, r.license_type, r.status, r.source_url, r.retrieved_at, make_pk('fp',[r.legal_name,r.website,r.state])))

    con.execute('''INSERT OR REPLACE INTO locations (location_pk, org_pk, canonical_name, address_1, city, state, zip, website_domain, phone, fit_score, last_crawled_at, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,0,NULL,COALESCE((SELECT created_at FROM locations WHERE location_pk=?),?),?)''',
                (loc_pk, org_pk, r.dba_name or r.legal_name, r.address_1, r.city, r.state, r.zip, domain, r.phone, loc_pk, now, now))

    if domain:
        website_pk = make_pk('cp', [loc_pk, 'website', domain])
        con.execute('''INSERT OR REPLACE INTO contact_points (contact_pk, location_pk, type, value, confidence, source_url, first_seen_at, last_seen_at)
                       VALUES (?,?,?,?,?,?,COALESCE((SELECT first_seen_at FROM contact_points WHERE contact_pk=?),?),?)''',
                    (website_pk, loc_pk, 'website', domain, 0.9, r.source_url or r.website, website_pk, now, now))

    if r.phone:
        phone_pk = make_pk('cp', [loc_pk, 'phone', r.phone])
        con.execute('''INSERT OR REPLACE INTO contact_points (contact_pk, location_pk, type, value, confidence, source_url, first_seen_at, last_seen_at)
                       VALUES (?,?,?,?,?,?,COALESCE((SELECT first_seen_at FROM contact_points WHERE contact_pk=?),?),?)''',
                    (phone_pk, loc_pk, 'phone', r.phone, 0.8, r.source_url or r.website, phone_pk, now, now))

    ev_pk = str(uuid.uuid4())
    con.execute('''INSERT OR REPLACE INTO evidence (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at)
                   VALUES (?,?,?,?,?,?,?,?)''',
                (ev_pk, 'location', loc_pk, 'website_domain', domain, r.source_url, 'source ingestion', now))


def ingest_all(con: sqlite3.Connection) -> int:
    adapters = build_adapters(BASE)
    if not adapters:
        print('No adapters enabled. Nothing to ingest.')
        return 0

    total = 0
    now = datetime.now().isoformat(timespec='seconds')
    for adapter in adapters:
        raw = adapter.fetch_raw()
        rows = adapter.normalize_rows(adapter.parse_raw_to_rows(raw))
        for row in rows:
            upsert_row(con, row, now)
        total += len(rows)
        print(f'Adapter {adapter.source_name}: ingested {len(rows)} rows')

    con.commit()
    return total


def main():
    DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)
    init_db(con)
    n = ingest_all(con)
    print(f'Ingested {n} total rows into canonical DB: {DB}')


if __name__ == '__main__':
    main()
