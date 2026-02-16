#!/usr/bin/env python3
from __future__ import annotations
import hashlib
import sqlite3
from datetime import datetime
from pathlib import Path
import uuid

from adapters.seeds_adapter import SeedsAdapter

BASE = Path('/Users/lunavanamburg/.openclaw/workspace/leads_engine')
DB = BASE / 'data/cannaradar_v1.db'
SCHEMA = BASE / 'db/schema.sql'
SEEDS = BASE / 'seeds.csv'


def make_pk(prefix: str, parts: list[str]) -> str:
    s = '|'.join((p or '').strip().lower() for p in parts)
    h = hashlib.sha1(s.encode('utf-8', errors='ignore')).hexdigest()[:16]
    return f"{prefix}_{h}"


def init_db(con: sqlite3.Connection):
    con.executescript(SCHEMA.read_text())
    con.commit()


def upsert_from_seed(con: sqlite3.Connection):
    adapter = SeedsAdapter(str(SEEDS))
    raw = adapter.fetch_raw()
    rows = adapter.normalize_rows(adapter.parse_raw_to_rows(raw))
    now = datetime.now().isoformat(timespec='seconds')

    for r in rows:
        org_pk = make_pk('org', [r.legal_name, r.state])
        loc_pk = make_pk('loc', [r.dba_name, r.website, r.state])
        lic_pk = make_pk('lic', [r.state, r.license_id or r.website, r.legal_name])

        con.execute('''INSERT OR REPLACE INTO organizations (org_pk, legal_name, dba_name, state, created_at, updated_at)
                       VALUES (?,?,?,?,COALESCE((SELECT created_at FROM organizations WHERE org_pk=?),?),?)''',
                    (org_pk, r.legal_name, r.dba_name, r.state, org_pk, now, now))

        con.execute('''INSERT OR REPLACE INTO licenses (license_pk, org_pk, state, license_id, license_type, status, source_url, retrieved_at, fingerprint)
                       VALUES (?,?,?,?,?,?,?,?,?)''',
                    (lic_pk, org_pk, r.state, r.license_id, r.license_type, r.status, r.source_url, r.retrieved_at, make_pk('fp',[r.legal_name,r.website,r.state])))

        con.execute('''INSERT OR REPLACE INTO locations (location_pk, org_pk, canonical_name, address_1, city, state, zip, website_domain, phone, fit_score, last_crawled_at, created_at, updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,0,NULL,COALESCE((SELECT created_at FROM locations WHERE location_pk=?),?),?)''',
                    (loc_pk, org_pk, r.dba_name or r.legal_name, r.address_1, r.city, r.state, r.zip, r.website, r.phone, loc_pk, now, now))

        if r.website:
            contact_pk = make_pk('cp', [loc_pk, 'website', r.website])
            con.execute('''INSERT OR REPLACE INTO contact_points (contact_pk, location_pk, type, value, confidence, source_url, first_seen_at, last_seen_at)
                           VALUES (?,?,?,?,?,?,COALESCE((SELECT first_seen_at FROM contact_points WHERE contact_pk=?),?),?)''',
                        (contact_pk, loc_pk, 'website', r.website, 0.9, r.source_url or r.website, contact_pk, now, now))

        ev_pk = str(uuid.uuid4())
        con.execute('''INSERT OR REPLACE INTO evidence (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at)
                       VALUES (?,?,?,?,?,?,?,?)''',
                    (ev_pk, 'location', loc_pk, 'website_domain', r.website, r.source_url, 'seed ingestion', now))

    con.commit()
    return len(rows)


def main():
    DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)
    init_db(con)
    n = upsert_from_seed(con)
    print(f'Ingested {n} seed rows into canonical DB: {DB}')


if __name__ == '__main__':
    main()
