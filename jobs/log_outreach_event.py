#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DB = BASE / 'data/cannaradar_v1.db'
SCHEMA = BASE / 'db/schema.sql'
VALID_OUTCOMES = {'bounced', 'replied', 'confirmed', 'no-fit'}


def init_db(con: sqlite3.Connection):
    con.executescript(SCHEMA.read_text())
    con.commit()


def resolve_location_pk(con: sqlite3.Connection, location_pk: str | None, website: str | None, name: str | None, state: str | None) -> str | None:
    if location_pk:
        row = con.execute('SELECT location_pk FROM locations WHERE location_pk=?', (location_pk,)).fetchone()
        return row[0] if row else None

    if website:
        w = website.strip().lower().replace('https://', '').replace('http://', '').strip('/')
        row = con.execute(
            '''SELECT location_pk FROM locations
               WHERE lower(website_domain)=?
               LIMIT 1''',
            (w,),
        ).fetchone()
        if row:
            return row[0]

    if name and state:
        row = con.execute(
            '''SELECT location_pk FROM locations
               WHERE lower(canonical_name)=? AND lower(state)=?
               LIMIT 1''',
            (name.strip().lower(), state.strip().lower()),
        ).fetchone()
        if row:
            return row[0]

    return None


def main():
    ap = argparse.ArgumentParser(description='Log outreach verification event into canonical DB')
    ap.add_argument('--location-pk', default='')
    ap.add_argument('--website', default='')
    ap.add_argument('--name', default='')
    ap.add_argument('--state', default='')
    ap.add_argument('--channel', required=True, help='email|sms|call|linkedin|other')
    ap.add_argument('--outcome', required=True, help='bounced|replied|confirmed|no-fit')
    ap.add_argument('--notes', default='')
    args = ap.parse_args()

    outcome = args.outcome.strip().lower()
    if outcome not in VALID_OUTCOMES:
        raise SystemExit(f'Invalid outcome: {outcome}. Expected one of: {", ".join(sorted(VALID_OUTCOMES))}')

    DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)
    init_db(con)

    loc_pk = resolve_location_pk(
        con,
        args.location_pk or None,
        args.website or None,
        args.name or None,
        args.state or None,
    )
    if not loc_pk:
        raise SystemExit('Could not resolve location. Pass --location-pk, or --website, or --name + --state.')

    event_pk = str(uuid.uuid4())
    now = datetime.now().isoformat(timespec='seconds')
    con.execute(
        '''INSERT INTO outreach_events (event_pk, location_pk, channel, outcome, notes, created_at)
           VALUES (?,?,?,?,?,?)''',
        (event_pk, loc_pk, args.channel.strip().lower(), outcome, args.notes.strip(), now),
    )
    con.commit()

    print(f'Logged outreach event: {event_pk} location={loc_pk} outcome={outcome} channel={args.channel}')


if __name__ == '__main__':
    main()
