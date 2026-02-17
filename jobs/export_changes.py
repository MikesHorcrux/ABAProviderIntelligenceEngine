#!/usr/bin/env python3
from __future__ import annotations

import csv
import sqlite3
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DB = BASE / 'data/cannaradar_v1.db'
SCHEMA = BASE / 'db/schema.sql'
OUT = BASE / 'out'
SNAPSHOT = OUT / 'outreach_dispensary_100.csv'
STATE_DIR = BASE / 'data/state'
PREV = STATE_DIR / 'last_outreach_snapshot.csv'


def init_db(con: sqlite3.Connection):
    con.executescript(SCHEMA.read_text())
    con.commit()


def key(row: dict) -> tuple[str, str]:
    return (
        (row.get('website') or '').strip().lower(),
        (row.get('dispensary') or '').strip().lower(),
    )


def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline='') as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    if not SNAPSHOT.exists():
        raise SystemExit(f'Missing snapshot: {SNAPSHOT}. Run postprocess/export first.')

    now = datetime.now().strftime('%Y%m%d-%H%M%S')
    diff_path = OUT / f'changes_{now}.csv'
    summary_path = OUT / f'changes_{now}.txt'

    current = load_csv(SNAPSHOT)
    previous = load_csv(PREV)

    cur_map = {key(r): r for r in current}
    prev_map = {key(r): r for r in previous}

    added = [cur_map[k] for k in cur_map.keys() - prev_map.keys()]
    removed = [prev_map[k] for k in prev_map.keys() - cur_map.keys()]

    modified = []
    tracked = ['score', 'email', 'phone', 'owner_name', 'owner_role', 'segment']
    for k in cur_map.keys() & prev_map.keys():
        cur = cur_map[k]
        prev = prev_map[k]
        changes = []
        for f in tracked:
            if (cur.get(f) or '') != (prev.get(f) or ''):
                changes.append(f'{f}: "{prev.get(f, "")}" -> "{cur.get(f, "")}"')
        if changes:
            row = cur.copy()
            row['change_type'] = 'modified'
            row['changes'] = '; '.join(changes)
            modified.append(row)

    rows = []
    for r in added:
        x = r.copy(); x['change_type'] = 'added'; x['changes'] = ''
        rows.append(x)
    for r in removed:
        x = r.copy(); x['change_type'] = 'removed'; x['changes'] = ''
        rows.append(x)
    rows.extend(modified)

    base_fields = list(current[0].keys()) if current else ['dispensary', 'website', 'state', 'score']
    fields = base_fields + ['change_type', 'changes']
    write_csv(diff_path, rows, fields)

    summary = [
        f'Change Report ({now})',
        f'Current rows: {len(current)}',
        f'Previous rows: {len(previous)}',
        f'Added: {len(added)}',
        f'Removed: {len(removed)}',
        f'Modified: {len(modified)}',
        f'Detail CSV: {diff_path.name}',
    ]
    summary_path.write_text('\n'.join(summary) + '\n')

    # Update baseline snapshot for next run.
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    PREV.write_text(SNAPSHOT.read_text())

    print(f'Wrote {diff_path}')
    print(f'Wrote {summary_path}')


if __name__ == '__main__':
    main()
