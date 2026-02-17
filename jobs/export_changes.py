#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
OUT = BASE / 'out'
SNAPSHOT = OUT / 'outreach_dispensary_100.csv'
STATE_DIR = BASE / 'data/state'
PREV = STATE_DIR / 'last_outreach_snapshot.csv'
CHANGES_META = STATE_DIR / 'last_change_metrics.json'


def key(row: dict) -> tuple[str, str, str]:
    return (
        (row.get('website') or '').strip().lower(),
        (row.get('dispensary') or '').strip().lower(),
        (row.get('state') or '').strip().lower(),
    )


def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline='') as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def merge_fields(*rows: list[dict]) -> list[str]:
    fields: list[str] = []
    seen: set[str] = set()
    for rowset in rows:
        for row in rowset:
            for k in row.keys():
                if k not in seen:
                    seen.add(k)
                    fields.append(k)
    return fields


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--snapshot', default=str(SNAPSHOT))
    ap.add_argument('--previous', default=str(PREV))
    ap.add_argument('--out-dir', default=str(OUT))
    ap.add_argument('--run-id', default=datetime.now().strftime('%Y%m%d-%H%M%S'))
    args = ap.parse_args()

    snapshot_path = Path(args.snapshot)
    previous_path = Path(args.previous)
    out_dir = Path(args.out_dir)
    run_id = args.run_id

    out_dir.mkdir(parents=True, exist_ok=True)
    if not snapshot_path.exists():
        raise SystemExit(f'Missing snapshot: {snapshot_path}. Run postprocess/export first.')

    if not previous_path.exists():
        previous_path.parent.mkdir(parents=True, exist_ok=True)
        previous_path.write_text(snapshot_path.read_text())
        print(f'Initialized baseline snapshot -> {previous_path}')

    now = datetime.now().strftime('%Y%m%d-%H%M%S')
    run_key = run_id.strip().replace(' ', '_') or now
    diff_path = out_dir / f'changes_{run_key}.csv'
    summary_path = out_dir / f'changes_{run_key}.txt'

    current = load_csv(snapshot_path)
    previous = load_csv(previous_path)

    cur_map = {key(r): r for r in current if any([key(r)[0], key(r)[1], key(r)[2]])}
    prev_map = {key(r): r for r in previous if any([key(r)[0], key(r)[1], key(r)[2]])}

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
        x = r.copy()
        x['change_type'] = 'added'
        x['changes'] = ''
        rows.append(x)
    for r in removed:
        x = r.copy()
        x['change_type'] = 'removed'
        x['changes'] = ''
        rows.append(x)
    rows.extend(modified)

    base_fields = merge_fields(current, previous)
    if not base_fields:
        base_fields = ['dispensary', 'website', 'state', 'score']
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
    summary_text = '\n'.join(summary)
    summary_path.write_text(summary_text + '\n')

    metrics = {
        'run_id': run_id,
        'snapshot': str(snapshot_path),
        'previous': str(previous_path),
        'current_count': len(current),
        'previous_count': len(previous),
        'added': len(added),
        'removed': len(removed),
        'modified': len(modified),
        'detail_csv': str(diff_path),
        'summary': summary_text,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
    }
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    CHANGES_META.write_text(json.dumps(metrics, indent=2))

    # Update baseline snapshot for next run.
    PREV.write_text(snapshot_path.read_text())

    print(f'Wrote {diff_path}')
    print(f'Wrote {summary_path}')


if __name__ == '__main__':
    main()
