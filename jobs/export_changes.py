#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
OUT = BASE / "out"
CURRENT = OUT / "outreach_dispensary_100.csv"
STATE_DIR = BASE / "data/state"
PREVIOUS = STATE_DIR / "last_outreach_snapshot.csv"
CHANGES_META = STATE_DIR / "last_change_metrics.json"
CHANGE_VERSION = "v1.5"


def _now_key() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _make_key(raw: str) -> str:
    run_id = (raw or "").strip()
    if not run_id:
        return _now_key()

    digits = re.sub(r"\D", "", run_id)
    if len(digits) >= 14:
        digits = digits[:14]
    elif len(digits) >= 8:
        digits = f"{digits}000000"[:14]

    if len(digits) >= 8:
        return f"{digits[:8]}-{digits[8:14].ljust(6, '0')}"

    return _now_key()


def _row_key(row: dict) -> tuple[str, str, str]:
    return (
        (row.get("website") or "").strip().lower(),
        (row.get("dispensary") or row.get("company_name") or "").strip().lower(),
        (row.get("state") or "").strip().lower(),
    )


def _load_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [dict(r) for r in reader]


def _write(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _all_headers(*rows: list[dict]) -> list[str]:
    fields: list[str] = []
    seen: set[str] = set()
    for rowset in rows:
        for row in rowset:
            for key in row:
                if key not in seen:
                    seen.add(key)
                    fields.append(key)
    return fields


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare current and prior outreach snapshots.")
    ap.add_argument("--snapshot", default=str(CURRENT))
    ap.add_argument("--previous", default=str(PREVIOUS))
    ap.add_argument("--out-dir", default=str(OUT))
    ap.add_argument("--run-id", default="")
    ap.add_argument("--version", default=CHANGE_VERSION)
    args = ap.parse_args()

    snapshot_path = Path(args.snapshot)
    previous_path = Path(args.previous)
    out_dir = Path(args.out_dir)
    run_key = _make_key(args.run_id)

    if not snapshot_path.exists():
        raise SystemExit(f"Missing snapshot: {snapshot_path}. Run outreach export first.")

    if not previous_path.exists():
        previous_path.parent.mkdir(parents=True, exist_ok=True)
        previous_path.write_text(snapshot_path.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"Initialized baseline snapshot -> {previous_path}")

    now = _now_key()
    diff_csv = out_dir / f"changes_{run_key}.csv"
    diff_txt = out_dir / f"changes_{run_key}.txt"

    current = _load_rows(snapshot_path)
    previous = _load_rows(previous_path)
    current_map = {_row_key(r): r for r in current if any(_row_key(r))}
    previous_map = {_row_key(r): r for r in previous if any(_row_key(r))}

    added = [current_map[k] for k in current_map.keys() - previous_map.keys()]
    removed = [previous_map[k] for k in previous_map.keys() - current_map.keys()]

    modified = []
    tracked_fields = ("score", "email", "phone", "owner_name", "owner_role", "segment")
    for key in current_map.keys() & previous_map.keys():
        before = previous_map[key]
        after = current_map[key]
        changes = []
        for field in tracked_fields:
            if (before.get(field) or "") != (after.get(field) or ""):
                changes.append(f"{field}: \"{before.get(field, '')}\" -> \"{after.get(field, '')}\"")
        if changes:
            row = after.copy()
            row["change_type"] = "modified"
            row["changes"] = "; ".join(changes)
            modified.append(row)

    change_rows = []
    for row in added:
        item = dict(row)
        item["change_type"] = "added"
        item["changes"] = ""
        change_rows.append(item)
    for row in removed:
        item = dict(row)
        item["change_type"] = "removed"
        item["changes"] = ""
        change_rows.append(item)
    change_rows.extend(modified)

    headers = _all_headers(current, previous)
    if not headers:
        headers = ["dispensary", "website", "state", "score"]
    headers = [*headers, "change_type", "changes"]
    _write(diff_csv, change_rows, headers)

    summary_lines = [
        f"Change Report ({now})",
        f"Run id: {run_key}",
        f"Run version: {args.version}",
        f"Current rows: {len(current)}",
        f"Previous rows: {len(previous)}",
        f"Added: {len(added)}",
        f"Removed: {len(removed)}",
        f"Modified: {len(modified)}",
        f"Snapshot: {snapshot_path.name}",
        f"Diff CSV: {diff_csv.name}",
    ]
    diff_txt.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    metrics = {
        "version": args.version,
        "run_id": run_key,
        "run_version": args.version,
        "snapshot": str(snapshot_path),
        "previous": str(previous_path),
        "current_count": len(current),
        "previous_count": len(previous),
        "added": len(added),
        "removed": len(removed),
        "modified": len(modified),
        "diff_csv": str(diff_csv),
        "summary_txt": str(diff_txt),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    CHANGES_META.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

    PREVIOUS.write_text(snapshot_path.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"Wrote {diff_csv}")
    print(f"Wrote {diff_txt}")


if __name__ == "__main__":
    main()
