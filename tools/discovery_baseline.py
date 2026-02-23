#!/usr/bin/env python3
"""Generate a baseline discovery report from CSV files."""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
REPORT_PATH = ROOT / "reports" / "discovery_baseline_latest.json"
DISCOVERIES_PATH = ROOT / "discoveries.csv"
SEEDS_PATH = ROOT / "seeds.csv"


def _normalize(value: str) -> str:
    return value.strip().lower()


def analyze_csv(path: Path, include_top_states: bool = False) -> dict[str, Any]:
    if not path.exists():
        return {
            "exists": False,
            "rows": 0,
            "missing_website_count": 0,
            "duplicate_website_count": 0,
            "top_states": [] if include_top_states else None,
        }

    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    websites: list[str] = []
    missing_website_count = 0

    for row in rows:
        website = _normalize((row.get("website") or ""))
        if website:
            websites.append(website)
        else:
            missing_website_count += 1

    website_counts = Counter(websites)
    duplicate_website_count = sum(count - 1 for count in website_counts.values() if count > 1)

    result: dict[str, Any] = {
        "exists": True,
        "rows": len(rows),
        "missing_website_count": missing_website_count,
        "duplicate_website_count": duplicate_website_count,
    }

    if include_top_states:
        state_counts: Counter[str] = Counter()
        for row in rows:
            state = (row.get("state") or "").strip()
            if state:
                state_counts[state] += 1

        result["top_states"] = [
            {"state": state, "count": count}
            for state, count in state_counts.most_common(5)
        ]

    return result


def main() -> None:
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "discoveries.csv": analyze_csv(DISCOVERIES_PATH, include_top_states=True),
        "seeds.csv": analyze_csv(SEEDS_PATH, include_top_states=False),
    }

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_PATH.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
        handle.write("\n")


if __name__ == "__main__":
    main()
