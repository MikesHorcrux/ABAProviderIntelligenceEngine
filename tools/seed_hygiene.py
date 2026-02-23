#!/usr/bin/env python3
"""Clean seed CSV input and emit a hygiene report."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = ROOT / "seeds.csv"
OUTPUT_CSV_PATH = ROOT / "out" / "seeds_clean.csv"
OUTPUT_REPORT_PATH = ROOT / "out" / "seed_hygiene_report.json"


def _normalize_text(value: str | None) -> str:
    return (value or "").strip()


def normalize_domain(url_or_domain: str) -> str:
    value = _normalize_text(url_or_domain).lower()
    if not value:
        return ""
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlparse(value)
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def normalize_website(raw: str) -> str:
    value = _normalize_text(raw)
    if not value:
        return ""

    try:
        if "://" not in value:
            value = f"https://{value}"
        parsed = urlparse(value)
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if not host:
            return ""
        path = parsed.path or "/"
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        query_items = parse_qsl(parsed.query, keep_blank_values=False)
        query = urlencode(sorted(query_items))
        # Canonicalize to https for stable dedupe.
        return urlunparse(("https", host, path, "", query, ""))
    except Exception:
        return ""


def main() -> None:
    with INPUT_PATH.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        source_rows = list(reader)
        source_fields = list(reader.fieldnames or [])

    output_fields = list(source_fields)
    if "domain" not in output_fields:
        output_fields.append("domain")

    cleaned_rows: list[dict[str, str]] = []
    seen_websites: set[str] = set()
    missing_website_count = 0
    duplicate_website_count = 0

    for row in source_rows:
        normalized_website = normalize_website(row.get("website", ""))
        if not normalized_website:
            missing_website_count += 1
            continue
        if normalized_website in seen_websites:
            duplicate_website_count += 1
            continue

        seen_websites.add(normalized_website)
        cleaned = dict(row)
        cleaned["website"] = normalized_website
        cleaned["domain"] = normalize_domain(normalized_website)
        cleaned_rows.append(cleaned)

    OUTPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(cleaned_rows)

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": str(INPUT_PATH),
        "output_csv_path": str(OUTPUT_CSV_PATH),
        "output_report_path": str(OUTPUT_REPORT_PATH),
        "input_rows": len(source_rows),
        "output_rows": len(cleaned_rows),
        "removed_missing_website": missing_website_count,
        "removed_duplicate_normalized_website": duplicate_website_count,
        "unique_normalized_websites": len(seen_websites),
    }

    with OUTPUT_REPORT_PATH.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
        handle.write("\n")


if __name__ == "__main__":
    main()
