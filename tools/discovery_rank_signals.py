#!/usr/bin/env python3
"""Rank discovery seeds using a small set of explicit quality signals."""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parent.parent
PREFERRED_INPUT_PATH = ROOT / "out" / "seeds_clean.csv"
FALLBACK_INPUT_PATH = ROOT / "seeds.csv"
OUTPUT_CSV_PATH = ROOT / "out" / "discovery_ranked.csv"
OUTPUT_REPORT_PATH = ROOT / "out" / "discovery_rank_report.json"

KNOWN_MSO_NAMES = {
    "curaleaf",
    "trulieve",
    "green thumb industries",
    "verano",
    "cresco labs",
    "ayr wellness",
    "jushi holdings",
    "cannabist company",
    "columbia care",
    "planet 13",
}

PLACEHOLDER_DOMAINS = {
    "example.com",
    "example.org",
    "example.net",
    "localhost",
    "test.com",
    "placeholder.com",
    "yourdomain.com",
    "domain.invalid",
}


def _normalize_text(value: str | None) -> str:
    return (value or "").strip()


def _normalize_name(value: str | None) -> list[str]:
    raw = _normalize_text(value).lower()
    return "".join(ch if ch.isalnum() or ch.isspace() else " " for ch in raw).split()


def _normalized_name_string(value: str | None) -> str:
    tokens = _normalize_name(value)
    return " ".join(tokens)


def _has_state(row: dict[str, str]) -> bool:
    return bool(_normalize_text(row.get("state")))


def _has_market(row: dict[str, str]) -> bool:
    return bool(_normalize_text(row.get("market")))


def _known_mso_name_match(row: dict[str, str]) -> bool:
    return _normalized_name_string(row.get("name")) in KNOWN_MSO_NAMES


def _website_parts(website: str) -> tuple[str, str]:
    value = _normalize_text(website)
    if not value:
        return "", ""
    parsed = urlparse(value)
    scheme = (parsed.scheme or "").lower()
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return scheme, host


def _non_placeholder_domain(host: str) -> bool:
    if not host:
        return False
    if host in PLACEHOLDER_DOMAINS:
        return False
    banned_tokens = ("example", "placeholder", "invalid", "localhost")
    return not any(token in host for token in banned_tokens)


def _website_quality(row: dict[str, str]) -> tuple[bool, str]:
    scheme, host = _website_parts(row.get("website", ""))
    if scheme != "https":
        return False, "website_quality_missing_https"
    if not _non_placeholder_domain(host):
        return False, "website_quality_placeholder_domain"
    return True, "website_quality"


def _score_row(row: dict[str, str]) -> tuple[int, list[str], dict[str, bool]]:
    reasons: list[str] = []
    signals: dict[str, bool] = {
        "has_state": _has_state(row),
        "has_market": _has_market(row),
        "known_mso_name_match": _known_mso_name_match(row),
    }

    website_ok, website_reason = _website_quality(row)
    signals["website_quality"] = website_ok

    score = 0
    for signal_name in ("has_state", "has_market", "known_mso_name_match", "website_quality"):
        if signals[signal_name]:
            score += 1
            reasons.append(signal_name)

    if not website_ok:
        reasons.append(website_reason)

    return score, reasons, signals


def _input_path() -> Path:
    return PREFERRED_INPUT_PATH if PREFERRED_INPUT_PATH.exists() else FALLBACK_INPUT_PATH


def main() -> None:
    input_path = _input_path()
    if not input_path.exists():
        raise SystemExit(f"Input CSV not found: {input_path}")

    with input_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        source_rows = list(reader)
        source_fields = list(reader.fieldnames or [])

    output_fields = list(source_fields)
    for field in ("rank_score", "rank_reasons"):
        if field not in output_fields:
            output_fields.append(field)

    scored_rows: list[dict[str, str]] = []
    score_counter: Counter[int] = Counter()
    signal_counter: Counter[str] = Counter()

    for row in source_rows:
        score, reasons, signals = _score_row(row)
        score_counter[score] += 1
        for signal_name, passed in signals.items():
            if passed:
                signal_counter[signal_name] += 1

        ranked = dict(row)
        ranked["rank_score"] = str(score)
        ranked["rank_reasons"] = ",".join(reasons)
        scored_rows.append(ranked)

    scored_rows.sort(
        key=lambda r: (
            -int(r["rank_score"]),
            _normalize_text(r.get("name")).lower(),
            _normalize_text(r.get("website")).lower(),
        )
    )

    OUTPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(scored_rows)

    total_rows = len(source_rows)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": str(input_path),
        "output_csv_path": str(OUTPUT_CSV_PATH),
        "output_report_path": str(OUTPUT_REPORT_PATH),
        "rows_total": total_rows,
        "score_distribution": {str(score): count for score, count in sorted(score_counter.items())},
        "signal_hits": {
            signal: {
                "count": count,
                "pct_rows": (count / total_rows) if total_rows else 0.0,
            }
            for signal, count in sorted(signal_counter.items())
        },
        "score_summary": {
            "min": min(score_counter) if score_counter else 0,
            "max": max(score_counter) if score_counter else 0,
            "avg": (
                sum(score * count for score, count in score_counter.items()) / total_rows
                if total_rows
                else 0.0
            ),
        },
    }

    with OUTPUT_REPORT_PATH.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
        handle.write("\n")


if __name__ == "__main__":
    main()
