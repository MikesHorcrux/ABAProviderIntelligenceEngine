from __future__ import annotations

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from pipeline.utils import normalize_url


@dataclass(frozen=True)
class DiscoverySeed:
    name: str
    website: str
    state: str
    market: str
    source: str = "seed_pack"
    priority: int = 0
    tier: str = ""
    source_type: str = ""
    browser_required: bool = False
    extraction_profile: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DiscoveryBatch:
    seeds: tuple[DiscoverySeed, ...]
    total: int
    source: str = "seed_pack"


def _seed_from_mapping(
    row: dict[str, Any],
    *,
    source: str,
    priority: int,
) -> DiscoverySeed | None:
    website = normalize_url((row.get("website") or "").strip())
    if not website:
        return None
    seed_priority = int(row.get("priority") or priority)
    return DiscoverySeed(
        name=str(row.get("name") or row.get("label") or website),
        website=website,
        state=str(row.get("state") or "").strip(),
        market=str(row.get("metro") or row.get("market") or "").strip(),
        source=source,
        priority=seed_priority,
        tier=str(row.get("tier") or "").strip(),
        source_type=str(row.get("source_type") or row.get("sourceType") or "").strip(),
        browser_required=bool(row.get("browser_required") or row.get("browserRequired") or False),
        extraction_profile=str(row.get("extraction_profile") or row.get("extractionProfile") or "").strip(),
        metadata={k: v for k, v in row.items() if k not in {"name", "website", "state", "metro", "market", "tier", "source_type", "sourceType", "browser_required", "browserRequired", "extraction_profile", "extractionProfile", "priority"}},
    )


def load_seeds(
    path: str,
    *,
    source: str = "seed_pack",
    priority: int = 0,
) -> DiscoveryBatch:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    seen = set[tuple[str, str, str]]()
    items: list[DiscoverySeed] = []

    if p.suffix.lower() == ".json":
        payload = json.loads(p.read_text(encoding="utf-8"))
        rows = list(payload.get("sources") or [])
    else:
        rows = list(csv.DictReader(p.open(encoding="utf-8")))

    for row in rows:
        seed = _seed_from_mapping(dict(row), source=source, priority=priority)
        if seed is None:
            continue
        key = (seed.website, seed.state.lower(), seed.tier.lower())
        if key in seen:
            continue
        seen.add(key)
        items.append(seed)

    items = sorted(items, key=lambda item: (item.priority, item.name.lower(), item.website), reverse=True)
    return DiscoveryBatch(seeds=tuple(items), total=len(items), source=source)


def dedupe_seeds(seeds: Iterable[DiscoverySeed], limit: int | None = None) -> list[DiscoverySeed]:
    out: list[DiscoverySeed] = []
    seen: set[tuple[str, str]] = set()
    for seed in seeds:
        key = (seed.website, seed.state.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(seed)
        if limit and len(out) >= limit:
            break
    return out
