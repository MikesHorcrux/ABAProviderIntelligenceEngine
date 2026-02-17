from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from pipeline.utils import normalize_url


@dataclass(frozen=True)
class DiscoverySeed:
    name: str
    website: str
    state: str
    market: str
    source: str = "seed_file"
    priority: int = 0


@dataclass(frozen=True)
class DiscoveryBatch:
    seeds: tuple[DiscoverySeed, ...]
    total: int
    source: str = "seed_file"


def load_seeds(
    path: str,
    *,
    source: str = "seed_file",
    priority: int = 0,
) -> DiscoveryBatch:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    seen = set[tuple[str, str]]()
    items: list[DiscoverySeed] = []
    for row in csv.DictReader(p.open()):
        website = normalize_url((row.get("website") or "").strip())
        if not website:
            continue
        name = (row.get("name") or "").strip()
        state = (row.get("state") or "").strip()
        market = (row.get("market") or "").strip()
        key = (website, state.lower())
        if key in seen:
            continue
        seen.add(key)
        items.append(
            DiscoverySeed(
                name=name,
                website=website,
                state=state,
                market=market,
                source=source,
                priority=priority,
            )
        )
    return DiscoveryBatch(seeds=tuple(items), total=len(items), source=source)


def dedupe_seeds(seeds: Iterable[DiscoverySeed], limit: int | None = None) -> list[DiscoverySeed]:
    out: list[DiscoverySeed] = []
    seen: set[tuple[str, str]] = set()
    for s in seeds:
        key = (s.website, s.state.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(s)
        if limit and len(out) >= limit:
            break
    return out
