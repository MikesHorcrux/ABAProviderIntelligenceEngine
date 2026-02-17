#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
from typing import List

from .base import SourceAdapter
from .seeds_adapter import SeedsAdapter


def build_adapters(base_dir: Path) -> List[SourceAdapter]:
    """Return enabled source adapters for ingestion.

    V1 default: local seeds CSV adapter.
    Additional adapters can be appended here as they are implemented.
    """
    seed_file = base_dir / 'seeds.csv'
    adapters: List[SourceAdapter] = []
    if seed_file.exists():
        adapters.append(SeedsAdapter(str(seed_file)))
    return adapters
