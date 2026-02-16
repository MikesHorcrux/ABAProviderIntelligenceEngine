#!/usr/bin/env python3
from __future__ import annotations
import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable

from .base import SourceAdapter


class SeedsAdapter(SourceAdapter):
    source_name = "seeds_csv"

    def __init__(self, seed_file: str):
        self.seed_file = Path(seed_file)

    def fetch_raw(self) -> Any:
        return list(csv.DictReader(self.seed_file.open()))

    def parse_raw_to_rows(self, raw: Any) -> Iterable[Dict[str, Any]]:
        now = datetime.now().isoformat(timespec='seconds')
        for r in raw:
            yield {
                'state': r.get('state', ''),
                'license_id': '',
                'license_type': 'retail_dispensary_candidate',
                'status': 'unknown',
                'legal_name': r.get('name', ''),
                'dba_name': r.get('name', ''),
                'address_1': '',
                'city': '',
                'zip': '',
                'website': r.get('website', ''),
                'phone': '',
                'source_url': r.get('website', ''),
                'retrieved_at': now,
            }
