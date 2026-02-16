#!/usr/bin/env python3
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable, Dict, Any, List

@dataclass
class LicenseRow:
    state: str
    license_id: str
    license_type: str
    status: str
    legal_name: str
    dba_name: str
    address_1: str
    city: str
    zip: str
    website: str
    phone: str
    source_url: str
    retrieved_at: str


class SourceAdapter:
    """Plugin interface for state/public source adapters."""
    source_name: str = "base"

    def fetch_raw(self) -> Any:
        raise NotImplementedError

    def parse_raw_to_rows(self, raw: Any) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError

    def normalize_rows(self, rows: Iterable[Dict[str, Any]]) -> List[LicenseRow]:
        out: List[LicenseRow] = []
        for r in rows:
            out.append(LicenseRow(
                state=(r.get('state') or '').strip(),
                license_id=(r.get('license_id') or '').strip(),
                license_type=(r.get('license_type') or '').strip(),
                status=(r.get('status') or '').strip(),
                legal_name=(r.get('legal_name') or '').strip(),
                dba_name=(r.get('dba_name') or '').strip(),
                address_1=(r.get('address_1') or '').strip(),
                city=(r.get('city') or '').strip(),
                zip=(r.get('zip') or '').strip(),
                website=(r.get('website') or '').strip(),
                phone=(r.get('phone') or '').strip(),
                source_url=(r.get('source_url') or '').strip(),
                retrieved_at=(r.get('retrieved_at') or '').strip(),
            ))
        return out
