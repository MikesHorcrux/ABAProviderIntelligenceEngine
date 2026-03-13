from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Optional

from jobs.ingest_sources import init_db


DEFAULT_DB_TIMEOUT_MS = 30000


def normalized_db_timeout_ms(timeout_ms: int | None) -> int:
    if timeout_ms is None:
        return DEFAULT_DB_TIMEOUT_MS
    return max(1, int(timeout_ms))


def sqlite_timeout_seconds(timeout_ms: int | None) -> float:
    return normalized_db_timeout_ms(timeout_ms) / 1000.0


def connect_db(path: str | Path, schema_sql: str | Path | None = None, *, timeout_ms: int | None = None) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    effective_timeout_ms = normalized_db_timeout_ms(timeout_ms)
    con = sqlite3.connect(db_path, timeout=sqlite_timeout_seconds(effective_timeout_ms))
    con.execute(f"PRAGMA busy_timeout = {effective_timeout_ms}")
    con.execute("PRAGMA foreign_keys = ON")
    con.row_factory = sqlite3.Row
    if schema_sql:
        init_db(con)
    return con


def fetch_one(con: sqlite3.Connection, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
    row = con.execute(sql, params).fetchone()
    return row
