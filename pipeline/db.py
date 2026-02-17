from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Optional


def connect_db(path: str | Path, schema_sql: str | Path | None = None) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON")
    con.row_factory = sqlite3.Row
    if schema_sql:
        con.executescript(Path(schema_sql).read_text())
        con.commit()
    return con


def fetch_one(con: sqlite3.Connection, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
    row = con.execute(sql, params).fetchone()
    return row
