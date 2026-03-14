from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from cli.errors import ConfigError, DataValidationError
from pipeline.db import normalized_db_timeout_ms, sqlite_timeout_seconds
from pipeline.run_control import load_run_control, summarize_run_control
from pipeline.run_state import latest_run_state, load_run_state
from runtime_context import RuntimePaths, default_runtime_paths


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNTIME_PATHS = default_runtime_paths()
MANIFEST_PATH = DEFAULT_RUNTIME_PATHS.manifest_path
LOCK_PATH = DEFAULT_RUNTIME_PATHS.lock_path
OUT_DIR = DEFAULT_RUNTIME_PATHS.provider_out_dir
READ_ONLY_PREFIXES = ("select", "with")
FORBIDDEN_SQL_TERMS = ("insert", "update", "delete", "alter", "drop", "create", "replace", "attach", "detach", "vacuum", "reindex")

PRESET_QUERIES = {
    "failed-domains": """
        SELECT seed_domain, last_status_code, last_run_status, last_run_success_pages, last_run_failure_pages, last_run_completed_at
        FROM seed_telemetry
        WHERE last_run_success_pages = 0 OR last_run_status <> 'completed'
        ORDER BY last_run_completed_at DESC, seed_domain ASC
        LIMIT ?
    """,
    "blocked-domains": """
        SELECT seed_domain, last_status_code, last_run_status, last_run_completed_at
        FROM seed_telemetry
        WHERE last_status_code IN (401, 403, 429, 503)
        ORDER BY last_run_completed_at DESC, seed_domain ASC
        LIMIT ?
    """,
    "low-confidence-records": """
        SELECT provider_name_snapshot AS provider_name, practice_name_snapshot AS practice_name, record_confidence, review_status, blocked_reason
        FROM provider_practice_records
        WHERE record_confidence < 0.60
        ORDER BY record_confidence ASC, updated_at DESC
        LIMIT ?
    """,
    "review-queue": """
        SELECT review_type, provider_name, practice_name, reason, source_url, status, created_at
        FROM review_queue
        ORDER BY created_at DESC
        LIMIT ?
    """,
    "contradictions": """
        SELECT c.field_name, c.preferred_value, c.conflicting_value, c.preferred_source_url, c.conflicting_source_url,
               pr.provider_name_snapshot AS provider_name, pr.practice_name_snapshot AS practice_name
        FROM contradictions c
        INNER JOIN provider_practice_records pr ON pr.record_id = c.record_id
        ORDER BY c.created_at DESC
        LIMIT ?
    """,
    "outreach-ready": """
        SELECT provider_name_snapshot AS provider_name, practice_name_snapshot AS practice_name, outreach_fit_score,
               record_confidence, diagnoses_asd, diagnoses_adhd, license_status
        FROM provider_practice_records
        WHERE outreach_ready=1
        ORDER BY outreach_fit_score DESC, record_confidence DESC, provider_name_snapshot ASC
        LIMIT ?
    """,
}


def _connect_readonly(db_path: str | Path, *, db_timeout_ms: int | None = None) -> sqlite3.Connection:
    path = Path(db_path).expanduser().resolve()
    if not path.exists():
        raise ConfigError(f"SQLite DB not found: {path}")
    effective_timeout_ms = normalized_db_timeout_ms(db_timeout_ms)
    con = sqlite3.connect(
        f"file:{path}?mode=ro",
        uri=True,
        timeout=sqlite_timeout_seconds(effective_timeout_ms),
    )
    con.execute(f"PRAGMA busy_timeout = {effective_timeout_ms}")
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only = ON")
    return con


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _file_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "path": str(path)}
    stat = path.stat()
    return {
        "exists": True,
        "path": str(path),
        "size_bytes": stat.st_size,
        "modified_at_epoch": int(stat.st_mtime),
    }


def run_status(
    *,
    db_path: str,
    run_id: str | None,
    run_state_dir: str | None,
    db_timeout_ms: int | None = None,
    runtime_paths: RuntimePaths | None = None,
) -> dict[str, Any]:
    paths = runtime_paths or DEFAULT_RUNTIME_PATHS
    manifest = _read_json(paths.manifest_path) or {}
    checkpoint = None
    if run_id:
        try:
            checkpoint = load_run_state(run_id, run_state_dir)
        except FileNotFoundError:
            checkpoint = None
    if checkpoint is None:
        checkpoint = latest_run_state(run_state_dir)

    control_summary: dict[str, Any] = {}
    control_run_id = str((checkpoint or {}).get("run_id") or run_id or "")
    if control_run_id:
        try:
            control_summary = summarize_run_control(load_run_control(control_run_id, run_state_dir))
        except FileNotFoundError:
            control_summary = {}

    con = _connect_readonly(db_path, db_timeout_ms=db_timeout_ms)
    counts = {
        "providers": int(con.execute("SELECT COUNT(*) FROM providers").fetchone()[0]),
        "practices": int(con.execute("SELECT COUNT(*) FROM practices").fetchone()[0]),
        "records": int(con.execute("SELECT COUNT(*) FROM provider_practice_records").fetchone()[0]),
        "approved_records": int(con.execute("SELECT COUNT(*) FROM provider_practice_records WHERE export_status='approved'").fetchone()[0]),
        "outreach_ready_records": int(con.execute("SELECT COUNT(*) FROM provider_practice_records WHERE outreach_ready=1").fetchone()[0]),
        "review_queue": int(con.execute("SELECT COUNT(*) FROM review_queue").fetchone()[0]),
        "contradictions": int(con.execute("SELECT COUNT(*) FROM contradictions").fetchone()[0]),
    }
    con.close()

    latest_records = None
    provider_out_dir = paths.provider_out_dir
    if provider_out_dir.exists():
        candidates = sorted(provider_out_dir.glob("provider_records_*.csv"), key=lambda path: path.stat().st_mtime, reverse=True)
        latest_records = _file_snapshot(candidates[0]) if candidates else None
    recent_failures: list[dict[str, Any]] = []
    last_error = dict((checkpoint or {}).get("last_error") or {})
    if last_error:
        recent_failures.append(last_error)

    return {
        "db": _file_snapshot(Path(db_path).expanduser().resolve()),
        "manifest": manifest,
        "checkpoint": checkpoint or {},
        "control": control_summary,
        "lock": _file_snapshot(paths.lock_path),
        "counts": counts,
        "recent_failures": recent_failures,
        "outputs": {
            "records_csv": latest_records or _file_snapshot(provider_out_dir / "missing.csv"),
            "records_json": _file_snapshot(next(iter(sorted(provider_out_dir.glob("provider_records_*.json"), key=lambda path: path.stat().st_mtime, reverse=True)), provider_out_dir / "missing.json")),
            "review_queue_csv": _file_snapshot(next(iter(sorted(provider_out_dir.glob("review_queue_*.csv"), key=lambda path: path.stat().st_mtime, reverse=True)), provider_out_dir / "missing_review.csv")),
            "sales_report_csv": _file_snapshot(next(iter(sorted(provider_out_dir.glob("sales_report_*.csv"), key=lambda path: path.stat().st_mtime, reverse=True)), provider_out_dir / "missing_sales.csv")),
            "profiles_dir": _file_snapshot(provider_out_dir / "profiles"),
            "evidence_dir": _file_snapshot(provider_out_dir / "evidence"),
            "outreach_dir": _file_snapshot(provider_out_dir / "outreach"),
        },
    }


def run_search(*, db_path: str, query: str | None, preset: str | None, limit: int, db_timeout_ms: int | None = None) -> dict[str, Any]:
    con = _connect_readonly(db_path, db_timeout_ms=db_timeout_ms)
    try:
        if preset:
            sql = PRESET_QUERIES.get(preset)
            if not sql:
                raise DataValidationError(f"Unknown search preset: {preset}")
            rows = [dict(row) for row in con.execute(sql, (limit,)).fetchall()]
            return {"preset": preset, "row_count": len(rows), "rows": rows}

        needle = (query or "").strip().lower()
        if not needle:
            raise DataValidationError("Search query is required when no preset is provided.")
        rows = [
            dict(row)
            for row in con.execute(
                """
                SELECT provider_name_snapshot AS provider_name, practice_name_snapshot AS practice_name,
                       license_status, diagnoses_asd, diagnoses_adhd, prescriptive_authority, record_confidence, outreach_fit_score, outreach_ready
                FROM provider_practice_records
                WHERE lower(provider_name_snapshot) LIKE ?
                   OR lower(practice_name_snapshot) LIKE ?
                ORDER BY outreach_fit_score DESC, record_confidence DESC, provider_name_snapshot ASC
                LIMIT ?
                """,
                (f"%{needle}%", f"%{needle}%", limit),
            ).fetchall()
        ]
        return {"query": query, "row_count": len(rows), "rows": rows}
    finally:
        con.close()


def run_sql(*, db_path: str, query: str, limit: int, db_timeout_ms: int | None = None) -> dict[str, Any]:
    normalized = (query or "").strip()
    if not normalized:
        raise DataValidationError("SQL query is required.")
    lowered = normalized.lower()
    if not lowered.startswith(READ_ONLY_PREFIXES):
        raise DataValidationError("SQL command must start with SELECT or WITH.")
    if any(term in lowered for term in FORBIDDEN_SQL_TERMS):
        raise DataValidationError("SQL command must be read-only.")

    con = _connect_readonly(db_path, db_timeout_ms=db_timeout_ms)
    try:
        rows = con.execute(f"SELECT * FROM ({normalized}) LIMIT ?", (limit,)).fetchall()
        data = [dict(row) for row in rows]
        return {"query": normalized, "row_count": len(data), "rows": data}
    finally:
        con.close()
