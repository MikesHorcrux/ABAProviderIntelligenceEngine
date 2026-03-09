from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from agent_runtime.router import status_snapshot as agent_runtime_status_snapshot
from cli.errors import ConfigError, DataValidationError
from pipeline.run_control import load_run_control, summarize_run_control
from pipeline.run_state import latest_run_state, load_run_state


ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "data" / "state" / "last_run_manifest.json"
LOCK_PATH = ROOT / "data" / "state" / "run_v4.lock"
OUT_DIR = ROOT / "out"
AGENT_RUNTIME_CONFIG_PATH = ROOT / "config" / "agent_runtime.json"
EXTERNAL_RESEARCH_ALLOWED_STATUSES = {"pending", "in_progress", "completed", "failed"}

READ_ONLY_PREFIXES = ("select", "with")
FORBIDDEN_SQL_TERMS = (
    "insert",
    "update",
    "delete",
    "alter",
    "drop",
    "create",
    "replace",
    "attach",
    "detach",
    "pragma journal_mode",
    "vacuum",
    "reindex",
)

PRESET_QUERIES = {
    "failed-domains": """
        SELECT seed_domain,
               last_status_code,
               last_run_status,
               last_run_success_pages,
               last_run_failure_pages,
               last_run_completed_at
        FROM seed_telemetry
        WHERE last_run_success_pages = 0
           OR last_run_status <> 'completed'
        ORDER BY last_run_completed_at DESC, seed_domain ASC
        LIMIT ?
    """,
    "blocked-domains": """
        SELECT seed_domain,
               last_status_code,
               last_run_status,
               last_run_success_pages,
               last_run_failure_pages,
               last_run_completed_at
        FROM seed_telemetry
        WHERE last_status_code IN (401, 403, 429, 503)
        ORDER BY last_run_completed_at DESC, seed_domain ASC
        LIMIT ?
    """,
    "stale-records": """
        SELECT canonical_name AS company_name,
               website_domain AS website,
               state,
               last_crawled_at,
               updated_at
        FROM locations
        WHERE COALESCE(deleted_at, '') = ''
          AND (
            last_crawled_at IS NULL
            OR last_crawled_at = ''
            OR datetime(last_crawled_at) <= datetime('now', '-30 days')
          )
        ORDER BY COALESCE(last_crawled_at, '') ASC, updated_at DESC
        LIMIT ?
    """,
    "low-confidence-leads": """
        SELECT l.canonical_name AS company_name,
               l.website_domain AS website,
               l.state,
               COALESCE(ls.score_total, 0) AS score,
               COALESCE(ls.tier, 'C') AS tier
        FROM locations l
        LEFT JOIN lead_scores ls ON ls.location_pk = l.location_pk
        WHERE COALESCE(l.deleted_at, '') = ''
          AND COALESCE(ls.score_total, 0) < 40
        ORDER BY COALESCE(ls.score_total, 0) ASC, l.updated_at DESC
        LIMIT ?
    """,
    "research-needed": """
        SELECT l.canonical_name AS company_name,
               l.website_domain AS website,
               l.state,
               COALESCE((
                 SELECT ls.score_total
                 FROM lead_scores ls
                 WHERE ls.location_pk = l.location_pk
                 ORDER BY ls.as_of DESC
                 LIMIT 1
               ), 0) AS score,
               COALESCE((
                 SELECT e.field_value
                 FROM evidence e
                 WHERE e.entity_type = 'location'
                   AND e.entity_pk = l.location_pk
                   AND e.field_name = 'agent_research_status'
                   AND COALESCE(e.deleted_at, '') = ''
                 ORDER BY e.captured_at DESC
                 LIMIT 1
               ), '') AS research_status,
               COALESCE((
                 SELECT e.field_value
                 FROM evidence e
                 WHERE e.entity_type = 'location'
                   AND e.entity_pk = l.location_pk
                   AND e.field_name = 'agent_research_gaps'
                   AND COALESCE(e.deleted_at, '') = ''
                 ORDER BY e.captured_at DESC
                 LIMIT 1
               ), '') AS research_gaps
        FROM locations l
        WHERE COALESCE(l.deleted_at, '') = ''
          AND COALESCE(l.website_domain, '') <> ''
          AND COALESCE((
            SELECT e.field_value
            FROM evidence e
            WHERE e.entity_type = 'location'
              AND e.entity_pk = l.location_pk
              AND e.field_name = 'agent_research_status'
              AND COALESCE(e.deleted_at, '') = ''
            ORDER BY e.captured_at DESC
            LIMIT 1
          ), 'research_needed') <> 'ready'
        ORDER BY score DESC, l.updated_at DESC
        LIMIT ?
    """,
}


def _connect_readonly(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path).expanduser().resolve()
    if not path.exists():
        raise ConfigError(f"SQLite DB not found: {path}")
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
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


def _external_research_summary(out_dir: Path) -> dict[str, Any]:
    dossier_dir = out_dir / "lead_intelligence"
    manifest_path = dossier_dir / "lead_intelligence_manifest.json"
    manifest = _read_json(manifest_path) or {}
    packages = list(manifest.get("packages") or [])
    package_rows: list[dict[str, Any]] = []
    counts = {
        "pending": 0,
        "in_progress": 0,
        "completed": 0,
        "failed": 0,
        "contract_missing": 0,
        "invalid": 0,
    }
    completed_with_report_count = 0

    for package in packages:
        if not isinstance(package, dict):
            continue
        package_dir_rel = str(package.get("package_dir") or "").strip()
        if not package_dir_rel:
            continue
        package_dir = dossier_dir / package_dir_rel
        status_rel = str(package.get("external_research_status") or f"{package_dir_rel}/external_research_status.json")
        report_rel = str(package.get("external_research_report") or f"{package_dir_rel}/external_research_report.md")
        status_path = dossier_dir / status_rel
        report_path = dossier_dir / report_rel
        status_payload = _read_json(status_path) if status_path.exists() else None
        raw_status = str((status_payload or {}).get("status") or "").strip().lower()
        if not status_path.exists():
            status = "contract_missing"
        elif raw_status in EXTERNAL_RESEARCH_ALLOWED_STATUSES:
            status = raw_status
        else:
            status = "invalid"
        counts[status] = int(counts.get(status, 0)) + 1
        report_exists = report_path.exists()
        if status == "completed" and report_exists:
            completed_with_report_count += 1
        package_rows.append(
            {
                "lead_id": str(package.get("lead_id") or ""),
                "company_name": str(package.get("company_name") or ""),
                "package_dir": str(package_dir),
                "status": status,
                "status_file": str(status_path),
                "status_file_exists": status_path.exists(),
                "report_path": str(report_path),
                "report_exists": report_exists,
                "agent_name": str((status_payload or {}).get("agent_name") or ""),
                "started_at": str((status_payload or {}).get("started_at") or ""),
                "completed_at": str((status_payload or {}).get("completed_at") or ""),
                "updated_at": str((status_payload or {}).get("updated_at") or ""),
                "source_count": int((status_payload or {}).get("source_count") or 0),
                "last_error": str((status_payload or {}).get("last_error") or ""),
            }
        )

    return {
        "manifest_path": str(manifest_path),
        "manifest_exists": manifest_path.exists(),
        "contract_version": str(manifest.get("external_research_contract_version") or ""),
        "package_count": len(package_rows),
        "pending_count": counts["pending"],
        "in_progress_count": counts["in_progress"],
        "completed_count": counts["completed"],
        "failed_count": counts["failed"],
        "completed_with_report_count": completed_with_report_count,
        "contract_missing_count": counts["contract_missing"],
        "invalid_count": counts["invalid"],
        "packages": package_rows,
    }


def run_status(*, db_path: str, run_id: str | None, run_state_dir: str | None) -> dict[str, Any]:
    manifest = _read_json(MANIFEST_PATH) or {}
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
        except Exception:
            control_summary = {}

    db_summary: dict[str, Any] = {}
    recent_failures: list[dict[str, Any]] = []
    if Path(db_path).expanduser().resolve().exists():
        con = _connect_readonly(db_path)
        db_summary = {
            "locations": con.execute("SELECT COUNT(*) FROM locations WHERE COALESCE(deleted_at,'')=''").fetchone()[0],
            "lead_scores": con.execute("SELECT COUNT(*) FROM lead_scores WHERE COALESCE(deleted_at,'')=''").fetchone()[0],
            "contacts": con.execute("SELECT COUNT(*) FROM contacts WHERE COALESCE(deleted_at,'')=''").fetchone()[0],
            "crawl_jobs": con.execute("SELECT COUNT(*) FROM crawl_jobs").fetchone()[0],
        }
        recent_failures = [
            dict(row)
            for row in con.execute(
                """
                SELECT seed_domain, last_status_code, last_run_status,
                       last_run_success_pages, last_run_failure_pages, last_run_completed_at
                FROM seed_telemetry
                WHERE last_run_success_pages = 0
                   OR last_status_code IN (401, 403, 429, 503)
                ORDER BY last_run_completed_at DESC, seed_domain ASC
                LIMIT 10
                """
            ).fetchall()
        ]
        con.close()

    return {
        "db": {"path": str(Path(db_path).expanduser().resolve()), **db_summary},
        "manifest": manifest,
        "checkpoint": checkpoint or {},
        "control": control_summary,
        "agent_runtime": agent_runtime_status_snapshot(AGENT_RUNTIME_CONFIG_PATH),
        "external_research": _external_research_summary(OUT_DIR),
        "lock": _file_snapshot(LOCK_PATH),
        "outputs": {
            "research_queue": _file_snapshot(OUT_DIR / "research_queue.csv"),
            "agent_research_queue": _file_snapshot(OUT_DIR / "agent_research_queue.csv"),
            "lead_intelligence_index": _file_snapshot(OUT_DIR / "lead_intelligence" / "lead_intelligence_index.csv"),
            "lead_intelligence_table": _file_snapshot(OUT_DIR / "lead_intelligence" / "lead_intelligence_table.md"),
            "lead_intelligence_manifest": _file_snapshot(OUT_DIR / "lead_intelligence" / "lead_intelligence_manifest.json"),
            "outreach_legacy": _file_snapshot(OUT_DIR / "outreach_dispensary_100.csv"),
            "quality": _file_snapshot(OUT_DIR / "quality_report.json"),
        },
        "recent_failures": recent_failures,
    }


def _validate_readonly_query(query: str) -> str:
    sql = (query or "").strip()
    if not sql:
        raise DataValidationError("SQL query cannot be empty.")
    if sql.count(";") > 1 or (sql.endswith(";") and ";" in sql[:-1]):
        raise DataValidationError("Only a single SELECT statement is allowed.")
    normalized = sql.rstrip(";").strip().lower()
    if not normalized.startswith(READ_ONLY_PREFIXES):
        raise DataValidationError("SQL command must start with SELECT or WITH.")
    if any(term in normalized for term in FORBIDDEN_SQL_TERMS):
        raise DataValidationError("SQL command must be read-only.")
    return sql.rstrip(";")


def run_sql(*, db_path: str, query: str, limit: int) -> dict[str, Any]:
    sql = _validate_readonly_query(query)
    con = _connect_readonly(db_path)
    cursor = con.execute(sql)
    rows = cursor.fetchmany(max(1, limit))
    columns = [col[0] for col in (cursor.description or [])]
    payload_rows = [dict(zip(columns, row)) for row in rows]
    con.close()
    return {
        "query": sql,
        "row_count": len(payload_rows),
        "limit": max(1, limit),
        "columns": columns,
        "rows": payload_rows,
    }


def run_search(*, db_path: str, query: str | None, preset: str | None, limit: int) -> dict[str, Any]:
    con = _connect_readonly(db_path)
    if preset:
        if preset not in PRESET_QUERIES:
            con.close()
            raise DataValidationError(
                f"Unknown search preset: {preset}",
                details={"available_presets": sorted(PRESET_QUERIES)},
            )
        rows = [dict(row) for row in con.execute(PRESET_QUERIES[preset], (max(1, limit),)).fetchall()]
        con.close()
        return {"preset": preset, "row_count": len(rows), "rows": rows}

    search_term = (query or "").strip()
    if not search_term:
        con.close()
        raise DataValidationError("Provide a text query or `--preset` for search.")

    like_value = f"%{search_term.lower()}%"
    rows = [
        dict(row)
        for row in con.execute(
            """
            SELECT l.canonical_name AS company_name,
                   l.website_domain AS website,
                   l.state,
                   COALESCE(MAX(ls.score_total), 0) AS score,
                   COALESCE(MAX(ls.tier), 'C') AS tier,
                   COALESCE(MAX(c.full_name), '') AS contact_name,
                   COALESCE(MAX(c.role), '') AS contact_role
            FROM locations l
            LEFT JOIN lead_scores ls ON ls.location_pk = l.location_pk
            LEFT JOIN contacts c ON c.location_pk = l.location_pk AND COALESCE(c.deleted_at, '') = ''
            LEFT JOIN evidence e ON e.entity_pk = l.location_pk AND COALESCE(e.deleted_at, '') = ''
            WHERE COALESCE(l.deleted_at, '') = ''
              AND (
                lower(l.canonical_name) LIKE ?
                OR lower(l.website_domain) LIKE ?
                OR lower(COALESCE(c.full_name, '')) LIKE ?
                OR lower(COALESCE(c.role, '')) LIKE ?
                OR lower(COALESCE(c.email, '')) LIKE ?
                OR lower(COALESCE(e.field_value, '')) LIKE ?
                OR lower(COALESCE(e.source_url, '')) LIKE ?
              )
            GROUP BY l.location_pk, l.canonical_name, l.website_domain, l.state
            ORDER BY COALESCE(MAX(ls.score_total), 0) DESC, l.updated_at DESC
            LIMIT ?
            """,
            (
                like_value,
                like_value,
                like_value,
                like_value,
                like_value,
                like_value,
                like_value,
                max(1, limit),
            ),
        ).fetchall()
    ]
    con.close()
    return {"query": search_term, "row_count": len(rows), "rows": rows}
