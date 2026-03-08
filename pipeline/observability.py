from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

_BASE_LOG_RECORD = logging.makeLogRecord({})
_STANDARD_LOG_RECORD_KEYS = set(_BASE_LOG_RECORD.__dict__.keys())


@dataclass
class Metrics:
    job_id: str
    counters: dict[str, int] = field(default_factory=dict)

    def inc(self, key: str, delta: int = 1) -> None:
        self.counters[key] = int(self.counters.get(key, 0)) + int(delta)

    def snapshot(self) -> dict[str, int]:
        return dict(self.counters)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "job_id": getattr(record, "job_id", None),
            "stage": getattr(record, "stage", None),
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _STANDARD_LOG_RECORD_KEYS:
                continue
            if key.startswith("_cannaradar_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def build_logger(job_id: str, stage: str) -> logging.Logger:
    logger = logging.getLogger(f"cannaradar.{job_id}.{stage}")
    if getattr(logger, "_cannaradar_ready", False):
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger._cannaradar_ready = True
    logger._cannaradar_job_id = job_id
    logger._cannaradar_stage = stage
    return logger


def log_with_context(logger: logging.Logger, stage: str, message: str, job_id: str, **fields: object) -> None:
    logger.info(message, extra={**fields, "job_id": job_id, "stage": stage})


def log_stage_start(logger: logging.Logger, stage: str, job_id: str) -> float:
    logger.info("stage_start", extra={"job_id": job_id, "stage": stage, "event": "start", "started_at": time.time()})
    return time.time()


def log_stage_end(logger: logging.Logger, stage: str, job_id: str, started_at: float, counters: dict[str, int] | None = None) -> None:
    payload = {"event": "complete", "duration_ms": round((time.time() - started_at) * 1000, 2), "job_id": job_id, "stage": stage}
    if counters is not None:
        payload.update(counters)
    logger.info("stage_complete", extra=payload)
