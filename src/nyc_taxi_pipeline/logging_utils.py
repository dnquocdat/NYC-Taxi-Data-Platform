"""Structured JSON logging helpers for pipeline jobs."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any


class JsonFormatter(logging.Formatter):
    """Format log records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        """Return a stable JSON representation of a log record."""
        payload: dict[str, Any]
        if isinstance(record.msg, dict):
            payload = dict(record.msg)
        else:
            payload = {"message": record.getMessage()}

        payload.setdefault("timestamp", datetime.fromtimestamp(record.created, UTC).isoformat())
        payload.setdefault("level", record.levelname)
        payload.setdefault("logger", record.name)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, ensure_ascii=False, sort_keys=True)


def configure_logging(level: str = "INFO") -> None:
    """Configure root logging to write structured JSON to stdout."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level.upper())


def get_logger(name: str) -> logging.Logger:
    """Return a named logger after structured logging has been configured."""
    return logging.getLogger(name)


def log_event(
    logger: logging.Logger,
    event: str,
    job_name: str,
    *,
    batch_id: str | None = None,
    level: int = logging.INFO,
    **metadata: Any,
) -> None:
    """Write a structured event log for a pipeline job."""
    payload: dict[str, Any] = {
        "event": event,
        "job_name": job_name,
        "batch_id": batch_id,
        **metadata,
    }
    logger.log(level, payload)
