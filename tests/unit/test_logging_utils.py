"""Tests for structured logging helpers."""

from __future__ import annotations

import io
import json
import logging

from nyc_taxi_pipeline.logging_utils import JsonFormatter, log_event


def test_log_event_outputs_json_payload() -> None:
    """Structured event logs should include job metadata."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("test-structured-logger")
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    log_event(
        logger,
        "job_started",
        "transform_silver",
        batch_id="batch-001",
        records_processed=10,
    )

    payload = json.loads(stream.getvalue())
    assert payload["event"] == "job_started"
    assert payload["job_name"] == "transform_silver"
    assert payload["batch_id"] == "batch-001"
    assert payload["records_processed"] == 10
    assert payload["level"] == "INFO"
    assert "timestamp" in payload
