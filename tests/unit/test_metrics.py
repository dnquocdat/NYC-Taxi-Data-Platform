"""Tests for metrics recording."""

from __future__ import annotations

import json
from pathlib import Path

from nyc_taxi_pipeline.metrics import MetricsRecorder, record_standard_job_metrics


def test_metrics_recorder_writes_jsonl(tmp_path: Path) -> None:
    """Metrics should be appended as one JSON object per line."""
    output_path = tmp_path / "metrics" / "pipeline.jsonl"
    recorder = MetricsRecorder(output_path)

    recorder.record(
        "ingest_bronze",
        "records_processed",
        123,
        batch_id="batch-001",
        unit="records",
    )

    lines = output_path.read_text(encoding="utf-8").splitlines()
    payload = json.loads(lines[0])
    assert payload["job_name"] == "ingest_bronze"
    assert payload["metric_name"] == "records_processed"
    assert payload["value"] == 123
    assert payload["batch_id"] == "batch-001"


def test_record_standard_job_metrics_only_records_provided_values(tmp_path: Path) -> None:
    """Optional standard metrics should be emitted only when values are present."""
    output_path = tmp_path / "metrics.jsonl"
    recorder = MetricsRecorder(output_path)

    records = record_standard_job_metrics(
        recorder,
        "transform_silver",
        batch_id="batch-001",
        records_processed=10,
        invalid_records_count=2,
        duplicates_dropped=1,
    )

    assert [record.metric_name for record in records] == [
        "records_processed",
        "invalid_records_count",
        "duplicates_dropped",
    ]
    assert len(output_path.read_text(encoding="utf-8").splitlines()) == 3


def test_job_timer_records_duration(tmp_path: Path) -> None:
    """The timer context manager should emit job duration."""
    output_path = tmp_path / "metrics.jsonl"
    recorder = MetricsRecorder(output_path)

    with recorder.timer("load_clickhouse", batch_id="batch-001"):
        pass

    payload = json.loads(output_path.read_text(encoding="utf-8").splitlines()[0])
    assert payload["metric_name"] == "job_duration_seconds"
    assert payload["unit"] == "seconds"
    assert payload["metadata"]["status"] == "succeeded"
