"""Pipeline metrics recording helpers."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any


@dataclass(frozen=True)
class MetricRecord:
    """One JSONL metric emitted by a pipeline job."""

    job_name: str
    metric_name: str
    value: int | float
    batch_id: str | None = None
    unit: str | None = None
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    metadata: Mapping[str, Any] = field(default_factory=dict)


class MetricsRecorder:
    """Append pipeline metrics to a local JSONL file."""

    def __init__(self, output_path: str | Path) -> None:
        self.output_path = Path(output_path)

    def record(
        self,
        job_name: str,
        metric_name: str,
        value: int | float,
        *,
        batch_id: str | None = None,
        unit: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> MetricRecord:
        """Append one metric record to the JSONL sink."""
        record = MetricRecord(
            job_name=job_name,
            metric_name=metric_name,
            value=value,
            batch_id=batch_id,
            unit=unit,
            metadata={} if metadata is None else dict(metadata),
        )
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(record), default=str, sort_keys=True))
            handle.write("\n")
        return record

    def timer(self, job_name: str, *, batch_id: str | None = None) -> JobTimer:
        """Create a context manager that records `job_duration_seconds`."""
        return JobTimer(self, job_name, batch_id=batch_id)


class JobTimer:
    """Context manager that records job duration on exit."""

    def __init__(
        self,
        recorder: MetricsRecorder,
        job_name: str,
        *,
        batch_id: str | None = None,
    ) -> None:
        self.recorder = recorder
        self.job_name = job_name
        self.batch_id = batch_id
        self._started_at: float | None = None

    def __enter__(self) -> JobTimer:
        self._started_at = time.monotonic()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._started_at is None:
            return
        duration = time.monotonic() - self._started_at
        self.recorder.record(
            self.job_name,
            "job_duration_seconds",
            round(duration, 6),
            batch_id=self.batch_id,
            unit="seconds",
            metadata={"status": "failed" if exc_type else "succeeded"},
        )


def record_standard_job_metrics(
    recorder: MetricsRecorder,
    job_name: str,
    *,
    batch_id: str | None = None,
    records_processed: int | None = None,
    invalid_records_count: int | None = None,
    duplicates_dropped: int | None = None,
    data_freshness_hours: float | None = None,
) -> list[MetricRecord]:
    """Record the standard metrics expected by the project rubric."""
    records: list[MetricRecord] = []
    if records_processed is not None:
        records.append(
            recorder.record(job_name, "records_processed", records_processed, batch_id=batch_id)
        )
    if invalid_records_count is not None:
        records.append(
            recorder.record(
                job_name, "invalid_records_count", invalid_records_count, batch_id=batch_id
            )
        )
    if duplicates_dropped is not None:
        records.append(
            recorder.record(job_name, "duplicates_dropped", duplicates_dropped, batch_id=batch_id)
        )
    if data_freshness_hours is not None:
        records.append(
            recorder.record(
                job_name,
                "data_freshness_hours",
                data_freshness_hours,
                batch_id=batch_id,
                unit="hours",
            )
        )
    return records
