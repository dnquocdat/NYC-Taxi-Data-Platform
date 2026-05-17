"""Dataset threshold validation for NYC TLC source selections."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from nyc_taxi_pipeline.config import DatasetConfig
from nyc_taxi_pipeline.ingestion.source_discovery import SourceFile
from nyc_taxi_pipeline.logging_utils import log_event


class DatasetThresholdError(ValueError):
    """Raised when the selected full dataset does not meet project thresholds."""


@dataclass(frozen=True)
class SourceSize:
    """Size metadata for one source file."""

    source_file: SourceFile
    raw_bytes: int | None
    record_count: int | None


@dataclass(frozen=True)
class DatasetSizeResult:
    """Aggregated dataset size validation result."""

    source_count: int
    total_raw_bytes: int | None
    total_record_count: int | None
    min_raw_bytes: int
    min_record_count: int
    sample_mode: bool
    threshold_passed: bool

    @property
    def total_raw_gb(self) -> float | None:
        """Return total raw size in GiB when size metadata is available."""
        if self.total_raw_bytes is None:
            return None
        return self.total_raw_bytes / (1024**3)


def fetch_content_length(source_url: str, timeout_seconds: int = 10) -> int | None:
    """Fetch `Content-Length` for a source URL using HTTP HEAD."""
    request = Request(source_url, method="HEAD")
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            content_length = response.headers.get("Content-Length")
    except (HTTPError, URLError, TimeoutError):
        return None

    if content_length is None:
        return None
    try:
        return int(content_length)
    except ValueError:
        return None


def collect_source_sizes(
    sources: Sequence[SourceFile],
    record_count_metadata: Mapping[str, int],
    *,
    fetch_remote_size: bool = True,
    logger: logging.Logger | None = None,
) -> list[SourceSize]:
    """Collect raw byte and record-count metadata for selected sources."""
    sizes: list[SourceSize] = []
    for source in sources:
        raw_bytes = fetch_content_length(source.source_url) if fetch_remote_size else None
        record_count = record_count_metadata.get(source.month_key)
        sizes.append(SourceSize(source, raw_bytes=raw_bytes, record_count=record_count))
        if logger is not None and fetch_remote_size and raw_bytes is None:
            log_event(
                logger,
                "source_size_unavailable",
                "check_dataset_size",
                source_file=source.file_name,
                source_url=source.source_url,
            )
    return sizes


def validate_dataset_threshold(
    source_sizes: Sequence[SourceSize],
    *,
    min_raw_bytes: int,
    min_record_count: int,
    sample_mode: bool,
    logger: logging.Logger | None = None,
) -> DatasetSizeResult:
    """Validate that selected full-mode data is not a toy dataset."""
    total_raw_bytes = _sum_optional(size.raw_bytes for size in source_sizes)
    total_record_count = _sum_optional(size.record_count for size in source_sizes)

    raw_size_passed = total_raw_bytes is not None and total_raw_bytes >= min_raw_bytes
    record_count_passed = total_record_count is not None and total_record_count >= min_record_count
    threshold_passed = sample_mode or raw_size_passed or record_count_passed

    result = DatasetSizeResult(
        source_count=len(source_sizes),
        total_raw_bytes=total_raw_bytes,
        total_record_count=total_record_count,
        min_raw_bytes=min_raw_bytes,
        min_record_count=min_record_count,
        sample_mode=sample_mode,
        threshold_passed=threshold_passed,
    )

    if sample_mode:
        if logger is not None:
            log_event(
                logger,
                "dataset_threshold_skipped_for_sample_mode",
                "check_dataset_size",
                level=logging.WARNING,
                source_count=result.source_count,
            )
        return result

    if not threshold_passed:
        raw_gb = "unknown" if result.total_raw_gb is None else f"{result.total_raw_gb:.2f} GiB"
        records = "unknown" if total_record_count is None else str(total_record_count)
        msg = (
            "Selected full-mode dataset is too small or cannot be verified. "
            f"sources={result.source_count}, raw_size={raw_gb}, records={records}, "
            f"required_raw_bytes={min_raw_bytes}, required_records={min_record_count}."
        )
        raise DatasetThresholdError(msg)

    return result


def check_dataset_size(
    sources: Sequence[SourceFile],
    dataset_config: DatasetConfig,
    *,
    fetch_remote_size: bool = True,
    logger: logging.Logger | None = None,
) -> DatasetSizeResult:
    """Collect metadata and validate the configured dataset threshold."""
    source_sizes = collect_source_sizes(
        sources,
        dataset_config.record_count_metadata,
        fetch_remote_size=fetch_remote_size,
        logger=logger,
    )
    return validate_dataset_threshold(
        source_sizes,
        min_raw_bytes=dataset_config.minimum_raw_bytes,
        min_record_count=dataset_config.minimum_records,
        sample_mode=dataset_config.sample_mode,
        logger=logger,
    )


def _sum_optional(values: Sequence[int | None]) -> int | None:
    concrete_values = [value for value in values if value is not None]
    if not concrete_values:
        return None
    return sum(concrete_values)
