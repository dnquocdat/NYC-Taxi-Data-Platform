"""Tests for dataset threshold validation."""

import logging

import pytest

from nyc_taxi_pipeline.ingestion.dataset_size import (
    DatasetThresholdError,
    SourceSize,
    validate_dataset_threshold,
)
from nyc_taxi_pipeline.ingestion.source_discovery import SourceFile


def _source(month: int) -> SourceFile:
    return SourceFile(
        year=2023,
        month=month,
        file_name=f"yellow_tripdata_2023-{month:02d}.parquet",
        source_url=f"https://example.test/yellow_tripdata_2023-{month:02d}.parquet",
    )


def test_threshold_fails_when_dataset_is_too_small() -> None:
    """Full mode should fail when neither raw bytes nor records reach threshold."""
    source_sizes = [SourceSize(_source(1), raw_bytes=100, record_count=10)]

    with pytest.raises(DatasetThresholdError, match="Selected full-mode dataset is too small"):
        validate_dataset_threshold(
            source_sizes,
            min_raw_bytes=1_000,
            min_record_count=100,
            sample_mode=False,
        )


def test_threshold_passes_when_record_count_is_large_enough() -> None:
    """Full mode should pass if the record-count threshold is satisfied."""
    source_sizes = [SourceSize(_source(1), raw_bytes=100, record_count=20_000_000)]

    result = validate_dataset_threshold(
        source_sizes,
        min_raw_bytes=10 * 1024 * 1024 * 1024,
        min_record_count=20_000_000,
        sample_mode=False,
    )

    assert result.threshold_passed is True
    assert result.total_record_count == 20_000_000


def test_sample_mode_skips_threshold(caplog: pytest.LogCaptureFixture) -> None:
    """Sample mode should allow tiny selections but emit a warning event."""
    source_sizes = [SourceSize(_source(1), raw_bytes=100, record_count=10)]

    with caplog.at_level(logging.WARNING):
        result = validate_dataset_threshold(
            source_sizes,
            min_raw_bytes=1_000,
            min_record_count=100,
            sample_mode=True,
            logger=logging.getLogger("test-dataset-size"),
        )

    assert result.threshold_passed is True
    assert "dataset_threshold_skipped_for_sample_mode" in caplog.text
