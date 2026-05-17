"""Tests for NYC TLC source discovery."""

import pytest

from nyc_taxi_pipeline.ingestion.source_discovery import (
    discover_yellow_taxi_sources,
    iter_months,
)


def test_discover_yellow_taxi_sources_generates_expected_urls() -> None:
    """Discovery should generate one URL per month in the inclusive range."""
    sources = discover_yellow_taxi_sources(
        "2023-01",
        "2023-03",
        "https://example.test/trip-data",
    )

    assert [source.file_name for source in sources] == [
        "yellow_tripdata_2023-01.parquet",
        "yellow_tripdata_2023-02.parquet",
        "yellow_tripdata_2023-03.parquet",
    ]
    assert sources[0].year == 2023
    assert sources[0].month == 1
    assert sources[0].source_url == (
        "https://example.test/trip-data/yellow_tripdata_2023-01.parquet"
    )


@pytest.mark.parametrize("month_value", ["2023-1", "2023-13", "bad-value"])
def test_invalid_month_format_fails(month_value: str) -> None:
    """Month parsing should reject anything other than YYYY-MM."""
    with pytest.raises(ValueError, match="Expected format YYYY-MM"):
        iter_months(month_value, "2023-03")


def test_start_month_after_end_month_fails() -> None:
    """The month range must move forward."""
    with pytest.raises(ValueError, match="must be <= end_month"):
        iter_months("2023-03", "2023-01")
