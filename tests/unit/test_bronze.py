"""Tests for Bronze ingestion helpers."""

from datetime import UTC, datetime

from nyc_taxi_pipeline.ingestion.source_discovery import SourceFile
from nyc_taxi_pipeline.spark.bronze import build_bronze_metadata


def test_build_bronze_metadata_includes_required_columns() -> None:
    """Bronze metadata should contain lineage and partition values."""
    source = SourceFile(
        year=2023,
        month=1,
        file_name="yellow_tripdata_2023-01.parquet",
        source_url="https://example.test/yellow_tripdata_2023-01.parquet",
    )
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)

    metadata = build_bronze_metadata(source, "batch-001", timestamp)

    assert metadata == {
        "source_file": "yellow_tripdata_2023-01.parquet",
        "source_url": "https://example.test/yellow_tripdata_2023-01.parquet",
        "ingestion_timestamp": timestamp,
        "batch_id": "batch-001",
        "dataset_year": 2023,
        "dataset_month": 1,
    }
