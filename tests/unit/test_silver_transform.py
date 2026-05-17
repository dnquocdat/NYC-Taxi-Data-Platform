"""Tests for Silver transform business logic."""

from datetime import datetime, timedelta

import pytest

from nyc_taxi_pipeline.quality.rules import validate_trip_record
from nyc_taxi_pipeline.spark.schemas import SchemaValidationError, resolve_column_mapping
from nyc_taxi_pipeline.spark.silver import (
    build_trip_id_from_values,
    calculate_average_speed_mph,
    calculate_trip_duration_minutes,
    deduplicate_records,
)


def _valid_record() -> dict[str, object]:
    pickup_datetime = datetime(2023, 1, 1, 8, 0, 0)
    return {
        "vendor_id": 1,
        "pickup_datetime": pickup_datetime,
        "dropoff_datetime": pickup_datetime + timedelta(minutes=30),
        "pickup_location_id": 100,
        "dropoff_location_id": 200,
        "fare_amount": 12.5,
        "total_amount": 18.0,
        "trip_distance": 6.0,
    }


def test_trip_id_is_deterministic() -> None:
    """The same business columns should always produce the same trip id."""
    record = _valid_record()

    assert build_trip_id_from_values(record) == build_trip_id_from_values(dict(record))


def test_duplicate_records_are_removed() -> None:
    """Deduplication should keep only one record for each trip id."""
    record = _valid_record()
    trip_id = build_trip_id_from_values(record)
    older = {**record, "trip_id": trip_id, "ingestion_timestamp": "2023-01-01T00:00:00Z"}
    newer = {**record, "trip_id": trip_id, "ingestion_timestamp": "2023-01-02T00:00:00Z"}

    deduplicated = deduplicate_records([older, newer])

    assert len(deduplicated) == 1
    assert deduplicated[0]["ingestion_timestamp"] == "2023-01-02T00:00:00Z"


def test_negative_distance_goes_to_quarantine() -> None:
    """Negative distances should fail validation and be quarantine candidates."""
    record = {**_valid_record(), "trip_distance": -1.0}

    assert "trip_distance <= 0" in validate_trip_record(record)


def test_dropoff_before_pickup_is_invalid() -> None:
    """Dropoff timestamps must be after pickup timestamps."""
    pickup_datetime = datetime(2023, 1, 1, 8, 0, 0)
    record = {
        **_valid_record(),
        "pickup_datetime": pickup_datetime,
        "dropoff_datetime": pickup_datetime - timedelta(minutes=1),
    }

    assert "dropoff_datetime <= pickup_datetime" in validate_trip_record(record)


def test_duration_and_speed_are_computed_correctly() -> None:
    """Derived trip duration and speed should use minutes and miles per hour."""
    pickup_datetime = datetime(2023, 1, 1, 8, 0, 0)
    dropoff_datetime = pickup_datetime + timedelta(minutes=30)

    duration = calculate_trip_duration_minutes(pickup_datetime, dropoff_datetime)
    speed = calculate_average_speed_mph(6.0, duration)

    assert duration == 30
    assert speed == 12


def test_required_columns_are_present() -> None:
    """Schema mapping should resolve required NYC TLC raw columns."""
    mapping = resolve_column_mapping(
        [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
            "fare_amount",
            "total_amount",
        ]
    )

    assert mapping["vendor_id"] == "VendorID"
    assert mapping["pickup_datetime"] == "tpep_pickup_datetime"
    assert mapping["pickup_location_id"] == "PULocationID"


def test_missing_required_columns_fail_clearly() -> None:
    """Schema errors should list missing required columns."""
    with pytest.raises(SchemaValidationError, match="pickup_datetime"):
        resolve_column_mapping(["VendorID"])
