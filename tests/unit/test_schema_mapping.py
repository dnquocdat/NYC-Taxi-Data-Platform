import pytest

from nyc_taxi_pipeline.spark.schemas import (
    SchemaValidationError,
    missing_optional_columns,
    required_target_columns,
    resolve_column_mapping,
)

RAW_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]


def test_required_columns_are_present() -> None:
    mapping = resolve_column_mapping(RAW_COLUMNS)

    assert set(required_target_columns()) <= set(mapping)
    assert mapping["vendor_id"] == "VendorID"
    assert mapping["pickup_datetime"] == "tpep_pickup_datetime"
    assert mapping["pickup_location_id"] == "PULocationID"


def test_missing_required_columns_fail_clearly() -> None:
    with pytest.raises(SchemaValidationError, match="pickup_datetime"):
        resolve_column_mapping(["VendorID"])


def test_optional_columns_can_be_missing() -> None:
    assert "passenger_count" in missing_optional_columns(RAW_COLUMNS)
