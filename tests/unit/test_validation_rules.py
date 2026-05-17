from datetime import datetime, timedelta

from nyc_taxi_pipeline.quality.rules import validate_trip_record


def _valid_record() -> dict[str, object]:
    pickup_datetime = datetime(2023, 1, 1, 8, 0)
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


def test_negative_distance_goes_to_quarantine() -> None:
    record = {**_valid_record(), "trip_distance": -1.0}

    assert validate_trip_record(record) == ["trip_distance <= 0"]


def test_dropoff_before_pickup_is_invalid() -> None:
    pickup_datetime = datetime(2023, 1, 1, 8, 0)
    record = {
        **_valid_record(),
        "pickup_datetime": pickup_datetime,
        "dropoff_datetime": pickup_datetime - timedelta(minutes=1),
    }

    assert "dropoff_datetime <= pickup_datetime" in validate_trip_record(record)


def test_missing_location_ids_are_invalid() -> None:
    record = {**_valid_record(), "pickup_location_id": None, "dropoff_location_id": None}

    assert validate_trip_record(record) == [
        "pickup_location_id is null",
        "dropoff_location_id is null",
    ]
