from datetime import datetime, timedelta

from nyc_taxi_pipeline.spark.silver import build_trip_id_from_values


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


def test_trip_id_is_deterministic() -> None:
    record = _valid_record()

    assert build_trip_id_from_values(record) == build_trip_id_from_values(dict(record))


def test_trip_id_changes_when_business_column_changes() -> None:
    record = _valid_record()
    changed_record = {**record, "total_amount": 19.0}

    assert build_trip_id_from_values(record) != build_trip_id_from_values(changed_record)
