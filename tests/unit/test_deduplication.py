from datetime import datetime, timedelta

from nyc_taxi_pipeline.spark.silver import build_trip_id_from_values, deduplicate_records


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


def test_duplicate_records_are_removed() -> None:
    record = _valid_record()
    trip_id = build_trip_id_from_values(record)
    older = {**record, "trip_id": trip_id, "ingestion_timestamp": "2023-01-01T00:00:00Z"}
    newer = {**record, "trip_id": trip_id, "ingestion_timestamp": "2023-01-02T00:00:00Z"}

    deduplicated = deduplicate_records([older, newer])

    assert len(deduplicated) == 1
    assert deduplicated[0]["ingestion_timestamp"] == "2023-01-02T00:00:00Z"


def test_deduplication_keeps_distinct_trip_ids() -> None:
    first_record = _valid_record()
    second_record = {**first_record, "dropoff_location_id": 201}
    records = [
        {**first_record, "trip_id": build_trip_id_from_values(first_record)},
        {**second_record, "trip_id": build_trip_id_from_values(second_record)},
    ]

    assert len(deduplicate_records(records)) == 2
