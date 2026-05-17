from datetime import datetime, timedelta

from nyc_taxi_pipeline.spark.silver import (
    calculate_average_speed_mph,
    calculate_trip_duration_minutes,
)


def test_duration_and_speed_are_computed_correctly() -> None:
    pickup_datetime = datetime(2023, 1, 1, 8, 0)
    dropoff_datetime = pickup_datetime + timedelta(minutes=30)

    duration = calculate_trip_duration_minutes(pickup_datetime, dropoff_datetime)
    speed = calculate_average_speed_mph(6.0, duration)

    assert duration == 30
    assert speed == 12


def test_average_speed_is_null_for_non_positive_duration() -> None:
    assert calculate_average_speed_mph(6.0, 0.0) is None
