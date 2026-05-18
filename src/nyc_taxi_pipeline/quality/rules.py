"""Business validation rules for taxi trip records."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any


def validate_trip_record(record: Mapping[str, Any]) -> list[str]:
    """Validate one normalized taxi trip record and return error reasons."""
    errors: list[str] = []
    pickup_datetime = record.get("pickup_datetime")
    dropoff_datetime = record.get("dropoff_datetime")

    if pickup_datetime is None:
        errors.append("pickup_datetime is null")
    if dropoff_datetime is None:
        errors.append("dropoff_datetime is null")
    if (
        isinstance(pickup_datetime, datetime)
        and isinstance(dropoff_datetime, datetime)
        and dropoff_datetime <= pickup_datetime
    ):
        errors.append("dropoff_datetime <= pickup_datetime")
    if _is_less_than_or_equal_zero(record.get("trip_distance")):
        errors.append("trip_distance <= 0")
    if _is_negative(record.get("fare_amount")):
        errors.append("fare_amount < 0")
    if _is_negative(record.get("total_amount")):
        errors.append("total_amount < 0")
    if record.get("pickup_location_id") is None:
        errors.append("pickup_location_id is null")
    if record.get("dropoff_location_id") is None:
        errors.append("dropoff_location_id is null")
    return errors


def add_validation_columns(dataframe: Any) -> Any:
    """Add `error_reason` and `is_valid` columns to a Spark DataFrame."""
    from pyspark.sql import functions as func  # noqa: PLC0415

    failed_reason_columns = [
        func.when(func.col("pickup_datetime").isNull(), func.lit("pickup_datetime is null")),
        func.when(func.col("dropoff_datetime").isNull(), func.lit("dropoff_datetime is null")),
        func.when(
            func.col("dropoff_datetime") <= func.col("pickup_datetime"),
            func.lit("dropoff_datetime <= pickup_datetime"),
        ),
        func.when(
            func.col("trip_distance").isNull() | (func.col("trip_distance") <= func.lit(0)),
            func.lit("trip_distance <= 0"),
        ),
        func.when(
            func.col("fare_amount").isNull() | (func.col("fare_amount") < func.lit(0)),
            func.lit("fare_amount < 0"),
        ),
        func.when(
            func.col("total_amount").isNull() | (func.col("total_amount") < func.lit(0)),
            func.lit("total_amount < 0"),
        ),
        func.when(
            func.col("pickup_location_id").isNull(),
            func.lit("pickup_location_id is null"),
        ),
        func.when(
            func.col("dropoff_location_id").isNull(),
            func.lit("dropoff_location_id is null"),
        ),
    ]
    with_reason = dataframe.withColumn("error_reason", func.concat_ws("; ", *failed_reason_columns))
    return with_reason.withColumn("is_valid", func.length(func.col("error_reason")) == 0)


def _is_negative(value: Any) -> bool:
    return value is None or float(value) < 0


def _is_less_than_or_equal_zero(value: Any) -> bool:
    return value is None or float(value) <= 0
