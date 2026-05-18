"""Silver Delta transformation job."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from nyc_taxi_pipeline.config import PipelineConfig
from nyc_taxi_pipeline.logging_utils import log_event
from nyc_taxi_pipeline.metrics import MetricsRecorder
from nyc_taxi_pipeline.quality.rules import add_validation_columns
from nyc_taxi_pipeline.spark.quarantine import write_quarantine
from nyc_taxi_pipeline.spark.schemas import (
    BRONZE_METADATA_COLUMNS,
    YELLOW_TAXI_COLUMN_SPECS,
    resolve_column_mapping,
)

JOB_NAME = "transform_silver"
TRIP_ID_COLUMNS = (
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "fare_amount",
    "total_amount",
    "trip_distance",
)
SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60


@dataclass(frozen=True)
class SilverTransformResult:
    """Aggregated result for a Bronze to Silver transform run."""

    batch_id: str
    records_read: int
    valid_records_count: int
    invalid_records_count: int
    invalid_records_ratio: float
    duplicates_dropped: int
    silver_path: str
    quarantine_path: str


def build_trip_id_from_values(record: Mapping[str, Any]) -> str:
    """Build deterministic SHA-256 trip id from business columns."""
    values = [_stable_string(record.get(column_name)) for column_name in TRIP_ID_COLUMNS]
    return hashlib.sha256("||".join(values).encode("utf-8")).hexdigest()


def calculate_trip_duration_minutes(
    pickup_datetime: datetime,
    dropoff_datetime: datetime,
) -> float:
    """Calculate trip duration in minutes."""
    return (dropoff_datetime - pickup_datetime).total_seconds() / SECONDS_PER_MINUTE


def calculate_average_speed_mph(trip_distance: float, trip_duration_minutes: float) -> float | None:
    """Calculate average speed in miles per hour, returning None for non-positive durations."""
    if trip_duration_minutes <= 0:
        return None
    return trip_distance / (trip_duration_minutes / MINUTES_PER_HOUR)


def deduplicate_records(records: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Deduplicate normalized records by deterministic `trip_id`."""
    deduplicated: dict[str, Mapping[str, Any]] = {}
    for record in records:
        trip_id = str(record["trip_id"])
        existing = deduplicated.get(trip_id)
        if existing is None or _stable_string(record.get("ingestion_timestamp")) >= _stable_string(
            existing.get("ingestion_timestamp")
        ):
            deduplicated[trip_id] = record
    return list(deduplicated.values())


def transform_silver(
    spark: Any,
    config: PipelineConfig,
    batch_id: str,
    *,
    logger: logging.Logger | None = None,
    metrics_recorder: MetricsRecorder | None = None,
) -> SilverTransformResult:
    """Transform Bronze Delta records into validated, deduplicated Silver records."""
    if logger is not None:
        log_event(
            logger,
            "silver_transform_started",
            JOB_NAME,
            batch_id=batch_id,
            bronze_path=config.storage.bronze_path,
            silver_path=config.storage.silver_path,
            quarantine_path=config.storage.quarantine_path,
        )

    timer = (
        metrics_recorder.timer(JOB_NAME, batch_id=batch_id)
        if metrics_recorder is not None
        else nullcontext()
    )
    with timer:
        bronze_dataframe = spark.read.format("delta").load(config.storage.bronze_path)
        normalized_dataframe = normalize_bronze_dataframe(bronze_dataframe)
        enriched_dataframe = add_silver_columns(normalized_dataframe)
        validated_dataframe = add_validation_columns(enriched_dataframe)

        records_read = validated_dataframe.count()
        valid_dataframe = validated_dataframe.filter("is_valid")
        invalid_dataframe = validated_dataframe.filter("NOT is_valid")
        valid_before_dedup_count = valid_dataframe.count()
        invalid_records_count = invalid_dataframe.count()
        quarantine_written = write_quarantine(
            invalid_dataframe.drop("is_valid"),
            config.storage.quarantine_path,
            batch_id,
        )
        deduplicated_dataframe = deduplicate_valid_dataframe(valid_dataframe)
        valid_records_count = deduplicated_dataframe.count()
        duplicates_dropped = valid_before_dedup_count - valid_records_count
        _merge_silver_records(
            spark,
            deduplicated_dataframe.drop("is_valid"),
            config.storage.silver_path,
        )

    invalid_records_ratio = invalid_records_count / records_read if records_read else 0.0
    if metrics_recorder is not None:
        metrics_recorder.record(JOB_NAME, "records_read", records_read, batch_id=batch_id)
        metrics_recorder.record(
            JOB_NAME, "valid_records_count", valid_records_count, batch_id=batch_id
        )
        metrics_recorder.record(
            JOB_NAME, "invalid_records_count", invalid_records_count, batch_id=batch_id
        )
        metrics_recorder.record(
            JOB_NAME, "invalid_records_ratio", invalid_records_ratio, batch_id=batch_id
        )
        metrics_recorder.record(
            JOB_NAME, "duplicates_dropped", duplicates_dropped, batch_id=batch_id
        )

    if logger is not None:
        log_event(
            logger,
            "silver_transform_completed",
            JOB_NAME,
            batch_id=batch_id,
            records_read=records_read,
            valid_records_count=valid_records_count,
            invalid_records_count=invalid_records_count,
            invalid_records_ratio=invalid_records_ratio,
            duplicates_dropped=duplicates_dropped,
            quarantine_written=quarantine_written,
        )

    return SilverTransformResult(
        batch_id=batch_id,
        records_read=records_read,
        valid_records_count=valid_records_count,
        invalid_records_count=invalid_records_count,
        invalid_records_ratio=invalid_records_ratio,
        duplicates_dropped=duplicates_dropped,
        silver_path=config.storage.silver_path,
        quarantine_path=config.storage.quarantine_path,
    )


def normalize_bronze_dataframe(dataframe: Any) -> Any:
    """Normalize NYC TLC raw columns to the canonical Silver schema."""
    from pyspark.sql import functions as func  # noqa: PLC0415

    mapping = resolve_column_mapping(dataframe.columns)
    selected_columns = []
    for spec in YELLOW_TAXI_COLUMN_SPECS:
        source_column = mapping.get(spec.target_name)
        if source_column is None:
            selected_columns.append(func.lit(None).cast(spec.data_type).alias(spec.target_name))
        else:
            selected_columns.append(
                func.col(source_column).cast(spec.data_type).alias(spec.target_name)
            )

    for metadata_column in BRONZE_METADATA_COLUMNS:
        if metadata_column in dataframe.columns:
            selected_columns.append(func.col(metadata_column))

    return dataframe.select(*selected_columns)


def add_silver_columns(dataframe: Any) -> Any:
    """Add deterministic trip id and derived analytical columns."""
    from pyspark.sql import functions as func  # noqa: PLC0415

    trip_id_input = func.concat_ws(
        "||",
        *[
            func.coalesce(func.col(column_name).cast("string"), func.lit(""))
            for column_name in TRIP_ID_COLUMNS
        ],
    )
    duration_seconds = func.col("dropoff_datetime").cast("long") - func.col("pickup_datetime").cast(
        "long"
    )
    duration_minutes = duration_seconds / func.lit(SECONDS_PER_MINUTE)
    return (
        dataframe.withColumn("trip_id", func.sha2(trip_id_input, 256))
        .withColumn("pickup_date", func.to_date("pickup_datetime"))
        .withColumn("pickup_hour", func.hour("pickup_datetime"))
        .withColumn("trip_duration_minutes", duration_minutes)
        .withColumn(
            "average_speed_mph",
            func.when(
                func.col("trip_duration_minutes") > 0,
                func.col("trip_distance")
                / (func.col("trip_duration_minutes") / func.lit(MINUTES_PER_HOUR)),
            ),
        )
        .withColumn("pickup_year", func.year("pickup_datetime"))
        .withColumn("pickup_month", func.month("pickup_datetime"))
    )


def deduplicate_valid_dataframe(dataframe: Any) -> Any:
    """Keep the latest valid record per `trip_id`."""
    from pyspark.sql import Window  # noqa: PLC0415
    from pyspark.sql import functions as func  # noqa: PLC0415

    window = Window.partitionBy("trip_id").orderBy(
        func.col("ingestion_timestamp").desc_nulls_last()
    )
    return (
        dataframe.withColumn("_dedup_rank", func.row_number().over(window))
        .filter(func.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )


def _merge_silver_records(spark: Any, dataframe: Any, silver_path: str) -> None:
    """Merge late-arriving Silver records by `trip_id`."""
    from delta.tables import DeltaTable  # noqa: PLC0415

    if not DeltaTable.isDeltaTable(spark, silver_path):
        (
            dataframe.write.format("delta")
            .mode("append")
            .partitionBy("pickup_year", "pickup_month")
            .save(silver_path)
        )
        return

    target = DeltaTable.forPath(spark, silver_path)
    update_assignments = {column: f"source.{column}" for column in dataframe.columns}
    (
        target.alias("target")
        .merge(dataframe.alias("source"), "target.trip_id = source.trip_id")
        .whenMatchedUpdate(
            condition=(
                "source.ingestion_timestamp >= target.ingestion_timestamp "
                "OR target.ingestion_timestamp IS NULL"
            ),
            set=update_assignments,
        )
        .whenNotMatchedInsert(values=update_assignments)
        .execute()
    )


def _stable_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
