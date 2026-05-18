"""ClickHouse loading job."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from nyc_taxi_pipeline.config import ClickHouseConfig, PipelineConfig
from nyc_taxi_pipeline.logging_utils import log_event
from nyc_taxi_pipeline.metrics import MetricsRecorder

JOB_NAME = "load_clickhouse"
CLICKHOUSE_TABLE = "silver_yellow_taxi_trips"

CLICKHOUSE_COLUMNS = (
    "trip_id",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type_id",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "source_file",
    "source_url",
    "ingestion_timestamp",
    "batch_id",
    "pickup_date",
    "pickup_hour",
    "trip_duration_minutes",
    "average_speed_mph",
    "pickup_year",
    "pickup_month",
)


@dataclass(frozen=True)
class ClickHouseLoadResult:
    """Result for a Silver to ClickHouse load."""

    batch_id: str
    table: str
    records_loaded: int
    affected_partitions: list[int]


def build_jdbc_url(config: ClickHouseConfig) -> str:
    """Build the ClickHouse JDBC URL from resolved config."""
    return f"jdbc:clickhouse://{config.host}:{config.http_port}/{config.database}"


def build_jdbc_properties(config: ClickHouseConfig) -> dict[str, str]:
    """Build JDBC properties without hard-coded credentials."""
    properties = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": config.user,
    }
    if config.password is not None:
        properties["password"] = config.password
    return properties


def build_delete_partitions_query(database: str, table: str, partitions: Iterable[int]) -> str:
    """Build a deterministic ClickHouse mutation for affected monthly partitions."""
    partition_values = sorted(set(partitions))
    if not partition_values:
        return ""
    joined_values = ", ".join(str(partition) for partition in partition_values)
    return (
        f"ALTER TABLE {database}.{table} "
        f"DELETE WHERE toYYYYMM(pickup_datetime) IN ({joined_values}) "
        "SETTINGS mutations_sync = 2"
    )


def execute_clickhouse_query(
    config: ClickHouseConfig,
    query: str,
    timeout_seconds: int = 60,
) -> None:
    """Execute a ClickHouse query through the HTTP interface."""
    if not query:
        return
    params = urlencode({"query": query})
    request = Request(
        f"http://{config.host}:{config.http_port}/?{params}",
        method="POST",
    )
    if config.user:
        request.add_header("X-ClickHouse-User", config.user)
    if config.password:
        request.add_header("X-ClickHouse-Key", config.password)

    try:
        with urlopen(request, timeout=timeout_seconds):
            return
    except (HTTPError, URLError, TimeoutError) as exc:
        msg = f"ClickHouse query failed: {exc}"
        raise RuntimeError(msg) from exc


def load_silver_to_clickhouse(
    spark: Any,
    config: PipelineConfig,
    batch_id: str,
    *,
    logger: logging.Logger | None = None,
    metrics_recorder: MetricsRecorder | None = None,
) -> ClickHouseLoadResult:
    """Load Silver Delta records into ClickHouse with partition-level idempotency."""
    if logger is not None:
        log_event(
            logger,
            "clickhouse_load_started",
            JOB_NAME,
            batch_id=batch_id,
            silver_path=config.storage.silver_path,
            clickhouse_database=config.clickhouse.database,
            clickhouse_table=CLICKHOUSE_TABLE,
        )

    timer = (
        metrics_recorder.timer(JOB_NAME, batch_id=batch_id)
        if metrics_recorder is not None
        else nullcontext()
    )
    with timer:
        silver_dataframe = spark.read.format("delta").load(config.storage.silver_path)
        clickhouse_dataframe = select_clickhouse_columns(silver_dataframe)
        affected_partitions = collect_affected_partitions(clickhouse_dataframe)
        records_loaded = clickhouse_dataframe.count()
        delete_query = build_delete_partitions_query(
            config.clickhouse.database,
            CLICKHOUSE_TABLE,
            affected_partitions,
        )
        execute_clickhouse_query(config.clickhouse, delete_query)
        write_dataframe_to_clickhouse(clickhouse_dataframe, config.clickhouse)

    if metrics_recorder is not None:
        metrics_recorder.record(JOB_NAME, "records_loaded", records_loaded, batch_id=batch_id)
        metrics_recorder.record(
            JOB_NAME,
            "affected_partitions_count",
            len(affected_partitions),
            batch_id=batch_id,
        )

    if logger is not None:
        log_event(
            logger,
            "clickhouse_load_completed",
            JOB_NAME,
            batch_id=batch_id,
            records_loaded=records_loaded,
            affected_partitions=affected_partitions,
        )

    return ClickHouseLoadResult(
        batch_id=batch_id,
        table=f"{config.clickhouse.database}.{CLICKHOUSE_TABLE}",
        records_loaded=records_loaded,
        affected_partitions=affected_partitions,
    )


def select_clickhouse_columns(dataframe: Any) -> Any:
    """Select and order the Silver columns expected by ClickHouse."""
    return dataframe.select(*CLICKHOUSE_COLUMNS)


def collect_affected_partitions(dataframe: Any) -> list[int]:
    """Collect distinct ClickHouse monthly partitions from a Silver DataFrame."""
    from pyspark.sql import functions as func  # noqa: PLC0415

    rows = (
        dataframe.select(
            func.date_format("pickup_datetime", "yyyyMM").cast("int").alias("partition")
        )
        .distinct()
        .collect()
    )
    return sorted(row["partition"] for row in rows)


def write_dataframe_to_clickhouse(dataframe: Any, config: ClickHouseConfig) -> None:
    """Append a Spark DataFrame to the ClickHouse serving table."""
    (
        dataframe.write.format("jdbc")
        .option("url", build_jdbc_url(config))
        .option("dbtable", f"{config.database}.{CLICKHOUSE_TABLE}")
        .options(**build_jdbc_properties(config))
        .mode("append")
        .save()
    )
