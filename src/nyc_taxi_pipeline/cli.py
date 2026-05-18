"""Command-line entry points for local pipeline operations."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import click

from nyc_taxi_pipeline.config import PROJECT_ROOT, DatasetConfig, PipelineConfig, load_config
from nyc_taxi_pipeline.ingestion.dataset_size import DatasetThresholdError, check_dataset_size
from nyc_taxi_pipeline.ingestion.source_discovery import SourceFile, discover_sources_from_config
from nyc_taxi_pipeline.logging_utils import configure_logging, get_logger, log_event
from nyc_taxi_pipeline.metrics import MetricsRecorder
from nyc_taxi_pipeline.spark.bronze import ingest_bronze
from nyc_taxi_pipeline.spark.clickhouse_load import load_silver_to_clickhouse
from nyc_taxi_pipeline.spark.session import create_spark_session
from nyc_taxi_pipeline.spark.silver import transform_silver


@click.group()
def main() -> None:
    """Run NYC taxi pipeline commands."""


@main.command("run-sample")
@click.option(
    "--work-dir",
    type=click.Path(path_type=Path, file_okay=False),
    default=PROJECT_ROOT / "tmp" / "sample_pipeline",
    help="Local directory for generated sample input and Delta outputs.",
)
@click.option("--batch-id", type=str, default=None, help="Pipeline batch id.")
def run_sample(work_dir: Path, batch_id: str | None) -> None:
    """Run a tiny local Bronze to Silver sample path without downloading TLC data."""
    resolved_batch_id = batch_id or datetime.now(UTC).strftime("sample_%Y%m%dT%H%M%SZ")
    run_dir = work_dir.resolve() / resolved_batch_id
    source_path = run_dir / "source_yellow_taxi"
    bronze_path = run_dir / "bronze_delta"
    silver_path = run_dir / "silver_delta"
    quarantine_path = run_dir / "quarantine_delta"
    manifest_path = run_dir / "manifest.jsonl"
    metrics_path = run_dir / "metrics.jsonl"

    config = load_config(
        env_file=None,
        env={
            "SPARK_MASTER_URL": "local[2]",
            "BRONZE_DELTA_PATH": bronze_path.as_uri(),
            "SILVER_DELTA_PATH": silver_path.as_uri(),
            "QUARANTINE_DELTA_PATH": quarantine_path.as_uri(),
            "INGESTION_MANIFEST_PATH": str(manifest_path),
            "METRICS_OUTPUT_PATH": str(metrics_path),
            "MINIO_ROOT_USER": "",
            "MINIO_ROOT_PASSWORD": "",
        },
    )
    configure_logging(config.runtime.log_level)
    logger = get_logger("nyc_taxi_pipeline.run_sample")

    try:
        spark = create_spark_session("nyc-taxi-run-sample", config)
    except Exception as exc:
        msg = (
            "Local Spark/Delta runtime is not available. "
            "Install project dependencies with `python -m pip install -r requirements-dev.txt`."
        )
        raise click.ClickException(msg) from exc

    try:
        _write_sample_source_parquet(spark, source_path)
        source = SourceFile(
            year=2023,
            month=1,
            file_name="sample_yellow_tripdata_2023-01.parquet",
            source_url=source_path.as_uri(),
        )
        metrics_recorder = MetricsRecorder(metrics_path)
        bronze_result = ingest_bronze(
            spark,
            [source],
            config,
            resolved_batch_id,
            logger=logger,
            metrics_recorder=metrics_recorder,
        )
        silver_result = transform_silver(
            spark,
            config,
            resolved_batch_id,
            logger=logger,
            metrics_recorder=metrics_recorder,
        )
    finally:
        spark.stop()

    log_event(
        logger,
        "sample_pipeline_completed",
        "run_sample",
        batch_id=resolved_batch_id,
        output_dir=str(run_dir),
        records_written=bronze_result.records_written,
        valid_records_count=silver_result.valid_records_count,
        invalid_records_count=silver_result.invalid_records_count,
        duplicates_dropped=silver_result.duplicates_dropped,
    )


@main.command("ingest-bronze")
@click.option("--start-month", type=str, default=None, help="Override dataset start month YYYY-MM.")
@click.option("--end-month", type=str, default=None, help="Override dataset end month YYYY-MM.")
@click.option("--batch-id", type=str, default=None, help="Pipeline batch id.")
@click.option(
    "--sample-mode",
    is_flag=True,
    help="Skip full dataset threshold validation for a small test run.",
)
@click.option(
    "--skip-head",
    is_flag=True,
    help="Skip HTTP HEAD size checks and rely on configured record-count metadata.",
)
def ingest_bronze_command(
    start_month: str | None,
    end_month: str | None,
    batch_id: str | None,
    sample_mode: bool,
    skip_head: bool,
) -> None:
    """Ingest configured NYC TLC sources into the Bronze Delta table."""
    config = _config_with_dataset_overrides(
        load_config(),
        start_month=start_month,
        end_month=end_month,
        sample_mode=sample_mode,
    )
    resolved_batch_id = batch_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    configure_logging(config.runtime.log_level)
    logger = get_logger("nyc_taxi_pipeline.ingest_bronze")
    sources = discover_sources_from_config(config.dataset)

    try:
        check_dataset_size(
            sources,
            config.dataset,
            fetch_remote_size=not skip_head,
            logger=logger,
        )
    except DatasetThresholdError as exc:
        log_event(
            logger,
            "dataset_threshold_failed",
            "ingest_bronze",
            batch_id=resolved_batch_id,
            error=str(exc),
        )
        raise click.ClickException(str(exc)) from exc

    spark = create_spark_session("nyc-taxi-ingest-bronze", config)
    metrics_recorder = MetricsRecorder(config.runtime.metrics_output_path)
    try:
        result = ingest_bronze(
            spark,
            sources,
            config,
            resolved_batch_id,
            logger=logger,
            metrics_recorder=metrics_recorder,
        )
    finally:
        spark.stop()
    log_event(
        logger,
        "bronze_cli_completed",
        "ingest_bronze",
        batch_id=resolved_batch_id,
        target_path=result.target_path,
        processed_sources=result.processed_sources,
        skipped_sources=result.skipped_sources,
        records_written=result.records_written,
    )


@main.command("transform-silver")
@click.option("--batch-id", type=str, default=None, help="Pipeline batch id.")
def transform_silver_command(batch_id: str | None) -> None:
    """Transform Bronze Delta records into Silver and Quarantine Delta tables."""
    config = load_config()
    resolved_batch_id = batch_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    configure_logging(config.runtime.log_level)
    logger = get_logger("nyc_taxi_pipeline.transform_silver")
    spark = create_spark_session("nyc-taxi-transform-silver", config)
    metrics_recorder = MetricsRecorder(config.runtime.metrics_output_path)
    try:
        result = transform_silver(
            spark,
            config,
            resolved_batch_id,
            logger=logger,
            metrics_recorder=metrics_recorder,
        )
    finally:
        spark.stop()
    log_event(
        logger,
        "silver_cli_completed",
        "transform_silver",
        batch_id=resolved_batch_id,
        records_read=result.records_read,
        valid_records_count=result.valid_records_count,
        invalid_records_count=result.invalid_records_count,
        duplicates_dropped=result.duplicates_dropped,
    )


@main.command("load-clickhouse")
@click.option("--batch-id", type=str, default=None, help="Pipeline batch id.")
def load_clickhouse_command(batch_id: str | None) -> None:
    """Load Silver Delta records into ClickHouse."""
    config = load_config()
    resolved_batch_id = batch_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    configure_logging(config.runtime.log_level)
    logger = get_logger("nyc_taxi_pipeline.load_clickhouse")
    spark = create_spark_session("nyc-taxi-load-clickhouse", config)
    metrics_recorder = MetricsRecorder(config.runtime.metrics_output_path)
    try:
        result = load_silver_to_clickhouse(
            spark,
            config,
            resolved_batch_id,
            logger=logger,
            metrics_recorder=metrics_recorder,
        )
    finally:
        spark.stop()
    log_event(
        logger,
        "clickhouse_cli_completed",
        "load_clickhouse",
        batch_id=resolved_batch_id,
        table=result.table,
        records_loaded=result.records_loaded,
        affected_partitions=result.affected_partitions,
    )


def _config_with_dataset_overrides(
    config: PipelineConfig,
    *,
    start_month: str | None,
    end_month: str | None,
    sample_mode: bool,
) -> PipelineConfig:
    dataset: DatasetConfig = config.dataset
    if start_month is not None:
        dataset = replace(dataset, start_month=start_month)
    if end_month is not None:
        dataset = replace(dataset, end_month=end_month)
    if sample_mode:
        dataset = replace(dataset, sample_mode=True)
    return replace(config, dataset=dataset)


def _write_sample_source_parquet(spark: object, source_path: Path) -> None:
    """Create a tiny deterministic Yellow Taxi sample Parquet dataset."""
    pickup = datetime(2023, 1, 1, 8, 0, tzinfo=UTC)
    sample_rows = [
        _raw_sample_row(
            pickup, pickup + timedelta(minutes=30), fare_amount=12.5, total_amount=18.0
        ),
        _raw_sample_row(
            pickup, pickup + timedelta(minutes=30), fare_amount=12.5, total_amount=18.0
        ),
        _raw_sample_row(
            pickup + timedelta(hours=1),
            pickup + timedelta(hours=1, minutes=20),
            fare_amount=8.0,
            total_amount=11.0,
            pickup_location_id=101,
            dropoff_location_id=201,
            trip_distance=4.0,
        ),
        _raw_sample_row(
            pickup + timedelta(hours=2),
            pickup + timedelta(hours=2, minutes=15),
            fare_amount=-5.0,
            total_amount=0.0,
            pickup_location_id=102,
            dropoff_location_id=202,
        ),
        _raw_sample_row(
            pickup + timedelta(hours=3),
            pickup + timedelta(hours=2, minutes=50),
            fare_amount=9.0,
            total_amount=12.0,
            pickup_location_id=103,
            dropoff_location_id=203,
        ),
    ]
    spark.createDataFrame(sample_rows).write.mode("overwrite").parquet(source_path.as_uri())


def _raw_sample_row(
    pickup_datetime: datetime,
    dropoff_datetime: datetime,
    *,
    fare_amount: float,
    total_amount: float,
    pickup_location_id: int = 100,
    dropoff_location_id: int = 200,
    trip_distance: float = 6.0,
) -> dict[str, object]:
    return {
        "VendorID": 1,
        "tpep_pickup_datetime": pickup_datetime.replace(tzinfo=None),
        "tpep_dropoff_datetime": dropoff_datetime.replace(tzinfo=None),
        "trip_distance": trip_distance,
        "PULocationID": pickup_location_id,
        "DOLocationID": dropoff_location_id,
        "fare_amount": fare_amount,
        "total_amount": total_amount,
    }


if __name__ == "__main__":
    main()
