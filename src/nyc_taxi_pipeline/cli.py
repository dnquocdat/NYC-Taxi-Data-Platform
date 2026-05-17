"""Command-line entry points for local pipeline operations."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import click

from nyc_taxi_pipeline.config import DatasetConfig, PipelineConfig, load_config
from nyc_taxi_pipeline.ingestion.dataset_size import DatasetThresholdError, check_dataset_size
from nyc_taxi_pipeline.ingestion.source_discovery import discover_sources_from_config
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
def run_sample() -> None:
    """Run the small sample pipeline path once it is implemented."""
    raise click.ClickException(
        "Sample pipeline is not implemented yet. It will be added in Phase 5-6."
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
    result = ingest_bronze(
        spark,
        sources,
        config,
        resolved_batch_id,
        logger=logger,
        metrics_recorder=metrics_recorder,
    )
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
    result = transform_silver(
        spark,
        config,
        resolved_batch_id,
        logger=logger,
        metrics_recorder=metrics_recorder,
    )
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
    result = load_silver_to_clickhouse(
        spark,
        config,
        resolved_batch_id,
        logger=logger,
        metrics_recorder=metrics_recorder,
    )
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


if __name__ == "__main__":
    main()
