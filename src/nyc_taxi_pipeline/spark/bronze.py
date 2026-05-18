"""Bronze Delta ingestion job."""

from __future__ import annotations

import logging
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from nyc_taxi_pipeline.config import PipelineConfig
from nyc_taxi_pipeline.ingestion.download import stage_source_file
from nyc_taxi_pipeline.ingestion.manifest import (
    IngestionManifest,
    ManifestEntry,
    filter_pending_sources,
)
from nyc_taxi_pipeline.ingestion.source_discovery import SourceFile
from nyc_taxi_pipeline.logging_utils import log_event
from nyc_taxi_pipeline.metrics import MetricsRecorder

JOB_NAME = "ingest_bronze"


@dataclass(frozen=True)
class BronzeSourceResult:
    """Ingestion result for one source file."""

    source_file: str
    source_url: str
    status: str
    records_read: int
    records_written: int


@dataclass(frozen=True)
class BronzeIngestionResult:
    """Aggregated Bronze ingestion result."""

    batch_id: str
    target_path: str
    processed_sources: int
    skipped_sources: int
    records_read: int
    records_written: int
    source_results: list[BronzeSourceResult]


def build_bronze_metadata(
    source: SourceFile,
    batch_id: str,
    ingestion_timestamp: datetime | None = None,
) -> dict[str, Any]:
    """Build metadata values added to each Bronze row for a source file."""
    resolved_timestamp = ingestion_timestamp or datetime.now(UTC)
    return {
        "source_file": source.file_name,
        "source_url": source.source_url,
        "ingestion_timestamp": resolved_timestamp,
        "batch_id": batch_id,
        "dataset_year": source.year,
        "dataset_month": source.month,
    }


def add_bronze_metadata_columns(dataframe: Any, source: SourceFile, batch_id: str) -> Any:
    """Add required Bronze metadata columns to a Spark DataFrame."""
    from pyspark.sql import functions as func  # noqa: PLC0415

    metadata = build_bronze_metadata(source, batch_id)
    return (
        dataframe.withColumn("source_file", func.lit(metadata["source_file"]))
        .withColumn("source_url", func.lit(metadata["source_url"]))
        .withColumn("ingestion_timestamp", func.lit(metadata["ingestion_timestamp"]))
        .withColumn("batch_id", func.lit(metadata["batch_id"]))
        .withColumn("dataset_year", func.lit(metadata["dataset_year"]))
        .withColumn("dataset_month", func.lit(metadata["dataset_month"]))
    )


def coerce_raw_columns_to_string(dataframe: Any) -> Any:
    """Preserve raw values with stable Bronze column types across monthly files."""
    from pyspark.sql import functions as func  # noqa: PLC0415

    return dataframe.select(
        *[
            func.col(column_name).cast("string").alias(column_name)
            for column_name in dataframe.columns
        ]
    )


def ingest_bronze(
    spark: Any,
    sources: list[SourceFile],
    config: PipelineConfig,
    batch_id: str,
    *,
    logger: logging.Logger | None = None,
    metrics_recorder: MetricsRecorder | None = None,
) -> BronzeIngestionResult:
    """Ingest selected NYC TLC Parquet source files into the Bronze Delta table."""
    manifest = IngestionManifest(config.storage.ingestion_manifest_path)
    successful_urls = manifest.successful_source_urls()
    pending_sources = filter_pending_sources(sources, successful_urls)
    skipped_sources = len(sources) - len(pending_sources)

    if logger is not None:
        log_event(
            logger,
            "bronze_ingestion_started",
            JOB_NAME,
            batch_id=batch_id,
            source_count=len(sources),
            skipped_sources=skipped_sources,
            target_path=config.storage.bronze_path,
        )

    source_results: list[BronzeSourceResult] = []
    records_read_total = 0
    records_written_total = 0

    timer = (
        metrics_recorder.timer(JOB_NAME, batch_id=batch_id)
        if metrics_recorder is not None
        else nullcontext()
    )
    with timer:
        for source in pending_sources:
            result = _ingest_one_source(spark, source, config, batch_id, manifest, logger)
            source_results.append(result)
            records_read_total += result.records_read
            records_written_total += result.records_written

    if metrics_recorder is not None:
        metrics_recorder.record(JOB_NAME, "records_read", records_read_total, batch_id=batch_id)
        metrics_recorder.record(
            JOB_NAME, "records_written", records_written_total, batch_id=batch_id
        )

    if logger is not None:
        log_event(
            logger,
            "bronze_ingestion_completed",
            JOB_NAME,
            batch_id=batch_id,
            processed_sources=len(pending_sources),
            skipped_sources=skipped_sources,
            records_read=records_read_total,
            records_written=records_written_total,
        )

    return BronzeIngestionResult(
        batch_id=batch_id,
        target_path=config.storage.bronze_path,
        processed_sources=len(pending_sources),
        skipped_sources=skipped_sources,
        records_read=records_read_total,
        records_written=records_written_total,
        source_results=source_results,
    )


def _ingest_one_source(
    spark: Any,
    source: SourceFile,
    config: PipelineConfig,
    batch_id: str,
    manifest: IngestionManifest,
    logger: logging.Logger | None,
) -> BronzeSourceResult:
    try:
        staged_path = stage_source_file(source.source_url, config.storage.source_staging_dir)
        raw_dataframe = spark.read.parquet(staged_path.resolve().as_uri())
        records_read = raw_dataframe.count()
        stable_raw_dataframe = coerce_raw_columns_to_string(raw_dataframe)
        bronze_dataframe = add_bronze_metadata_columns(stable_raw_dataframe, source, batch_id)
        _delete_existing_source_rows(spark, config.storage.bronze_path, source.source_url)
        (
            bronze_dataframe.write.format("delta")
            .mode("append")
            .partitionBy("dataset_year", "dataset_month")
            .save(config.storage.bronze_path)
        )
        records_written = records_read
        manifest.append(
            ManifestEntry(
                source_url=source.source_url,
                source_file=source.file_name,
                batch_id=batch_id,
                status="success",
                records_written=records_written,
            )
        )
        if logger is not None:
            log_event(
                logger,
                "bronze_source_ingested",
                JOB_NAME,
                batch_id=batch_id,
                source_file=source.file_name,
                source_url=source.source_url,
                records_written=records_written,
            )
        return BronzeSourceResult(
            source_file=source.file_name,
            source_url=source.source_url,
            status="success",
            records_read=records_read,
            records_written=records_written,
        )
    except Exception as exc:
        manifest.append(
            ManifestEntry(
                source_url=source.source_url,
                source_file=source.file_name,
                batch_id=batch_id,
                status="failed",
                error_message=str(exc),
            )
        )
        if logger is not None:
            log_event(
                logger,
                "bronze_source_failed",
                JOB_NAME,
                batch_id=batch_id,
                level=logging.ERROR,
                source_file=source.file_name,
                source_url=source.source_url,
                error=str(exc),
            )
        raise


def _delete_existing_source_rows(spark: Any, target_path: str, source_url: str) -> None:
    """Delete existing rows for one source URL before append, when the Delta table exists."""
    from delta.tables import DeltaTable  # noqa: PLC0415
    from pyspark.sql import functions as func  # noqa: PLC0415

    if DeltaTable.isDeltaTable(spark, target_path):
        DeltaTable.forPath(spark, target_path).delete(
            func.col("source_url") == func.lit(source_url)
        )
