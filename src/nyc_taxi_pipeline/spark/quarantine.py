"""Quarantine Delta writing helpers."""

from __future__ import annotations

from typing import Any


def add_quarantine_metadata(dataframe: Any, batch_id: str) -> Any:
    """Add quarantine metadata columns to invalid records."""
    from pyspark.sql import functions as func  # noqa: PLC0415

    return (
        dataframe.withColumn("quarantine_timestamp", func.current_timestamp())
        .withColumn("batch_id", func.lit(batch_id))
        .withColumn("source_file", func.col("source_file"))
    )


def write_quarantine(dataframe: Any, quarantine_path: str, batch_id: str) -> int:
    """Append invalid records to the Quarantine Delta table and return row count."""
    quarantine_dataframe = add_quarantine_metadata(dataframe, batch_id)
    row_count = quarantine_dataframe.count()
    if row_count == 0:
        return 0
    quarantine_dataframe.write.format("delta").mode("append").save(quarantine_path)
    return row_count
