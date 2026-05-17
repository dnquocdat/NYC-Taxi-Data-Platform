from __future__ import annotations

import importlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from nyc_taxi_pipeline.config import load_config
from nyc_taxi_pipeline.ingestion.source_discovery import SourceFile
from nyc_taxi_pipeline.spark.bronze import ingest_bronze
from nyc_taxi_pipeline.spark.silver import transform_silver

pytestmark = [pytest.mark.integration, pytest.mark.spark]


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    configure_spark_with_delta_pip = importlib.import_module("delta").configure_spark_with_delta_pip
    spark_session_class = importlib.import_module("pyspark.sql").SparkSession

    builder = (
        spark_session_class.builder.appName("nyc-taxi-sample-pipeline-test")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as exc:
        pytest.skip(f"Local Spark/Delta runtime is not available: {exc}")

    yield spark
    spark.stop()


def test_sample_bronze_to_silver_pipeline(spark_session, tmp_path: Path) -> None:
    source_path = tmp_path / "source_yellow_taxi"
    bronze_path = tmp_path / "bronze_delta"
    silver_path = tmp_path / "silver_delta"
    quarantine_path = tmp_path / "quarantine_delta"
    pickup = datetime(2023, 1, 1, 8, 0, tzinfo=UTC)

    sample_rows = [
        _raw_row(pickup, pickup + timedelta(minutes=30), fare_amount=12.5, total_amount=18.0),
        _raw_row(pickup, pickup + timedelta(minutes=30), fare_amount=12.5, total_amount=18.0),
        _raw_row(
            pickup + timedelta(hours=1),
            pickup + timedelta(hours=1, minutes=20),
            fare_amount=8.0,
            total_amount=11.0,
            pickup_location_id=101,
            dropoff_location_id=201,
            trip_distance=4.0,
        ),
        _raw_row(
            pickup + timedelta(hours=2),
            pickup + timedelta(hours=2, minutes=15),
            fare_amount=-5.0,
            total_amount=0.0,
            pickup_location_id=102,
            dropoff_location_id=202,
        ),
        _raw_row(
            pickup + timedelta(hours=3),
            pickup + timedelta(hours=2, minutes=50),
            fare_amount=9.0,
            total_amount=12.0,
            pickup_location_id=103,
            dropoff_location_id=203,
        ),
    ]
    spark_session.createDataFrame(sample_rows).write.mode("overwrite").parquet(source_path.as_uri())

    config = load_config(
        env_file=None,
        env={
            "SPARK_MASTER_URL": "local[2]",
            "BRONZE_DELTA_PATH": bronze_path.as_uri(),
            "SILVER_DELTA_PATH": silver_path.as_uri(),
            "QUARANTINE_DELTA_PATH": quarantine_path.as_uri(),
            "INGESTION_MANIFEST_PATH": str(tmp_path / "manifest.jsonl"),
            "METRICS_OUTPUT_PATH": str(tmp_path / "metrics.jsonl"),
            "MINIO_ROOT_USER": "",
            "MINIO_ROOT_PASSWORD": "",
        },
    )
    source = SourceFile(
        year=2023,
        month=1,
        file_name="yellow_tripdata_2023-01.parquet",
        source_url=source_path.as_uri(),
    )

    bronze_result = ingest_bronze(spark_session, [source], config, "sample-batch")
    silver_result = transform_silver(spark_session, config, "sample-batch")

    silver = spark_session.read.format("delta").load(silver_path.as_uri())
    quarantine = spark_session.read.format("delta").load(quarantine_path.as_uri())
    silver_rows = silver.collect()
    durations = {row["pickup_location_id"]: row["trip_duration_minutes"] for row in silver_rows}
    speeds = {row["pickup_location_id"]: row["average_speed_mph"] for row in silver_rows}

    assert bronze_result.records_written == 5
    assert silver_result.valid_records_count == 2
    assert silver_result.invalid_records_count == 2
    assert silver_result.duplicates_dropped == 1
    assert silver.count() == 2
    assert quarantine.count() == 2
    assert silver.select("trip_id").distinct().count() == 2
    assert durations[100] == 30
    assert speeds[100] == 12
    assert {
        "trip_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id",
        "fare_amount",
        "total_amount",
        "pickup_date",
        "pickup_hour",
        "trip_duration_minutes",
        "average_speed_mph",
    } <= set(silver.columns)
    assert {
        "error_reason",
        "quarantine_timestamp",
        "batch_id",
        "source_file",
    } <= set(quarantine.columns)


def _raw_row(
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
