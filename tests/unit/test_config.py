"""Tests for configuration loading."""

from pathlib import Path

from nyc_taxi_pipeline.config import load_config, read_env_file


def test_load_config_resolves_env_placeholders(tmp_path: Path) -> None:
    """YAML placeholders should resolve from the supplied environment."""
    config_path = tmp_path / "pipeline.yml"
    config_path.write_text(
        """
project:
  name: ${PROJECT_NAME}
  environment: ${ENVIRONMENT}
dataset:
  name: nyc_tlc_yellow_taxi
  base_url: ${NYC_TLC_BASE_URL}
  taxi_zone_lookup_url: ${TAXI_ZONE_LOOKUP_URL}
  start_month: ${DATASET_START_MONTH}
  end_month: ${DATASET_END_MONTH}
  sample_mode: ${SAMPLE_MODE}
  minimum_records: ${MIN_DATASET_RECORDS}
  minimum_raw_bytes: ${MIN_DATASET_RAW_BYTES}
storage:
  bucket: ${S3_BUCKET}
  warehouse_path: ${S3_WAREHOUSE_PATH}
  bronze_path: ${S3_WAREHOUSE_PATH}/bronze/yellow_taxi_trips
  silver_path: ${S3_WAREHOUSE_PATH}/silver/yellow_taxi_trips
  quarantine_path: ${S3_WAREHOUSE_PATH}/quarantine/yellow_taxi_trips
  metrics_path: ${S3_WAREHOUSE_PATH}/metrics/pipeline_runs
quality:
  row_count_minimum: 10
  invalid_ratio_warning_threshold: 0.1
  freshness_warning_hours: 12
""",
        encoding="utf-8",
    )

    config = load_config(
        config_path=config_path,
        env_file=None,
        env={
            "PROJECT_NAME": "test-project",
            "ENVIRONMENT": "test",
            "LOG_LEVEL": "DEBUG",
            "METRICS_OUTPUT_PATH": str(tmp_path / "metrics.jsonl"),
            "NYC_TLC_BASE_URL": "https://example.test/trip-data",
            "TAXI_ZONE_LOOKUP_URL": "https://example.test/taxi_zone_lookup.csv",
            "DATASET_START_MONTH": "2023-01",
            "DATASET_END_MONTH": "2023-02",
            "SAMPLE_MODE": "true",
            "MIN_DATASET_RECORDS": "100",
            "MIN_DATASET_RAW_BYTES": "200",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ROOT_USER": "local-user",
            "MINIO_ROOT_PASSWORD": "local-password",
            "S3_BUCKET": "test-bucket",
            "S3_WAREHOUSE_PATH": "s3a://test-bucket/delta",
            "SPARK_MASTER_URL": "local[*]",
            "SPARK_DRIVER_MEMORY": "1g",
            "SPARK_EXECUTOR_MEMORY": "1g",
            "SPARK_EXECUTOR_CORES": "1",
            "CLICKHOUSE_HOST": "localhost",
            "CLICKHOUSE_HTTP_PORT": "8123",
            "CLICKHOUSE_NATIVE_PORT": "9000",
            "CLICKHOUSE_DATABASE": "nyc_taxi",
            "CLICKHOUSE_USER": "default",
            "CLICKHOUSE_PASSWORD": "",
        },
    )

    assert config.runtime.project_name == "test-project"
    assert config.dataset.sample_mode is True
    assert config.dataset.minimum_records == 100
    assert config.storage.bronze_path == "s3a://test-bucket/delta/bronze/yellow_taxi_trips"
    assert config.storage.minio_secret_key == "local-password"
    assert config.spark.master_url == "local[*]"
    assert config.quality.freshness_warning_hours == 12


def test_read_env_file_ignores_comments_and_blank_lines(tmp_path: Path) -> None:
    """The lightweight env reader should parse simple local .env files."""
    env_path = tmp_path / ".env"
    env_path.write_text(
        """
# comment
LOG_LEVEL=INFO
S3_BUCKET='nyc-taxi'

""",
        encoding="utf-8",
    )

    values = read_env_file(env_path)

    assert values == {"LOG_LEVEL": "INFO", "S3_BUCKET": "nyc-taxi"}
