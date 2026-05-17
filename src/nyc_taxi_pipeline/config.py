"""Configuration loading for the NYC taxi data platform.

The pipeline has two configuration sources:

1. `configs/pipeline.yml` describes project, dataset, storage, and quality settings.
2. Environment variables, usually loaded from `.env`, provide local overrides and secrets.

YAML values may reference environment variables with `${VAR_NAME}`. The loader resolves those
placeholders after merging safe local defaults, an optional env file, and the process environment.
Credentials are not hard-coded here; if they are absent, downstream connectors can decide whether
to fail fast or run without that integration.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "configs" / "pipeline.yml"
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")

_LOCAL_DEFAULTS: dict[str, str] = {
    "PROJECT_NAME": "nyc-taxi-data-platform",
    "ENVIRONMENT": "local",
    "LOG_LEVEL": "INFO",
    "METRICS_OUTPUT_PATH": "metrics/pipeline_metrics.jsonl",
    "DATASET_START_MONTH": "2023-01",
    "DATASET_END_MONTH": "2023-12",
    "SAMPLE_MODE": "false",
    "MIN_DATASET_RECORDS": "20000000",
    "MIN_RAW_SIZE_GB": "10",
    "MIN_DATASET_RAW_BYTES": str(10 * 1024 * 1024 * 1024),
    "NYC_TLC_BASE_URL": "https://d37ci6vzurychx.cloudfront.net/trip-data",
    "TAXI_ZONE_LOOKUP_URL": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    "MINIO_ENDPOINT": "http://minio:9000",
    "S3_BUCKET": "nyc-taxi",
    "S3_WAREHOUSE_PATH": "s3a://nyc-taxi/delta",
    "SPARK_MASTER_URL": "spark://spark-master:7077",
    "SPARK_DRIVER_MEMORY": "2g",
    "SPARK_EXECUTOR_MEMORY": "2g",
    "SPARK_EXECUTOR_CORES": "2",
    "CLICKHOUSE_HOST": "clickhouse",
    "CLICKHOUSE_HTTP_PORT": "8123",
    "CLICKHOUSE_NATIVE_PORT": "9000",
    "CLICKHOUSE_DATABASE": "nyc_taxi",
    "CLICKHOUSE_USER": "default",
}


@dataclass(frozen=True)
class DatasetConfig:
    """Dataset source and threshold configuration."""

    name: str
    base_url: str
    taxi_zone_lookup_url: str
    start_month: str
    end_month: str
    sample_mode: bool
    minimum_records: int
    minimum_raw_bytes: int
    minimum_raw_size_gb: float
    record_count_metadata: Mapping[str, int]


@dataclass(frozen=True)
class StorageConfig:
    """Object storage and Delta table paths."""

    bucket: str
    warehouse_path: str
    bronze_path: str
    silver_path: str
    quarantine_path: str
    metrics_path: str
    minio_endpoint: str
    minio_access_key: str | None
    minio_secret_key: str | None


@dataclass(frozen=True)
class SparkConfig:
    """Spark runtime configuration."""

    master_url: str
    driver_memory: str
    executor_memory: str
    executor_cores: int


@dataclass(frozen=True)
class ClickHouseConfig:
    """ClickHouse connection configuration."""

    host: str
    http_port: int
    native_port: int
    database: str
    user: str
    password: str | None


@dataclass(frozen=True)
class QualityConfig:
    """Pipeline-level quality thresholds."""

    row_count_minimum: int
    invalid_ratio_warning_threshold: float
    freshness_warning_hours: int


@dataclass(frozen=True)
class RuntimeConfig:
    """Runtime metadata shared by jobs."""

    project_name: str
    environment: str
    log_level: str
    metrics_output_path: Path


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved project configuration used by pipeline jobs."""

    runtime: RuntimeConfig
    dataset: DatasetConfig
    storage: StorageConfig
    spark: SparkConfig
    clickhouse: ClickHouseConfig
    quality: QualityConfig


def read_env_file(path: Path) -> dict[str, str]:
    """Read simple KEY=VALUE lines from an env file if it exists."""
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def build_env(
    env_file: Path | None = DEFAULT_ENV_FILE, env: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Merge local defaults, optional env file values, and process env values."""
    merged = dict(_LOCAL_DEFAULTS)
    if env_file is not None:
        merged.update(read_env_file(env_file))
    merged.update(dict(os.environ if env is None else env))
    return merged


def load_yaml_config(path: Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    """Load a YAML config file into a dictionary."""
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        msg = f"Expected mapping at root of config file: {path}"
        raise ValueError(msg)
    return loaded


def resolve_env_placeholders(value: Any, env: Mapping[str, str]) -> Any:
    """Recursively resolve `${VAR_NAME}` placeholders in YAML values."""
    if isinstance(value, str):
        return _ENV_PATTERN.sub(lambda match: env.get(match.group(1), ""), value)
    if isinstance(value, list):
        return [resolve_env_placeholders(item, env) for item in value]
    if isinstance(value, dict):
        return {key: resolve_env_placeholders(item, env) for key, item in value.items()}
    return value


def as_bool(value: str | bool) -> bool:
    """Convert common string booleans to `bool`."""
    if isinstance(value, bool):
        return value
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _as_optional(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    return value


def load_config(
    config_path: Path | str = DEFAULT_CONFIG_PATH,
    env_file: Path | str | None = DEFAULT_ENV_FILE,
    env: Mapping[str, str] | None = None,
) -> PipelineConfig:
    """Load the pipeline configuration from YAML and environment variables."""
    resolved_config_path = Path(config_path)
    resolved_env_file = None if env_file is None else Path(env_file)
    merged_env = build_env(resolved_env_file, env)
    raw_config = resolve_env_placeholders(load_yaml_config(resolved_config_path), merged_env)

    project = raw_config.get("project", {})
    dataset = raw_config.get("dataset", {})
    storage = raw_config.get("storage", {})
    quality = raw_config.get("quality", {})

    min_raw_size_gb = float(dataset.get("min_raw_size_gb", merged_env["MIN_RAW_SIZE_GB"]))
    minimum_raw_bytes = int(
        dataset.get(
            "minimum_raw_bytes",
            dataset.get("min_raw_bytes", merged_env.get("MIN_DATASET_RAW_BYTES", 0)),
        )
        or int(min_raw_size_gb * 1024 * 1024 * 1024)
    )
    if minimum_raw_bytes <= 0:
        minimum_raw_bytes = int(min_raw_size_gb * 1024 * 1024 * 1024)

    raw_record_count_metadata = dataset.get("record_count_metadata", {})
    if not isinstance(raw_record_count_metadata, dict):
        msg = "dataset.record_count_metadata must be a mapping of YYYY-MM to record count"
        raise ValueError(msg)

    runtime = RuntimeConfig(
        project_name=str(project.get("name", merged_env["PROJECT_NAME"])),
        environment=str(project.get("environment", merged_env["ENVIRONMENT"])),
        log_level=merged_env.get("LOG_LEVEL", "INFO"),
        metrics_output_path=Path(
            merged_env.get("METRICS_OUTPUT_PATH", "metrics/pipeline_metrics.jsonl")
        ),
    )

    return PipelineConfig(
        runtime=runtime,
        dataset=DatasetConfig(
            name=str(dataset.get("dataset_name", dataset.get("name", "nyc_tlc_yellow_taxi"))),
            base_url=str(
                dataset.get(
                    "source_base_url",
                    dataset.get("base_url", merged_env["NYC_TLC_BASE_URL"]),
                )
            ),
            taxi_zone_lookup_url=str(
                dataset.get("taxi_zone_lookup_url", merged_env["TAXI_ZONE_LOOKUP_URL"])
            ),
            start_month=str(dataset.get("start_month", merged_env["DATASET_START_MONTH"])),
            end_month=str(dataset.get("end_month", merged_env["DATASET_END_MONTH"])),
            sample_mode=as_bool(str(dataset.get("sample_mode", merged_env["SAMPLE_MODE"]))),
            minimum_records=int(
                dataset.get(
                    "min_record_count",
                    dataset.get("minimum_records", merged_env["MIN_DATASET_RECORDS"]),
                )
            ),
            minimum_raw_bytes=minimum_raw_bytes,
            minimum_raw_size_gb=min_raw_size_gb,
            record_count_metadata={
                str(month): int(count) for month, count in raw_record_count_metadata.items()
            },
        ),
        storage=StorageConfig(
            bucket=str(storage.get("bucket", merged_env["S3_BUCKET"])),
            warehouse_path=str(storage.get("warehouse_path", merged_env["S3_WAREHOUSE_PATH"])),
            bronze_path=str(storage.get("bronze_path")),
            silver_path=str(storage.get("silver_path")),
            quarantine_path=str(storage.get("quarantine_path")),
            metrics_path=str(storage.get("metrics_path")),
            minio_endpoint=merged_env["MINIO_ENDPOINT"],
            minio_access_key=_as_optional(merged_env.get("MINIO_ROOT_USER")),
            minio_secret_key=_as_optional(merged_env.get("MINIO_ROOT_PASSWORD")),
        ),
        spark=SparkConfig(
            master_url=merged_env["SPARK_MASTER_URL"],
            driver_memory=merged_env["SPARK_DRIVER_MEMORY"],
            executor_memory=merged_env["SPARK_EXECUTOR_MEMORY"],
            executor_cores=int(merged_env["SPARK_EXECUTOR_CORES"]),
        ),
        clickhouse=ClickHouseConfig(
            host=merged_env["CLICKHOUSE_HOST"],
            http_port=int(merged_env["CLICKHOUSE_HTTP_PORT"]),
            native_port=int(merged_env["CLICKHOUSE_NATIVE_PORT"]),
            database=merged_env["CLICKHOUSE_DATABASE"],
            user=merged_env["CLICKHOUSE_USER"],
            password=_as_optional(merged_env.get("CLICKHOUSE_PASSWORD")),
        ),
        quality=QualityConfig(
            row_count_minimum=int(quality.get("row_count_minimum", 1)),
            invalid_ratio_warning_threshold=float(
                quality.get("invalid_ratio_warning_threshold", 0.02)
            ),
            freshness_warning_hours=int(quality.get("freshness_warning_hours", 48)),
        ),
    )
