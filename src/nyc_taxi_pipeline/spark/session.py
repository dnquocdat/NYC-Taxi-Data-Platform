"""SparkSession construction for Delta Lake, MinIO, and downstream connectors."""

from __future__ import annotations

from typing import Any

from nyc_taxi_pipeline.config import PipelineConfig, load_config

SPARK_EXTRA_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.clickhouse:clickhouse-jdbc:0.6.4",
]


def create_spark_session(app_name: str, config: PipelineConfig | None = None) -> Any:
    """Create a SparkSession configured for Delta Lake and MinIO/S3A.

    The function imports PySpark lazily so unit tests and lightweight tooling can import
    this module without requiring a running Spark installation. Credentials are read from
    `PipelineConfig`, which resolves them from environment variables or `.env`; no access
    keys are embedded in source code.

    Parameters
    ----------
    app_name:
        Human-readable Spark application name shown in the Spark UI and logs.
    config:
        Optional preloaded pipeline configuration. When omitted, `load_config()` reads the default
        YAML and environment.
    """
    resolved_config = load_config() if config is None else config

    from delta import configure_spark_with_delta_pip  # noqa: PLC0415
    from pyspark.sql import SparkSession  # noqa: PLC0415

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_config.spark.master_url)
        .config("spark.driver.memory", resolved_config.spark.driver_memory)
        .config("spark.executor.memory", resolved_config.spark.executor_memory)
        .config("spark.executor.cores", str(resolved_config.spark.executor_cores))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.hadoop.fs.s3a.endpoint", resolved_config.storage.minio_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )

    if resolved_config.storage.minio_access_key:
        builder = builder.config(
            "spark.hadoop.fs.s3a.access.key", resolved_config.storage.minio_access_key
        )
    if resolved_config.storage.minio_secret_key:
        builder = builder.config(
            "spark.hadoop.fs.s3a.secret.key", resolved_config.storage.minio_secret_key
        )

    return configure_spark_with_delta_pip(
        builder,
        extra_packages=SPARK_EXTRA_PACKAGES,
    ).getOrCreate()
