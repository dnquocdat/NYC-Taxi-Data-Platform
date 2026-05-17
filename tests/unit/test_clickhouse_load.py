"""Tests for ClickHouse load helpers."""

from pathlib import Path

from nyc_taxi_pipeline.config import ClickHouseConfig
from nyc_taxi_pipeline.spark.clickhouse_load import (
    build_delete_partitions_query,
    build_jdbc_properties,
    build_jdbc_url,
)


def test_build_jdbc_url_uses_resolved_config() -> None:
    """JDBC URL should be built from config rather than hard-coded hostnames."""
    config = ClickHouseConfig(
        host="clickhouse.local",
        http_port=8123,
        native_port=9000,
        database="analytics",
        user="service_user",
        password=None,
    )

    assert build_jdbc_url(config) == "jdbc:clickhouse://clickhouse.local:8123/analytics"


def test_build_jdbc_properties_omits_absent_password() -> None:
    """Password should not be invented when it is absent."""
    config = ClickHouseConfig(
        host="clickhouse",
        http_port=8123,
        native_port=9000,
        database="nyc_taxi",
        user="default",
        password=None,
    )

    assert build_jdbc_properties(config) == {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": "default",
    }


def test_delete_partitions_query_is_deterministic() -> None:
    """Partition delete query should sort and de-duplicate affected partitions."""
    query = build_delete_partitions_query(
        "nyc_taxi",
        "silver_yellow_taxi_trips",
        [202302, 202301, 202302],
    )

    assert query == (
        "ALTER TABLE nyc_taxi.silver_yellow_taxi_trips "
        "DELETE WHERE toYYYYMM(pickup_datetime) IN (202301, 202302) "
        "SETTINGS mutations_sync = 2"
    )


def test_clickhouse_table_sql_declares_analytics_layout() -> None:
    """DDL should use the expected engine, partition key, and order key."""
    sql = Path("scripts/create_clickhouse_tables.sql").read_text(encoding="utf-8")

    assert "CREATE DATABASE IF NOT EXISTS nyc_taxi" in sql
    assert "CREATE TABLE IF NOT EXISTS nyc_taxi.silver_yellow_taxi_trips" in sql
    assert "ENGINE = MergeTree" in sql
    assert "PARTITION BY toYYYYMM(pickup_datetime)" in sql
    assert "ORDER BY (pickup_date, pickup_location_id, dropoff_location_id, trip_id)" in sql
