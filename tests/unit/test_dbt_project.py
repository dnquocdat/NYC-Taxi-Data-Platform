from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_ROOT = PROJECT_ROOT / "dbt" / "nyc_taxi"


def test_dbt_project_files_exist() -> None:
    expected_files = [
        "dbt_project.yml",
        "profiles.yml",
        "profiles.yml.example",
        "models/sources.yml",
        "models/schema.yml",
        "models/staging/stg_yellow_taxi_trips.sql",
        "models/staging/stg_taxi_zones.sql",
        "models/marts/core/fact_trips.sql",
        "models/marts/analytics/mart_daily_revenue.sql",
        "tests/generic/expression_is_true.sql",
    ]

    missing = [path for path in expected_files if not (DBT_ROOT / path).exists()]

    assert missing == []


def test_fact_trips_documents_grain_and_has_quality_tests() -> None:
    schema = yaml.safe_load((DBT_ROOT / "models" / "schema.yml").read_text(encoding="utf-8"))
    fact_model = next(model for model in schema["models"] if model["name"] == "fact_trips")
    fact_columns = {column["name"]: column for column in fact_model["columns"]}

    assert "one row equals one validated yellow taxi trip" in fact_model["description"]
    assert "Primary key: trip_id" in fact_model["description"]
    assert {"trip_id", "pickup_datetime", "dropoff_datetime"} <= set(fact_columns)
    assert len(fact_model["data_tests"]) >= 4
    assert len(fact_columns["trip_id"]["data_tests"]) >= 2
