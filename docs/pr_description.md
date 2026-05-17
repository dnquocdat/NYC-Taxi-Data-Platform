# Final PR Description

Use this as the description for the final GitHub PR.

## Summary

This project implements a compact end-to-end NYC Taxi data platform using Spark, Delta Lake on MinIO, ClickHouse, dbt, Airflow 3, Superset, Docker Compose, pytest, ruff, black, and GitHub Actions.

The pipeline ingests public NYC TLC Yellow Taxi data, validates the configured full dataset threshold, writes Bronze/Silver/Quarantine Delta tables, loads cleaned trips to ClickHouse, builds a dbt star schema and analytics marts, and documents a Superset dashboard that queries ClickHouse marts directly.

## Key Engineering Points

- Real public dataset with full-mode threshold guard.
- Spark as the primary distributed engine.
- Bronze/Silver/Quarantine lakehouse layout with explicit partitioning.
- Deterministic `trip_id`, deduplication, late-arriving merge handling, and bad-record quarantine.
- dbt star schema with documented `fact_trips` grain and primary key.
- Airflow 3 DAG with retries and idempotent rerun strategy.
- Structured JSON logging and JSONL metrics.
- Docker Compose stack for MinIO, Spark, ClickHouse, Airflow, Superset, and dbt image execution.
- CI for lint, format check, tests, compose config validation, and Docker build.

## Validation Commands

```bash
python -m ruff check src tests dags scripts
python -m black --check src tests dags scripts
python -m pytest
docker compose --env-file .env.example config --quiet
python scripts/check_dataset_size.py --env-file .env.example --skip-head
docker build -f dbt/nyc_taxi/Dockerfile -t nyc-taxi-dbt:ci .
docker compose --env-file .env.example build superset
```

## Known Honest Limitations

- Superset dashboard export is documented but not committed yet because export IDs depend on a running Superset metadata database and populated ClickHouse marts.
- The committed taxi zone lookup seed contains only the official header; full demo runs should download the official NYC TLC lookup before `dbt seed`.
- Docker Compose is intended for local development/demo, not production high availability.
