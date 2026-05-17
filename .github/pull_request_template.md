# Summary

Build an engineering-grade NYC Taxi data platform for the Nexlab Data Engineer Internship Entrance Project.

This PR includes:

- NYC TLC source discovery and dataset threshold validation.
- Spark Bronze ingestion into Delta Lake with lineage metadata and manifest-based idempotency.
- Spark Silver transformation with schema normalization, deterministic `trip_id`, validation, deduplication, late-arriving merge handling, and Quarantine output.
- ClickHouse serving table and idempotent month-partition replacement load.
- dbt staging, star schema, analytics marts, and data quality tests.
- Airflow 3 end-to-end DAG with retries and DockerOperator-based dbt execution.
- Superset ClickHouse dashboard documentation and custom Superset image with ClickHouse driver.
- Tests, CI, Docker Compose stack, structured logging, metrics, and documentation.

# Validation

- [ ] `python -m ruff check src tests dags scripts`
- [ ] `python -m black --check src tests dags scripts`
- [ ] `python -m pytest`
- [ ] `docker compose --env-file .env.example config --quiet`
- [ ] `python scripts/check_dataset_size.py --env-file .env.example --skip-head`
- [ ] `docker build -f dbt/nyc_taxi/Dockerfile -t nyc-taxi-dbt:ci .`
- [ ] `docker compose --env-file .env.example build superset`

# Notes For Reviewer

- `sample_mode=true` is only for tests and local smoke runs. The default full-mode dataset selection is 2023-01 through 2023-12 and exceeds 20 million records by configured metadata.
- The committed `taxi_zone_lookup.csv` is header-only to avoid fake production geography. Download the official NYC TLC lookup before a full dbt/Superset demo.
- Superset dashboard export is not committed yet because it requires a running Superset metadata database with populated ClickHouse marts.
- Local `.env.example` values are development defaults only and must be changed for any non-local deployment.
