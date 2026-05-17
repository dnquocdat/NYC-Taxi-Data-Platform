# Design Document

## Problem Statement

Build a compact end-to-end data platform that ingests real NYC taxi data, validates and models it, and serves analytics through a live dashboard. The goal is to demonstrate engineering judgment, operational reliability, and explainable trade-offs.

## Dataset

Primary dataset: NYC TLC Yellow Taxi Trip Records in Parquet format.

Lookup dataset: NYC TLC Taxi Zone Lookup CSV.

Production runs must cover enough monthly files to meet at least one entrance-project threshold:

- at least 20 million records, or
- at least 10 GiB raw data.

Sample data is only for tests and local smoke runs.

## Architecture

The platform uses Airflow for orchestration, Spark for distributed processing, MinIO as S3-compatible object storage, Delta Lake for Bronze/Silver/Quarantine tables, ClickHouse for serving, dbt for modeling/tests, and Superset for dashboarding.

See [architecture.mmd](architecture.mmd).

## Data Flow

1. Discover configured NYC TLC monthly Parquet files.
2. Validate that the configured production dataset meets the project threshold.
3. Ingest source files into Bronze Delta with source metadata.
4. Normalize schema, validate rules, generate deterministic `trip_id`, deduplicate, and derive analytical columns.
5. Write valid records to Silver and invalid records to Quarantine.
6. Load Silver records into ClickHouse.
7. Build dbt dimensions, fact table, and analytics marts.
8. Query marts from Superset.

## Modeling

The core model is a star schema.

- Fact grain: one row in `fact_trips` equals one taxi trip.
- Fact primary key: `trip_id`.
- Dimensions: date, time, location, vendor, payment type, and rate code.
- Analytics marts: daily revenue, hourly demand, location performance, and payment summary.

## Partition Strategy

Bronze is partitioned by `dataset_year` and `dataset_month` because source files are delivered by month and this keeps ingestion idempotency simple.

Silver is partitioned by pickup year/month derived from `pickup_datetime` because most analytics and incremental loads are time-based.

ClickHouse is partitioned by `toYYYYMM(pickup_datetime)` and ordered by `pickup_date`, location IDs, and `trip_id` to support common dashboard filters.

## Failure Handling

Invalid business records are written to Quarantine with `error_reason`, source metadata, and quarantine timestamp.

dbt test failures block downstream dashboard trust and fail the Airflow task.

Airflow tasks use retries and structured logs. Errors are not swallowed.

## Observability

Each major job logs structured JSON and writes reviewable run metrics, including:

- `job_duration_seconds`
- `records_processed`
- `invalid_records_count`
- `invalid_records_ratio`
- `duplicates_dropped`
- `data_freshness_hours`

## Trade-offs

This project uses a local Docker Compose stack to keep the demo reproducible. That is less scalable than managed cloud services, but better for a one-week entrance project because every component can be explained and run locally.

Delta Lake on MinIO provides open table storage without requiring a cloud account. The main integration risk is Spark Delta S3A configuration, so configuration is centralized and tested with a sample path first.

## Scalability

At 10x scale, the first pressure points are Spark executor resources, ClickHouse partition sizing, and dashboard query latency. The design can scale by adding Spark workers, tuning partitions, using incremental dbt models, and moving MinIO/ClickHouse to managed or larger infrastructure.

## Cost and Limitations

Local Docker Compose keeps cost near zero but is limited by machine CPU, memory, and disk. Full historical NYC TLC ingestion may require careful month selection and disk budgeting.

## Improvements With More Time

- Add Great Expectations or Soda alongside dbt tests.
- Export and version Superset dashboard assets.
- Add OpenLineage or Marquez for lineage.
- Add Terraform for cloud deployment.
- Add automated performance regression checks.

