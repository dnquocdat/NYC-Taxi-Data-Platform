# Runbook

Use this runbook when the Airflow DAG, local commands, or dashboard data look wrong. Start with the failed task logs, then check structured JSON metrics in `METRICS_OUTPUT_PATH`.

## Failure Mode 1: Source File Unavailable

| Field | Details |
| --- | --- |
| Symptom | Airflow `check_source_available` fails, or dataset validation reports 404/timeout for a source URL. |
| Likely cause | Wrong month range, NYC TLC file not published yet, temporary network/DNS issue, or upstream URL pattern changed. |
| Detection signal | Airflow task failure, HTTP status not 200, missing `Content-Length`, or source discovery logs showing unavailable URLs. |
| Recovery steps | Confirm `DATASET_START_MONTH` and `DATASET_END_MONTH`; run `python scripts/check_dataset_size.py`; open the generated URL manually; adjust the month range if the file is not published; rerun the failed Airflow task. |
| Prevention | Keep default full-mode months pinned to a known available range and update source discovery tests if TLC changes the URL pattern. |

## Failure Mode 2: Schema Drift

| Field | Details |
| --- | --- |
| Symptom | `transform_silver` fails with a missing required column message, or new source columns are not mapped. |
| Likely cause | NYC TLC renamed a column, changed capitalization, removed a required field, or added a new optional field. |
| Detection signal | Spark schema validation error, failed `test_required_columns_are_present`, or Bronze schema differs from expected `schemas.py`. |
| Recovery steps | Inspect the affected Bronze schema; update `COLUMN_MAPPING`, required/optional columns, and casts in `schemas.py`; add a regression test; rerun Bronze to Silver for the affected month. |
| Prevention | Keep schema mapping centralized and review source schema before expanding the production month range. |

## Failure Mode 3: Invalid Records Spike

| Field | Details |
| --- | --- |
| Symptom | `invalid_records_ratio` jumps, Quarantine grows unexpectedly, or dashboard totals drop for a month. |
| Likely cause | Source quality issue, stricter validation rule, schema drift causing null casts, or an upstream data anomaly. |
| Detection signal | Metrics JSONL shows high `invalid_records_count`; Quarantine grouped by `error_reason` shows a dominant failure reason. |
| Recovery steps | Query Quarantine by `batch_id`, `source_file`, and `error_reason`; compare with previous runs; if source data is bad, keep records quarantined and document impact; if validation is wrong, patch the rule and add a test; rerun Silver. |
| Prevention | Monitor invalid ratio per batch and set a review threshold before dashboard publication. |

## Failure Mode 4: Duplicate Records After Rerun

| Field | Details |
| --- | --- |
| Symptom | `trip_id` dbt unique test fails, trip counts increase after rerunning the same month, or ClickHouse has repeated trip IDs. |
| Likely cause | Manifest was deleted, Bronze controlled replacement did not run, Silver merge failed, or ClickHouse partition replacement did not complete. |
| Detection signal | dbt `unique` failure on `fact_trips.trip_id`, high `duplicates_dropped`, or duplicate query on `silver_yellow_taxi_trips`. |
| Recovery steps | Check Bronze manifest entries for the source URL; rerun Silver so `trip_id` dedup/merge is applied; rerun `load_clickhouse` for affected months; rerun `dbt run` and `dbt test`. |
| Prevention | Do not manually edit manifest files during normal runs; keep ClickHouse mutations synchronous for local deterministic loads. |

## Failure Mode 5: ClickHouse Load Failure

| Field | Details |
| --- | --- |
| Symptom | `load_clickhouse` fails, dbt sources are empty, or Superset charts show no rows. |
| Likely cause | ClickHouse container unhealthy, wrong env config, missing table, mutation timeout, or Spark JDBC connectivity issue. |
| Detection signal | `docker compose ps clickhouse` unhealthy, ClickHouse connection errors, failed `create_clickhouse_tables`, or JDBC write errors. |
| Recovery steps | Verify ClickHouse health and `.env` values; run `make create-clickhouse-tables`; rerun `load_clickhouse`; if a mutation partially ran, rerun the same month because the loader is partition-idempotent. |
| Prevention | Create tables before loading, keep month batches modest locally, and monitor ClickHouse disk usage. |

## Failure Mode 6: dbt Test Failure

| Field | Details |
| --- | --- |
| Symptom | Airflow `dbt_test` task fails and `log_pipeline_success` does not run. |
| Likely cause | Duplicate `trip_id`, missing taxi zone lookup rows, invalid numeric values reaching serving, or model logic regression. |
| Detection signal | dbt test output names the failed model/column/test; Airflow task exits non-zero. |
| Recovery steps | Inspect dbt failure output; for relationship failures, download the official taxi zone lookup and rerun `dbt seed`; for fact failures, inspect Silver/Quarantine and patch Spark validation or dbt model logic; rerun `dbt run` and `dbt test`. |
| Prevention | Keep tests in CI where possible and run `dbt test` before dashboard demos. |

## Failure Mode 7: Airflow Timeout Or Stuck Task

| Field | Details |
| --- | --- |
| Symptom | A task stays running too long, retries repeatedly, or the Airflow UI cannot show fresh logs. |
| Likely cause | Docker resource limits, Spark job contention, network slowness, first-time Airflow dependency install, or DockerOperator waiting on an image pull. |
| Detection signal | Airflow task duration exceeds normal run time, Docker logs show slow package install or image pull, host CPU/memory is saturated. |
| Recovery steps | Check `docker compose logs airflow-scheduler airflow-apiserver`; verify Docker Desktop resources; rerun after the dbt image has been pulled; reduce the month range for local testing. |
| Prevention | Pre-pull Docker images before demos and consider a custom Airflow image for pinned dependencies. |

## Failure Mode 8: MinIO Unavailable

| Field | Details |
| --- | --- |
| Symptom | Spark cannot read/write `s3a://` paths, Bronze/Silver Delta writes fail, or MinIO console is unreachable. |
| Likely cause | MinIO container unhealthy, bucket not initialized, wrong access key/secret, or Docker network issue. |
| Detection signal | Spark S3A authentication/connection errors, `docker compose ps minio` unhealthy, missing `nyc-taxi` bucket. |
| Recovery steps | Restart MinIO; check `.env` MinIO values; confirm `minio-init` ran; open MinIO console; rerun the failed Spark task after the bucket exists. |
| Prevention | Keep MinIO credentials in `.env`, avoid changing bucket names mid-run, and run `docker compose --env-file .env config` after config edits. |
