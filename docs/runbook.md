# Runbook

## Failure Mode 1: Source File Unavailable

Symptoms:

- Airflow `check_source_available` fails.
- Source URL returns 404 or timeout.

Recovery:

1. Confirm the configured `DATASET_START_MONTH` and `DATASET_END_MONTH`.
2. Open the NYC TLC source URL manually or with the source discovery script.
3. If the file was renamed or not yet published, adjust the month range.
4. Re-run the failed Airflow task.

## Failure Mode 2: Schema Drift

Symptoms:

- Silver transform fails schema validation.
- New, missing, or renamed columns appear in the source Parquet.

Recovery:

1. Inspect the Bronze schema for the affected source file.
2. Update `schemas.py` and column mapping deliberately.
3. Add or update unit tests for the schema change.
4. Re-run Bronze to Silver for the affected partition.

## Failure Mode 3: Bad Records Spike

Symptoms:

- `invalid_records_ratio` exceeds the warning threshold.
- Quarantine table grows unexpectedly.

Recovery:

1. Query Quarantine grouped by `error_reason`, `source_file`, and batch.
2. Confirm whether the spike is source quality or a validation bug.
3. If source quality is the cause, keep the records quarantined and document impact.
4. If validation is too strict, update the rule and add a regression test.

## Failure Mode 4: ClickHouse Load Failure

Symptoms:

- `load_clickhouse` task fails.
- ClickHouse connection errors or partition replacement errors appear.

Recovery:

1. Confirm ClickHouse container health and credentials from `.env`.
2. Validate the target database and table exist.
3. Retry the task after ClickHouse is healthy.
4. If a partial partition was loaded, re-run the idempotent partition load.

## Failure Mode 5: dbt Test Failure

Symptoms:

- `dbt_test` fails.
- Downstream dashboard data should be considered untrusted.

Recovery:

1. Open the failing dbt test output.
2. Identify whether the failure is data quality, relationship integrity, or model logic.
3. Fix source data handling or dbt model logic.
4. Re-run `dbt run` and `dbt test`.

