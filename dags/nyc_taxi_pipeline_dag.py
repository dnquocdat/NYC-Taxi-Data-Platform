"""Airflow DAG for the NYC taxi monthly data pipeline."""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator

DAG_ID = "nyc_taxi_monthly_pipeline"
PROJECT_DIR = os.environ.get("NYC_TAXI_PROJECT_DIR", "/opt/airflow/project")
DBT_PROJECT_DIR = "/app/dbt/nyc_taxi"
DBT_IMAGE = os.environ.get("DBT_IMAGE", "dnquocdat/nyc-taxi-dbt:latest")
DOCKER_URL = os.environ.get("DOCKER_HOST", "unix://var/run/docker.sock")
DOCKER_NETWORK_MODE = os.environ.get(
    "DATA_PLATFORM_NETWORK",
    "nyc-taxi-data-platform-network",
)

COMMON_ENV = {
    "PYTHONPATH": f"{PROJECT_DIR}/src",
    "DATASET_START_MONTH": "{{ dag_run.conf.get('start_month', params.start_month) }}",
    "DATASET_END_MONTH": "{{ dag_run.conf.get('end_month', params.end_month) }}",
    "SAMPLE_MODE": "{{ dag_run.conf.get('sample_mode', params.sample_mode) | string | lower }}",
    "BATCH_ID": "{{ dag_run.conf.get('batch_id', params.batch_id) or ts_nodash }}",
}

DBT_ENV = {
    "CLICKHOUSE_HOST": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
    "CLICKHOUSE_PORT": os.environ.get("CLICKHOUSE_PORT", "8123"),
    "CLICKHOUSE_DATABASE": os.environ.get("CLICKHOUSE_DATABASE", "nyc_taxi"),
    "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "default"),
    "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def project_command(command: str) -> str:
    """Return a Bash command that runs from the mounted project directory."""
    return f"set -euo pipefail\ncd {PROJECT_DIR}\n{command}"


def dbt_operator(task_id: str, command: str) -> DockerOperator:
    """Build a DockerOperator task that runs dbt from the published dbt image."""
    return DockerOperator(
        task_id=task_id,
        image=DBT_IMAGE,
        command=command,
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK_MODE,
        environment=DBT_ENV,
        force_pull=True,
        auto_remove="success",
        mount_tmp_dir=False,
    )


with DAG(
    dag_id=DAG_ID,
    description="End-to-end NYC TLC taxi pipeline from dataset validation to dbt tests.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "start_month": Param("2023-01", type="string"),
        "end_month": Param("2023-12", type="string"),
        "sample_mode": Param(False, type="boolean"),
        "batch_id": Param("", type="string"),
    },
    tags=["nyc-taxi", "spark", "delta", "clickhouse", "dbt"],
) as dag:
    validate_dataset_size = BashOperator(
        task_id="validate_dataset_size",
        bash_command=project_command(
            "python scripts/check_dataset_size.py "
            "--config configs/pipeline.yml "
            "--env-file .env "
            "--skip-head"
        ),
        env=COMMON_ENV,
    )

    check_source_available = BashOperator(
        task_id="check_source_available",
        bash_command=project_command(
            "python scripts/check_source_available.py "
            "--config configs/pipeline.yml "
            "--env-file .env "
            "--timeout-seconds 20"
        ),
        env=COMMON_ENV,
    )

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=project_command(
            'SAMPLE_FLAG=""\n'
            'if [ "${SAMPLE_MODE}" = "true" ]; then SAMPLE_FLAG="--sample-mode"; fi\n'
            "python -m nyc_taxi_pipeline.cli ingest-bronze "
            '--start-month "${DATASET_START_MONTH}" '
            '--end-month "${DATASET_END_MONTH}" '
            '--batch-id "${BATCH_ID}" '
            "--skip-head "
            "${SAMPLE_FLAG}"
        ),
        env=COMMON_ENV,
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=project_command(
            'python -m nyc_taxi_pipeline.cli transform-silver --batch-id "${BATCH_ID}"'
        ),
        env=COMMON_ENV,
    )

    create_clickhouse_tables = BashOperator(
        task_id="create_clickhouse_tables",
        bash_command=project_command(
            "python scripts/create_clickhouse_tables.py "
            "--config configs/pipeline.yml "
            "--env-file .env "
            "--sql-file scripts/create_clickhouse_tables.sql"
        ),
        env=COMMON_ENV,
    )

    load_clickhouse = BashOperator(
        task_id="load_clickhouse",
        bash_command=project_command(
            'python -m nyc_taxi_pipeline.cli load-clickhouse --batch-id "${BATCH_ID}"'
        ),
        env=COMMON_ENV,
    )

    dbt_seed = dbt_operator(
        task_id="dbt_seed",
        command=f"seed --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_run = dbt_operator(
        task_id="dbt_run",
        command=f"run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = dbt_operator(
        task_id="dbt_test",
        command=f"test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    log_pipeline_success = BashOperator(
        task_id="log_pipeline_success",
        bash_command=project_command(
            'python -c "'
            "import json, logging, os; "
            "logging.basicConfig(level=logging.INFO, format='%(message)s'); "
            "logging.info(json.dumps({"
            "'event': 'pipeline_completed', "
            "'job_name': 'airflow_dag', "
            "'batch_id': os.environ['BATCH_ID'], "
            "'start_month': os.environ['DATASET_START_MONTH'], "
            "'end_month': os.environ['DATASET_END_MONTH'], "
            "'sample_mode': os.environ['SAMPLE_MODE']"
            "}))"
            '"'
        ),
        env=COMMON_ENV,
    )

    (
        validate_dataset_size
        >> check_source_available
        >> ingest_bronze
        >> transform_silver
        >> create_clickhouse_tables
        >> load_clickhouse
        >> dbt_seed
        >> dbt_run
        >> dbt_test
        >> log_pipeline_success
    )
