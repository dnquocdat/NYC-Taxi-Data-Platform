import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DAG_PATH = PROJECT_ROOT / "dags" / "nyc_taxi_pipeline_dag.py"


def test_airflow_dag_file_parses_without_airflow_runtime() -> None:
    ast.parse(DAG_PATH.read_text(encoding="utf-8"))


def test_airflow_dag_defines_required_tasks_and_policy() -> None:
    dag_text = DAG_PATH.read_text(encoding="utf-8")
    required_fragments = [
        'DAG_ID = "nyc_taxi_monthly_pipeline"',
        '"retries": 2',
        "timedelta(minutes=5)",
        'task_id="validate_dataset_size"',
        'task_id="check_source_available"',
        'task_id="ingest_bronze"',
        'task_id="transform_silver"',
        'task_id="create_clickhouse_tables"',
        'task_id="load_clickhouse"',
        'task_id="dbt_seed"',
        'task_id="dbt_run"',
        'task_id="dbt_test"',
        'task_id="log_pipeline_success"',
        "DockerOperator",
        "dnquocdat/nyc-taxi-dbt:latest",
        "force_pull=True",
        "BATCH_ID",
        'command=f"test --project-dir',
    ]

    missing = [fragment for fragment in required_fragments if fragment not in dag_text]

    assert missing == []
