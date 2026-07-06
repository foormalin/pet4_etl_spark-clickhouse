from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/opt/airflow/project"))
PYTHON_BIN = os.getenv("PYTHON_BIN", "python")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def python_job(script: str) -> str:
    return f'cd "{PROJECT_ROOT}" && {PYTHON_BIN} jobs/{script}'


with DAG(
    dag_id="spark_clickhouse_lakehouse_etl",
    description="Generate Parquet data, build bronze/silver/gold layers, and load ClickHouse marts.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "clickhouse", "lakehouse", "etl"],
) as dag:
    generate_raw_parquet = BashOperator(
        task_id="generate_raw_parquet",
        bash_command=python_job("generate_sample_data.py"),
    )

    init_clickhouse = BashOperator(
        task_id="init_clickhouse",
        bash_command=python_job("init_clickhouse.py"),
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=python_job("01_load_bronze.py"),
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=python_job("02_transform_silver.py"),
    )

    build_gold_and_load_clickhouse = BashOperator(
        task_id="build_gold_and_load_clickhouse",
        bash_command=python_job("03_build_gold_and_load_clickhouse.py"),
    )

    generate_raw_parquet >> init_clickhouse >> load_bronze >> transform_silver >> build_gold_and_load_clickhouse
