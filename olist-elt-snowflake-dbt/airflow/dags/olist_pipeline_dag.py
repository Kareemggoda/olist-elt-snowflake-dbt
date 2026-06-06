from __future__ import annotations

import os
import sys  
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── 1. Default args & Constants ─────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR     = "/opt/dbt/olist_dbt"
INGEST_DIR  = "/opt/airflow/ingestion"
DBT_TARGET  = "prod"
DBT_PROFILE = "olist_snowflake"

def _run_ingestion():
    """Calls the Python ingestion script inside the DAG."""
    import sys
    sys.path.insert(0, INGEST_DIR)
    from upload_to_snowflake import main
    main()

with DAG(
    dag_id="olist_pipeline",
    default_args=default_args,
    description="End-to-end Olist data pipeline",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "production", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1. Ingest CSVs into Snowflake raw schema
    ingest = PythonOperator(
        task_id="ingest_csvs_to_snowflake",
        python_callable=_run_ingestion,
    )

    # 2. dbt deps (install packages)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt deps "
            f"--profiles-dir {DBT_DIR} "
            f"--target {DBT_TARGET}"
        ),
    )

    # 3. dbt seed (translation CSV)
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt seed "
            f"--profiles-dir {DBT_DIR} "
            f"--target {DBT_TARGET} "
            f"--full-refresh"
        ),
    )

    # 4. dbt snapshot (SCD2 customers)
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt snapshot "
            f"--profiles-dir {DBT_DIR} "
            f"--target {DBT_TARGET}"
        ),
    )

    # 5. dbt run staging
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run "
            f"--select staging "
            f"--profiles-dir {DBT_DIR} "
            f"--target {DBT_TARGET}"
        ),
    )

    # 6. dbt run intermediate + marts
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run "
            f"--select intermediate marts "
            f"--profiles-dir {DBT_DIR} "
            f"--target {DBT_TARGET}"
        ),
    )

    # 7. dbt test (all layers)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test "
            f"--profiles-dir {DBT_DIR} "
            f"--target {DBT_TARGET}"
        ),
    )

    # 8. dbt docs generate
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt docs generate "
            f"--profiles-dir {DBT_DIR} "
            f"--target {DBT_TARGET}"
        ),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # End marker
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        start
        >> ingest
        >> dbt_deps
        >> dbt_seed
        >> dbt_snapshot
        >> dbt_run_staging
        >> dbt_run_marts
        >> dbt_test
        >> dbt_docs
        >> end
    )