"""
dbt_batch_reconciliation.py

Airflow DAG — runs on a schedule to:
  1. Run dbt snapshot (SCD2 refresh for dim_device)
  2. Run dbt incremental models (reconcile late-arriving data into Gold)
  3. Run dbt tests to assert Gold quality

Runs every 15 minutes to keep Gold reasonably fresh.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = os.getenv("DBT_DIR", "/opt/dbt/weighstream")
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = os.getenv("TRINO_PORT", "8080")

DBT_CMD = (
    f"cd {DBT_DIR} && "
    f"TRINO_HOST={TRINO_HOST} TRINO_PORT={TRINO_PORT} "
    "dbt"
)

default_args = {
    "owner": "weighstream",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="dbt_batch_reconciliation",
    description="Refresh Gold layer: SCD2 snapshot + incremental models + tests",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weighstream", "dbt", "gold"],
) as dag:

    snapshot = BashOperator(
        task_id="dbt_snapshot_dim_device",
        bash_command=f"{DBT_CMD} snapshot --select dim_device_snapshot",
        doc_md="Refresh SCD2 history for `dim_device_snapshot` in Gold.",
    )

    run_gold = BashOperator(
        task_id="dbt_run_gold_models",
        bash_command=(
            f"{DBT_CMD} run "
            "--select staging.stg_weigh_readings+ "
            "--exclude dim_device"
        ),
        doc_md="Run all staging and Gold incremental/table models.",
    )

    test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"{DBT_CMD} test --select gold",
        doc_md="Assert uniqueness, not_null, accepted_values, relationships on Gold.",
    )

    snapshot >> run_gold >> test_gold
