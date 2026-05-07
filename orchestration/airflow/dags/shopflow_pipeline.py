"""
ShopFlow Enterprise Data Lake Pipeline
======================================
DAG: shopflow_datalake_pipeline
Schedule: daily at 06:00 UTC

Task graph:
  trigger_airbyte_mysql_sync
        |
  trigger_airbyte_sap_sync
        |
  trigger_airbyte_rest_sync
        |
  wait_for_minio_files ──── run_dbt_source_freshness
        |
  [extract_saas_to_clickhouse joins here]
        |
  run_dbt_silver
        |
  run_dbt_gold
        |
  observability_check
        |
  run_dbt_tests
        |
  data_quality_check
        |
  notify_success
        |
  refresh_superset_dashboard

Task implementations live in dags/shopflow/*.py.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.alerting import notify_failure, notify_sla_miss
from shopflow.airbyte_tasks import trigger_airbyte_mysql_sync
from shopflow.dbt_tasks import (
    run_dbt_gold,
    run_dbt_silver,
    run_dbt_source_freshness,
    run_dbt_tests,
)
from shopflow.ingest_tasks import trigger_airbyte_rest_sync, trigger_airbyte_sap_sync
from shopflow.minio_tasks import wait_for_minio_files
from shopflow.quality_tasks import data_quality_check, notify_success, observability_check
from shopflow.saas_tasks import extract_saas_to_clickhouse
from shopflow.superset_tasks import refresh_superset_dashboard

# ── DAG defaults ───────────────────────────────────────────────────────────────

default_args = {
    "owner": "shopflow-datalake",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": notify_failure,
}

# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="shopflow_datalake_pipeline",
    default_args=default_args,
    description="ShopFlow Enterprise Data Lake — MySQL CDC + SAP + REST → MinIO → dbt → Gold",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=notify_sla_miss,
    tags=["shopflow", "datalake", "phase6"],
) as dag:

    t1 = PythonOperator(
        task_id="trigger_airbyte_mysql_sync",
        python_callable=trigger_airbyte_mysql_sync,
        sla=timedelta(hours=1),
    )

    t2 = PythonOperator(
        task_id="trigger_airbyte_sap_sync",
        python_callable=trigger_airbyte_sap_sync,
        sla=timedelta(hours=1, minutes=30),
    )

    t3 = PythonOperator(
        task_id="trigger_airbyte_rest_sync",
        python_callable=trigger_airbyte_rest_sync,
        sla=timedelta(hours=2),
    )

    t4 = PythonOperator(
        task_id="wait_for_minio_files",
        python_callable=wait_for_minio_files,
    )

    t4b = PythonOperator(
        task_id="run_dbt_source_freshness",
        python_callable=run_dbt_source_freshness,
        trigger_rule="all_success",
    )

    t_saas = PythonOperator(
        task_id="extract_saas_to_clickhouse",
        python_callable=extract_saas_to_clickhouse,
        execution_timeout=timedelta(minutes=20),
        sla=timedelta(hours=2),
    )

    t5 = PythonOperator(
        task_id="run_dbt_silver",
        python_callable=run_dbt_silver,
        execution_timeout=timedelta(minutes=15),
        sla=timedelta(hours=3),
        trigger_rule="all_success",
    )

    t6 = PythonOperator(
        task_id="run_dbt_gold",
        python_callable=run_dbt_gold,
        execution_timeout=timedelta(minutes=15),
        sla=timedelta(hours=3, minutes=30),
    )

    t_obs = PythonOperator(
        task_id="observability_check",
        python_callable=observability_check,
        execution_timeout=timedelta(minutes=5),
    )

    t7 = PythonOperator(
        task_id="run_dbt_tests",
        python_callable=run_dbt_tests,
        execution_timeout=timedelta(minutes=10),
        sla=timedelta(hours=4),
    )

    t8 = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
        sla=timedelta(hours=4, minutes=30),
    )

    t9 = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule="all_success",
    )

    t10 = PythonOperator(
        task_id="refresh_superset_dashboard",
        python_callable=refresh_superset_dashboard,
    )

    # ShopFlow path: Airbyte → MinIO → dbt silver
    t1 >> t2 >> t3 >> t4 >> [t4b, t5]
    # SaaS path: PG extract runs in parallel with Airbyte, merges at dbt silver
    t1 >> t_saas >> t5
    # Shared path: silver → gold → observability → tests → quality → notify → superset
    t5 >> t6 >> t_obs >> t7 >> t8 >> t9 >> t10
