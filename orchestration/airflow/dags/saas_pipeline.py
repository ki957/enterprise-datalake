"""
SaaS Data Pipeline
===================
DAG: saas_data_pipeline
Schedule: daily at 06:30 UTC (30 min after ShopFlow to stagger load)

Extracts SaaS data from PostgreSQL into ClickHouse raw schema,
then runs dbt staging + mart models for the SaaS domain.

Task graph:
  extract_postgres_to_clickhouse
        |
  run_dbt_saas_staging
        |
  run_dbt_saas_marts
        |
  data_quality_check
        |
  notify_success
"""

import json
import os
import subprocess
from datetime import datetime, timedelta
from decimal import Decimal

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.alerting import notify_failure
from common.helpers import ch_exec, ch_insert

# ── Constants ─────────────────────────────────────────────────────────────────

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
PG_DB   = os.getenv("POSTGRES_DB", "airflow")

# Imported from common.helpers: CH_URL, CH_USER, CH_PASSWORD, CH_AUTH, DBT_BIN, DBT_PROJECT
from common.helpers import CH_URL, CH_USER, CH_PASSWORD, CH_AUTH, DBT_BIN, DBT_PROJECT


# ── Task functions ─────────────────────────────────────────────────────────────

def extract_postgres_to_clickhouse(**context):
    """Extract saas_users, saas_events, saas_subscriptions from PostgreSQL → ClickHouse raw."""
    import psycopg2

    pg = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        database=PG_DB, user=PG_USER, password=PG_PASS,
    )
    try:
        cur = pg.cursor()

        cur.execute(
            "SELECT id, email, name, company, country, plan, mrr, status, created_at, updated_at "
            "FROM saas_users"
        )
        users = cur.fetchall()
        ch_exec("TRUNCATE TABLE IF EXISTS raw.saas_users")
        ch_insert(
            "raw.saas_users", users,
            ["id", "email", "name", "company", "country", "plan", "mrr", "status", "created_at", "updated_at"],
        )
        print(f"Extracted {len(users)} users")

        cur.execute("SELECT id, user_id, event_type, page, occurred_at FROM saas_events")
        events = cur.fetchall()
        ch_exec("TRUNCATE TABLE IF EXISTS raw.saas_events")
        ch_insert(
            "raw.saas_events", events,
            ["id", "user_id", "event_type", "page", "occurred_at"],
        )
        print(f"Extracted {len(events)} events")

        cur.execute(
            "SELECT id, user_id, plan, status, billing_cycle, mrr, started_at, expires_at "
            "FROM saas_subscriptions"
        )
        subs = cur.fetchall()
        ch_exec("TRUNCATE TABLE IF EXISTS raw.saas_subscriptions")
        ch_insert(
            "raw.saas_subscriptions", subs,
            ["id", "user_id", "plan", "status", "billing_cycle", "mrr", "started_at", "expires_at"],
        )
        print(f"Extracted {len(subs)} subscriptions")

        cur.close()
    finally:
        pg.close()

    context["ti"].xcom_push(
        key="row_counts",
        value={"users": len(users), "events": len(events), "subs": len(subs)},
    )


def run_dbt_saas_staging(**context):
    result = subprocess.run(
        [DBT_BIN, "run", "--select", "staging", "--profiles-dir", "/opt/airflow",
         "--profile", "datalake_transforms",
         "--target-path", "/tmp/dbt_target", "--log-path", "/tmp/dbt_logs"],
        cwd=DBT_PROJECT,
        capture_output=True,
        text=True,
    )
    print(result.stdout[-3000:])
    if result.returncode != 0:
        raise RuntimeError(f"dbt staging failed:\n{result.stderr[-2000:]}")


def run_dbt_saas_marts(**context):
    result = subprocess.run(
        [DBT_BIN, "run", "--select", "marts", "--profiles-dir", "/opt/airflow",
         "--profile", "datalake_transforms",
         "--target-path", "/tmp/dbt_target", "--log-path", "/tmp/dbt_logs"],
        cwd=DBT_PROJECT,
        capture_output=True,
        text=True,
    )
    print(result.stdout[-3000:])
    if result.returncode != 0:
        raise RuntimeError(f"dbt marts failed:\n{result.stderr[-2000:]}")


def observability_check(**context):
    """Diff gold SaaS mart row counts against previous run.
    Writes results to pipeline_metadata.observability_runs in ClickHouse.
    Fails if any mart drops more than 25% — anomaly guard.
    """
    run_date = context["ds"]
    SAAS_TABLES = [
        ("gold", "dim_users"),
        ("gold", "fct_daily_active_users"),
        ("gold", "fct_event_funnel"),
        ("gold", "fct_mrr"),
    ]

    ch_exec("CREATE DATABASE IF NOT EXISTS pipeline_metadata")
    ch_exec("""
        CREATE TABLE IF NOT EXISTS pipeline_metadata.observability_runs (
            run_date     Date,
            pipeline     String,
            table_name   String,
            row_count    Int64,
            prev_count   Int64,
            delta_pct    Float64,
            status       String,
            created_at   DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (run_date, pipeline, table_name)
    """)

    anomalies = []
    rows = []

    for schema, tbl in SAAS_TABLES:
        try:
            current = int(ch_exec(f"SELECT count() FROM {schema}.{tbl}"))
        except Exception:
            current = 0
        prev_result = ch_exec(
            f"SELECT row_count FROM pipeline_metadata.observability_runs "
            f"WHERE pipeline = 'saas' AND table_name = '{tbl}' "
            f"AND run_date < toDate('{run_date}') "
            f"ORDER BY run_date DESC LIMIT 1"
        )
        prev = int(prev_result) if prev_result else current
        delta_pct = round((current - prev) / max(prev, 1) * 100, 2)
        status = "anomaly" if prev > 0 and delta_pct < -25 else "ok"
        if status == "anomaly":
            anomalies.append(f"{schema}.{tbl}: {prev:,} → {current:,} ({delta_pct:+.1f}%)")
        rows.append(f"{run_date}\tsaas\t{tbl}\t{current}\t{prev}\t{delta_pct}\t{status}")
        print(f"  {schema}.{tbl}: {prev:,} → {current:,} ({delta_pct:+.1f}%) [{status}]")

    if rows:
        ch_exec(
            "INSERT INTO pipeline_metadata.observability_runs "
            "(run_date, pipeline, table_name, row_count, prev_count, delta_pct, status) "
            "FORMAT TabSeparated\n" + "\n".join(rows)
        )

    if anomalies:
        raise RuntimeError(
            f"SaaS observability FAILED — {len(anomalies)} anomalies:\n" + "\n".join(anomalies)
        )
    print(f"SaaS observability passed — {len(SAAS_TABLES)} tables checked.")


def data_quality_check(**context):
    users  = int(ch_exec("SELECT count() FROM raw.saas_users"))
    events = int(ch_exec("SELECT count() FROM raw.saas_events"))
    assert users > 0,  "No users in raw.saas_users"
    assert events > 0, "No events in raw.saas_events"
    print(f"Quality check passed — {users:,} users, {events:,} events")


def notify_success(**context):
    counts = context["ti"].xcom_pull(task_ids="extract_postgres_to_clickhouse", key="row_counts") or {}
    print(
        f"SaaS pipeline complete — "
        f"{counts.get('users', '?'):,} users / {counts.get('events', '?'):,} events"
    )


# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner":             "datalake",
    "retries":           2,
    "retry_delay":       timedelta(minutes=3),
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="saas_data_pipeline",
    default_args=default_args,
    description="SaaS Data Lake: PostgreSQL → ClickHouse raw → dbt staging → marts",
    schedule_interval="30 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["saas", "datalake", "postgresql", "dbt"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_postgres_to_clickhouse",
        python_callable=extract_postgres_to_clickhouse,
    )

    t_staging = PythonOperator(
        task_id="run_dbt_saas_staging",
        python_callable=run_dbt_saas_staging,
    )

    t_marts = PythonOperator(
        task_id="run_dbt_saas_marts",
        python_callable=run_dbt_saas_marts,
    )

    t_obs = PythonOperator(
        task_id="observability_check",
        python_callable=observability_check,
        execution_timeout=timedelta(minutes=5),
    )

    t_quality = PythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    t_extract >> t_staging >> t_marts >> t_obs >> t_quality >> t_notify
