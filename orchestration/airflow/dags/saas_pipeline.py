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

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def _send_slack(message: str) -> None:
    import requests as _r
    if not SLACK_WEBHOOK_URL:
        print(f"[ALERT] {message}")
        return
    try:
        _r.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as exc:
        print(f"[ALERT - send failed: {exc}] {message}")


def notify_failure(context) -> None:
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id  = context["run_id"]
    log_url = context["task_instance"].log_url
    exc     = context.get("exception", "unknown error")
    _send_slack(
        f":red_circle: *DAG FAILURE*\n"
        f"*DAG:* `{dag_id}`  *Task:* `{task_id}`\n"
        f"*Run:* `{run_id}`\n"
        f"*Error:* {str(exc)[:300]}\n"
        f"*Logs:* {log_url}"
    )

# ── Constants ─────────────────────────────────────────────────────────────────

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
PG_DB   = os.getenv("POSTGRES_DB", "airflow")

CH_URL      = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
CH_USER     = os.getenv("DBT_CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("DBT_CLICKHOUSE_PASSWORD", "")
CH_AUTH     = (CH_USER, CH_PASSWORD)

DATALAKE_HOME = os.getenv("DATALAKE_HOME", "/opt/datalake")
DBT_PROJECT   = f"{DATALAKE_HOME}/transformation/dbt/datalake_transforms"
DBT_PROFILES  = "/opt/airflow/dbt_profiles.yml"
DBT_BIN       = "/home/airflow/.local/bin/dbt"

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ch(sql: str) -> str:
    import requests
    resp = requests.post(CH_URL, data=sql, auth=CH_AUTH, timeout=120)
    resp.raise_for_status()
    return resp.text.strip()


def _ch_insert(table: str, rows: list, columns: list) -> None:
    import requests
    if not rows:
        return
    lines = []
    for row in rows:
        fields = []
        for v in row:
            if v is None:
                fields.append("\\N")
            elif isinstance(v, datetime):
                fields.append(v.strftime("%Y-%m-%d %H:%M:%S"))
            elif isinstance(v, Decimal):
                fields.append(str(float(v)))
            else:
                fields.append(str(v).replace("\t", " ").replace("\n", " "))
        lines.append("\t".join(fields))

    tsv = "\n".join(lines)
    col_list = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_list}) FORMAT TabSeparated"
    resp = requests.post(
        CH_URL,
        params={"query": sql, "database": "raw"},
        data=tsv.encode(),
        auth=CH_AUTH,
        timeout=300,
    )
    resp.raise_for_status()


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
        _ch("TRUNCATE TABLE IF EXISTS raw.saas_users")
        _ch_insert(
            "raw.saas_users", users,
            ["id", "email", "name", "company", "country", "plan", "mrr", "status", "created_at", "updated_at"],
        )
        print(f"Extracted {len(users)} users")

        cur.execute("SELECT id, user_id, event_type, page, occurred_at FROM saas_events")
        events = cur.fetchall()
        _ch("TRUNCATE TABLE IF EXISTS raw.saas_events")
        _ch_insert(
            "raw.saas_events", events,
            ["id", "user_id", "event_type", "page", "occurred_at"],
        )
        print(f"Extracted {len(events)} events")

        cur.execute(
            "SELECT id, user_id, plan, status, billing_cycle, mrr, started_at, ended_at "
            "FROM saas_subscriptions"
        )
        subs = cur.fetchall()
        _ch("TRUNCATE TABLE IF EXISTS raw.saas_subscriptions")
        _ch_insert(
            "raw.saas_subscriptions", subs,
            ["id", "user_id", "plan", "status", "billing_cycle", "mrr", "started_at", "ended_at"],
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
         "--profile", "datalake_transforms"],
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
         "--profile", "datalake_transforms"],
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

    _ch("CREATE DATABASE IF NOT EXISTS pipeline_metadata")
    _ch("""
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
            current = int(_ch(f"SELECT count() FROM {schema}.{tbl}"))
        except Exception:
            current = 0
        prev_result = _ch(
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
        rows.append(f"'{run_date}'\t'saas'\t'{tbl}'\t{current}\t{prev}\t{delta_pct}\t'{status}'")
        print(f"  {schema}.{tbl}: {prev:,} → {current:,} ({delta_pct:+.1f}%) [{status}]")

    if rows:
        _ch(
            f"INSERT INTO pipeline_metadata.observability_runs "
            f"(run_date, pipeline, table_name, row_count, prev_count, delta_pct, status) "
            f"FORMAT TabSeparated\n" + "\n".join(rows)
        )

    if anomalies:
        raise RuntimeError(
            f"SaaS observability FAILED — {len(anomalies)} anomalies:\n" + "\n".join(anomalies)
        )
    print(f"SaaS observability passed — {len(SAAS_TABLES)} tables checked.")


def data_quality_check(**context):
    users  = int(_ch("SELECT count() FROM raw.saas_users"))
    events = int(_ch("SELECT count() FROM raw.saas_events"))
    assert users > 0,  f"No users in raw.saas_users"
    assert events > 0, f"No events in raw.saas_events"
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
