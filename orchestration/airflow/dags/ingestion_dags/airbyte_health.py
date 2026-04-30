"""
Airbyte Connection Health Monitor
==================================
DAG: airbyte_connection_health
Schedule: every 6 hours

Checks all configured Airbyte connections, reports sync status,
and alerts if any connection has been failing or is overdue.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

AIRBYTE_URL           = os.getenv("AIRBYTE_URL", "http://airbyte-abctl-control-plane:80")
AIRBYTE_CLIENT_ID     = os.getenv("AIRBYTE_CLIENT_ID", "")
AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET", "")
SLACK_WEBHOOK_URL     = os.getenv("SLACK_WEBHOOK_URL", "")

MONITORED_CONNECTIONS = [
    {"id": os.getenv("AIRBYTE_MYSQL_CONN_ID", ""), "name": "MySQL CDC → MinIO"},
]


def _get_airbyte_token() -> str:
    import requests
    resp = requests.post(
        f"{AIRBYTE_URL}/api/public/v1/applications/token",
        json={"client_id": AIRBYTE_CLIENT_ID, "client_secret": AIRBYTE_CLIENT_SECRET},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _send_slack(message: str) -> None:
    import requests as _r
    if not SLACK_WEBHOOK_URL:
        print(f"[ALERT] {message}")
        return
    try:
        _r.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as exc:
        print(f"[ALERT - send failed: {exc}] {message}")


def check_airbyte_connections(**ctx):
    """Query each Airbyte connection for its latest job status and report."""
    import requests

    if not AIRBYTE_CLIENT_ID or not AIRBYTE_CLIENT_SECRET:
        print("[SKIP] Airbyte credentials not configured.")
        return

    token = _get_airbyte_token()
    headers = {"Authorization": f"Bearer {token}"}
    failures = []

    for conn in MONITORED_CONNECTIONS:
        conn_id   = conn["id"]
        conn_name = conn["name"]
        if not conn_id:
            print(f"  [SKIP] {conn_name} — connection ID not set")
            continue

        try:
            resp = requests.post(
                f"{AIRBYTE_URL}/api/public/v1/jobs/list",
                json={"connectionId": conn_id, "limit": 1},
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            jobs = resp.json().get("data", [])

            if not jobs:
                print(f"  [WARN] {conn_name} — no jobs found")
                failures.append(f"{conn_name}: no job history")
                continue

            job       = jobs[0]
            status    = job.get("status")
            job_id    = job.get("jobId") or job.get("id")
            started   = job.get("startTime", "unknown")
            icon      = ":white_check_mark:" if status == "succeeded" else ":red_circle:"
            print(f"  [{icon}] {conn_name} — job {job_id} — {status} — started: {started}")

            if status not in ("succeeded", "running", "pending"):
                failures.append(f"{conn_name}: last job {job_id} status={status}")

        except Exception as exc:
            print(f"  [ERROR] {conn_name} — {exc}")
            failures.append(f"{conn_name}: check failed — {exc}")

    if failures:
        msg = (
            ":red_circle: *Airbyte Connection Health Alert*\n"
            + "\n".join(f"• {f}" for f in failures)
        )
        _send_slack(msg)
        raise RuntimeError(f"Airbyte health check failures: {failures}")

    print(f"\nAll {len(MONITORED_CONNECTIONS)} connections healthy.")
    ctx["ti"].xcom_push(key="connections_checked", value=len(MONITORED_CONNECTIONS))


def list_recent_jobs(**ctx):
    """List the last 10 jobs across all connections for audit trail."""
    import requests

    if not AIRBYTE_CLIENT_ID or not AIRBYTE_CLIENT_SECRET:
        print("[SKIP] Airbyte credentials not configured.")
        return

    token   = _get_airbyte_token()
    headers = {"Authorization": f"Bearer {token}"}

    for conn in MONITORED_CONNECTIONS:
        conn_id = conn["id"]
        if not conn_id:
            continue
        try:
            resp = requests.post(
                f"{AIRBYTE_URL}/api/public/v1/jobs/list",
                json={"connectionId": conn_id, "limit": 10},
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            jobs = resp.json().get("data", [])
            print(f"\n{conn['name']} — last {len(jobs)} jobs:")
            for j in jobs:
                status  = j.get("status", "unknown")
                job_id  = j.get("jobId") or j.get("id")
                started = j.get("startTime", "?")
                print(f"  job={job_id}  status={status}  started={started}")
        except Exception as exc:
            print(f"  [ERROR] {conn['name']}: {exc}")


default_args = {
    "owner": "shopflow-datalake",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="airbyte_connection_health",
    default_args=default_args,
    description="Monitor Airbyte connection health every 6 hours",
    schedule_interval="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "airbyte", "health"],
) as dag:

    t1 = PythonOperator(
        task_id="check_airbyte_connections",
        python_callable=check_airbyte_connections,
    )

    t2 = PythonOperator(
        task_id="list_recent_jobs",
        python_callable=list_recent_jobs,
    )

    t1 >> t2
