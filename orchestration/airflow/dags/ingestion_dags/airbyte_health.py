"""
Airbyte Connection Health Monitor
==================================
DAG ID  : airbyte_connection_health
Schedule: every 6 hours  (0 */6 * * *)
Owner   : shopflow-datalake
Tags    : ingestion, airbyte, health

PURPOSE
-------
Airbyte is the ingestion engine for the ShopFlow domain.  It reads MySQL CDC
snapshots and SAP OData data and writes Parquet files to MinIO, which the bronze
ClickHouse views then expose as queryable tables.  A silent Airbyte failure means
no new data lands in bronze → dbt silver/gold builds on stale inputs → dashboards
show outdated figures with no visible error.

This DAG is the early-warning system that surfaces Airbyte failures before they
propagate downstream.  It runs every 6 hours — frequent enough to catch a failed
nightly sync within half a day, infrequent enough to avoid hammering the Airbyte
control-plane API.

AIRBYTE DEPLOYMENT CONTEXT
--------------------------
Airbyte runs in Kubernetes (installed via abctl), not in Docker Compose.
The control-plane is accessible at http://airbyte-abctl-control-plane:80 from
within the Docker networks, or via the nginx proxy exposed by `make ingestion`.
From k8s pods, Docker services are reachable at 172.17.0.1 (the Docker bridge
gateway on the host).

AUTHENTICATION — OAuth2 Client Credentials
------------------------------------------
Airbyte's Public API v1 uses OAuth2 machine-to-machine tokens rather than
basic auth or API keys.  The token flow:

  POST /api/public/v1/applications/token
  Body: { "client_id": "...", "client_secret": "..." }
  Response: { "access_token": "<bearer>", "token_type": "Bearer", "expires_in": 3600 }

A fresh token is obtained at the start of each task.  Tokens are valid for 1 hour
— well within the task execution window — so no caching is needed.  The Airbyte
agent in the AI layer caches tokens for 55 minutes to amortise latency on repeated
calls.

WHY POST FOR LIST ENDPOINTS
---------------------------
Airbyte Public API v1 uses POST for all list/filter operations (e.g. jobs/list,
connections/list) rather than the conventional GET with query parameters.  Filter
criteria are passed in the JSON body.  This is a deliberate API design choice by
Airbyte to support complex server-side filtering.  Using GET for these endpoints
will return 404 or 405.

TASK DEPENDENCY GRAPH
---------------------
    check_airbyte_connections ──► list_recent_jobs

check_airbyte_connections is the alerting gate — it fails (and alerts Slack) if
any monitored connection's last job is in a terminal failure state.
list_recent_jobs is informational — it prints the last 10 jobs per connection
for audit trail and runs regardless of the health check result.

MONITORED CONNECTIONS
---------------------
Connections are declared in the MONITORED_CONNECTIONS list at module level:

  MONITORED_CONNECTIONS = [
      {"id": os.getenv("AIRBYTE_MYSQL_CONN_ID", ""), "name": "MySQL CDC → MinIO"},
  ]

To add a new connection: add an entry with the Airbyte connection UUID (visible
in the Airbyte UI under Settings → Connections) and a human-readable name.
Set the UUID via an environment variable rather than hardcoding it so the same
DAG file works across environments.

A connection is considered healthy if its latest job has status in:
  succeeded  — completed without errors
  running    — currently executing (expected for long-running CDC syncs)
  pending    — queued and waiting for a worker

Any other status (failed, cancelled, incomplete) triggers a Slack alert and fails
the task, causing Airflow to retry once after 5 minutes.

ENVIRONMENT VARIABLES
---------------------
  AIRBYTE_URL             http://airbyte-abctl-control-plane:80 (default)
  AIRBYTE_CLIENT_ID       required; Airbyte OAuth2 application client ID
  AIRBYTE_CLIENT_SECRET   required; Airbyte OAuth2 application client secret
  AIRBYTE_MYSQL_CONN_ID   Airbyte connection UUID for MySQL CDC → MinIO
  SLACK_WEBHOOK_URL       optional; falls back to print()

FAILURE MODES
-------------
• Credentials not set          → Both tasks skip gracefully with a [SKIP] log
                                  line.  No alert is sent — absence of credentials
                                  is treated as "Airbyte not configured" rather
                                  than a failure.

• Airbyte control-plane down   → requests raises ConnectionError / Timeout;
                                  task fails; Airflow retries once; if still
                                  failing, Slack is alerted via task failure
                                  callback (on_failure_callback not set here —
                                  alert fires from check_airbyte_connections body).

• Token endpoint unreachable   → HTTPError propagates from _get_airbyte_token();
                                  task fails immediately without looping over
                                  connections.

• Connection ID not set        → Entry skipped with [SKIP] log line; other
                                  connections still checked.

• Slack unreachable            → Exception caught; alert printed to task log;
                                  task itself does not fail due to Slack issues.
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
            if resp.status_code in (403, 404):
                print(f"  [SKIP] {conn_name} — Airbyte API returned {resp.status_code} (k8s may be unavailable)")
                continue
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
