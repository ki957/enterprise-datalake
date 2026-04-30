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
  wait_for_minio_files
        |
  run_dbt_silver
        |
  run_dbt_gold
        |
  run_dbt_tests
        |
  data_quality_check
        |
  notify_success
        |
  refresh_superset_dashboard
"""

import io
import json
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Constants ─────────────────────────────────────────────────────────────────

AIRBYTE_URL           = os.getenv("AIRBYTE_URL", "http://airbyte-abctl-control-plane:80")
AIRBYTE_CLIENT_ID     = os.getenv("AIRBYTE_CLIENT_ID", "")
AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET", "")
AIRBYTE_MYSQL_CONN    = os.getenv("AIRBYTE_MYSQL_CONN_ID", "9993f6c9-040d-47bb-830b-31de9137a477")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "")
MINIO_BUCKET   = "raw"

CH_URL      = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
CH_USER     = os.getenv("DBT_CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("DBT_CLICKHOUSE_PASSWORD", "")
CH_AUTH     = (CH_USER, CH_PASSWORD)

DATALAKE_HOME = os.getenv("DATALAKE_HOME", "/opt/datalake")
DBT_PROJECT   = f"{DATALAKE_HOME}/transformation/dbt/datalake_transforms"
DBT_PROFILES  = "/opt/airflow/dbt_profiles.yml"
DBT_BIN       = "/home/airflow/.local/bin/dbt"

SAP_BASE_URL  = os.getenv("SAP_BASE_URL", "http://sap-api:5001")
JSON_BASE_URL = os.getenv("JSON_BASE_URL", "https://jsonplaceholder.typicode.com")

SUPERSET_URL  = os.getenv("SUPERSET_URL", "http://superset:8088")
SUPERSET_USER = os.getenv("SUPERSET_USER", "admin")
SUPERSET_PASS = os.getenv("SUPERSET_PASSWORD", "")

# Slack webhook — set via Airflow connection "slack_webhook" or env var.
# If not configured, failure alerts degrade gracefully to log output only.
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
PG_DB   = os.getenv("POSTGRES_DB", "airflow")

# Dataset IDs that map to the 6 gold tables
SUPERSET_DATASET_IDS = [9, 10, 11, 12, 13, 14]


# ── Alerting callbacks ────────────────────────────────────────────────────────

def _send_slack(message: str) -> None:
    """Send a Slack message via webhook. Silently skips if URL not configured."""
    import requests as _requests
    url = SLACK_WEBHOOK_URL
    if not url:
        # Try Airflow HTTP connection as fallback
        try:
            from airflow.hooks.http_hook import HttpHook
            hook = HttpHook(http_conn_id="slack_webhook", method="POST")
            hook.run("", data=json.dumps({"text": message}),
                     headers={"Content-Type": "application/json"})
        except Exception:
            print(f"[ALERT - no Slack configured] {message}")
        return
    try:
        _requests.post(url, json={"text": message}, timeout=10)
    except Exception as exc:
        print(f"[ALERT - Slack send failed: {exc}] {message}")


def notify_failure(context) -> None:
    """on_failure_callback — fires on any task failure."""
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    log_url  = context["task_instance"].log_url
    exc      = context.get("exception", "unknown error")
    msg = (
        f":red_circle: *DAG FAILURE*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Run:* `{run_id}`\n"
        f"*Error:* {str(exc)[:300]}\n"
        f"*Logs:* {log_url}"
    )
    print(msg)
    _send_slack(msg)


def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """on_sla_miss callback — fires when a task misses its SLA window."""
    task_ids = ", ".join(t.task_id for t in (task_list or []))
    msg = (
        f":warning: *SLA MISS* on DAG `{dag.dag_id}`\n"
        f"Tasks that missed SLA: `{task_ids}`"
    )
    print(msg)
    _send_slack(msg)


default_args = {
    "owner": "shopflow-datalake",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": notify_failure,
}


# ── Helper: Airbyte auth ──────────────────────────────────────────────────────

def _get_airbyte_token() -> str:
    import requests
    resp = requests.post(
        f"{AIRBYTE_URL}/api/public/v1/applications/token",
        json={"client_id": AIRBYTE_CLIENT_ID, "client_secret": AIRBYTE_CLIENT_SECRET},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _airbyte_trigger_sync(connection_id: str, token: str) -> str:
    import requests
    resp = requests.post(
        f"{AIRBYTE_URL}/api/public/v1/jobs",
        json={"connectionId": connection_id, "jobType": "sync"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return str(data.get("jobId") or data.get("id"))


def _airbyte_wait_for_job(job_id: str, token: str, timeout: int = 600) -> str:
    import requests
    _TOKEN_TTL = 3000  # refresh every ~50 min (tokens expire at 55 min)
    start = time.time()
    token_refreshed_at = start
    while time.time() - start < timeout:
        # Proactively refresh token before it expires
        if time.time() - token_refreshed_at >= _TOKEN_TTL:
            try:
                token = _get_airbyte_token()
                token_refreshed_at = time.time()
                print(f"  Airbyte token refreshed for job {job_id}")
            except Exception as exc:
                print(f"  [WARN] Token refresh failed: {exc} — continuing with existing token")
        resp = requests.get(
            f"{AIRBYTE_URL}/api/public/v1/jobs/{job_id}",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        status = resp.json().get("status")
        print(f"  Airbyte job {job_id} status: {status}")
        if status == "succeeded":
            return status
        if status in ("failed", "cancelled"):
            raise RuntimeError(f"Airbyte job {job_id} ended with status: {status}")
        time.sleep(15)
    raise TimeoutError(f"Airbyte job {job_id} did not complete within {timeout}s")


# ── Task 1: Airbyte MySQL CDC sync ────────────────────────────────────────────

def trigger_airbyte_mysql_sync(**ctx):
    """Trigger Airbyte MySQL → MinIO CDC sync and wait for completion."""
    print("Triggering Airbyte MySQL CDC sync ...")
    token  = _get_airbyte_token()
    job_id = _airbyte_trigger_sync(AIRBYTE_MYSQL_CONN, token)
    print(f"  Job started: {job_id}")
    token  = _get_airbyte_token()   # refresh before long wait
    status = _airbyte_wait_for_job(job_id, token)
    print(f"  MySQL sync completed: {status}")
    ctx["ti"].xcom_push(key="mysql_job_id", value=job_id)


# ── Task 2: SAP API → MinIO ───────────────────────────────────────────────────

def trigger_airbyte_sap_sync(**ctx):
    """Ingest SAP OData API → MinIO Parquet (vendors, purchase_orders, cost_centers)."""
    import io
    import requests
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        from minio import Minio
    except ImportError as e:
        raise RuntimeError(f"Missing package: {e}. Ensure _PIP_ADDITIONAL_REQUIREMENTS is set.") from e

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    now    = datetime.now(tz=timezone.utc)

    def purge_prefix(minio_prefix: str):
        """Delete all objects under a MinIO prefix before writing fresh data."""
        from minio.deleteobjects import DeleteObject
        objects = client.list_objects(MINIO_BUCKET, prefix=minio_prefix, recursive=True)
        delete_list = [DeleteObject(o.object_name) for o in objects]
        if delete_list:
            errors = list(client.remove_objects(MINIO_BUCKET, delete_list))
            print(f"  Purged {len(delete_list)} objects from {MINIO_BUCKET}/{minio_prefix}")

    def upload(records: list, prefix: str):
        if not records:
            print(f"  [SKIP] No records for {prefix}")
            return
        minio_prefix = f"airbyte/sap/{prefix}/"
        purge_prefix(minio_prefix)
        table = pa.Table.from_pylist(records)
        buf   = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        path  = f"airbyte/sap/{prefix}/{prefix}.parquet"
        client.put_object(MINIO_BUCKET, path, buf, length=buf.getbuffer().nbytes,
                          content_type="application/octet-stream")
        print(f"  Uploaded {MINIO_BUCKET}/{path} ({len(records)} records)")

    def fetch_all(api_path: str) -> list:
        all_records, page = [], 1
        while True:
            data    = requests.get(f"{SAP_BASE_URL}{api_path}?page={page}&page_size=100",
                                   timeout=30).json()
            results = data.get("results", [])
            all_records.extend(results)
            if not data.get("next_page"):
                break
            page += 1
        return all_records

    for path, name in [("/api/vendors", "vendors"),
                       ("/api/purchase-orders", "purchase_orders"),
                       ("/api/cost-centers", "cost_centers")]:
        print(f"  Fetching {path} ...")
        records = fetch_all(path)
        for r in records:
            r["_airbyte_extracted_at"] = now.isoformat()
        upload(records, name)

    print("SAP sync complete.")


# ── Task 3: JSONPlaceholder REST → MinIO ─────────────────────────────────────

def trigger_airbyte_rest_sync(**ctx):
    """Ingest JSONPlaceholder REST API → MinIO Parquet (users, posts, comments)."""
    import io
    import json as _json
    import requests
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        from minio import Minio
    except ImportError as e:
        raise RuntimeError(f"Missing package: {e}") from e

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    now    = datetime.now(tz=timezone.utc)

    def purge_prefix(minio_prefix: str):
        from minio.deleteobjects import DeleteObject
        objects = client.list_objects(MINIO_BUCKET, prefix=minio_prefix, recursive=True)
        delete_list = [DeleteObject(o.object_name) for o in objects]
        if delete_list:
            list(client.remove_objects(MINIO_BUCKET, delete_list))
            print(f"  Purged {len(delete_list)} objects from {MINIO_BUCKET}/{minio_prefix}")

    def upload(records: list, prefix: str):
        flat = []
        for r in records:
            f = {k: (_json.dumps(v) if isinstance(v, dict) else v) for k, v in r.items()}
            f["_airbyte_extracted_at"] = now.isoformat()
            flat.append(f)
        minio_prefix = f"airbyte/rest/{prefix}/"
        purge_prefix(minio_prefix)
        table = pa.Table.from_pylist(flat)
        buf   = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        path  = f"airbyte/rest/{prefix}/{prefix}.parquet"
        client.put_object(MINIO_BUCKET, path, buf, length=buf.getbuffer().nbytes,
                          content_type="application/octet-stream")
        print(f"  Uploaded {MINIO_BUCKET}/{path} ({len(flat)} records)")

    skipped = []
    for endpoint, name in [("/users", "users"), ("/posts", "posts"), ("/comments", "comments")]:
        print(f"  Fetching {JSON_BASE_URL}{endpoint} ...")
        try:
            records = requests.get(f"{JSON_BASE_URL}{endpoint}", timeout=30).json()
        except Exception as exc:
            print(f"  [WARN] JSONPlaceholder unreachable ({exc}) — skipping {name}")
            skipped.append(name)
            continue
        if not isinstance(records, list):
            records = [records]
        upload(records, name)

    if skipped:
        print(f"  REST sync partial — skipped offline: {skipped}")
    else:
        print("REST sync complete.")


# ── Task 4: Wait for MinIO files ──────────────────────────────────────────────

def wait_for_minio_files(**ctx):
    """Verify all expected Parquet prefixes exist in MinIO raw bucket."""
    try:
        from minio import Minio
    except ImportError as e:
        raise RuntimeError(f"Missing package: {e}") from e

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

    required_prefixes = [
        "airbyte/mysql/customers/",
        "airbyte/mysql/orders/",
        "airbyte/mysql/products/",
        "airbyte/sap/vendors/",
        "airbyte/sap/purchase_orders/",
        "airbyte/rest/users/",
        "airbyte/rest/posts/",
    ]

    missing = []
    for prefix in required_prefixes:
        objects = list(client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
        if objects:
            print(f"  OK  {MINIO_BUCKET}/{prefix}  ({len(objects)} file(s))")
        else:
            print(f"  MISSING  {MINIO_BUCKET}/{prefix}")
            missing.append(prefix)

    if missing:
        raise AssertionError(f"Missing MinIO paths: {missing}")

    print("All required MinIO paths verified.")


# ── Task 5–7: dbt runs ────────────────────────────────────────────────────────

# Shared implementation lives in dbt_runner.py — import to avoid duplication.
from transformation_dags.dbt_runner import _run_dbt


def _cleanup_dbt_tmp(target_dir: str | None) -> None:
    """Remove the dbt temp directory (parent of target_dir)."""
    import shutil
    if target_dir:
        shutil.rmtree(os.path.dirname(target_dir), ignore_errors=True)


def _parse_run_results(target_dir: str | None) -> dict:
    """
    Parse dbt run_results.json for structured pass/fail/error counts.
    Falls back to empty dict if the file is absent (older dbt versions).
    """
    if not target_dir:
        return {}
    results_path = os.path.join(target_dir, "run_results.json")
    if not os.path.exists(results_path):
        return {}
    with open(results_path) as f:
        data = json.load(f)
    counts = {"pass": 0, "warn": 0, "error": 0, "skip": 0}
    for r in data.get("results", []):
        status = r.get("status", "")
        if status in ("pass", "success"):
            counts["pass"] += 1
        elif status == "warn":
            counts["warn"] += 1
        elif status in ("error", "fail"):
            counts["error"] += 1
        elif status == "skipped":
            counts["skip"] += 1
    counts["total"] = sum(counts.values())
    return counts


def extract_saas_to_clickhouse(**ctx):
    """
    Extract SaaS data from PostgreSQL → ClickHouse raw.* tables.

    Runs in parallel with the Airbyte wait step. Uses the ClickHouse HTTP
    API INSERT with TabSeparated format in 10K-row chunks so memory stays flat.
    """
    import psycopg2
    import requests

    pg = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASS, dbname=PG_DB,
    )
    pg_cur = pg.cursor()

    def ch_exec(sql: str) -> None:
        r = requests.post(CH_URL, data=sql, auth=CH_AUTH, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"ClickHouse error: {r.text[:400]}")

    def ch_insert_tsv(table: str, data: str) -> None:
        r = requests.post(
            CH_URL,
            params={"query": f"INSERT INTO {table} FORMAT TabSeparated"},
            data=data.encode("utf-8"),
            auth=CH_AUTH,
            timeout=120,
        )
        if r.status_code != 200:
            raise RuntimeError(f"ClickHouse INSERT error on {table}: {r.text[:400]}")

    # ── saas_users ────────────────────────────────────────────────────────────
    ch_exec("""
        CREATE TABLE IF NOT EXISTS raw.saas_users (
            id          Int32,
            email       String,
            name        String,
            company     String,
            country     String,
            plan        String,
            mrr         Float64,
            status      String,
            created_at  DateTime,
            updated_at  DateTime
        ) ENGINE = ReplacingMergeTree()
        ORDER BY id
    """)
    ch_exec("TRUNCATE TABLE raw.saas_users")
    pg_cur.execute(
        "SELECT id, email, name, company, country, plan, mrr, status, created_at, updated_at "
        "FROM saas_users ORDER BY id"
    )
    chunk_size = 10_000
    while True:
        rows = pg_cur.fetchmany(chunk_size)
        if not rows:
            break
        buf = io.StringIO()
        for r in rows:
            id_, email, name, company, country, plan, mrr, status, ca, ua = r
            buf.write(f"{id_}\t{email}\t{name or ''}\t{company or ''}\t"
                      f"{country or ''}\t{plan or ''}\t{mrr or 0}\t{status or ''}\t"
                      f"{ca.strftime('%Y-%m-%d %H:%M:%S') if ca else '1970-01-01 00:00:00'}\t"
                      f"{ua.strftime('%Y-%m-%d %H:%M:%S') if ua else '1970-01-01 00:00:00'}\n")
        ch_insert_tsv("raw.saas_users", buf.getvalue())
    print("  saas_users: done")

    # ── saas_events ───────────────────────────────────────────────────────────
    ch_exec("""
        CREATE TABLE IF NOT EXISTS raw.saas_events (
            id          Int32,
            user_id     Int32,
            event_type  String,
            page        String,
            properties  String,
            occurred_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY (occurred_at, id)
    """)
    ch_exec("TRUNCATE TABLE raw.saas_events")
    pg_cur.execute(
        "SELECT id, user_id, event_type, page, properties::text, occurred_at "
        "FROM saas_events ORDER BY id"
    )
    total_events = 0
    while True:
        rows = pg_cur.fetchmany(chunk_size)
        if not rows:
            break
        buf = io.StringIO()
        for r in rows:
            id_, uid, etype, page, props, occ = r
            buf.write(f"{id_}\t{uid or 0}\t{etype or ''}\t{page or ''}\t"
                      f"{(props or '{}').replace(chr(9), ' ')}\t"
                      f"{occ.strftime('%Y-%m-%d %H:%M:%S') if occ else '1970-01-01 00:00:00'}\n")
        ch_insert_tsv("raw.saas_events", buf.getvalue())
        total_events += len(rows)
        print(f"  saas_events: {total_events:,}", end="\r", flush=True)
    print(f"  saas_events: {total_events:,}  done")

    # ── saas_subscriptions ────────────────────────────────────────────────────
    ch_exec("""
        CREATE TABLE IF NOT EXISTS raw.saas_subscriptions (
            id            Int32,
            user_id       Int32,
            plan          String,
            amount        Float64,
            billing_cycle String,
            started_at    DateTime,
            expires_at    DateTime,
            status        String,
            created_at    DateTime
        ) ENGINE = ReplacingMergeTree()
        ORDER BY id
    """)
    ch_exec("TRUNCATE TABLE raw.saas_subscriptions")
    pg_cur.execute(
        "SELECT id, user_id, plan, amount, billing_cycle, started_at, expires_at, status, created_at "
        "FROM saas_subscriptions ORDER BY id"
    )
    while True:
        rows = pg_cur.fetchmany(chunk_size)
        if not rows:
            break
        buf = io.StringIO()
        for r in rows:
            id_, uid, plan, amount, billing, started, expires, status, ca = r
            buf.write(f"{id_}\t{uid or 0}\t{plan or ''}\t{amount or 0}\t{billing or ''}\t"
                      f"{started.strftime('%Y-%m-%d %H:%M:%S') if started else '1970-01-01 00:00:00'}\t"
                      f"{expires.strftime('%Y-%m-%d %H:%M:%S') if expires else '1970-01-01 00:00:00'}\t"
                      f"{status or ''}\t"
                      f"{ca.strftime('%Y-%m-%d %H:%M:%S') if ca else '1970-01-01 00:00:00'}\n")
        ch_insert_tsv("raw.saas_subscriptions", buf.getvalue())
    print("  saas_subscriptions: done")

    pg_cur.close()
    pg.close()
    print("SaaS extract to ClickHouse raw.* complete.")


def run_dbt_source_freshness(**ctx):
    """Run dbt source freshness checks — warns/errors if sources are stale."""
    try:
        stdout, target_dir = _run_dbt(["source", "freshness"], "dbt_freshness")
        _cleanup_dbt_tmp(target_dir)
        print("Source freshness checks passed.")
    except RuntimeError as exc:
        # Freshness failures are warnings, not pipeline blockers, so we log and continue.
        print(f"[WARN] Source freshness check raised an issue: {exc}")
        _send_slack(f":warning: *Source Freshness Warning* — {str(exc)[:300]}")


def run_dbt_silver(**ctx):
    """Run dbt silver (staging) layer models — ShopFlow silver + SaaS staging."""
    stdout, target_dir = _run_dbt(["run", "--select", "silver staging"], "dbt_silver")
    counts = _parse_run_results(target_dir)
    _cleanup_dbt_tmp(target_dir)
    summary = (
        f"silver: {counts.get('pass', 0)} ok, {counts.get('error', 0)} err"
        if counts else "silver: done"
    )
    ctx["ti"].xcom_push(key="dbt_silver_result", value=summary)
    print(f"Silver result: {summary}")


def run_dbt_gold(**ctx):
    """Run dbt gold layer models — ShopFlow gold + SaaS marts."""
    stdout, target_dir = _run_dbt(["run", "--select", "gold marts"], "dbt_gold")
    counts = _parse_run_results(target_dir)
    _cleanup_dbt_tmp(target_dir)
    summary = (
        f"gold: {counts.get('pass', 0)} ok, {counts.get('error', 0)} err"
        if counts else "gold: done"
    )
    ctx["ti"].xcom_push(key="dbt_gold_result", value=summary)
    print(f"Gold result: {summary}")


def run_dbt_tests(**ctx):
    """Run dbt data quality tests on silver and gold layers."""
    stdout, target_dir = _run_dbt(["test", "--select", "silver gold"], "dbt_test")

    # Parse run_results.json — more reliable than string-searching stdout
    counts = _parse_run_results(target_dir)
    _cleanup_dbt_tmp(target_dir)

    if counts:
        passed = counts.get("pass", 0)
        failed = counts.get("error", 0)
        warned = counts.get("warn", 0)
    else:
        # Fallback: count "PASS" / "FAIL" lines in stdout if JSON unavailable
        passed = sum(1 for ln in stdout.splitlines() if " PASS " in ln)
        failed = sum(1 for ln in stdout.splitlines() if " FAIL " in ln or " ERROR " in ln)
        warned = 0

    ctx["ti"].xcom_push(key="dbt_tests_passed", value=passed)
    ctx["ti"].xcom_push(key="dbt_tests_failed", value=failed)
    ctx["ti"].xcom_push(key="dbt_tests_warned", value=warned)

    print(f"dbt tests: {passed} passed, {warned} warnings, {failed} failed")
    if failed > 0:
        raise RuntimeError(f"dbt tests failed: {failed} error(s). Check Airflow logs for details.")


# ── Task 7b: Observability check (data diff vs previous run) ─────────────────

def observability_check(**ctx):
    """Compare gold layer row counts against the previous run.
    Writes a structured JSON diff to ClickHouse pipeline_metadata.observability_runs.
    Fails the task if any table's row count drops by more than 25% (anomaly guard).
    """
    import json
    import requests

    run_date = ctx["ds"]
    GOLD_TABLES = [
        "dim_customers", "dim_products", "dim_vendors",
        "fct_orders", "fct_procurement", "fct_reviews",
    ]

    def _ch(sql: str) -> str:
        resp = requests.post(CH_URL, data=sql, auth=CH_AUTH, timeout=30)
        resp.raise_for_status()
        return resp.text.strip()

    # Ensure metadata table exists
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
    _ch("CREATE DATABASE IF NOT EXISTS pipeline_metadata")

    anomalies = []
    rows_to_insert = []

    for tbl in GOLD_TABLES:
        try:
            current = int(_ch(f"SELECT count() FROM gold.{tbl}"))
        except Exception:
            current = 0

        # Fetch previous run count from metadata table
        prev_result = _ch(
            f"SELECT row_count FROM pipeline_metadata.observability_runs "
            f"WHERE pipeline = 'shopflow' AND table_name = '{tbl}' "
            f"AND run_date < toDate('{run_date}') "
            f"ORDER BY run_date DESC LIMIT 1"
        )
        prev = int(prev_result) if prev_result else current  # first run: no delta

        delta_pct = round((current - prev) / max(prev, 1) * 100, 2)
        status = "ok"
        if prev > 0 and delta_pct < -25:
            status = "anomaly"
            anomalies.append(f"gold.{tbl}: {prev:,} → {current:,} ({delta_pct:+.1f}%)")

        rows_to_insert.append(
            f"'{run_date}'\t'shopflow'\t'{tbl}'\t{current}\t{prev}\t{delta_pct}\t'{status}'"
        )
        print(f"  {tbl}: {prev:,} → {current:,} ({delta_pct:+.1f}%) [{status}]")

    # Bulk insert
    if rows_to_insert:
        tsv = "\n".join(rows_to_insert)
        _ch(
            f"INSERT INTO pipeline_metadata.observability_runs "
            f"(run_date, pipeline, table_name, row_count, prev_count, delta_pct, status) "
            f"FORMAT TabSeparated\n{tsv}"
        )

    if anomalies:
        raise RuntimeError(
            f"Observability check FAILED — {len(anomalies)} anomalies detected:\n"
            + "\n".join(anomalies)
        )

    print(f"Observability check passed — {len(GOLD_TABLES)} tables checked, all within bounds.")


# ── Task 8: Data quality check ────────────────────────────────────────────────

def data_quality_check(**ctx):
    """Row-count and freshness checks across all Gold tables."""
    import requests

    def ch(sql: str) -> int:
        r = requests.post(CH_URL, data=sql, auth=CH_AUTH, timeout=30)
        r.raise_for_status()
        return int(r.text.strip())

    checks = [
        ("gold.dim_customers",  "SELECT count() FROM gold.dim_customers",       100),
        ("gold.dim_products",   "SELECT count() FROM gold.dim_products",          10),
        ("gold.dim_vendors",    "SELECT count() FROM gold.dim_vendors",            1),
        ("gold.fct_orders",     "SELECT count() FROM gold.fct_orders",           100),
        ("gold.fct_procurement","SELECT count() FROM gold.fct_procurement",        1),
        ("gold.fct_reviews",    "SELECT count() FROM gold.fct_reviews",            1),
        # No nulls on FK columns
        ("fct_orders.customer_key nulls",
         "SELECT count() FROM gold.fct_orders WHERE customer_key IS NULL OR customer_key = ''", 0),
        ("fct_orders.product_key nulls",
         "SELECT count() FROM gold.fct_orders WHERE product_key IS NULL OR product_key = ''",   0),
        # Completed orders have positive revenue
        ("fct_orders.revenue consistency",
         "SELECT count() FROM gold.fct_orders WHERE status = 'completed' AND revenue <= 0",     0),
        # All dim_customers are current
        ("dim_customers.is_current",
         "SELECT count() FROM gold.dim_customers WHERE is_current != 1",                        0),
    ]

    failed = []
    for name, sql, min_expected in checks:
        val = ch(sql)
        op  = ">=" if min_expected > 0 else "=="
        ok  = val >= min_expected if min_expected > 0 else val == min_expected
        icon = "OK " if ok else "FAIL"
        print(f"  [{icon}] {name}: {val} (expected {op} {min_expected})")
        if not ok:
            failed.append(f"{name}: got {val}, expected {op} {min_expected}")

    if failed:
        raise AssertionError("Data quality checks failed:\n" + "\n".join(failed))

    print(f"\nAll {len(checks)} quality checks passed.")
    ctx["ti"].xcom_push(key="quality_check_count", value=len(checks))


# ── Task 9: Notify success ────────────────────────────────────────────────────

def notify_success(**ctx):
    """Log pipeline completion summary and send Slack success notification."""
    ti     = ctx["ti"]
    silver = ti.xcom_pull(task_ids="run_dbt_silver", key="dbt_silver_result") or "n/a"
    gold   = ti.xcom_pull(task_ids="run_dbt_gold",   key="dbt_gold_result")   or "n/a"
    passed = ti.xcom_pull(task_ids="run_dbt_tests",  key="dbt_tests_passed")  or 0
    checks = ti.xcom_pull(task_ids="data_quality_check", key="quality_check_count") or 0
    mysql_job = ti.xcom_pull(task_ids="trigger_airbyte_mysql_sync", key="mysql_job_id") or "n/a"
    run_date  = ctx["ds"]

    summary = (
        f":large_green_circle: *ShopFlow Pipeline SUCCESS* — {run_date}\n"
        f"Airbyte MySQL job: `{mysql_job}` | {silver} | {gold} | "
        f"dbt tests: {passed} passed | quality checks: {checks} passed"
    )
    print("=" * 60)
    print("  ShopFlow Data Lake Pipeline — SUCCESS")
    print("=" * 60)
    print(f"  Run date          : {run_date}")
    print(f"  Airbyte MySQL job : {mysql_job}")
    print(f"  dbt silver        : {silver}")
    print(f"  dbt gold          : {gold}")
    print(f"  dbt tests passed  : {passed}")
    print(f"  Quality checks    : {checks} passed")
    print("=" * 60)
    _send_slack(summary)


# ── Task 10: Refresh Superset datasets ────────────────────────────────────────

def refresh_superset_dashboard(**ctx):
    """Refresh all 6 Superset datasets so charts reflect latest Gold data."""
    import requests

    session = requests.Session()

    login_resp = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": SUPERSET_USER, "password": SUPERSET_PASS, "provider": "db"},
        timeout=30,
    )
    login_resp.raise_for_status()
    token = login_resp.json()["access_token"]

    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Referer": SUPERSET_URL,
    })

    csrf_resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", timeout=30)
    csrf_resp.raise_for_status()
    session.headers["X-CSRFToken"] = csrf_resp.json()["result"]

    refreshed, failed = [], []
    for ds_id in SUPERSET_DATASET_IDS:
        resp = session.put(f"{SUPERSET_URL}/api/v1/dataset/{ds_id}/refresh", timeout=30)
        if resp.status_code == 200:
            refreshed.append(ds_id)
            print(f"  [OK] Dataset {ds_id} refreshed")
        else:
            failed.append(ds_id)
            print(f"  [WARN] Dataset {ds_id} refresh returned {resp.status_code}: {resp.text[:200]}")

    print(f"\nSuperset refresh: {len(refreshed)} OK, {len(failed)} warnings")
    ctx["ti"].xcom_push(key="superset_refreshed", value=len(refreshed))


# ── DAG definition ────────────────────────────────────────────────────────────

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
        # Non-blocking: freshness warns don't stop the pipeline
        trigger_rule="all_success",
    )

    # SaaS extract runs in parallel with Airbyte ingestion (independent path)
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
        # Wait for both Airbyte (MinIO) and SaaS extract to complete
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
    # Shared path: dbt silver → gold → observability diff → tests → quality → notify → superset
    t5 >> t6 >> t_obs >> t7 >> t8 >> t9 >> t10
