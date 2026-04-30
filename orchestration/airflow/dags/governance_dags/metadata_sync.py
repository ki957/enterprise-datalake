"""
Metadata Sync & Governance DAG
================================
DAG: metadata_sync
Schedule: daily at 08:00 UTC (after main pipeline + quality suite complete)

Governance tasks:
1. Publish dbt docs to the nginx container (always fresh after each pipeline run)
2. Log pipeline run metadata to a ClickHouse audit table
3. Check for schema drift — compare current dbt manifest column list against
   the live ClickHouse schema and alert on unexpected additions/removals
"""

import json
import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DATALAKE_HOME  = os.getenv("DATALAKE_HOME", "/opt/datalake")
DBT_PROJECT    = f"{DATALAKE_HOME}/transformation/dbt/datalake_transforms"
DBT_PROFILES   = "/opt/airflow/dbt_profiles.yml"
DBT_BIN        = "/home/airflow/.local/bin/dbt"

CH_URL         = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
CH_USER        = os.getenv("DBT_CLICKHOUSE_USER", "default")
CH_PASSWORD    = os.getenv("DBT_CLICKHOUSE_PASSWORD", "")
SLACK_WEBHOOK  = os.getenv("SLACK_WEBHOOK_URL", "")


def _ch(sql: str) -> str:
    import requests
    resp = requests.post(CH_URL, data=sql, auth=(CH_USER, CH_PASSWORD), timeout=30)
    resp.raise_for_status()
    return resp.text.strip()


def _send_slack(message: str) -> None:
    import requests as _r
    if not SLACK_WEBHOOK:
        print(f"[ALERT] {message}")
        return
    try:
        _r.post(SLACK_WEBHOOK, json={"text": message}, timeout=10)
    except Exception as exc:
        print(f"[Slack send failed: {exc}] {message}")


def publish_dbt_docs(**ctx):
    """
    Run dbt docs generate and copy artifacts to the nginx-served target/ directory.
    The dbt-docs container at :8082 picks up the new files automatically.
    """
    import shutil, tempfile

    tmp_dir = tempfile.mkdtemp(prefix="dbt_docs_")
    project_copy = os.path.join(tmp_dir, "project")
    try:
        shutil.copytree(DBT_PROJECT, project_copy, dirs_exist_ok=True)
        dbt_exe = next(
            (d for d in [DBT_BIN, "/usr/local/bin/dbt", "/home/airflow/.local/bin/dbt"] if os.path.exists(d)),
            "dbt",
        )
        profiles_dir = (
            os.path.dirname(DBT_PROFILES)
            if os.path.exists(DBT_PROFILES)
            else os.path.expanduser("~/.dbt")
        )
        cmd = [dbt_exe, "docs", "generate",
               "--project-dir", project_copy,
               "--profiles-dir", profiles_dir,
               "--no-version-check"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            print("STDERR:", result.stderr[-1000:])
            raise RuntimeError("dbt docs generate failed")

        # Copy artifacts to the nginx-served target/
        nginx_dir = f"{DBT_PROJECT}/target"
        os.makedirs(nginx_dir, exist_ok=True)
        for fname in ["manifest.json", "catalog.json", "index.html", "graph.gpickle", "run_results.json"]:
            src = os.path.join(project_copy, "target", fname)
            if os.path.exists(src):
                shutil.copy2(src, os.path.join(nginx_dir, fname))
                print(f"  Published {fname}")

        print(f"dbt docs published to {nginx_dir}. Browse at http://localhost:8082")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def log_pipeline_metadata(**ctx):
    """
    Write pipeline run metadata to ClickHouse governance.pipeline_runs table.
    Creates the table if it doesn't exist.
    """
    run_date = ctx["ds"]
    run_id   = ctx["run_id"]
    dag_id   = ctx["dag"].dag_id

    # Ensure the governance schema and table exist
    _ch("CREATE DATABASE IF NOT EXISTS governance")
    _ch("""
        CREATE TABLE IF NOT EXISTS governance.pipeline_runs (
            run_date      Date,
            run_id        String,
            dag_id        String,
            recorded_at   DateTime DEFAULT now(),
            fct_orders_rows    UInt64,
            dim_customers_rows UInt64,
            gold_tables_rows   UInt64
        ) ENGINE = MergeTree()
        ORDER BY (run_date, dag_id)
    """)

    # Collect current row counts
    def safe_count(table: str) -> int:
        try:
            return int(_ch(f"SELECT count() FROM {table}"))
        except Exception:
            return 0

    fct_orders    = safe_count("gold.fct_orders")
    dim_customers = safe_count("gold.dim_customers")

    gold_total = sum(
        safe_count(t)
        for t in [
            "gold.fct_orders", "gold.fct_procurement", "gold.fct_reviews",
            "gold.dim_customers", "gold.dim_products", "gold.dim_vendors",
        ]
    )

    safe_run_id = run_id.replace("'", "''")
    safe_dag_id = dag_id.replace("'", "''")
    safe_run_date = run_date.replace("'", "''")
    _ch(f"""
        INSERT INTO governance.pipeline_runs
            (run_date, run_id, dag_id, fct_orders_rows, dim_customers_rows, gold_tables_rows)
        VALUES ('{safe_run_date}', '{safe_run_id}', '{safe_dag_id}', {fct_orders}, {dim_customers}, {gold_total})
    """)
    print(f"Logged run metadata: run_date={run_date}, fct_orders={fct_orders:,}, gold_total={gold_total:,}")


def schema_drift_check(**ctx):
    """
    Compare expected columns (from dbt manifest.json) against actual ClickHouse
    columns. Alert on any added or removed columns in gold tables.
    """
    manifest_path = f"{DBT_PROJECT}/target/manifest.json"
    if not os.path.exists(manifest_path):
        print("[SKIP] manifest.json not found — run dbt docs generate first.")
        return

    with open(manifest_path) as f:
        manifest = json.load(f)

    nodes = manifest.get("nodes", {})
    drifts = []

    for node_key, node in nodes.items():
        if node.get("resource_type") != "model":
            continue
        schema   = node.get("schema", "")
        name     = node.get("name", "")
        if schema not in ("gold", "staging"):
            continue

        expected_cols = set(node.get("columns", {}).keys())
        if not expected_cols:
            continue

        try:
            safe_schema = schema.replace("'", "''")
            safe_name   = name.replace("'", "''")
            result = _ch(
                f"SELECT name FROM system.columns "
                f"WHERE database = '{safe_schema}' AND table = '{safe_name}'"
                f" FORMAT TabSeparated"
            )
            actual_cols = set(result.splitlines()) if result else set()
        except Exception as exc:
            print(f"  [SKIP] {schema}.{name} — {exc}")
            continue

        added   = actual_cols - expected_cols
        removed = expected_cols - actual_cols

        if added or removed:
            drift_msg = f"`{schema}.{name}`"
            if added:
                drift_msg += f" added: {sorted(added)}"
            if removed:
                drift_msg += f" removed: {sorted(removed)}"
            drifts.append(drift_msg)
            print(f"  [DRIFT] {drift_msg}")
        else:
            print(f"  [OK] {schema}.{name} — {len(expected_cols)} columns match")

    if drifts:
        msg = (
            ":warning: *Schema Drift Detected*\n"
            + "\n".join(f"• {d}" for d in drifts)
            + "\nUpdate dbt models or manifest to reflect schema changes."
        )
        _send_slack(msg)
        # Schema drift is a warning, not a pipeline failure
        print(f"\nSchema drift in {len(drifts)} model(s) — Slack alert sent.")
    else:
        print("\nNo schema drift detected.")


default_args = {
    "owner": "shopflow-datalake",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="metadata_sync",
    default_args=default_args,
    description="Daily governance: publish dbt docs, log metadata, detect schema drift",
    schedule_interval="0 8 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["governance", "metadata", "dbt-docs"],
) as dag:

    t1 = PythonOperator(
        task_id="publish_dbt_docs",
        python_callable=publish_dbt_docs,
        execution_timeout=timedelta(minutes=10),
    )

    t2 = PythonOperator(
        task_id="log_pipeline_metadata",
        python_callable=log_pipeline_metadata,
    )

    t3 = PythonOperator(
        task_id="schema_drift_check",
        python_callable=schema_drift_check,
    )

    t1 >> [t2, t3]
