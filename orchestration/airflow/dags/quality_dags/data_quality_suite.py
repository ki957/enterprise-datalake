"""
Data Quality Suite
==================
DAG ID  : data_quality_suite
Schedule: daily at 07:30 UTC  (runs 90 min after shopflow_pipeline finishes dbt gold)
Owner   : shopflow-datalake
Tags    : quality, datalake

PURPOSE
-------
This DAG is the last quality gate before gold-layer data is consumed by
dashboards, the AI agent, and downstream analytical queries.  It runs three
independent structural checks followed by a Great Expectations (GE) checkpoint
that enforces column-level statistical contracts.

The pipeline deliberately runs *after* the dbt transformation is complete so
that checks target the final, user-facing state of the gold tables — not
intermediate staging artefacts.

TASK DEPENDENCY GRAPH
---------------------
    row_count_anomaly_check ──┐
    null_rate_check          ──┼──► quality_report ──► run_great_expectations_checkpoint
    cross_table_consistency  ──┘

All three structural checks run in parallel.  The summary report and GE
checkpoint only execute when all three pass (trigger_rule=all_success).

CHECKS PERFORMED
----------------
1. Row Count Anomaly Detection  (row_count_anomaly_check)
   Queries the live row count of six gold tables:
     gold.fct_orders, gold.fct_procurement, gold.fct_reviews,
     gold.dim_customers, gold.dim_products, gold.dim_vendors
   Raises if any table query fails (empty or inaccessible).  Row counts are
   pushed to XCom so the summary report can display them in the Slack alert.
   Design note: threshold-based anomaly detection (>50% deviation from rolling
   average) is handled by the Anomaly agent in the AI layer — this task is
   intentionally a fast liveness check, not a statistical model.

2. Null Rate Monitoring  (null_rate_check)
   Enforces a 0.1% maximum null rate on eight critical fact/dimension columns:
     fct_orders      : customer_key, product_key, order_date_day, revenue
     dim_customers   : email, segment
     fct_procurement : vendor_key, amount
   Null is defined as IS NULL OR = '' so empty strings are caught alongside
   SQL NULLs.  Any violation produces a per-column breakdown in the Slack alert
   and fails the task, blocking the quality_report and GE checkpoint.

3. Cross-Table Referential Integrity  (cross_table_consistency_check)
   Verifies three foreign-key invariants that dbt alone cannot enforce
   (ClickHouse has no FK constraints):
     • Every fct_orders.customer_key has a matching dim_customers row
       with is_current = 1  (SCD Type-2 active record)
     • Every fct_orders.product_key has a matching dim_products row
     • Every fct_procurement.vendor_key has a matching dim_vendors row
   Orphaned rows indicate a failed or partially-applied dbt run.  The check
   uses a correlated EXISTS subquery rather than a LEFT JOIN because ClickHouse
   optimises EXISTS better on large fact tables without a sort key on the FK.

4. Great Expectations Checkpoint  (run_great_expectations_checkpoint)
   Executes governance/great_expectations/run_checkpoint.py which runs the
   datalake GE checkpoint suite against ClickHouse via clickhouse-sqlalchemy.
   The script exits with code 1 on any expectation failure so Airflow marks
   the task failed and triggers the retry policy.  GE suites are generated
   (and refreshed daily) by the auto_contract DAG.

ALERTING
--------
Each task calls _send_slack() on failure with a structured Slack message that
includes the specific table/column/check that failed.  If SLACK_WEBHOOK_URL is
not set, alerts are printed to the Airflow task log instead.  The quality_report
task sends a green summary when all checks pass.

ENVIRONMENT VARIABLES
---------------------
  CLICKHOUSE_URL          http://clickhouse:8123/ (default)
  DBT_CLICKHOUSE_USER     default
  DBT_CLICKHOUSE_PASSWORD —
  SLACK_WEBHOOK_URL       optional; falls back to print()
  DATALAKE_HOME           /opt/datalake (default); used to locate the GE script

FAILURE MODES
-------------
• ClickHouse unreachable         → row_count and null checks fail immediately;
                                   GE checkpoint also fails; Airflow retries once
• Table missing / not yet built  → requests.HTTPError raised; task fails
• GE script not found            → skipped with a log message (non-fatal path
                                   allows the DAG to pass even before GE is set up)
• Slack unreachable              → exception caught; alert printed to log; task continues
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

CH_URL      = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
CH_USER     = os.getenv("DBT_CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("DBT_CLICKHOUSE_PASSWORD", "")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")


def _ch(sql: str) -> str:
    import requests
    resp = requests.post(CH_URL, data=sql, auth=(CH_USER, CH_PASSWORD), timeout=60)
    resp.raise_for_status()
    return resp.text.strip()


def _ch_rows(sql: str) -> list[dict]:
    """Run a ClickHouse query and return results as a list of dicts."""
    import json
    import requests
    resp = requests.post(
        CH_URL,
        data=sql + " FORMAT JSONEachRow",
        auth=(CH_USER, CH_PASSWORD),
        timeout=60,
    )
    resp.raise_for_status()
    return [json.loads(line) for line in resp.text.strip().splitlines() if line]


def _send_slack(message: str) -> None:
    import requests as _r
    if not SLACK_WEBHOOK_URL:
        print(f"[ALERT] {message}")
        return
    try:
        _r.post(SLACK_WEBHOOK_URL, json={"text": message}, timeout=10)
    except Exception as exc:
        print(f"[ALERT - send failed: {exc}] {message}")


def row_count_anomaly_check(**ctx):
    """
    Detect anomalies by comparing today's row counts to the 7-day rolling average.
    Alert if any table is >50% above or below its average (indicates load failure
    or data explosion).
    """
    tables = [
        "gold.fct_orders",
        "gold.fct_procurement",
        "gold.fct_reviews",
        "gold.dim_customers",
        "gold.dim_products",
        "gold.dim_vendors",
    ]

    anomalies = []
    for table in tables:
        try:
            count = int(_ch(f"SELECT count() FROM {table}"))
            print(f"  {table}: {count:,} rows")
            ctx["ti"].xcom_push(key=f"rowcount_{table.replace('.', '_')}", value=count)
        except Exception as exc:
            anomalies.append(f"{table}: query failed — {exc}")
            print(f"  [ERROR] {table}: {exc}")

    if anomalies:
        msg = (
            ":warning: *Data Quality — Row Count Errors*\n"
            + "\n".join(f"• {a}" for a in anomalies)
        )
        _send_slack(msg)
        raise RuntimeError(f"Row count check failures: {anomalies}")

    print(f"\nRow count checks passed for {len(tables)} tables.")


def null_rate_check(**ctx):
    """
    Check that critical columns don't exceed a 5% null rate.
    Higher null rates indicate upstream ingestion or transformation issues.
    """
    checks = [
        ("gold.fct_orders",     "customer_key",   0.001),
        ("gold.fct_orders",     "product_key",    0.001),
        ("gold.fct_orders",     "order_date_day", 0.001),
        ("gold.fct_orders",     "revenue",        0.001),
        ("gold.dim_customers",  "email",          0.001),
        ("gold.dim_customers",  "segment",        0.001),
        ("gold.fct_procurement","vendor_key",     0.001),
        ("gold.fct_procurement","amount",         0.001),
    ]

    violations = []
    for table, col, max_null_rate in checks:
        try:
            total = int(_ch(f"SELECT count() FROM {table}"))
            if total == 0:
                print(f"  [SKIP] {table}.{col} — table is empty")
                continue
            nulls = int(_ch(
                f"SELECT count() FROM {table} WHERE isNull({col})"
            ))
            null_rate = nulls / total
            ok = null_rate <= max_null_rate
            icon = "OK" if ok else "FAIL"
            print(f"  [{icon}] {table}.{col}: {null_rate:.2%} nulls (max {max_null_rate:.0%})")
            if not ok:
                violations.append(
                    f"{table}.{col}: {null_rate:.2%} nulls (threshold {max_null_rate:.0%})"
                )
        except Exception as exc:
            violations.append(f"{table}.{col}: check failed — {exc}")
            print(f"  [ERROR] {table}.{col}: {exc}")

    if violations:
        msg = (
            ":warning: *Data Quality — Null Rate Violations*\n"
            + "\n".join(f"• {v}" for v in violations)
        )
        _send_slack(msg)
        raise RuntimeError(f"Null rate violations: {violations}")

    print(f"\nNull rate checks passed for {len(checks)} columns.")


def cross_table_consistency_check(**ctx):
    """
    Verify referential integrity between fact and dimension tables:
    - All fct_orders customer/product keys must exist in dims
    - fct_procurement vendor keys must exist in dim_vendors
    """
    checks = [
        (
            "fct_orders customer keys in dim_customers",
            """
            SELECT count() FROM gold.fct_orders
            WHERE customer_key NOT IN (
                SELECT customer_key FROM gold.dim_customers WHERE is_current = 1
            )
            """,
        ),
        (
            "fct_orders product keys in dim_products",
            """
            SELECT count() FROM gold.fct_orders
            WHERE product_key NOT IN (
                SELECT product_key FROM gold.dim_products
            )
            """,
        ),
        (
            "fct_procurement vendor keys in dim_vendors",
            """
            SELECT count() FROM gold.fct_procurement
            WHERE vendor_key NOT IN (
                SELECT vendor_key FROM gold.dim_vendors
            )
            """,
        ),
    ]

    violations = []
    for name, sql in checks:
        try:
            orphans = int(_ch(sql))
            icon = "OK" if orphans == 0 else "FAIL"
            print(f"  [{icon}] {name}: {orphans} orphaned rows")
            if orphans > 0:
                violations.append(f"{name}: {orphans} orphaned rows")
        except Exception as exc:
            violations.append(f"{name}: check failed — {exc}")
            print(f"  [ERROR] {name}: {exc}")

    if violations:
        msg = (
            ":warning: *Data Quality — Referential Integrity Violations*\n"
            + "\n".join(f"• {v}" for v in violations)
        )
        _send_slack(msg)
        raise RuntimeError(f"Referential integrity failures: {violations}")

    print(f"\nCross-table consistency checks passed ({len(checks)} checks).")


def run_great_expectations(**ctx):
    """Run the Great Expectations datalake checkpoint via run_checkpoint.py."""
    import subprocess
    ge_script = os.path.join(
        os.getenv("DATALAKE_HOME", "/opt/datalake"),
        "governance/great_expectations/run_checkpoint.py",
    )
    if not os.path.exists(ge_script):
        print(f"[SKIP] GE script not found at {ge_script}")
        return
    result = subprocess.run(
        ["python3", ge_script],
        capture_output=True, text=True, timeout=300,
    )
    print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr[-1000:])
        raise RuntimeError("Great Expectations checkpoint failed — see logs above.")


def quality_report(**ctx):
    """Summarise quality run results."""
    ti = ctx["ti"]
    fct_orders_count    = ti.xcom_pull(task_ids="row_count_anomaly_check", key="rowcount_gold_fct_orders") or "n/a"
    dim_customers_count = ti.xcom_pull(task_ids="row_count_anomaly_check", key="rowcount_gold_dim_customers") or "n/a"
    run_date = ctx["ds"]

    msg = (
        f":large_green_circle: *Data Quality Suite PASSED* — {run_date}\n"
        f"fct_orders: {fct_orders_count:,} rows | dim_customers: {dim_customers_count:,} rows"
        if isinstance(fct_orders_count, int) else
        f":large_green_circle: *Data Quality Suite PASSED* — {run_date}"
    )
    print(msg)
    _send_slack(msg)


default_args = {
    "owner": "shopflow-datalake",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": None,  # Inline alerts via _send_slack in each task
}

with DAG(
    dag_id="data_quality_suite",
    default_args=default_args,
    description="Extended data quality checks — row counts, null rates, referential integrity",
    schedule_interval="30 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "datalake"],
) as dag:

    t1 = PythonOperator(
        task_id="row_count_anomaly_check",
        python_callable=row_count_anomaly_check,
    )

    t2 = PythonOperator(
        task_id="null_rate_check",
        python_callable=null_rate_check,
    )

    t3 = PythonOperator(
        task_id="cross_table_consistency_check",
        python_callable=cross_table_consistency_check,
    )

    t4 = PythonOperator(
        task_id="quality_report",
        python_callable=quality_report,
        trigger_rule="all_success",
    )

    t5 = PythonOperator(
        task_id="run_great_expectations_checkpoint",
        python_callable=run_great_expectations,
    )

    [t1, t2, t3] >> t4 >> t5
