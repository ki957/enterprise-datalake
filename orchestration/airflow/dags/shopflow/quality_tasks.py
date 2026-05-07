"""Observability check, data quality check, and success notification tasks."""

import os

from common.alerting import send_slack

CH_URL      = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
CH_USER     = os.getenv("DBT_CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("DBT_CLICKHOUSE_PASSWORD", "")
CH_AUTH     = (CH_USER, CH_PASSWORD)

_GOLD_TABLES = [
    "dim_customers", "dim_products", "dim_vendors",
    "fct_orders", "fct_procurement", "fct_reviews",
]

_QUALITY_CHECKS = [
    ("gold.dim_customers",   "SELECT count() FROM gold.dim_customers",       100),
    ("gold.dim_products",    "SELECT count() FROM gold.dim_products",          10),
    ("gold.dim_vendors",     "SELECT count() FROM gold.dim_vendors",            1),
    ("gold.fct_orders",      "SELECT count() FROM gold.fct_orders",           100),
    ("gold.fct_procurement", "SELECT count() FROM gold.fct_procurement",        1),
    ("gold.fct_reviews",     "SELECT count() FROM gold.fct_reviews",            1),
    ("fct_orders.customer_key nulls",
     "SELECT count() FROM gold.fct_orders WHERE customer_key IS NULL OR customer_key = ''", 0),
    ("fct_orders.product_key nulls",
     "SELECT count() FROM gold.fct_orders WHERE product_key IS NULL OR product_key = ''",   0),
    ("fct_orders.revenue consistency",
     "SELECT count() FROM gold.fct_orders WHERE status = 'completed' AND revenue <= 0",     0),
    ("dim_customers.is_current",
     "SELECT count() FROM gold.dim_customers WHERE is_current != 1",                        0),
]


def observability_check(**ctx):
    """Compare gold layer row counts against the previous run.

    Writes a structured diff to ClickHouse pipeline_metadata.observability_runs.
    Fails if any table's row count drops by more than 25%.
    """
    import requests

    run_date = ctx["ds"]

    def _ch(sql: str) -> str:
        resp = requests.post(CH_URL, data=sql, auth=CH_AUTH, timeout=30)
        resp.raise_for_status()
        return resp.text.strip()

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

    anomalies      = []
    rows_to_insert = []

    for tbl in _GOLD_TABLES:
        try:
            current = int(_ch(f"SELECT count() FROM gold.{tbl}"))
        except Exception:
            current = 0

        prev_result = _ch(
            f"SELECT row_count FROM pipeline_metadata.observability_runs "
            f"WHERE pipeline = 'shopflow' AND table_name = '{tbl}' "
            f"AND run_date < toDate('{run_date}') "
            f"ORDER BY run_date DESC LIMIT 1"
        )
        prev      = int(prev_result) if prev_result else current
        delta_pct = round((current - prev) / max(prev, 1) * 100, 2)
        status    = "ok"

        if prev > 0 and delta_pct < -25:
            status = "anomaly"
            anomalies.append(f"gold.{tbl}: {prev:,} → {current:,} ({delta_pct:+.1f}%)")

        rows_to_insert.append(
            f"{run_date}\tshopflow\t{tbl}\t{current}\t{prev}\t{delta_pct}\t{status}"
        )
        print(f"  {tbl}: {prev:,} → {current:,} ({delta_pct:+.1f}%) [{status}]")

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

    print(f"Observability check passed — {len(_GOLD_TABLES)} tables checked, all within bounds.")


def data_quality_check(**ctx):
    """Row-count and freshness checks across all Gold tables."""
    import requests

    def ch(sql: str) -> int:
        r = requests.post(CH_URL, data=sql, auth=CH_AUTH, timeout=30)
        r.raise_for_status()
        return int(r.text.strip())

    failed = []
    for name, sql, min_expected in _QUALITY_CHECKS:
        val = ch(sql)
        op  = ">=" if min_expected > 0 else "=="
        ok  = val >= min_expected if min_expected > 0 else val == min_expected
        print(f"  [{'OK ' if ok else 'FAIL'}] {name}: {val} (expected {op} {min_expected})")
        if not ok:
            failed.append(f"{name}: got {val}, expected {op} {min_expected}")

    if failed:
        raise AssertionError("Data quality checks failed:\n" + "\n".join(failed))

    print(f"\nAll {len(_QUALITY_CHECKS)} quality checks passed.")
    ctx["ti"].xcom_push(key="quality_check_count", value=len(_QUALITY_CHECKS))


def notify_success(**ctx):
    """Log pipeline completion summary and send Slack success notification."""
    ti        = ctx["ti"]
    silver    = ti.xcom_pull(task_ids="run_dbt_silver",     key="dbt_silver_result") or "n/a"
    gold      = ti.xcom_pull(task_ids="run_dbt_gold",       key="dbt_gold_result")   or "n/a"
    passed    = ti.xcom_pull(task_ids="run_dbt_tests",      key="dbt_tests_passed")  or 0
    checks    = ti.xcom_pull(task_ids="data_quality_check", key="quality_check_count") or 0
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
    send_slack(summary)
