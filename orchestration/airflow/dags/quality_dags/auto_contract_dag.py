"""
Auto-Contract DAG
==================
DAG: auto_contract
Schedule: daily at 09:00 UTC (after data_quality_suite at 07:30)

Tasks:
1. collect_column_usage  — query system.query_log for last 24h,
                           identify hot/cold columns across gold.*,
                           write results to raw.column_usage_stats
2. generate_contracts    — for hot columns (query_count > 5),
                           profile the table and write GE expectation suites
3. flag_deprecations     — log cold columns (query_count = 0) to Slack/stdout

Integration with AI Agent:
  raw.column_usage_stats is queryable by the Performance Agent.
  GE suites are written to governance/great_expectations/expectations/
  and are queryable by the Contract Agent via list_expectation_suites.
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

CH_URL      = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
CH_USER     = os.getenv("DBT_CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("DBT_CLICKHOUSE_PASSWORD", "")

DATALAKE_HOME = os.getenv("DATALAKE_HOME", "/opt/datalake")
GE_DIR        = Path(f"{DATALAKE_HOME}/governance/great_expectations/expectations")

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

# Columns queried this many times or more → generate a contract for the table
HOT_THRESHOLD = 5


def _ch(sql: str) -> str:
    import requests
    resp = requests.post(CH_URL, data=sql, auth=(CH_USER, CH_PASSWORD), timeout=30)
    resp.raise_for_status()
    return resp.text.strip()


def _send_slack(msg: str) -> None:
    import requests as _r
    if not SLACK_WEBHOOK:
        print(f"[ALERT] {msg}")
        return
    try:
        _r.post(SLACK_WEBHOOK, json={"text": msg}, timeout=10)
    except Exception as exc:
        print(f"[Slack send failed: {exc}] {msg}")


def collect_column_usage(**ctx):
    """
    Parse system.query_log for the last 24h.
    For each known column in gold.*, count how many queries referenced it.
    Write results to raw.column_usage_stats.
    """
    # Ensure raw schema and table exist
    _ch("CREATE DATABASE IF NOT EXISTS raw")
    _ch("""
        CREATE TABLE IF NOT EXISTS raw.column_usage_stats (
            schema_name    LowCardinality(String),
            table_name     LowCardinality(String),
            column_name    String,
            query_count_24h UInt32,
            last_queried_at Nullable(DateTime),
            captured_at    Date DEFAULT today()
        ) ENGINE = MergeTree()
        ORDER BY (captured_at, schema_name, table_name, column_name)
        PARTITION BY captured_at
        TTL captured_at + INTERVAL 90 DAY
    """)

    # Fetch all gold columns from system.columns
    raw = _ch(
        "SELECT database, table, name FROM system.columns "
        "WHERE database = 'gold' FORMAT TabSeparated"
    )
    if not raw:
        print("No columns found in gold schema — skipping.")
        return

    columns_by_table: dict[str, list[str]] = {}
    for line in raw.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        db, table, col = parts
        columns_by_table.setdefault(table, []).append(col)

    # Fetch last 24h of queries from system.query_log
    query_log = _ch("""
        SELECT query
        FROM system.query_log
        WHERE event_time >= now() - INTERVAL 24 HOUR
          AND type = 'QueryFinish'
          AND query_kind = 'Select'
          AND positionCaseInsensitive(query, 'gold.') > 0
        FORMAT TabSeparated
    """)

    queries = query_log.splitlines() if query_log else []
    print(f"Found {len(queries)} SELECT queries in last 24h touching gold.*")

    # Count column references: simple case-insensitive string presence check
    # This is exact-match on known column names — reliable for known schema
    from collections import defaultdict
    usage: dict[tuple, int] = defaultdict(int)
    last_seen: dict[tuple, str] = {}

    for q in queries:
        q_lower = q.lower()
        for table, cols in columns_by_table.items():
            for col in cols:
                if col.lower() in q_lower:
                    key = ("gold", table, col)
                    usage[key] += 1
                    last_seen[key] = "now()"  # approximate — query_log has event_time

    # Build all column keys (including zero-usage ones)
    all_keys = {
        ("gold", table, col)
        for table, cols in columns_by_table.items()
        for col in cols
    }

    # Delete today's existing rows before inserting fresh data
    _ch("ALTER TABLE raw.column_usage_stats DELETE WHERE captured_at = today()")

    if not all_keys:
        print("No gold columns to track.")
        return

    # Batch insert
    rows = []
    for key in all_keys:
        schema_name, table_name, col_name = key
        count = usage.get(key, 0)
        rows.append(
            f"('{schema_name}', '{table_name}', '{col_name}', {count}, "
            f"{'now()' if count > 0 else 'NULL'}, today())"
        )

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        _ch(
            "INSERT INTO raw.column_usage_stats "
            "(schema_name, table_name, column_name, query_count_24h, last_queried_at, captured_at) VALUES "
            + ", ".join(batch)
        )

    hot  = sum(1 for k in all_keys if usage.get(k, 0) >= HOT_THRESHOLD)
    cold = sum(1 for k in all_keys if usage.get(k, 0) == 0)
    print(f"Column usage recorded: {len(all_keys)} columns | hot={hot} | cold={cold}")


def generate_contracts(**ctx):
    """
    For each gold table with at least one hot column (query_count >= HOT_THRESHOLD),
    profile the table and generate a Great Expectations suite.
    Writes JSON to governance/great_expectations/expectations/{schema}_{table}.json
    """
    GE_DIR.mkdir(parents=True, exist_ok=True)

    # Find tables with hot columns today
    raw = _ch(f"""
        SELECT DISTINCT table_name
        FROM raw.column_usage_stats
        WHERE schema_name = 'gold'
          AND query_count_24h >= {HOT_THRESHOLD}
          AND captured_at = today()
        FORMAT TabSeparated
    """)

    if not raw:
        print("No hot tables today — skipping contract generation.")
        return

    hot_tables = [t.strip() for t in raw.splitlines() if t.strip()]
    print(f"Generating contracts for {len(hot_tables)} hot table(s): {hot_tables}")

    for table in hot_tables:
        try:
            _generate_table_contract("gold", table)
        except Exception as exc:
            print(f"  [ERROR] {table}: {exc}")


def _generate_table_contract(schema: str, table: str) -> None:
    """Profile a table and write a rule-based GE expectation suite."""
    import requests

    # Get columns for this table
    cols_raw = _ch(
        f"SELECT name, type FROM system.columns "
        f"WHERE database = '{schema}' AND table = '{table}' "
        f"FORMAT TabSeparated"
    )
    if not cols_raw:
        return

    columns = []
    for line in cols_raw.splitlines():
        parts = line.split("\t")
        if len(parts) == 2:
            columns.append({"name": parts[0], "type": parts[1]})

    # Get row count
    row_count = int(_ch(f"SELECT count() FROM {schema}.{table}"))

    expectations = []

    # Table-level: row count sanity (within 10% of current)
    lo = max(0, int(row_count * 0.9))
    hi = int(row_count * 1.1) + 100
    expectations.append({
        "expectation_type": "expect_table_row_count_to_be_between",
        "kwargs": {"min_value": lo, "max_value": hi},
        "meta": {"auto_generated": True, "basis": f"current count {row_count}"},
    })

    for col in columns:
        col_name = col["name"]
        col_type = col["type"]

        # All hot columns must exist
        expectations.append({
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": col_name},
            "meta": {"auto_generated": True},
        })

        # Numeric columns: not-null + range check
        is_numeric = any(t in col_type for t in ["Int", "UInt", "Float", "Decimal"])
        is_nullable = "Nullable" in col_type

        if not is_nullable and col_name not in ("_sign", "_version"):
            expectations.append({
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col_name},
                "meta": {"auto_generated": True},
            })

        if is_numeric and not is_nullable:
            try:
                stats = _ch(
                    f"SELECT min({col_name}), max({col_name}) "
                    f"FROM {schema}.{table} FORMAT TabSeparated"
                )
                parts = stats.split("\t")
                if len(parts) == 2:
                    min_v = float(parts[0]) if parts[0] not in ("", "nan") else None
                    max_v = float(parts[1]) if parts[1] not in ("", "nan") else None
                    if min_v is not None and max_v is not None:
                        expectations.append({
                            "expectation_type": "expect_column_values_to_be_between",
                            "kwargs": {
                                "column":    col_name,
                                "min_value": min_v,
                                "max_value": max_v,
                            },
                            "meta": {"auto_generated": True},
                        })
            except Exception:
                pass

        # ID / key columns: uniqueness
        if col_name.endswith("_id") or col_name.endswith("_key"):
            expectations.append({
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": col_name},
                "meta": {"auto_generated": True},
            })

    # Write suite
    suite_name = f"{schema}_{table}"
    out_path   = GE_DIR / f"{suite_name}.json"

    if out_path.exists():
        import shutil
        shutil.copy2(out_path, out_path.with_suffix(".json.bak"))

    suite = {
        "expectation_suite_name": suite_name,
        "ge_cloud_id": None,
        "expectations": expectations,
        "data_asset_type": None,
        "meta": {
            "great_expectations_version": "0.18.0",
            "generated_by": "auto_contract_dag",
            "generated_at": datetime.utcnow().isoformat(),
            "row_count_at_generation": row_count,
        },
    }
    out_path.write_text(json.dumps(suite, indent=2))
    print(f"  [{schema}.{table}] {len(expectations)} expectations → {out_path.name}")


def flag_deprecations(**ctx):
    """
    Report columns with zero queries in last 24h as deprecation candidates.
    Only reports — never auto-removes anything.
    """
    raw = _ch("""
        SELECT schema_name, table_name, column_name
        FROM raw.column_usage_stats
        WHERE query_count_24h = 0
          AND schema_name = 'gold'
          AND captured_at = today()
        ORDER BY table_name, column_name
        FORMAT TabSeparated
    """)

    if not raw:
        print("No zero-usage columns today.")
        return

    lines = raw.splitlines()
    print(f"{len(lines)} zero-usage column(s) in gold.* today:")
    by_table: dict[str, list[str]] = {}
    for line in lines:
        parts = line.split("\t")
        if len(parts) == 3:
            by_table.setdefault(f"{parts[0]}.{parts[1]}", []).append(parts[2])

    report_lines = []
    for table, cols in sorted(by_table.items()):
        print(f"  {table}: {', '.join(cols)}")
        report_lines.append(f"• `{table}`: {', '.join(cols[:10])}" + (" …" if len(cols) > 10 else ""))

    if len(lines) > 20:
        _send_slack(
            f":mag: *Deprecation Candidates — {datetime.utcnow().strftime('%Y-%m-%d')}*\n"
            f"{len(lines)} zero-usage columns in gold.* over last 24h.\n"
            "Query `raw.column_usage_stats` for details.\n"
            + "\n".join(report_lines[:10])
        )


default_args = {
    "owner": "shopflow-datalake",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="auto_contract",
    default_args=default_args,
    description="Daily: column usage tracking + auto GE contract generation + deprecation flags",
    schedule_interval="0 9 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["governance", "contracts", "quality"],
) as dag:

    t1 = PythonOperator(
        task_id="collect_column_usage",
        python_callable=collect_column_usage,
        execution_timeout=timedelta(minutes=10),
    )

    t2 = PythonOperator(
        task_id="generate_contracts",
        python_callable=generate_contracts,
        execution_timeout=timedelta(minutes=15),
    )

    t3 = PythonOperator(
        task_id="flag_deprecations",
        python_callable=flag_deprecations,
    )

    t1 >> t2 >> t3
