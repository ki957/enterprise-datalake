"""ClickHouse and dbt helper functions shared across DAGs."""

import json
import os
from datetime import datetime, timedelta
from decimal import Decimal

import requests

# ── Configuration ──────────────────────────────────────────────────────────────

CH_URL      = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
CH_USER     = os.getenv("DBT_CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("DBT_CLICKHOUSE_PASSWORD", "")
CH_AUTH     = (CH_USER, CH_PASSWORD)

DATALAKE_HOME = os.getenv("DATALAKE_HOME", "/opt/datalake")
DBT_PROJECT   = f"{DATALAKE_HOME}/transformation/dbt/datalake_transforms"
DBT_PROFILES  = "/opt/airflow/dbt_profiles.yml"
DBT_BIN       = "/home/airflow/.local/bin/dbt"


# ── ClickHouse helpers ─────────────────────────────────────────────────────────

def ch_client():
    """Return a requests Session pre-configured for ClickHouse HTTP API."""
    session = requests.Session()
    session.auth = CH_AUTH
    session.timeout = 120
    return session


def ch_exec(sql: str, timeout: int = 120) -> str:
    """Execute a ClickHouse SQL statement via HTTP API and return the result."""
    resp = requests.post(CH_URL, data=sql, auth=CH_AUTH, timeout=timeout)
    resp.raise_for_status()
    return resp.text.strip()


def ch_insert_tsv(table: str, data: str, database: str = "raw", timeout: int = 300, columns: list = None) -> None:
    """Insert TabSeparated data into a ClickHouse table."""
    if not data:
        return
    col_spec = f" ({', '.join(columns)})" if columns else ""
    resp = requests.post(
        CH_URL,
        params={"query": f"INSERT INTO {table}{col_spec} FORMAT TabSeparated", "database": database},
        data=data.encode("utf-8"),
        auth=CH_AUTH,
        timeout=timeout,
    )
    resp.raise_for_status()


def ch_insert(table: str, rows: list, columns: list, database: str = "raw") -> None:
    """Insert rows into a ClickHouse table, converting Python types to TSV."""
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
    ch_insert_tsv(table, tsv, database=database, columns=columns)


# ── dbt helpers ────────────────────────────────────────────────────────────────

def cleanup_dbt_tmp(target_dir: str | None) -> None:
    """Remove the dbt temp directory (parent of target_dir)."""
    import shutil
    if target_dir:
        shutil.rmtree(os.path.dirname(target_dir), ignore_errors=True)


def parse_dbt_results(target_dir: str | None) -> dict:
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
