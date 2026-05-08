"""
Profiling tools shared by the anomaly detection and data contract agents.

profile_table_stats  — column-level null rate, cardinality, and percentiles
                       from ClickHouse. Used to generate AI data contracts.

get_airflow_run_history — last N DAG runs from Airflow REST API.
                          Used by anomaly agent to spot pipeline irregularities.

No LLM calls here — these tools are deterministic data collectors.
The LLM reasoning happens in the agent layer on top of their output.
"""

import os
import threading
import requests
from langchain_core.tools import tool

# ── ClickHouse connection — singleton cache (matches clickhouse_tools.py pattern) ─
_ch_cache: dict = {}
_ch_lock = threading.Lock()


def _ch_client():
    key = os.getenv("CLICKHOUSE_HOST", "localhost")
    if key not in _ch_cache:
        with _ch_lock:
            if key not in _ch_cache:
                from clickhouse_driver import Client
                _ch_cache[key] = Client(
                    host=key,
                    port=int(os.getenv("CLICKHOUSE_PORT", 9002)),
                    user=os.getenv("CLICKHOUSE_USER", "default"),
                    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                    connect_timeout=10,
                    send_receive_timeout=30,
                )
    return _ch_cache[key]


# ── Airflow helpers ───────────────────────────────────────────────────────────

def _af_base() -> str:
    return os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")


def _af_auth() -> tuple[str, str]:
    return (
        os.getenv("AIRFLOW_USER", "admin"),
        os.getenv("AIRFLOW_PASSWORD", ""),
    )


# ── Tools ─────────────────────────────────────────────────────────────────────

@tool
def profile_table_stats(table: str, schema: str = "gold") -> str:
    """Profile a ClickHouse table: null rate, distinct cardinality, and numeric
    percentiles (p5/p50/p95) per column. Use before generating data contract
    expectations or investigating data quality issues.
    Example: profile_table_stats('fct_orders', 'gold')"""
    try:
        ch = _ch_client()

        # Fetch column metadata
        cols = ch.execute(
            "SELECT name, type FROM system.columns "
            "WHERE database = %(db)s AND table = %(tbl)s",
            {"db": schema, "tbl": table},
        )
        if not cols:
            return f"Table `{schema}.{table}` not found or has no columns."

        cols = cols[:12]  # cap at 12 columns to stay within timeout
        # Single batch query — all null rates + cardinalities + numeric percentiles at once
        parts = ["count() AS _total_rows"]
        for name, typ in cols:
            is_numeric = any(t in typ for t in ("Int", "Float", "Decimal", "UInt"))
            safe = name  # ClickHouse identifiers are safe coming from system.columns
            parts.append(
                f"round(countIf(isNull({safe}) OR toString({safe}) = '') * 100.0 / count(), 1) AS _null_{safe}"
            )
            parts.append(f"uniq({safe}) AS _card_{safe}")
            if is_numeric:
                parts.append(f"quantile(0.05)({safe}) AS _p5_{safe}")
                parts.append(f"quantile(0.95)({safe}) AS _p95_{safe}")

        batch_sql = f"SELECT {', '.join(parts)} FROM {schema}.{table}"
        row = ch.execute(batch_sql)[0]

        # Parse result row into a dict by column alias position
        alias_list = ["_total_rows"]
        for name, typ in cols:
            is_numeric = any(t in typ for t in ("Int", "Float", "Decimal", "UInt"))
            alias_list.append(f"_null_{name}")
            alias_list.append(f"_card_{name}")
            if is_numeric:
                alias_list.append(f"_p5_{name}")
                alias_list.append(f"_p95_{name}")
        stats = dict(zip(alias_list, row))

        total = stats["_total_rows"]
        if total == 0:
            return f"`{schema}.{table}` is empty — cannot profile."

        lines = [f"**Profile: `{schema}.{table}`** ({total:,} rows)\n"]

        # Top values for low-cardinality string cols — one query per col (only when needed)
        for name, typ in cols:
            is_string  = "String" in typ or "Enum" in typ
            is_numeric = any(t in typ for t in ("Int", "Float", "Decimal", "UInt"))
            null_rate  = stats[f"_null_{name}"]
            card       = stats[f"_card_{name}"]

            stat = f"null={null_rate}% | cardinality={card:,}"

            if is_numeric:
                p5  = round(float(stats[f"_p5_{name}"]),  2)
                p95 = round(float(stats[f"_p95_{name}"]), 2)
                stat += f" | p5={p5} p95={p95}"
            elif is_string and card <= 20:
                try:
                    top_q = (
                        f"SELECT {name}, count() AS n FROM {schema}.{table} "
                        f"GROUP BY {name} ORDER BY n DESC LIMIT 5"
                    )
                    top_vals = [str(r[0]) for r in ch.execute(top_q)]
                    stat += f" | top_values=[{', '.join(top_vals)}]"
                except Exception:
                    pass

            lines.append(f"- `{name}` ({typ}): {stat}")

        return "\n".join(lines)

    except Exception as e:
        return f"Error profiling `{schema}.{table}`: {e}"


@tool
def get_airflow_run_history(dag_id: str, limit: int = 30) -> str:
    """Fetch the last N DAG run records from Airflow: state, start time, duration.
    Used to detect patterns like recurring failures on specific weekdays or
    consistently slow runs. Default limit is 30 (covers ~1 month of daily runs)."""
    try:
        resp = requests.get(
            f"{_af_base()}/api/v1/dags/{dag_id}/dagRuns",
            auth=_af_auth(),
            params={"limit": limit, "order_by": "-start_date"},
            timeout=15,
        )
        resp.raise_for_status()
        runs = resp.json().get("dag_runs", [])
        if not runs:
            return f"No run history found for DAG `{dag_id}`."

        lines = [f"**Run history for `{dag_id}`** (last {len(runs)} runs):\n"]
        for r in runs:
            state    = r.get("state", "unknown")
            start    = (r.get("start_date") or "")[:16]
            run_id   = r.get("dag_run_id", "")[:30]
            # compute duration in minutes if both times available
            duration = ""
            if r.get("start_date") and r.get("end_date"):
                from datetime import datetime
                try:
                    fmt = "%Y-%m-%dT%H:%M:%S"
                    s = datetime.strptime(r["start_date"][:19], fmt)
                    e = datetime.strptime(r["end_date"][:19], fmt)
                    mins = round((e - s).total_seconds() / 60, 1)
                    duration = f" | {mins}m"
                except Exception:
                    pass
            icon = {"success": "✅", "failed": "❌", "running": "🔄"}.get(state, "⚪")
            lines.append(f"{icon} {start} | {state}{duration} | {run_id}")

        return "\n".join(lines)

    except requests.exceptions.ConnectionError:
        return "Airflow unreachable — is `make governance` running?"
    except Exception as e:
        return f"Error fetching run history: {e}"
