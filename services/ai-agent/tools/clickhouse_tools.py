"""
ClickHouse tools for the AI agent.

Connection pooling: a single module-level Client instance is reused across
all tool calls within a container process. This avoids the overhead of a
new TCP handshake per query and keeps resource usage low.
"""

import os
import re
import threading
from typing import Optional

from clickhouse_driver import Client
from langchain_core.tools import tool

# ── Connection pool (singleton per process) ──────────────────────────────────

_client_lock    = threading.Lock()
_client_cache: dict[str, Client] = {}


def _make_client(database: str) -> Client:
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", 9002)),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=database,
        connect_timeout=5,
        send_receive_timeout=25,  # was 120 — prevent a slow query from hanging the agent
        settings={"connection_pool_size": 5},
    )


def get_ch_client(database: Optional[str] = None) -> Client:
    """
    Return a cached Client for the given database.
    Creates a new connection only on the first call per database.
    Thread-safe via a module-level lock.
    """
    db = database or os.getenv("CLICKHOUSE_DB", "gold")
    with _client_lock:
        if db not in _client_cache:
            _client_cache[db] = _make_client(db)
        return _client_cache[db]


def _execute(sql: str, database: Optional[str] = None):
    """
    Execute a query, retrying once on a broken connection.
    clickhouse-driver raises NetworkReceiveError / BrokenPipeError on
    connection reuse after server restart — reconnect and retry.
    """
    import clickhouse_driver.errors as ch_errors

    db = database or os.getenv("CLICKHOUSE_DB", "gold")
    try:
        client = get_ch_client(db)
        return client.execute(sql, with_column_types=True)
    except (OSError, EOFError, ch_errors.NetworkError,
            ch_errors.SocketTimeoutError) as exc:
        # Stale connection — evict and retry once
        with _client_lock:
            _client_cache.pop(db, None)
        client = get_ch_client(db)
        return client.execute(sql, with_column_types=True)


# ── Tools ─────────────────────────────────────────────────────────────────────

_BLOCKED_SQL_KEYWORDS = frozenset([
    "drop", "delete", "insert", "update", "truncate",
    "alter", "create", "replace", "rename", "attach",
    "detach", "optimize", "system reload", "system drop",
])


def _is_safe_sql(sql: str) -> tuple[bool, str]:
    """
    Reject DDL / DML statements — this agent is read-only.
    Returns (is_safe, reason).
    """
    normalized = " ".join(sql.lower().split())
    for kw in _BLOCKED_SQL_KEYWORDS:
        if re.search(rf"\b{re.escape(kw)}\b", normalized):
            return False, f"Statement contains disallowed keyword '{kw}'. Only SELECT queries are permitted."
    return True, ""


def _format_as_markdown_table(columns: list, rows: list) -> str:
    """Format query results as a markdown table for clean LLM output."""
    if not rows:
        return "Query returned 0 rows."

    # Format each cell: round floats, convert Decimal, handle None
    from decimal import Decimal
    import datetime

    def fmt(v):
        if v is None:
            return "—"
        if isinstance(v, float):
            return f"{v:,.2f}" if abs(v) >= 1 else f"{v:.4f}"
        if isinstance(v, Decimal):
            return f"{float(v):,.2f}"
        if isinstance(v, (int,)) and not isinstance(v, bool):
            return f"{v:,}"
        if isinstance(v, (datetime.date, datetime.datetime)):
            return str(v)[:10]
        return str(v)

    # Header row
    header = "| " + " | ".join(columns) + " |"
    sep    = "| " + " | ".join(["---"] * len(columns)) + " |"
    body   = "\n".join(
        "| " + " | ".join(fmt(row[i]) for i in range(len(columns))) + " |"
        for row in rows
    )
    total_note = f"\n\n_Showing {len(rows)} row(s)_" if len(rows) >= 20 else ""
    return f"{header}\n{sep}\n{body}{total_note}"


@tool
def query_clickhouse(sql: str) -> str:
    """Run a SELECT query on ClickHouse. Use for business analytics and ad-hoc inspection.
    Always qualify table names with schema (e.g. gold.fct_orders).
    Rules: no LAG(), use count() not COUNT(*), is_current = 1 not true."""
    safe, reason = _is_safe_sql(sql)
    if not safe:
        return f"Query rejected: {reason}"
    try:
        result, cols = _execute(sql)
        columns = [c[0] for c in cols]
        rows = [tuple(row) for row in result[:20]]
        if not rows:
            return "Query returned 0 rows."
        table = _format_as_markdown_table(columns, rows)
        return f"Query OK — {len(rows)} row(s):\n\n{table}"
    except Exception as e:
        return f"Query failed: {str(e)}"


@tool
def get_table_row_counts(schema: str = "gold") -> str:
    """Get row counts for all tables in a ClickHouse schema.
    Use for data quality checks and monitoring."""
    try:
        client = get_ch_client(schema)
        tables = client.execute(f"SHOW TABLES FROM {schema}")
        rows = []
        for (table,) in tables:
            count_result = client.execute(f"SELECT count() FROM {schema}.{table}")
            rows.append((table, count_result[0][0]))
        rows.sort(key=lambda x: x[0])
        table_md = _format_as_markdown_table(["table", "row_count"], rows)
        return f"Row counts in schema **{schema}**:\n\n{table_md}"
    except Exception as e:
        return f"Error: {str(e)}"


@tool
def check_slow_queries(threshold_seconds: str = "1") -> str:
    """Check ClickHouse system.query_log for slow queries from today.
    threshold_seconds: minimum duration to flag, e.g. "1" for 1 second (default).
    Use to identify performance bottlenecks."""
    try:
        sql = """
        SELECT
            query_duration_ms,
            read_rows,
            formatReadableSize(memory_usage) AS mem,
            substring(query, 1, 100) AS query_snippet
        FROM system.query_log
        WHERE query_duration_ms > 1000
          AND event_date = today()
          AND type = 'QueryFinish'
        ORDER BY query_duration_ms DESC
        LIMIT 5
        """
        result, _ = _execute(sql, database="system")
        if not result:
            return "No slow queries found today — all queries under 1 second. ✅"
        header = "| Duration (ms) | Rows Read | Memory | Query |"
        sep    = "|---|---|---|---|"
        rows   = "\n".join(
            f"| {r[0]:,} | {r[1]:,} | {r[2]} | `{r[3].strip()[:90]}` |"
            for r in result
        )
        return f"**Slow queries today** (> 1 s threshold):\n\n{header}\n{sep}\n{rows}"
    except Exception as e:
        return f"Error checking slow queries: {str(e)}"


@tool
def describe_table(table: str, schema: str = "gold") -> str:
    """Describe columns of a ClickHouse table as a markdown table (name, type, default).
    Use when writing SQL to confirm column names and types."""
    try:
        result, _ = _execute(f"DESCRIBE TABLE {schema}.{table}", database=schema)
        header = "| Column | Type | Default |"
        sep    = "|---|---|---|"
        rows   = "\n".join(
            f"| {r[0]} | {r[1]} | {r[2] if r[2] else '—'} |"
            for r in result
        )
        return f"**Schema: {schema}.{table}**\n\n{header}\n{sep}\n{rows}"
    except Exception as e:
        return f"Error: {str(e)}"
