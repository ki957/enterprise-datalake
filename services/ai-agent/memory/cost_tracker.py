"""
Agent cost tracking — logs token usage and computes Groq API spend per call.

Table: agent_cost_log (auto-created in the shared airflow PostgreSQL DB)

Groq pricing for llama-4-scout-17b-16e-instruct (as of 2025):
  Input:  $0.11 / 1M tokens
  Output: $0.34 / 1M tokens
"""

import os
import threading
from contextlib import contextmanager
from datetime import date, datetime, timezone

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor

# ── Pricing constants ─────────────────────────────────────────────────────────
INPUT_COST_PER_M  = 0.11   # USD per million input tokens
OUTPUT_COST_PER_M = 0.34   # USD per million output tokens

# ── Connection pool (reused across calls) ─────────────────────────────────────
_pool: pg_pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = pg_pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=3,
                    host=os.getenv("POSTGRES_HOST", "localhost"),
                    port=int(os.getenv("POSTGRES_PORT", 5432)),
                    user=os.getenv("POSTGRES_USER", "postgres"),
                    password=os.getenv("POSTGRES_PASSWORD", ""),
                    dbname=os.getenv("POSTGRES_DB", "airflow"),
                )
    return _pool


@contextmanager
def _conn():
    p = _get_pool()
    conn = p.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


# ── Schema init ───────────────────────────────────────────────────────────────
_table_ready = False
_table_lock  = threading.Lock()


def _ensure_table() -> None:
    global _table_ready
    if _table_ready:
        return
    with _table_lock:
        if _table_ready:
            return
        try:
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS agent_cost_log (
                            id             SERIAL PRIMARY KEY,
                            session_id     TEXT NOT NULL,
                            agent          TEXT NOT NULL,
                            input_tokens   INTEGER NOT NULL DEFAULT 0,
                            output_tokens  INTEGER NOT NULL DEFAULT 0,
                            input_cost     NUMERIC(10,6) NOT NULL DEFAULT 0,
                            output_cost    NUMERIC(10,6) NOT NULL DEFAULT 0,
                            total_cost     NUMERIC(10,6) NOT NULL DEFAULT 0,
                            latency_ms     INTEGER,
                            created_at     TIMESTAMPTZ DEFAULT NOW()
                        )
                    """)
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_cost_log_date "
                        "ON agent_cost_log (created_at DESC)"
                    )
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_cost_log_agent "
                        "ON agent_cost_log (agent, created_at DESC)"
                    )
                conn.commit()
            _table_ready = True
        except Exception:
            pass


# ── Public API ─────────────────────────────────────────────────────────────────

def compute_cost(input_tokens: int, output_tokens: int) -> dict:
    """Return cost breakdown for a single call."""
    ic = (input_tokens  / 1_000_000) * INPUT_COST_PER_M
    oc = (output_tokens / 1_000_000) * OUTPUT_COST_PER_M
    return {
        "input_cost":  round(ic, 6),
        "output_cost": round(oc, 6),
        "total_cost":  round(ic + oc, 6),
    }


def log_call(
    session_id:    str,
    agent:         str,
    input_tokens:  int,
    output_tokens: int,
    latency_ms:    int | None = None,
) -> None:
    """Insert one row into agent_cost_log. Silently swallows DB errors."""
    _ensure_table()
    costs = compute_cost(input_tokens, output_tokens)
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO agent_cost_log "
                    "(session_id, agent, input_tokens, output_tokens, "
                    " input_cost, output_cost, total_cost, latency_ms) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        session_id, agent, input_tokens, output_tokens,
                        costs["input_cost"], costs["output_cost"], costs["total_cost"],
                        latency_ms,
                    ),
                )
            conn.commit()
    except Exception:
        pass  # cost tracking is non-critical — never break the main response path


def get_daily_costs(days: int = 14) -> list[dict]:
    """Daily spend aggregated by date and agent for the last N days."""
    _ensure_table()
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        DATE(created_at AT TIME ZONE 'UTC') AS day,
                        agent,
                        SUM(input_tokens)  AS input_tokens,
                        SUM(output_tokens) AS output_tokens,
                        SUM(total_cost)    AS total_cost,
                        COUNT(*)           AS calls,
                        AVG(latency_ms)    AS avg_latency_ms
                    FROM agent_cost_log
                    WHERE created_at >= NOW() - INTERVAL '%s days'
                    GROUP BY day, agent
                    ORDER BY day DESC, total_cost DESC
                    """,
                    (days,),
                )
                return [dict(r) for r in cur.fetchall()]
    except Exception:
        return []


def get_summary(days: int = 30) -> dict:
    """Overall totals + per-agent breakdown for the last N days."""
    _ensure_table()
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # totals
                cur.execute(
                    """
                    SELECT
                        COUNT(*)           AS total_calls,
                        SUM(input_tokens)  AS total_input_tokens,
                        SUM(output_tokens) AS total_output_tokens,
                        SUM(total_cost)    AS total_cost_usd,
                        AVG(latency_ms)    AS avg_latency_ms,
                        MIN(created_at)    AS first_call,
                        MAX(created_at)    AS last_call
                    FROM agent_cost_log
                    WHERE created_at >= NOW() - INTERVAL '%s days'
                    """,
                    (days,),
                )
                totals = dict(cur.fetchone() or {})

                # per-agent breakdown
                cur.execute(
                    """
                    SELECT
                        agent,
                        COUNT(*)           AS calls,
                        SUM(input_tokens)  AS input_tokens,
                        SUM(output_tokens) AS output_tokens,
                        SUM(total_cost)    AS total_cost,
                        AVG(latency_ms)    AS avg_latency_ms
                    FROM agent_cost_log
                    WHERE created_at >= NOW() - INTERVAL '%s days'
                    GROUP BY agent
                    ORDER BY total_cost DESC
                    """,
                    (days,),
                )
                by_agent = [dict(r) for r in cur.fetchall()]

        # coerce Decimal/datetime → Python native types for JSON
        def _clean(v):
            if hasattr(v, "isoformat"):
                return v.isoformat()
            if hasattr(v, "__float__"):
                return float(v)
            return v

        totals   = {k: _clean(v) for k, v in totals.items()}
        by_agent = [{k: _clean(v) for k, v in row.items()} for row in by_agent]

        return {"totals": totals, "by_agent": by_agent, "days": days}
    except Exception:
        return {"totals": {}, "by_agent": [], "days": days}


def get_recent_calls(limit: int = 50) -> list[dict]:
    """Most recent individual calls for the call log table."""
    _ensure_table()
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        id, session_id, agent,
                        input_tokens, output_tokens, total_cost, latency_ms,
                        created_at
                    FROM agent_cost_log
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = [dict(r) for r in cur.fetchall()]

        def _clean(v):
            if hasattr(v, "isoformat"):
                return v.isoformat()
            if hasattr(v, "__float__"):
                return float(v)
            return v

        return [{k: _clean(v) for k, v in row.items()} for row in rows]
    except Exception:
        return []
