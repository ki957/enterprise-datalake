"""
PostgreSQL-backed conversation memory.
Reuses the existing Airflow PostgreSQL instance (airflow DB).
Table: ai_agent_memory (auto-created at module import time, not per-call).

Connection pooling: ThreadedConnectionPool (min=1, max=5) avoids the overhead
of a new TCP handshake per message save/read. The pool is created lazily on
first use and shared for the process lifetime.
"""

import json
import os
import threading
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor

# ── Connection pool ───────────────────────────────────────────────────────────

_pool: pg_pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = pg_pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=5,
                    host=os.getenv("POSTGRES_HOST", "postgres"),
                    port=int(os.getenv("POSTGRES_PORT", 5432)),
                    user=os.getenv("POSTGRES_USER", "postgres"),
                    password=os.getenv("POSTGRES_PASSWORD", ""),
                    dbname=os.getenv("POSTGRES_DB", "airflow"),
                )
    return _pool


@contextmanager
def _conn():
    """Context manager that borrows a connection from the pool and returns it."""
    p = _get_pool()
    conn = p.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        p.putconn(conn)


# ── Schema init (runs once at module import) ──────────────────────────────────

_table_ready = False
_table_lock  = threading.Lock()


def _ensure_table() -> None:
    """Create memory table + index the first time this module is used."""
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
                        CREATE TABLE IF NOT EXISTS ai_agent_memory (
                            id          SERIAL PRIMARY KEY,
                            session_id  TEXT NOT NULL,
                            role        TEXT NOT NULL,
                            content     TEXT NOT NULL,
                            agent       TEXT,
                            metadata    JSONB DEFAULT '{}',
                            created_at  TIMESTAMP DEFAULT NOW()
                        )
                    """)
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_memory_session "
                        "ON ai_agent_memory (session_id, created_at DESC)"
                    )
                conn.commit()
            _table_ready = True
        except Exception:
            pass  # DB may not be available at import time — retry on first real call


# ── Public API ─────────────────────────────────────────────────────────────────

def save_message(session_id: str, role: str, content: str,
                 agent: str = "", metadata: dict | None = None) -> None:
    _ensure_table()
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ai_agent_memory "
                "(session_id, role, content, agent, metadata) "
                "VALUES (%s, %s, %s, %s, %s)",
                (session_id, role, content, agent,
                 json.dumps(metadata or {})),
            )
        conn.commit()


def get_history(session_id: str, limit: int = 20) -> list[dict]:
    """Return the last `limit` messages for a session, oldest first."""
    _ensure_table()
    with _conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT role, content, agent, created_at FROM ai_agent_memory "
                "WHERE session_id = %s ORDER BY created_at DESC LIMIT %s",
                (session_id, limit),
            )
            rows = cur.fetchall()
    return list(reversed(rows))


def clear_session(session_id: str) -> int:
    _ensure_table()
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ai_agent_memory WHERE session_id = %s",
                (session_id,),
            )
            deleted = cur.rowcount
        conn.commit()
    return deleted


def cleanup_old_sessions(days: int = 30) -> int:
    """
    Delete messages older than `days` days.
    Call periodically to prevent unbounded table growth.
    Returns the number of rows deleted.
    """
    _ensure_table()
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ai_agent_memory "
                "WHERE created_at < NOW() - make_interval(days => %s)",
                (days,),
            )
            deleted = cur.rowcount
        conn.commit()
    return deleted
