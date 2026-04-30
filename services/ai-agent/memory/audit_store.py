"""
Audit store for self-healing agent actions.

Three tables (auto-created on first import):
  agent_audit_log       — every action attempted, outcome, confidence
  agent_incidents       — structured incident records with resolution state
  agent_pending_actions — actions queued for human approval

Uses the same Airflow PostgreSQL instance as postgres_memory.py.
Connection pool is separate (max=3) so healing writes never starve chat reads.
"""

import json
import os
import threading
from contextlib import contextmanager
from typing import Optional

from psycopg2 import pool as pg_pool

# ── Connection pool ───────────────────────────────────────────────────────────

_pool: Optional[pg_pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = pg_pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=3,
                    host=os.getenv("POSTGRES_HOST", "postgres"),
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

_tables_ready = False
_tables_lock = threading.Lock()


def _ensure_tables() -> None:
    global _tables_ready
    if _tables_ready:
        return
    with _tables_lock:
        if _tables_ready:
            return
        try:
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS agent_audit_log (
                            id          SERIAL PRIMARY KEY,
                            action      TEXT        NOT NULL,
                            target      JSONB       NOT NULL DEFAULT '{}',
                            outcome     TEXT        NOT NULL,
                            confidence  FLOAT,
                            agent       TEXT        DEFAULT 'self_healing',
                            reasoning   TEXT,
                            error       TEXT,
                            created_at  TIMESTAMP   DEFAULT NOW()
                        );

                        CREATE TABLE IF NOT EXISTS agent_incidents (
                            id           SERIAL PRIMARY KEY,
                            entity       TEXT        NOT NULL,
                            problem      TEXT        NOT NULL,
                            action_taken TEXT,
                            resolved     BOOLEAN     DEFAULT FALSE,
                            created_at   TIMESTAMP   DEFAULT NOW(),
                            resolved_at  TIMESTAMP
                        );

                        CREATE TABLE IF NOT EXISTS agent_pending_actions (
                            id           SERIAL PRIMARY KEY,
                            action       TEXT        NOT NULL,
                            target       JSONB       NOT NULL DEFAULT '{}',
                            reasoning    TEXT,
                            status       TEXT        DEFAULT 'pending',
                            created_at   TIMESTAMP   DEFAULT NOW()
                        );

                        CREATE INDEX IF NOT EXISTS idx_audit_created
                            ON agent_audit_log (created_at DESC);
                        CREATE INDEX IF NOT EXISTS idx_incidents_resolved
                            ON agent_incidents (resolved, created_at DESC);
                    """)
                conn.commit()
            _tables_ready = True
        except Exception:
            pass  # DB may not be up at import time — retried on first real call


try:
    _ensure_tables()
except Exception:
    pass


# ── Public API ────────────────────────────────────────────────────────────────

def log_action(
    action: str,
    target: dict,
    outcome: str,
    confidence: Optional[float] = None,
    agent: str = "self_healing",
    reasoning: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    """Record every action attempt — success, failure, or blocked."""
    _ensure_tables()
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agent_audit_log
                        (action, target, outcome, confidence, agent, reasoning, error)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (action, json.dumps(target), outcome, confidence,
                     agent, reasoning, error),
                )
            conn.commit()
    except Exception:
        pass  # audit failure must never crash the caller


def create_incident(
    entity: str,
    problem: str,
    action_taken: Optional[str] = None,
) -> Optional[int]:
    """Open a new incident record. Returns the incident ID."""
    _ensure_tables()
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agent_incidents (entity, problem, action_taken)
                    VALUES (%s, %s, %s) RETURNING id
                    """,
                    (entity, problem, action_taken),
                )
                incident_id = cur.fetchone()[0]
            conn.commit()
            return incident_id
    except Exception:
        return None


def queue_for_approval(
    action: str,
    target: dict,
    reasoning: str,
) -> Optional[int]:
    """Park a high-risk action until a human approves it. Returns pending ID."""
    _ensure_tables()
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO agent_pending_actions (action, target, reasoning)
                    VALUES (%s, %s, %s) RETURNING id
                    """,
                    (action, json.dumps(target), reasoning),
                )
                pending_id = cur.fetchone()[0]
            conn.commit()
            return pending_id
    except Exception:
        return None


def get_recent_incidents(limit: int = 10) -> list[dict]:
    """Return the most recent incidents, newest first."""
    _ensure_tables()
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT entity, problem, action_taken, resolved, created_at
                    FROM agent_incidents
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        return [
            {
                "entity":       r[0],
                "problem":      r[1],
                "action_taken": r[2],
                "resolved":     r[3],
                "created_at":   str(r[4]),
            }
            for r in rows
        ]
    except Exception:
        return []
