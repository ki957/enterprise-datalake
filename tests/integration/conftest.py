"""
Integration test fixtures
==========================
Provides session-scoped connections to the test stack and creates the
required schemas/tables before any integration test runs.

Prerequisites:
    docker compose -f infrastructure/docker/docker-compose.test.yml up -d

Connection settings are read from environment variables so the same
fixtures work both locally (using the ports mapped in docker-compose.test.yml)
and in CI (where the same env vars are injected by the workflow).
"""

import os
import time

import psycopg2
import pytest
import requests

# ── Connection coordinates (overridable via env) ──────────────────────────────

CH_HOST = os.getenv("TEST_CLICKHOUSE_HOST", "localhost")
CH_PORT = os.getenv("TEST_CLICKHOUSE_PORT", "18123")
CH_USER = os.getenv("TEST_CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("TEST_CLICKHOUSE_PASSWORD", "test_password")
CH_URL  = f"http://{CH_HOST}:{CH_PORT}/"

PG_HOST = os.getenv("TEST_POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("TEST_POSTGRES_PORT", "15432"))
PG_USER = os.getenv("TEST_POSTGRES_USER", "postgres")
PG_PASS = os.getenv("TEST_POSTGRES_PASSWORD", "test_password")
PG_DB   = os.getenv("TEST_POSTGRES_DB", "saas_test")

_SKIP_MSG = (
    "Test services not reachable. Start them with:\n"
    "  docker compose -f infrastructure/docker/docker-compose.test.yml up -d"
)


def _ch_exec(sql: str) -> str:
    resp = requests.post(CH_URL, data=sql, auth=(CH_USER, CH_PASS), timeout=30)
    resp.raise_for_status()
    return resp.text.strip()


# ── Service readiness fixtures ────────────────────────────────────────────────

@pytest.fixture(scope="session")
def ch_conn():
    """Session-scoped ClickHouse query helper; skips if service unreachable."""
    for _ in range(20):
        try:
            r = requests.get(f"http://{CH_HOST}:{CH_PORT}/ping", timeout=3)
            if r.status_code == 200:
                return _ch_exec
        except Exception:
            pass
        time.sleep(3)
    pytest.skip(_SKIP_MSG)


@pytest.fixture(scope="session")
def pg_conn():
    """Session-scoped PostgreSQL connection; skips if service unreachable."""
    for _ in range(20):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                user=PG_USER, password=PG_PASS, dbname=PG_DB,
            )
            conn.autocommit = True
            return conn
        except Exception:
            pass
        time.sleep(3)
    pytest.skip(_SKIP_MSG)


# ── Schema bootstrap fixtures (run once per test session) ─────────────────────

@pytest.fixture(scope="session", autouse=True)
def clickhouse_schema(ch_conn):
    """Create the raw.saas_* tables in the test ClickHouse instance."""
    ch_conn("CREATE DATABASE IF NOT EXISTS raw")
    ch_conn("""
        CREATE TABLE IF NOT EXISTS raw.saas_users (
            id       UInt64,
            email    String,
            name     String,
            company  String,
            country  String,
            plan     String,
            mrr      Float64,
            status   String,
            created_at DateTime,
            updated_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY id
    """)
    ch_conn("""
        CREATE TABLE IF NOT EXISTS raw.saas_events (
            id          UInt64,
            user_id     UInt64,
            event_type  String,
            page        String,
            occurred_at DateTime
        ) ENGINE = MergeTree()
        ORDER BY id
    """)
    ch_conn("""
        CREATE TABLE IF NOT EXISTS raw.saas_subscriptions (
            id            UInt64,
            user_id       UInt64,
            plan          String,
            status        String,
            billing_cycle String,
            mrr           Float64,
            started_at    DateTime,
            ended_at      Nullable(DateTime)
        ) ENGINE = MergeTree()
        ORDER BY id
    """)


@pytest.fixture(scope="session", autouse=True)
def postgres_schema(pg_conn):
    """Create the SaaS source tables in the test PostgreSQL instance."""
    cur = pg_conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saas_users (
            id         SERIAL PRIMARY KEY,
            email      VARCHAR(255),
            name       VARCHAR(255),
            company    VARCHAR(255),
            country    VARCHAR(100),
            plan       VARCHAR(50),
            mrr        NUMERIC(10,2),
            status     VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saas_events (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER,
            event_type  VARCHAR(100),
            page        VARCHAR(255),
            occurred_at TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS saas_subscriptions (
            id            SERIAL PRIMARY KEY,
            user_id       INTEGER,
            plan          VARCHAR(50),
            status        VARCHAR(50),
            billing_cycle VARCHAR(50),
            mrr           NUMERIC(10,2),
            started_at    TIMESTAMP DEFAULT NOW(),
            ended_at      TIMESTAMP
        )
    """)
    cur.close()
