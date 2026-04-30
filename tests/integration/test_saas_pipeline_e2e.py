"""
SaaS Pipeline End-to-End Integration Tests
===========================================
Tests the real extract flow against live services:
  1. Seed PostgreSQL with 10 users + 25 events
  2. Call extract_postgres_to_clickhouse() — the actual function from saas_pipeline.py
  3. Assert rows landed correctly in ClickHouse raw schema
  4. Verify data quality check passes
  5. Verify idempotency (re-running does not double rows)

The pipeline module is imported with its constants patched to point at the
test-stack services (ports 18123 / 15432) instead of the production stack.

Run locally:
    docker compose -f infrastructure/docker/docker-compose.test.yml up -d
    pytest tests/integration/test_saas_pipeline_e2e.py -v
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# ── Test service coordinates ──────────────────────────────────────────────────

TEST_CH_URL  = (
    f"http://{os.getenv('TEST_CLICKHOUSE_HOST', 'localhost')}"
    f":{os.getenv('TEST_CLICKHOUSE_PORT', '18123')}/"
)
TEST_PG_HOST = os.getenv("TEST_POSTGRES_HOST", "localhost")
TEST_PG_PORT = int(os.getenv("TEST_POSTGRES_PORT", "15432"))
TEST_PG_PASS = os.getenv("TEST_POSTGRES_PASSWORD", "test_password")
TEST_PG_DB   = os.getenv("TEST_POSTGRES_DB", "saas_test")
TEST_CH_USER = os.getenv("TEST_CLICKHOUSE_USER", "default")
TEST_CH_PASS = os.getenv("TEST_CLICKHOUSE_PASSWORD", "test_password")

# ── Load the real pipeline module ─────────────────────────────────────────────
# Stub Airflow so the module-level DAG definition doesn't fail on import.
_airflow_stub = MagicMock()
for _mod in ["airflow", "airflow.models", "airflow.operators", "airflow.operators.python"]:
    sys.modules.setdefault(_mod, _airflow_stub)

# Set env vars BEFORE import so module-level os.getenv() calls pick them up.
os.environ.update({
    "POSTGRES_HOST":           TEST_PG_HOST,
    "POSTGRES_PORT":           str(TEST_PG_PORT),
    "POSTGRES_USER":           "postgres",
    "POSTGRES_PASSWORD":       TEST_PG_PASS,
    "POSTGRES_DB":             TEST_PG_DB,
    "CLICKHOUSE_URL":          TEST_CH_URL,
    "DBT_CLICKHOUSE_USER":     TEST_CH_USER,
    "DBT_CLICKHOUSE_PASSWORD": TEST_CH_PASS,
})

_DAGS_PATH = Path(__file__).parent.parent.parent / "orchestration" / "airflow" / "dags"
sys.path.insert(0, str(_DAGS_PATH))

import saas_pipeline as pipeline  # noqa: E402

# Belt-and-suspenders: patch the module-level constants directly in case they
# were frozen before we set the env vars.
pipeline.PG_HOST = TEST_PG_HOST
pipeline.PG_PORT = TEST_PG_PORT
pipeline.PG_PASS = TEST_PG_PASS
pipeline.PG_DB   = TEST_PG_DB
pipeline.CH_URL  = TEST_CH_URL
pipeline.CH_AUTH = (TEST_CH_USER, TEST_CH_PASS)


# ── Module-scoped fixtures ────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def seeded_postgres(pg_conn):
    """Insert 10 users + 25 events into PostgreSQL; clean up before seeding."""
    cur = pg_conn.cursor()
    cur.execute("TRUNCATE TABLE saas_events, saas_users RESTART IDENTITY CASCADE")
    for i in range(1, 11):
        cur.execute(
            "INSERT INTO saas_users (email, name, company, country, plan, mrr, status) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                f"user{i}@test.com", f"User {i}", f"Corp {i}", "US",
                "pro" if i % 2 == 0 else "starter",
                99.00 if i % 2 == 0 else 29.00,
                "active",
            ),
        )
    for j in range(1, 26):
        cur.execute(
            "INSERT INTO saas_events (user_id, event_type, page) VALUES (%s, %s, %s)",
            ((j % 10) + 1, "page_view", f"/page/{j}"),
        )
    cur.close()
    return {"users": 10, "events": 25}


@pytest.fixture(scope="module")
def after_extract(ch_conn, seeded_postgres):
    """
    Run the extract exactly once for this module.
    Returns the mock task instance so tests can inspect XCom calls.
    """
    ch_conn("TRUNCATE TABLE IF EXISTS raw.saas_users")
    ch_conn("TRUNCATE TABLE IF EXISTS raw.saas_events")
    mock_ti = MagicMock()
    pipeline.extract_postgres_to_clickhouse(ti=mock_ti)
    return mock_ti


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestExtractRowCounts:
    """Verify the correct number of rows land in ClickHouse after extract."""

    def test_correct_user_count(self, ch_conn, after_extract):
        count = int(ch_conn("SELECT count() FROM raw.saas_users"))
        assert count == 10, f"Expected 10 users in ClickHouse, got {count}"

    def test_correct_event_count(self, ch_conn, after_extract):
        count = int(ch_conn("SELECT count() FROM raw.saas_events"))
        assert count == 25, f"Expected 25 events in ClickHouse, got {count}"


class TestExtractDataIntegrity:
    """Spot-check that field values survive the TSV serialisation round-trip."""

    def test_user_emails_preserved(self, ch_conn, after_extract):
        emails = ch_conn(
            "SELECT email FROM raw.saas_users ORDER BY id"
        ).splitlines()
        assert len(emails) == 10
        assert all("@test.com" in e for e in emails)

    def test_mrr_values_are_numeric(self, ch_conn, after_extract):
        mrrs = ch_conn(
            "SELECT mrr FROM raw.saas_users ORDER BY id"
        ).splitlines()
        assert len(mrrs) == 10
        for mrr in mrrs:
            float(mrr)  # raises if not a valid float

    def test_event_user_ids_in_range(self, ch_conn, after_extract):
        """All event.user_id values should reference one of the 10 seeded users."""
        max_uid = int(ch_conn("SELECT max(user_id) FROM raw.saas_events"))
        assert 1 <= max_uid <= 10, f"event.user_id out of range: {max_uid}"

    def test_plan_values_are_valid(self, ch_conn, after_extract):
        plans = set(ch_conn(
            "SELECT DISTINCT plan FROM raw.saas_users"
        ).splitlines())
        assert plans <= {"pro", "starter"}, f"Unexpected plan values: {plans}"


class TestXComIntegration:
    """Verify the extract task pushes correct metadata via Airflow XCom."""

    def test_xcom_push_is_called_once(self, after_extract):
        after_extract.xcom_push.assert_called_once()

    def test_xcom_push_reports_user_count(self, after_extract):
        call = after_extract.xcom_push.call_args
        value = call.kwargs.get("value")
        assert value is not None, "xcom_push was not called with a 'value' kwarg"
        assert value.get("users") == 10

    def test_xcom_push_reports_event_count(self, after_extract):
        call = after_extract.xcom_push.call_args
        value = call.kwargs.get("value")
        assert value.get("events") == 25


class TestQualityCheck:
    """data_quality_check() should pass after a successful extract."""

    def test_quality_check_passes(self, after_extract):
        # Should not raise — both raw tables have rows
        pipeline.data_quality_check()


class TestIdempotency:
    """Re-running the extract must not accumulate duplicate rows."""

    def test_rerunning_does_not_double_users(self, ch_conn, seeded_postgres):
        mock_ti = MagicMock()
        pipeline.extract_postgres_to_clickhouse(ti=mock_ti)
        pipeline.extract_postgres_to_clickhouse(ti=mock_ti)
        count = int(ch_conn("SELECT count() FROM raw.saas_users"))
        assert count == 10, (
            f"Idempotency broken: expected 10 users after two runs, got {count}"
        )

    def test_rerunning_does_not_double_events(self, ch_conn, seeded_postgres):
        mock_ti = MagicMock()
        pipeline.extract_postgres_to_clickhouse(ti=mock_ti)
        pipeline.extract_postgres_to_clickhouse(ti=mock_ti)
        count = int(ch_conn("SELECT count() FROM raw.saas_events"))
        assert count == 25, (
            f"Idempotency broken: expected 25 events after two runs, got {count}"
        )
