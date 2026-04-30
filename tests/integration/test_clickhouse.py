"""
ClickHouse Integration Tests
=============================
Verifies connectivity, schema, and raw data operations against the test
ClickHouse instance.

Run locally:
    docker compose -f infrastructure/docker/docker-compose.test.yml up -d
    pytest tests/integration/test_clickhouse.py -v
"""

import os

import pytest
import requests

CH_HOST = os.getenv("TEST_CLICKHOUSE_HOST", "localhost")
CH_PORT = os.getenv("TEST_CLICKHOUSE_PORT", "18123")
CH_USER = os.getenv("TEST_CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("TEST_CLICKHOUSE_PASSWORD", "test_password")


class TestClickHouseConnectivity:
    """Basic smoke tests — if these fail, something fundamental is broken."""

    def test_ping_returns_ok(self):
        r = requests.get(f"http://{CH_HOST}:{CH_PORT}/ping", timeout=5)
        assert r.status_code == 200
        assert "Ok" in r.text

    def test_select_one(self, ch_conn):
        assert ch_conn("SELECT 1") == "1"

    def test_server_returns_version(self, ch_conn):
        version = ch_conn("SELECT version()")
        assert version  # any non-empty string is a valid version


class TestRawSchemaLayout:
    """Verify the raw schema and SaaS tables were created by the conftest fixture."""

    def test_raw_database_exists(self, ch_conn):
        dbs = ch_conn("SHOW DATABASES").splitlines()
        assert "raw" in dbs, f"'raw' database not found in: {dbs}"

    def test_saas_users_table_exists(self, ch_conn):
        tables = ch_conn("SHOW TABLES FROM raw").splitlines()
        assert "saas_users" in tables

    def test_saas_events_table_exists(self, ch_conn):
        tables = ch_conn("SHOW TABLES FROM raw").splitlines()
        assert "saas_events" in tables

    def test_saas_subscriptions_table_exists(self, ch_conn):
        tables = ch_conn("SHOW TABLES FROM raw").splitlines()
        assert "saas_subscriptions" in tables

    def test_saas_users_has_required_columns(self, ch_conn):
        result = ch_conn("DESCRIBE TABLE raw.saas_users")
        columns = {line.split("\t")[0] for line in result.splitlines()}
        required = {"id", "email", "name", "plan", "mrr", "status", "created_at"}
        missing = required - columns
        assert not missing, f"Missing columns in raw.saas_users: {missing}"

    def test_saas_users_mrr_is_float(self, ch_conn):
        result = ch_conn("DESCRIBE TABLE raw.saas_users")
        col_types = {
            line.split("\t")[0]: line.split("\t")[1]
            for line in result.splitlines()
            if "\t" in line
        }
        assert "Float64" in col_types.get("mrr", ""), (
            f"Expected mrr to be Float64, got: {col_types.get('mrr')}"
        )

    def test_saas_events_has_required_columns(self, ch_conn):
        result = ch_conn("DESCRIBE TABLE raw.saas_events")
        columns = {line.split("\t")[0] for line in result.splitlines()}
        required = {"id", "user_id", "event_type", "page", "occurred_at"}
        missing = required - columns
        assert not missing, f"Missing columns in raw.saas_events: {missing}"


class TestInsertAndQuery:
    """Verify that INSERT + SELECT round-trips work correctly."""

    def test_insert_user_row_and_read_back(self, ch_conn):
        ch_conn("TRUNCATE TABLE raw.saas_users")
        ch_conn(
            "INSERT INTO raw.saas_users "
            "(id, email, name, company, country, plan, mrr, status, created_at, updated_at) "
            "VALUES "
            "(42, 'alice@example.com', 'Alice', 'ACME', 'US', 'pro', 99.99, "
            "'active', '2026-01-01 00:00:00', '2026-01-01 00:00:00')"
        )
        count = ch_conn("SELECT count() FROM raw.saas_users")
        assert count == "1"

        email = ch_conn("SELECT email FROM raw.saas_users WHERE id = 42")
        assert email == "alice@example.com"

        mrr = ch_conn("SELECT mrr FROM raw.saas_users WHERE id = 42")
        assert float(mrr) == pytest.approx(99.99)

    def test_null_handling(self, ch_conn):
        """Nullable columns accept NULL values without error."""
        ch_conn("TRUNCATE TABLE raw.saas_subscriptions")
        ch_conn(
            "INSERT INTO raw.saas_subscriptions "
            "(id, user_id, plan, status, billing_cycle, mrr, started_at, ended_at) "
            "VALUES (1, 42, 'pro', 'active', 'monthly', 99.99, '2026-01-01 00:00:00', NULL)"
        )
        ended = ch_conn(
            "SELECT toString(ended_at) FROM raw.saas_subscriptions WHERE id = 1"
        )
        # NULL DateTime renders as the zero date in ClickHouse
        assert ended is not None

    def test_bulk_insert(self, ch_conn):
        """Can insert 100 rows in a single TSV batch."""
        ch_conn("TRUNCATE TABLE raw.saas_events")
        rows = "\n".join(
            f"{i}\t{(i % 10) + 1}\tpage_view\t/page/{i}\t2026-01-01 00:00:00"
            for i in range(1, 101)
        )
        ch_conn(
            "INSERT INTO raw.saas_events "
            "(id, user_id, event_type, page, occurred_at) "
            f"FORMAT TabSeparated\n{rows}"
        )
        count = ch_conn("SELECT count() FROM raw.saas_events")
        assert count == "100"
