"""
Data Generator Unit Tests
==========================
Tests the pure-Python logic in the synthetic data generation scripts
without connecting to any database.

Run with:
    pytest tests/test_data_generators.py -v
"""

import sys
from pathlib import Path
from datetime import datetime

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


# ── MySQL generator tests ──────────────────────────────────────────────────────

class TestMySQLGenerator:

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("generate_mysql_data")

    def test_rand_date_returns_datetime(self):
        assert isinstance(self.mod.rand_date(), datetime)

    def test_rand_date_range_respected(self):
        dt = self.mod.rand_date(start_days_ago=10, end_days_ago=0)
        assert (datetime.now() - dt).days <= 10

    def test_weighted_country_returns_valid_country(self):
        country = self.mod.weighted_country()
        assert country in list(self.mod.COUNTRY_WEIGHTS.keys())

    def test_customer_rows_generates_correct_count(self):
        rows = list(self.mod.customer_rows(5))
        assert len(rows) == 5

    def test_customer_row_has_required_fields(self):
        row = next(self.mod.customer_rows(1))
        required = {"email", "segment", "country", "created_at", "updated_at"}
        assert required.issubset(row.keys()), f"Missing fields: {required - row.keys()}"

    def test_customer_email_is_valid(self):
        row = next(self.mod.customer_rows(1))
        assert "@" in row["email"]

    def test_customer_segment_is_valid(self):
        row = next(self.mod.customer_rows(1))
        assert row["segment"] in self.mod.SEGMENTS

    def test_customer_country_is_valid(self):
        row = next(self.mod.customer_rows(1))
        assert row["country"] in self.mod.COUNTRY_WEIGHTS

    def test_product_row_has_required_fields(self):
        row = next(self.mod.product_rows(1))
        required = {"name", "category", "price", "created_at"}
        assert required.issubset(row.keys())

    def test_product_category_is_valid(self):
        row = next(self.mod.product_rows(1))
        assert row["category"] in self.mod.CATEGORIES

    def test_product_price_is_positive(self):
        row = next(self.mod.product_rows(1))
        assert row["price"] > 0

    def test_order_row_has_required_fields(self):
        row = next(self.mod.order_rows(1, [1, 2], [10, 11]))
        required = {"customer_id", "product_id", "amount", "quantity", "status", "order_date"}
        assert required.issubset(row.keys())

    def test_order_references_valid_customer(self):
        customer_ids = [1, 2, 3]
        rows = list(self.mod.order_rows(10, customer_ids, [10]))
        for row in rows:
            assert row["customer_id"] in customer_ids

    def test_order_status_is_valid(self):
        rows = list(self.mod.order_rows(5, [1], [1]))
        for row in rows:
            assert row["status"] in self.mod.ORDER_STATUSES

    def test_order_amount_is_positive(self):
        rows = list(self.mod.order_rows(5, [1], [1]))
        for row in rows:
            assert row["amount"] > 0

    def test_n_customers_default_is_large(self):
        assert self.mod.N_CUSTOMERS >= 50_000

    def test_n_orders_default_is_large(self):
        assert self.mod.N_ORDERS >= 500_000

    def test_chunk_size_reasonable(self):
        assert 100 <= self.mod.CHUNK_SIZE <= 50_000


# ── SaaS generator tests ───────────────────────────────────────────────────────

class TestSaaSGenerator:

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("generate_saas_data")

    def test_rand_dt_returns_datetime(self):
        assert isinstance(self.mod.rand_dt(), datetime)

    def test_user_row_has_required_fields(self):
        row = next(self.mod.user_rows(1))
        required = {"email", "name", "plan", "mrr", "status", "created_at"}
        assert required.issubset(row.keys())

    def test_user_email_is_valid(self):
        row = next(self.mod.user_rows(1))
        assert "@" in row["email"]

    def test_user_plan_is_valid(self):
        row = next(self.mod.user_rows(1))
        assert row["plan"] in self.mod.PLANS

    def test_user_mrr_is_non_negative(self):
        row = next(self.mod.user_rows(1))
        assert row["mrr"] >= 0

    def test_user_status_is_valid(self):
        row = next(self.mod.user_rows(1))
        assert row["status"] in self.mod.STATUS_OPTIONS

    def test_event_row_has_required_fields(self):
        row = next(self.mod.event_rows(1, [1]))
        required = {"user_id", "event_type", "page", "occurred_at"}
        assert required.issubset(row.keys())

    def test_event_references_valid_user(self):
        user_ids = [1, 2, 3]
        rows = list(self.mod.event_rows(10, user_ids))
        for row in rows:
            assert row["user_id"] in user_ids

    def test_event_type_is_valid(self):
        rows = list(self.mod.event_rows(10, [1]))
        for row in rows:
            assert row["event_type"] in self.mod.EVENT_TYPES

    def test_subscription_row_has_required_fields(self):
        row = next(self.mod.subscription_rows(1, [1]))
        required = {"user_id", "plan", "billing_cycle", "started_at"}
        assert required.issubset(row.keys())

    def test_subscription_billing_cycle_is_valid(self):
        rows = list(self.mod.subscription_rows(5, [1]))
        for row in rows:
            assert row["billing_cycle"] in self.mod.BILLING_CYCLES

    def test_plan_mrr_mapping_complete(self):
        for plan in self.mod.PLANS:
            assert plan in self.mod.PLAN_MRR, f"Plan '{plan}' missing from PLAN_MRR"

    def test_n_events_default_is_large(self):
        assert self.mod.N_EVENTS >= 200_000


# ── Vault setup tests ──────────────────────────────────────────────────────────

class TestVaultSetup:

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("setup_vault_secrets")

    def test_vault_write_constructs_correct_url(self, monkeypatch):
        calls = []

        class FakeResponse:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *a): pass

        import urllib.request
        monkeypatch.setattr(urllib.request, "urlopen", lambda req: (calls.append(req.full_url), FakeResponse())[1])
        self.mod.vault_write("http://vault:8200", "root", "secret/data/test", {"k": "v"})
        assert calls[0] == "http://vault:8200/v1/secret/data/test"

    def test_vault_write_wraps_data_in_kv2_envelope(self, monkeypatch):
        import json
        import urllib.request
        bodies = []

        class FakeResponse:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *a): pass

        monkeypatch.setattr(urllib.request, "urlopen", lambda req: (bodies.append(json.loads(req.data)), FakeResponse())[1])
        self.mod.vault_write("http://vault:8200", "root", "secret/data/test", {"host": "ch"})
        assert bodies[0] == {"data": {"host": "ch"}}

    def test_vault_write_sends_token_header(self, monkeypatch):
        import urllib.request
        headers_seen = {}

        class FakeResponse:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *a): pass

        monkeypatch.setattr(urllib.request, "urlopen", lambda req: (headers_seen.update(dict(req.headers)), FakeResponse())[1])
        self.mod.vault_write("http://vault:8200", "mytoken", "secret/data/test", {})
        assert headers_seen.get("X-vault-token") == "mytoken"
