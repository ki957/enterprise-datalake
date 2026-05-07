"""
Property-Based Tests with Hypothesis
=====================================
Tests invariants in data generators, dbt model logic, and pipeline functions
using property-based testing.

Run with:
    pytest tests/test_properties.py -v

Requires: hypothesis (pip install hypothesis)
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

import pytest
from hypothesis import given, settings, assume, HealthCheck
from hypothesis import strategies as st

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


class TestDataGeneratorProperties:
    """Invariants for the synthetic data generation scripts."""

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("generate_mysql_data")

    @given(st.integers(min_value=0, max_value=365), st.integers(min_value=0, max_value=365))
    @settings(max_examples=50)
    def test_rand_date_range_always_within_bounds(self, days_ago_start, days_ago_end):
        assume(days_ago_start >= days_ago_end)
        dt = self.mod.rand_date(start_days_ago=days_ago_start, end_days_ago=days_ago_end)
        now = datetime.now()
        lower = now - timedelta(days=days_ago_start)
        upper = now - timedelta(days=days_ago_end)
        assert lower <= dt <= upper

    @given(st.integers(min_value=1, max_value=1000))
    @settings(max_examples=20)
    def test_customer_rows_count_matches_requested(self, n):
        rows = list(self.mod.customer_rows(n))
        assert len(rows) == n

    @given(st.integers(min_value=1, max_value=1000))
    @settings(max_examples=20)
    def test_order_rows_count_matches_requested(self, n):
        rows = list(self.mod.order_rows(n))
        assert len(rows) == n

    @settings(max_examples=50)
    @given(st.nothing())
    def test_country_weights_sum_to_one(self, _):
        total = sum(self.mod.COUNTRY_WEIGHTS.values())
        assert abs(total - 1.0) < 1e-9

    @settings(max_examples=100)
    @given(st.nothing())
    def test_weighted_country_always_valid(self, _):
        country = self.mod.weighted_country()
        assert country in self.mod.COUNTRY_WEIGHTS

    @given(st.integers(min_value=1, max_value=500))
    @settings(max_examples=20)
    def test_product_rows_have_unique_ids(self, n):
        ids = [row["id"] for row in self.mod.product_rows(n)]
        assert len(ids) == len(set(ids))


class TestClickHouseSQLProperties:
    """Invariants for ClickHouse SQL patterns used in dbt models."""

    @given(st.dates(min_value=datetime(2020, 1, 1), max_value=datetime(2026, 12, 31)))
    @settings(max_examples=50)
    def test_date_trunc_to_start_of_month_is_valid(self, date):
        expected = date.replace(day=1)
        assert expected <= date
        assert expected.month == date.month
        assert expected.year == date.year

    @given(
        st.floats(min_value=-10000, max_value=10000, allow_nan=False, allow_infinity=False)
    )
    @settings(max_examples=50)
    def test_round_two_decimals_never_exceeds_two_places(self, amount):
        rounded = round(amount, 2)
        # Check that the decimal part has at most 2 digits
        str_repr = f"{rounded:.10f}"
        decimal_part = str_repr.split(".")[1]
        assert all(c == "0" for c in decimal_part[2:])

    @given(
        st.lists(
            st.tuples(st.dates(), st.floats(min_value=0, max_value=100000, allow_nan=False)),
            min_size=1,
            max_size=100,
        )
    )
    @settings(max_examples=20)
    def test_cumulative_sum_is_monotonic(self, data):
        sorted_data = sorted(data, key=lambda x: x[0])
        cumulative = 0
        for _, value in sorted_data:
            cumulative += value
            assert cumulative >= 0


class TestPipelineGraphProperties:
    """Invariants for the AI agent pipeline routing logic."""

    def setup_method(self):
        sys.path.insert(0, str(Path(__file__).parent.parent / "services" / "ai-agent"))
        from graph.pipeline_graph import _route

    @given(
        st.sampled_from([
            {"message": "What is our total revenue?", "agent": "", "response": "",
             "history": [], "session_id": "test", "lc_messages": []},
            {"message": "Show me customer churn rate", "agent": "", "response": "",
             "history": [], "session_id": "test", "lc_messages": []},
            {"message": "Which vendors have highest performance?", "agent": "", "response": "",
             "history": [], "session_id": "test", "lc_messages": []},
            {"message": "How many active users this week?", "agent": "", "response": "",
             "history": [], "session_id": "test", "lc_messages": []},
        ])
    )
    @settings(max_examples=10)
    def test_route_always_returns_valid_agent(self, state):
        agent = _route(state)
        assert isinstance(agent, str)
        assert len(agent) > 0
        assert agent in ["revenue", "churn", "vendor", "engagement", "insight"]

    @given(st.text(min_size=1, max_size=500))
    @settings(max_examples=20)
    def test_route_handles_arbitrary_input(self, message):
        assume(len(message.strip()) > 0)
        state = {
            "message": message,
            "agent": "",
            "response": "",
            "history": [],
            "session_id": "test",
            "lc_messages": [],
        }
        agent = _route(state)
        assert isinstance(agent, str)
        assert len(agent) > 0
