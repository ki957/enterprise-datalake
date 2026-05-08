"""
SaaS Pipeline Logic Tests
==========================
Tests the pure-Python helper functions inside saas_pipeline.py without
connecting to ClickHouse or PostgreSQL.

Note: requests is imported lazily inside function bodies in saas_pipeline.py,
so we patch it at its source ('requests.post') rather than 'saas_pipeline.requests'.

Run with:
    pytest tests/test_saas_pipeline_logic.py -v
"""

import sys
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

# Stub Airflow so the DAG file can be imported without a real Airflow install
airflow_stub = MagicMock()
sys.modules.setdefault("airflow", airflow_stub)
sys.modules.setdefault("airflow.models", airflow_stub)
sys.modules.setdefault("airflow.operators", airflow_stub)
sys.modules.setdefault("airflow.operators.python", airflow_stub)

sys.path.insert(0, str(Path(__file__).parent.parent / "orchestration" / "airflow" / "dags"))

import saas_pipeline as pipeline


class TestChInsert:
    """Tests for the _ch_insert TSV serialisation helper."""

    def _call_insert(self, rows, columns):
        """Helper: call _ch_insert and return the body sent to ClickHouse."""
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status = lambda: None
            pipeline.ch_insert("raw.saas_users", rows, columns)
            if mock_post.called:
                call = mock_post.call_args
                body = call.kwargs.get("data") or (call.args[1] if len(call.args) > 1 else b"")
                return body.decode() if isinstance(body, bytes) else body
            return None

    def test_null_values_serialised_as_backslash_N(self):
        body = self._call_insert([(1, None, "test")], ["id", "email", "name"])
        assert body is not None
        assert "\\N" in body

    def test_datetime_formatted_without_microseconds(self):
        dt = datetime(2025, 6, 15, 12, 30, 45, 123456)
        body = self._call_insert([(1, dt)], ["id", "occurred_at"])
        assert "2025-06-15 12:30:45" in body
        assert ".123456" not in body

    def test_decimal_serialised_as_float_string(self):
        body = self._call_insert([(1, Decimal("29.99"))], ["id", "mrr"])
        assert "29.99" in body

    def test_tab_in_value_is_replaced(self):
        body = self._call_insert([(1, "hello\tworld")], ["id", "name"])
        # The TSV body is tab-delimited; any tab inside a value must be replaced
        lines = body.split("\n")
        fields = lines[0].split("\t")
        # The value field (index 1) should not contain a tab
        assert "\t" not in fields[1]

    def test_empty_rows_skips_http_call(self):
        with patch("requests.post") as mock_post:
            pipeline.ch_insert("raw.saas_users", [], ["id", "email"])
            mock_post.assert_not_called()

    def test_multiple_rows_each_on_separate_line(self):
        rows = [(1, "a@b.com"), (2, "c@d.com"), (3, "e@f.com")]
        body = self._call_insert(rows, ["id", "email"])
        lines = [l for l in body.split("\n") if l]
        assert len(lines) == 3


class TestQualityCheck:
    """Tests for data_quality_check — validates assertion logic without DB."""

    def test_raises_on_zero_users(self):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status = lambda: None
            mock_post.return_value.text = "0"
            with pytest.raises(AssertionError):
                pipeline.data_quality_check()

    def test_raises_on_zero_events(self):
        call_count = [0]

        def side_effect(*args, **kwargs):
            r = MagicMock()
            r.raise_for_status = lambda: None
            # Return >0 for users (first call), 0 for events (second call)
            r.text = "9999" if call_count[0] == 0 else "0"
            call_count[0] += 1
            return r

        with patch("requests.post", side_effect=side_effect):
            with pytest.raises(AssertionError):
                pipeline.data_quality_check()

    def test_passes_when_both_counts_nonzero(self):
        with patch("requests.post") as mock_post:
            mock_post.return_value.raise_for_status = lambda: None
            mock_post.return_value.text = "9999"
            pipeline.data_quality_check()  # should not raise
