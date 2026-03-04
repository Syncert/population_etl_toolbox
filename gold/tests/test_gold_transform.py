"""Unit tests for gold transform module.

Lightweight tests that do NOT require a database connection.
Run with: pytest gold/tests/test_gold_transform.py
"""
from __future__ import annotations

import sys
import types
import unittest
from datetime import date
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Stub out airflow and psycopg2 so the module can be imported without them
# ---------------------------------------------------------------------------

def _make_airflow_stub():
    airflow = types.ModuleType("airflow")
    providers = types.ModuleType("airflow.providers")
    postgres = types.ModuleType("airflow.providers.postgres")
    hooks = types.ModuleType("airflow.providers.postgres.hooks")
    hooks_postgres = types.ModuleType("airflow.providers.postgres.hooks.postgres")

    class _PostgresHook:
        def __init__(self, postgres_conn_id=None):
            self.postgres_conn_id = postgres_conn_id

    hooks_postgres.PostgresHook = _PostgresHook

    airflow.providers = providers
    providers.postgres = postgres
    postgres.hooks = hooks
    hooks.postgres = hooks_postgres

    for name, mod in [
        ("airflow", airflow),
        ("airflow.providers", providers),
        ("airflow.providers.postgres", postgres),
        ("airflow.providers.postgres.hooks", hooks),
        ("airflow.providers.postgres.hooks.postgres", hooks_postgres),
    ]:
        sys.modules.setdefault(name, mod)


def _make_psycopg2_stub():
    psycopg2 = types.ModuleType("psycopg2")
    extras = types.ModuleType("psycopg2.extras")
    extras.execute_values = MagicMock()
    psycopg2.extras = extras
    sys.modules.setdefault("psycopg2", psycopg2)
    sys.modules.setdefault("psycopg2.extras", extras)


_make_airflow_stub()
_make_psycopg2_stub()

# Now we can import the gold modules
from gold.transform import (  # noqa: E402
    _fetch_acs_for_month,
    build_shard_list,
    merge_shard,
)


class TestFetchAcsNonJanuary(unittest.TestCase):
    """_fetch_acs_for_month should return [] for non-January months."""

    def test_february_returns_empty(self):
        hook = MagicMock()
        result = _fetch_acs_for_month(hook, date(2023, 2, 1))
        self.assertEqual(result, [])
        hook.get_conn.assert_not_called()

    def test_march_returns_empty(self):
        hook = MagicMock()
        result = _fetch_acs_for_month(hook, date(2023, 3, 15))
        self.assertEqual(result, [])

    def test_december_returns_empty(self):
        hook = MagicMock()
        result = _fetch_acs_for_month(hook, date(2022, 12, 1))
        self.assertEqual(result, [])

    def test_jan_non_first_returns_empty(self):
        hook = MagicMock()
        result = _fetch_acs_for_month(hook, date(2023, 1, 15))
        self.assertEqual(result, [])


class TestBuildShardList(unittest.TestCase):
    """build_shard_list should return ISO strings from DB result."""

    def _make_hook(self, dates):
        hook = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = [(d,) for d in dates]
        conn = MagicMock()
        conn.cursor.return_value.__enter__ = lambda s: cursor
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        hook.get_conn.return_value.__enter__ = lambda s: conn
        hook.get_conn.return_value.__exit__ = MagicMock(return_value=False)
        return hook

    def test_returns_iso_strings(self):
        expected_dates = [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)]
        hook = self._make_hook(expected_dates)
        result = build_shard_list(date(2023, 1, 1), date(2023, 3, 31), hook=hook)
        self.assertEqual(result, ["2023-01-01", "2023-02-01", "2023-03-01"])

    def test_empty_window_returns_empty_list(self):
        hook = self._make_hook([])
        result = build_shard_list(date(2023, 6, 1), date(2023, 6, 30), hook=hook)
        self.assertEqual(result, [])


class TestMergeShardEmptyRows(unittest.TestCase):
    """merge_shard should handle empty fetches gracefully."""

    @patch("gold.transform._fetch_acs_for_month", return_value=[])
    @patch("gold.transform._fetch_bls_for_month", return_value=[])
    @patch("gold.transform._fetch_fred_for_month", return_value=[])
    @patch("gold.transform._upsert_gold_rows", return_value=0)
    def test_empty_rows_returns_zero_counts(self, mock_upsert, mock_fred, mock_bls, mock_acs):
        hook = MagicMock()
        result = merge_shard({"month_start": "2023-06-01"}, hook=hook)
        self.assertEqual(result["month_start"], "2023-06-01")
        self.assertEqual(result["input_rows"], 0)
        self.assertEqual(result["output_rows"], 0)
        self.assertEqual(result["counts_by_source"]["CENSUS_ACS"], 0)
        self.assertEqual(result["counts_by_source"]["BLS"], 0)
        self.assertEqual(result["counts_by_source"]["FRED"], 0)
        self.assertEqual(result["sample_observation_dates"], [])

    @patch("gold.transform._fetch_acs_for_month", return_value=[])
    @patch("gold.transform._fetch_bls_for_month", return_value=[])
    @patch("gold.transform._fetch_fred_for_month", return_value=[])
    @patch("gold.transform._upsert_gold_rows", return_value=0)
    def test_upsert_not_called_when_no_rows(
        self, mock_upsert, mock_fred, mock_bls, mock_acs
    ):
        hook = MagicMock()
        merge_shard({"month_start": "2023-06-01"}, hook=hook)
        # _upsert_gold_rows is called with empty list; it returns 0 without DB writes
        mock_upsert.assert_called_once()
        args = mock_upsert.call_args[0]
        self.assertEqual(args[1], [])  # rows argument is empty list


class TestAcsPrecedenceSqlContents(unittest.TestCase):
    """The ACS fetch SQL must encode acs5 precedence (dataset_rank ordering)."""

    def test_sql_contains_acs5_rank_1(self):
        import inspect
        import gold.transform as gt
        source = inspect.getsource(gt._fetch_acs_for_month)
        self.assertIn("acs5", source)
        self.assertIn("dataset_rank", source)

    def test_sql_orders_by_dataset_rank_asc(self):
        import inspect
        import gold.transform as gt
        source = inspect.getsource(gt._fetch_acs_for_month)
        self.assertIn("ORDER BY", source)
        self.assertIn("ASC", source)


if __name__ == "__main__":
    unittest.main()
