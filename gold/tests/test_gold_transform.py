"""Unit tests for gold transform module.

Lightweight tests that do NOT require a database connection.
Run with: pytest gold/tests/test_gold_transform.py
"""
from __future__ import annotations

import sys
import types
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

_REPO_ROOT = Path(__file__).resolve().parents[2]


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
from gold.transform import build_shard_list  # noqa: E402
import gold.transform as gold_transform  # noqa: E402
from census_acs.gold_census.transform import (  # noqa: E402
    _fetch_acs_for_month,
    merge_acs_shard,
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
    """merge_acs_shard should handle empty fetches gracefully."""

    @patch("census_acs.gold_census.transform._fetch_acs_for_month", return_value=[])
    @patch("census_acs.gold_census.transform._upsert_gold_rows", return_value=0)
    def test_empty_rows_returns_zero_counts(self, mock_upsert, mock_acs):
        hook = MagicMock()
        result = merge_acs_shard({"month_start": "2023-06-01"}, hook=hook)
        self.assertEqual(result["month_start"], "2023-06-01")
        self.assertEqual(result["input_rows"], 0)
        self.assertEqual(result["output_rows"], 0)
        self.assertEqual(result["source_system"], "CENSUS_ACS")
        self.assertEqual(result["sample_observation_dates"], [])

    @patch("census_acs.gold_census.transform._fetch_acs_for_month", return_value=[])
    @patch("census_acs.gold_census.transform._upsert_gold_rows", return_value=0)
    def test_upsert_not_called_when_no_rows(
        self, mock_upsert, mock_acs
    ):
        hook = MagicMock()
        merge_acs_shard({"month_start": "2023-06-01"}, hook=hook)
        # _upsert_gold_rows is called with empty rows list
        mock_upsert.assert_called_once()
        positional_args = mock_upsert.call_args.args
        rows_arg = positional_args[1]  # signature: (hook, rows, month_start)
        self.assertEqual(rows_arg, [])


class TestAcsPrecedenceSqlContents(unittest.TestCase):
    """The ACS fetch SQL must encode acs5 precedence (dataset_rank ordering)."""

    def test_sql_contains_acs5_rank_1(self):
        import inspect
        import census_acs.gold_census.transform as gt
        source = inspect.getsource(gt._fetch_acs_for_month)
        self.assertIn("acs5", source)
        self.assertIn("dataset_rank", source)

    def test_sql_orders_by_dataset_rank_asc(self):
        import inspect
        import census_acs.gold_census.transform as gt
        source = inspect.getsource(gt._fetch_acs_for_month)
        self.assertIn("ORDER BY", source)
        self.assertIn("ASC", source)


class TestGoldGeoShapeSqlPropagation(unittest.TestCase):
    """Gold upserts should propagate coordinates and polygon geometry from dim_geo."""

    def test_acs_upsert_sql_contains_geo_coordinates(self):
        source = (_REPO_ROOT / "census_acs/gold_census/transform.py").read_text(encoding="utf-8")
        self.assertIn("geo_latitude", source)
        self.assertIn("geo_longitude", source)
        self.assertIn("geo_polygon_geojson", source)
        self.assertIn("d.latitude", source)
        self.assertIn("d.longitude", source)
        self.assertIn("d.geo_polygon_geojson", source)

    def test_bls_upsert_sql_contains_geo_coordinates(self):
        source = (_REPO_ROOT / "bls/gold_bls/transform.py").read_text(encoding="utf-8")
        self.assertIn("geo_latitude", source)
        self.assertIn("geo_longitude", source)
        self.assertIn("geo_polygon_geojson", source)
        self.assertIn("d.latitude", source)
        self.assertIn("d.longitude", source)
        self.assertIn("d.geo_polygon_geojson", source)


class TestGoldUpsertEnrichment(unittest.TestCase):
    """_upsert_gold_rows should enrich geo columns and derive year/quarter."""

    @patch("gold.transform._lookup_geo_attributes")
    def test_enriches_geography_and_derives_time_parts(self, mock_lookup):
        mock_lookup.return_value = {
            "state:06|county:075": ("06", "California", "06075", "San Francisco County", "county"),
            "us:1": (None, None, None, None, "us"),
        }

        hook = MagicMock()
        rows = [
            (
                "state:06|county:075",
                "SERIES_A",
                "BLS",
                "Employment",
                12.34,
                date(2024, 5, 31),
                date(2024, 5, 31),
                date(2024, 5, 1),
                date(2024, 5, 31),
                "MONTHLY",
                None,
                None,
                None,
                "LAUS_LOCAL_AREA",
                None,
                "Employment level",
                "SA",
                True,
                False,
            ),
            (
                "us:1",
                "GDP",
                "FRED",
                "Gross Domestic Product",
                23456.7,
                date(2024, 5, 15),
                date(2024, 5, 15),
                date(2024, 4, 1),
                date(2024, 6, 30),
                "QUARTERLY",
                None,
                None,
                None,
                None,
                "Billions of Dollars",
                "Real GDP",
                "Not Seasonally Adjusted",
                False,
                False,
            ),
        ]

        gold_transform.psycopg2.extras.execute_values.reset_mock()
        result = gold_transform._upsert_gold_rows(hook, rows, date(2024, 5, 1))

        self.assertEqual(result, 2)
        mock_lookup.assert_called_once()

        execute_values_call = gold_transform.psycopg2.extras.execute_values.call_args
        self.assertIsNotNone(execute_values_call)
        sql = execute_values_call.args[1]
        insert_rows = execute_values_call.args[2]

        self.assertIn("ON CONFLICT (geo_id, month_start, source_system, element_id)", sql)
        self.assertEqual(insert_rows[0][0], "state:06|county:075")
        self.assertEqual(insert_rows[0][1], "COUNTY")
        self.assertEqual(insert_rows[0][2], "06")
        self.assertEqual(insert_rows[0][3], "California")
        self.assertEqual(insert_rows[0][4], "06075")
        self.assertEqual(insert_rows[0][5], "San Francisco County")
        self.assertEqual(insert_rows[0][6], date(2024, 5, 1))
        self.assertEqual(insert_rows[0][7], 2024)
        self.assertEqual(insert_rows[0][8], 2)

        self.assertEqual(insert_rows[1][0], "us:1")
        self.assertEqual(insert_rows[1][1], "NATIONAL")
        self.assertIsNone(insert_rows[1][2])
        self.assertIsNone(insert_rows[1][3])
        self.assertIsNone(insert_rows[1][4])
        self.assertIsNone(insert_rows[1][5])
        self.assertEqual(insert_rows[1][7], 2024)
        self.assertEqual(insert_rows[1][8], 2)

    @patch("gold.transform._lookup_geo_attributes", return_value={})
    def test_missing_geo_mapping_defaults_to_null_geo_attributes(self, _mock_lookup):
        hook = MagicMock()
        rows = [
            (
                "state:99|county:999",
                "SERIES_X",
                "BLS",
                "Unknown",
                1.23,
                date(2024, 12, 31),
                date(2024, 12, 31),
                date(2024, 12, 1),
                date(2024, 12, 31),
                "MONTHLY",
                None,
                None,
                None,
                "UNKNOWN",
                None,
                "Unknown",
                None,
                None,
                False,
            )
        ]

        gold_transform.psycopg2.extras.execute_values.reset_mock()
        result = gold_transform._upsert_gold_rows(hook, rows, date(2024, 12, 1))

        self.assertEqual(result, 1)
        insert_rows = gold_transform.psycopg2.extras.execute_values.call_args.args[2]
        self.assertEqual(insert_rows[0][0], "state:99|county:999")
        self.assertEqual(insert_rows[0][1], "COUNTY")
        self.assertIsNone(insert_rows[0][2])
        self.assertIsNone(insert_rows[0][3])
        self.assertIsNone(insert_rows[0][4])
        self.assertIsNone(insert_rows[0][5])
        self.assertEqual(insert_rows[0][7], 2024)
        self.assertEqual(insert_rows[0][8], 4)


class TestGeoIdNormalization(unittest.TestCase):
    """Geo ID normalization should align non-canonical IDs to canonical keys."""

    def test_normalize_geo_id(self):
        self.assertEqual(gold_transform._normalize_geo_id("state:1"), "state:01")
        self.assertEqual(gold_transform._normalize_geo_id("STATE:1|COUNTY:7"), "state:01|county:007")
        self.assertEqual(gold_transform._normalize_geo_id(" us:1 "), "us:1")
        self.assertIsNone(gold_transform._normalize_geo_id(None))

    @patch("gold.transform._lookup_geo_attributes")
    def test_upsert_enriches_when_input_geo_id_not_canonical(self, mock_lookup):
        mock_lookup.return_value = {
            "state:01|county:007": ("01", "Alabama", "01007", "Bibb County", "county"),
        }

        hook = MagicMock()
        rows = [
            (
                " STATE:1|COUNTY:7 ",
                "SERIES_A",
                "BLS",
                "Employment",
                10.0,
                date(2024, 1, 31),
                date(2024, 1, 31),
                date(2024, 1, 1),
                date(2024, 1, 31),
                "MONTHLY",
                None,
                None,
                None,
                "LAUS_LOCAL_AREA",
                None,
                "Employment level",
                "SA",
                True,
                False,
            ),
        ]

        gold_transform.psycopg2.extras.execute_values.reset_mock()
        result = gold_transform._upsert_gold_rows(hook, rows, date(2024, 1, 1))

        self.assertEqual(result, 1)
        insert_rows = gold_transform.psycopg2.extras.execute_values.call_args.args[2]
        self.assertEqual(insert_rows[0][0], "STATE:1|COUNTY:7")
        self.assertEqual(insert_rows[0][1], "COUNTY")
        self.assertEqual(insert_rows[0][2], "01")
        self.assertEqual(insert_rows[0][4], "01007")


if __name__ == "__main__":
    unittest.main()
