"""Focused coverage for capture-first transform source selection."""

from __future__ import annotations

import importlib

import pytest

from data_ingestion_toolbox.bls.silver_bls import transform as bls_transform
from data_ingestion_toolbox.census_acs.silver_census import (
    transform as census_transform,
)

pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, *, one=None, many=None) -> None:
        self.one = one
        self.many = many or []
        self.statement = ""
        self.parameters = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, statement, parameters=None) -> None:
        self.statement = statement
        self.parameters = parameters

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.many


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def cursor(self):
        return self._cursor


class _Hook:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor = cursor

    def get_conn(self):
        return _Connection(self.cursor)


def test_revision_counts_and_years_never_query_legacy_raw() -> None:
    """Covers: DB-008, DB-009 — sizing queries use revisions exclusively."""
    count_cursor = _Cursor(one=(7,))
    assert bls_transform._get_program_row_count(_Hook(count_cursor), "la") == 7
    assert "silver_bls.observation_revision" in count_cursor.statement
    assert "raw_bls" not in count_cursor.statement

    years_cursor = _Cursor(many=[(2022,), (2023,)])
    assert bls_transform._get_program_years(_Hook(years_cursor), "la") == [2022, 2023]
    assert "raw_bls" not in years_cursor.statement

    census_cursor = _Cursor(one=(11,))
    assert census_transform._get_approx_row_count(_Hook(census_cursor)) == 11
    assert "silver_census" in census_cursor.statement
    assert "raw_census" not in census_cursor.statement


def test_source_gold_modules_load_without_legacy_compatibility() -> None:
    """Covers: ARC-001 — source gold modules retain independent contracts."""
    from data_ingestion_toolbox.bls.gold_bls import transform as bls_gold
    from data_ingestion_toolbox.census_acs.gold_census import transform as census_gold

    assert importlib.reload(bls_gold)._REQUIRED_RELATIONS
    assert importlib.reload(census_gold)._REQUIRED_RELATIONS
