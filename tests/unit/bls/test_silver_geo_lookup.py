"""Regression contracts for the BLS silver geography lookup frame."""

from __future__ import annotations

import polars as pl
import pytest

from data_ingestion_toolbox.bls.silver_bls import transform

pytestmark = pytest.mark.unit


class _Cursor:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *args: object) -> bool:
        return False


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _Cursor:
        return self._cursor

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *args: object) -> bool:
        return False


class _Hook:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def get_conn(self) -> _Connection:
        return _Connection(_Cursor(self._rows))


def _observations() -> pl.DataFrame:
    return pl.DataFrame(
        {"geo_level": ["state"], "geo_id": ["state:97"], "value": [1.0]}
    )


def test_empty_geo_lookup_input_yields_typed_joinable_frame() -> None:
    """Covers: ETL-024 — an empty lookup keeps string keys, so joins succeed."""
    looked_up = transform._load_geo_dim_for_list(_Hook([]), pl.DataFrame())

    assert looked_up.schema == transform._GEO_DIM_SCHEMA

    joined = _observations().join(looked_up, on=["geo_level", "geo_id"], how="left")
    assert joined.get_column("geo_sk").to_list() == [None]


def test_unmatched_geographies_join_as_unresolved_not_schema_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-024 — zero dimension matches flow to unresolved accounting."""
    monkeypatch.setattr(transform, "execute_values", lambda *args, **kwargs: None)

    looked_up = transform._load_geo_dim_for_list(_Hook([]), _observations())

    assert looked_up.schema == transform._GEO_DIM_SCHEMA
    joined = _observations().join(looked_up, on=["geo_level", "geo_id"], how="left")
    assert joined.height == 1
    assert joined.get_column("geo_sk").to_list() == [None]


def test_matched_geographies_receive_their_surrogate_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-024 — a matching dimension row supplies its surrogate key."""
    monkeypatch.setattr(transform, "execute_values", lambda *args, **kwargs: None)

    looked_up = transform._load_geo_dim_for_list(
        _Hook([(7, "state", "state:97")]), _observations()
    )

    joined = _observations().join(looked_up, on=["geo_level", "geo_id"], how="left")
    assert joined.get_column("geo_sk").to_list() == [7]
