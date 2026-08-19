"""Deterministic contracts for shared time and boundary reference parsing."""

from __future__ import annotations

import io
import json
import sys
import zipfile
from datetime import date
from types import ModuleType

import pytest
import shapefile

from data_ingestion_toolbox.silver_ref import time_dim
from data_ingestion_toolbox.silver_ref.geography_pipeline import parse_boundary_capture

pytestmark = pytest.mark.unit


class _RecordingCursor:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, _sql: str, params: dict) -> None:
        self.rows.append(params)


class _RecordingConnection:
    def __init__(self, cursor: _RecordingCursor) -> None:
        self._cursor = cursor
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def cursor(self) -> _RecordingCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1


def test_time_loader_resolves_optional_airflow_hook_lazily(monkeypatch) -> None:
    """Covers: ETL-024 — optional runtime integration remains lazy."""
    created_with: list[str] = []

    class PostgresHook:
        def __init__(self, postgres_conn_id: str) -> None:
            created_with.append(postgres_conn_id)

    names = (
        "airflow",
        "airflow.providers",
        "airflow.providers.postgres",
        "airflow.providers.postgres.hooks",
        "airflow.providers.postgres.hooks.postgres",
    )
    for name in names:
        module = ModuleType(name)
        module.__path__ = []  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, name, module)
    sys.modules[names[-1]].PostgresHook = PostgresHook  # type: ignore[attr-defined]
    assert isinstance(time_dim._get_hook(), PostgresHook)
    assert created_with == [time_dim.CONFIG.postgres_conn_id]


def test_time_dimension_loader_emits_exact_leap_and_calendar_flags(monkeypatch) -> None:
    """Covers: ETL-024, ETL-025 — calendar rows and counts reconcile exactly."""
    cursor = _RecordingCursor()
    connection = _RecordingConnection(cursor)
    monkeypatch.setattr(
        time_dim,
        "_get_hook",
        lambda: type("Hook", (), {"get_conn": lambda _self: connection})(),
    )
    assert time_dim.sync_time_dim(date(2024, 2, 28), date(2024, 3, 1)) == 3
    assert connection.commits == 1
    assert [row["date_key"] for row in cursor.rows] == [
        date(2024, 2, 28),
        date(2024, 2, 29),
        date(2024, 3, 1),
    ]
    assert cursor.rows[1]["is_month_end"] is True
    assert cursor.rows[2]["is_month_start"] is True


def _boundary_zip() -> bytes:
    shp, shx, dbf = io.BytesIO(), io.BytesIO(), io.BytesIO()
    writer = shapefile.Writer(shp=shp, shx=shx, dbf=dbf, shapeType=shapefile.POLYGON)
    writer.field("STATEFP", "C", size=2)
    writer.field("PLACEFP", "C", size=5)
    writer.poly(
        [[[-89.5, 43.0], [-89.3, 43.0], [-89.3, 43.2], [-89.5, 43.2], [-89.5, 43.0]]]
    )
    writer.record("55", "53000")
    writer.close()
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("place.shp", shp.getvalue())
        archive.writestr("place.shx", shx.getvalue())
        archive.writestr("place.dbf", dbf.getvalue())
    return payload.getvalue()


def test_captured_boundary_replays_place_geometry_offline() -> None:
    """Covers: ETL-024 — captured boundaries map to exact canonical identities."""
    records = parse_boundary_capture(
        _boundary_zip(), geo_type="place", boundary_vintage=2025
    )
    assert len(records) == 1
    assert records[0].geo_id == "state:55|place:53000"
    assert json.loads(records[0].geojson)["type"] == "Polygon"
