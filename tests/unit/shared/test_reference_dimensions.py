"""Deterministic contracts for production time and geography reference loaders."""

from __future__ import annotations

import io
import zipfile
from datetime import date

import httpx
import pytest
import shapefile

from data_ingestion_toolbox.silver_ref import geography, time_dim

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


def test_time_dimension_loader_emits_exact_leap_and_calendar_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-024, ETL-025 — production time rows reconcile exactly."""
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
    assert cursor.rows[1]["day_name"] == "Thursday"
    assert cursor.rows[2]["is_month_start"] is True
    assert all(row["quarter"] == 1 for row in cursor.rows)


def test_legacy_county_parsers_preserve_zero_padding_and_coordinates() -> None:
    """Covers: ETL-002, ETL-024 — legacy Gazetteer formats retain exact keys."""
    row_1990 = "55   025 Dane County".ljust(69) + " 043066700" + "-089400000"
    parsed_1990 = geography._parse_1990_counties(row_1990).to_dicts()
    assert parsed_1990 == [
        {
            "GEOID": "55025",
            "NAME": "Dane County",
            "INTPTLAT": pytest.approx(43.0667),
            "INTPTLONG": pytest.approx(-89.4),
        }
    ]

    row_2000 = "  55025Dane County".ljust(73) + "43.066700 -89.400000"
    parsed_2000 = geography._parse_2000_counties(row_2000).to_dicts()
    assert parsed_2000 == [
        {
            "GEOID": "55025",
            "NAME": "Dane County",
            "INTPTLAT": "43.066700",
            "INTPTLONG": "-89.400000",
        }
    ]


def _boundary_zip() -> bytes:
    shp = io.BytesIO()
    shx = io.BytesIO()
    dbf = io.BytesIO()
    writer = shapefile.Writer(shp=shp, shx=shx, dbf=dbf, shapeType=shapefile.POLYGON)
    writer.field("STATEFP", "C", size=2)
    writer.field("COUNTYFP", "C", size=3)
    writer.poly(
        [[[-89.5, 43.0], [-89.3, 43.0], [-89.3, 43.2], [-89.5, 43.2], [-89.5, 43.0]]]
    )
    writer.record("55", "025")
    writer.close()
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("county.shp", shp.getvalue())
        archive.writestr("county.shx", shx.getvalue())
        archive.writestr("county.dbf", dbf.getvalue())
    return payload.getvalue()


def test_boundary_zip_parser_returns_valid_feature_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-024 — production boundary parser reads shapefile features."""
    response = httpx.Response(
        200,
        content=_boundary_zip(),
        request=httpx.Request("GET", "https://fixture.invalid/county.zip"),
    )

    class Client:
        def __init__(self, **_kwargs) -> None:
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def get(self, _url: str) -> httpx.Response:
            return response

    monkeypatch.setattr(geography.httpx, "Client", Client)
    features = geography._fetch_boundary_features(
        "https://fixture.invalid/county.zip", retries=1
    )

    assert len(features) == 1
    assert features[0]["properties"] == {"STATEFP": "55", "COUNTYFP": "025"}
    assert features[0]["geometry"]["type"] == "Polygon"
    assert features[0]["geometry"]["coordinates"]
