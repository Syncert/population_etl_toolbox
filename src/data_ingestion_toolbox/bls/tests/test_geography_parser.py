"""Unit tests for canonical LAUS series and geography parsing."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.bls.geography import (
    build_laus_series_id,
    get_laus_series_ids,
    parse_laus_series_id,
)
from data_ingestion_toolbox.bls.silver_bls.geography_parser import parse_bls_geography


@pytest.mark.parametrize(
    ("series_id", "geo_level", "state_fips", "county_fips", "geo_id"),
    [
        (
            "LAUST010000000000003",
            "state",
            "01",
            None,
            "state:01",
        ),
        (
            "LAUCN010010000000003",
            "county",
            "01",
            "001",
            "state:01|county:001",
        ),
        (
            "LAUCN281070000000003",
            "county",
            "28",
            "107",
            "state:28|county:107",
        ),
    ],
)
def test_parse_official_laus_examples(
    series_id: str,
    geo_level: str,
    state_fips: str,
    county_fips: str | None,
    geo_id: str,
) -> None:
    parsed = parse_laus_series_id(series_id)

    assert parsed["program"] == "LA"
    assert parsed["seasonal"] == "U"
    assert parsed["area_code"] == series_id[3:18]
    assert parsed["measure_code"] == "03"
    assert parsed["geo_level"] == geo_level
    assert parsed["geo_id"] == geo_id
    assert parsed["state_fips"] == state_fips
    assert parsed["county_fips"] == county_fips


@pytest.mark.parametrize(
    "series_id",
    [
        "",
        "LAUCN01001000000003",
        "LAUCN0100100000000003",
        "LAUCN0100100000000030",
        "XXUCN010010000000003",
    ],
)
def test_invalid_laus_ids_are_not_parsed(series_id: str) -> None:
    parsed = parse_laus_series_id(series_id)

    assert parsed["geo_level"] is None
    assert parsed["geo_id"] is None


@pytest.mark.parametrize(
    "series_id",
    [
        "LAU00000000000000003",  # LAUS has no national series
        "LAUMT123450000000003",  # metro is unsupported by dim_geo
        "LAUCI123450000000003",  # city is unsupported by dim_geo
        "LAUCN010010000100003",  # invalid county padding
    ],
)
def test_unsupported_laus_area_patterns_are_not_recognized(series_id: str) -> None:
    parsed = parse_laus_series_id(series_id)

    assert parsed["geo_level"] is None
    assert parse_bls_geography(series_id, program="la")["geo_level"] is None


@pytest.mark.parametrize(
    ("area_code", "expected"),
    [
        ("ST0100000000000", "LAUST010000000000003"),
        ("CN0100100000000", "LAUCN010010000000003"),
    ],
)
def test_builder_accepts_15_character_area_codes(
    area_code: str, expected: str
) -> None:
    series_id = build_laus_series_id(area_code, "03")

    assert series_id == expected
    assert len(series_id) == 20


@pytest.mark.parametrize(
    "area_code",
    [
        "ST01000000",
        "000000000000000",
        "MT1234500000000",
        "CN0100100000001",
    ],
)
def test_builder_rejects_invalid_or_unsupported_areas(area_code: str) -> None:
    with pytest.raises(ValueError):
        build_laus_series_id(area_code, "03")


def test_raw_and_silver_use_identical_county_geography() -> None:
    pl = pytest.importorskip("polars")
    from data_ingestion_toolbox.bls.ingest import enrich_with_geography

    series_id = "LAUCN281070000000003"
    raw = enrich_with_geography(pl.DataFrame({"series_id": [series_id]}), "la").row(
        0, named=True
    )
    silver = parse_bls_geography(series_id, "la")

    for field in ("geo_level", "geo_id", "state_fips", "county_fips"):
        assert raw[field] == silver[field]


class _FakeCursor:
    def __init__(self, rows: list[tuple[str]]) -> None:
        self.rows = rows
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def execute(self, _sql: str, params: tuple) -> None:
        self.params = params

    def fetchall(self) -> list[tuple[str]]:
        return self.rows


class _FakeConnection:
    def __init__(self, rows: list[tuple[str]]) -> None:
        self.cursor_instance = _FakeCursor(rows)
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


def test_laus_requests_use_only_geography_specific_published_series(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The catalog publishes county measure 03 but not requested measure 07.
    conn = _FakeConnection(
        [
            ("LAUCN010010000000003",),
            ("LAUCN010030000000003",),
        ]
    )
    monkeypatch.setattr(
        "data_ingestion_toolbox.bls.geography._get_pg_connection",
        lambda: conn,
    )

    series_ids = get_laus_series_ids(
        ["03", "07"], geo_level="county", state_fips="01"
    )

    assert series_ids == [
        "LAUCN010010000000003",
        "LAUCN010030000000003",
    ]
    assert conn.cursor_instance.params == ("CN01%", "U", ["03", "07"])
    assert conn.closed


def test_national_laus_requests_are_rejected() -> None:
    with pytest.raises(ValueError, match="CPS/LN"):
        get_laus_series_ids(["03"], geo_level="us")
