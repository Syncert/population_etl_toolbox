"""Household grocery spending from the BLS Consumer Expenditure Survey.

Covers: ETL-124
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from data_ingestion_toolbox.bls import metadata
from data_ingestion_toolbox.bls.config import CONFIG
from data_ingestion_toolbox.bls.silver_bls.geography_parser import parse_bls_geography
from data_ingestion_toolbox.bls.silver_bls.time_utils import parse_bls_period_to_date

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]


def test_the_food_at_home_means_are_curated_for_the_nation_regions_and_household_sizes() -> (
    None
):
    """Covers: ETL-124 — ten series ids from BLS's own list, every one food at home."""
    assert "cx" in CONFIG.programs
    series = CONFIG.curated_by_program["cx"]
    assert series == [
        "CXUFOODHOMELB1101M",
        "CXUFOODHOMELB1102M",
        "CXUFOODHOMELB1103M",
        "CXUFOODHOMELB1104M",
        "CXUFOODHOMELB1105M",
        "CXUFOODHOMELB0502M",
        "CXUFOODHOMELB0504M",
        "CXUFOODHOMELB0505M",
        "CXUFOODHOMELB0506M",
        "CXUFOODHOMELB0507M",
    ]


@pytest.mark.parametrize(
    ("series_id", "geo_level", "geo_id"),
    [
        ("CXUFOODHOMELB1101M", "us", "us:1"),
        ("CXUFOODHOMELB1102M", "census_region", "region:1"),
        ("CXUFOODHOMELB1103M", "census_region", "region:2"),
        ("CXUFOODHOMELB1104M", "census_region", "region:3"),
        ("CXUFOODHOMELB1105M", "census_region", "region:4"),
        ("CXUFOODHOMELB0506M", "us", "us:1"),
    ],
)
def test_a_spending_mean_names_its_region_or_the_nation(
    series_id: str, geo_level: str, geo_id: str
) -> None:
    """Covers: ETL-124 — the region table's characteristic is the Census region, by code."""
    parsed = parse_bls_geography(series_id, "cx")
    assert (parsed["geo_level"], parsed["geo_id"]) == (geo_level, geo_id)


@pytest.mark.parametrize(
    "series_id", ["CXUFOODHOMELB1106M", "CXUFOODHOME", "APU0300709112"]
)
def test_a_series_of_another_shape_has_no_geography(series_id: str) -> None:
    """Covers: ETL-124 — an unknown region code or shape is refused, never guessed."""
    assert parse_bls_geography(series_id, "cx")["geo_id"] is None


def test_an_annual_mean_covers_its_calendar_year() -> None:
    """Covers: ETL-124 — `A01` is the whole year, never a month."""
    assert parse_bls_period_to_date(2025, "A01")[1:] == (
        date(2025, 1, 1),
        date(2025, 12, 31),
    )


class _Cursor:
    def __init__(self) -> None:
        self.rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return None

    def execute(self, _sql: str, params: tuple) -> None:
        self.rows.append(params)


class _Connection:
    def __init__(self) -> None:
        self.cursor_ = _Cursor()

    def cursor(self) -> _Cursor:
        return self.cursor_

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


def test_only_the_configured_spending_series_are_synced(monkeypatch) -> None:
    """Covers: ETL-124 — the 32 MB series list keeps its ten rows, not every table (storage rule)."""
    listed = [
        {"series_id": sid, "series_title": f"title {sid}", "item_code": "FOODHOME"}
        for sid in [
            *CONFIG.curated_by_program["cx"],
            "CXU001000LB0101M",
            "CXUFOODHOMELB0101M",
        ]
    ]
    connection = _Connection()
    monkeypatch.setattr(metadata, "fetch_bls_metadata", lambda program: (listed, {}))
    monkeypatch.setattr(metadata, "_get_pg_connection", lambda: connection)
    assert metadata.sync_bls_series_metadata("cx") == 10
    stored = [row[1] for row in connection.cursor_.rows]
    assert stored == CONFIG.curated_by_program["cx"]
    assert {row[4] for row in connection.cursor_.rows} == {"FOODHOME"}


def test_the_series_list_is_asked_for_with_the_projects_address_not_a_placeholder() -> (
    None
):
    """Covers: ETL-124 — download.bls.gov is told who is asking: the project, never a person."""
    assert "example.com" not in metadata.UA
    assert "github.com/Syncert/population_etl_toolbox" in metadata.UA


def test_spending_is_its_own_observation_basis() -> None:
    """Covers: ETL-124 — the survey row says spending, not prices, and the CHECK allows it."""
    transform = (
        ROOT / "src/data_ingestion_toolbox/bls/gold_bls/transform.py"
    ).read_text()
    ddl = (
        ROOT / "src/data_ingestion_toolbox/bls/gold_bls/DDL/gold_bls.sql"
    ).read_text()
    assert (
        "('CX', 'Consumer Expenditure Survey', 'Consumer units (households)', 'SPENDING',"
        in transform
    )
    assert "'FLOWS', 'SPENDING')" in ddl
    assert (
        "WHEN f.program = 'cx' THEN 'U.S. dollars per consumer unit per year'"
        in transform
    )
