"""Household grocery spending from the BLS Consumer Expenditure Survey.

Covers: ETL-124
"""

from __future__ import annotations

import json
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
    monkeypatch.setattr(metadata, "_api_catalog_records", lambda program: None)
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


#: BLS API v2's catalog entry for the Midwest series, as the API answered a
#: keyed ``catalog=true`` request from the warehouse (2026-11-09).
MIDWEST_CATALOG = {
    "series_title": "Food at home by Census Region: Region of residence: midwest",
    "series_id": "CXUFOODHOMELB1103M",
    "seasonality": "Not Seasonally Adjusted",
    "survey_name": "Consumer Expenditure Surveys",
    "survey_abbreviation": "CX",
    "measure_data_type": "Expenditures",
    "type_of_cases": "Food",
    "item": "Food at home",
}


def _catalog(series_id: str) -> dict[str, str]:
    return {
        **MIDWEST_CATALOG,
        "series_id": series_id,
        "series_title": f"title {series_id}",
    }


def test_the_spending_series_are_read_from_the_api_catalog_not_the_whole_list(
    monkeypatch,
) -> None:
    """Covers: ETL-124 — one keyed API catalog read stands in for the 32 MB download."""
    wanted = CONFIG.curated_by_program["cx"]
    asked: list[list[str]] = []

    def catalog(series_ids):
        asked.append(list(series_ids))
        return {
            sid: (MIDWEST_CATALOG if sid == "CXUFOODHOMELB1103M" else _catalog(sid))
            for sid in series_ids
        }

    connection = _Connection()
    monkeypatch.setattr(metadata, "fetch_bls_api_catalog", catalog)
    monkeypatch.setattr(
        metadata,
        "fetch_bls_metadata",
        lambda program: pytest.fail("download.bls.gov must not be read"),
    )
    monkeypatch.setattr(metadata, "_get_pg_connection", lambda: connection)
    assert metadata.sync_bls_series_metadata("cx") == 10
    assert asked == [wanted]
    rows = {row[1]: row for row in connection.cursor_.rows}
    assert list(rows) == wanted
    midwest = rows["CXUFOODHOMELB1103M"]
    assert midwest[2] == MIDWEST_CATALOG["series_title"]
    assert midwest[4] == "FOODHOME"
    assert json.loads(midwest[7]) == MIDWEST_CATALOG


@pytest.mark.parametrize("answer", ["missing", "refused"])
def test_a_catalog_the_api_cannot_give_falls_back_to_the_series_list(
    monkeypatch, capsys, answer: str
) -> None:
    """Covers: ETL-124 — a series without a catalog entry, or a refused read, uses download.bls.gov."""
    wanted = CONFIG.curated_by_program["cx"]

    def catalog(series_ids):
        if answer == "refused":
            raise RuntimeError("refused for key secret-key")
        return {sid: _catalog(sid) for sid in series_ids[1:]}

    listed = [
        {"series_id": sid, "series_title": f"title {sid}", "item_code": "FOODHOME"}
        for sid in wanted
    ]
    downloads: list[str] = []

    def download(program):
        downloads.append(program)
        return listed, {}

    connection = _Connection()
    monkeypatch.setattr(metadata, "fetch_bls_api_catalog", catalog)
    monkeypatch.setattr(metadata, "fetch_bls_metadata", download)
    monkeypatch.setattr(metadata, "_get_pg_connection", lambda: connection)
    assert metadata.sync_bls_series_metadata("cx") == 10
    assert downloads == ["cx"]
    assert json.loads(connection.cursor_.rows[0][7]) == listed[0]
    assert "secret-key" not in capsys.readouterr().out


class _Response:
    def __init__(self, body: dict) -> None:
        self.body = body

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.body


def test_the_catalog_request_is_one_keyed_latest_value_read(monkeypatch) -> None:
    """Covers: ETL-124 — the API is asked once, with the catalog, the newest value only and the project's name."""
    sent: list[dict] = []

    def post(url, json, headers, timeout):
        sent.append({"url": url, "json": json, "headers": headers})
        return _Response(
            {
                "status": "REQUEST_SUCCEEDED",
                "Results": {
                    "series": [
                        {"seriesID": "CXUFOODHOMELB1103M", "catalog": MIDWEST_CATALOG},
                        {"seriesID": "CXUFOODHOMELB1104M", "data": []},
                        {"seriesID": "CXUOTHERLB0101M", "catalog": MIDWEST_CATALOG},
                    ]
                },
            }
        )

    monkeypatch.setattr(metadata.requests, "post", post)
    monkeypatch.setattr(CONFIG, "bls_api_key", "fixture-key")
    found = metadata.fetch_bls_api_catalog(["CXUFOODHOMELB1103M", "CXUFOODHOMELB1104M"])
    assert found == {"CXUFOODHOMELB1103M": MIDWEST_CATALOG}
    (request,) = sent
    assert request["url"] == "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    assert request["json"]["catalog"] is True
    assert request["json"]["latest"] is True
    assert request["json"]["registrationkey"] == "fixture-key"
    assert request["headers"]["User-Agent"] == metadata.UA


def test_the_catalog_needs_the_key_and_a_served_request(monkeypatch) -> None:
    """Covers: ETL-124 — no key or an unserved request is a failure, never an empty catalog."""
    assert metadata.fetch_bls_api_catalog([]) == {}
    monkeypatch.setattr(CONFIG, "bls_api_key", "")
    with pytest.raises(RuntimeError, match="BLS_API_KEY"):
        metadata.fetch_bls_api_catalog(["CXUFOODHOMELB1103M"])
    monkeypatch.setattr(CONFIG, "bls_api_key", "fixture-key")
    monkeypatch.setattr(
        metadata.requests,
        "post",
        lambda *a, **k: _Response({"status": "REQUEST_NOT_PROCESSED"}),
    )
    with pytest.raises(RuntimeError, match="not served"):
        metadata.fetch_bls_api_catalog(["CXUFOODHOMELB1103M"])


def test_the_item_code_is_the_one_the_series_id_carries() -> None:
    """Covers: ETL-124 — a catalog entry has no item code; the id's own is used."""
    assert (
        metadata._series_item_code("cx", {"series_id": "CXUFOODHOMELB1103M"})
        == "FOODHOME"
    )
    assert metadata._series_item_code("cx", {"series_id": "CXUFOODHOME"}) == ""
    assert (
        metadata._series_item_code("cx", {"item_code": "X", "series_id": "CXUALB1M"})
        == "X"
    )
    assert metadata._series_item_code("ap", {"series_id": "APU0300709112"}) == ""
