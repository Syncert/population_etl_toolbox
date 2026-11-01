"""The age structure summary and its one-indicator map read (ADR-0013).

Covers: ETL-109 (the summary builder writes, for every place a release
        serves, the same group shares, ratios and margins `/age-structure`
        answers, null with the reason where a line is withheld, and a place
        with no total line is not given a row), API-187 (`/age-structure/map`
        answers one indicator for every place of a grain at the grain's newest
        summary release, cities and tracts one state at a time, a withheld
        value listed with its reason, and refuses an unknown indicator or
        grain).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.services import age_structure_service as service
from data_ingestion_toolbox.census_acs.gold_census import age_summary
from data_ingestion_toolbox.semantics.age_bands import default_scheme
from tests.unit.api.test_age_structure import DANE, MADISON, WISCONSIN, _rows

pytestmark = pytest.mark.unit

SCHEME = default_scheme()


def _as(value: Decimal | None, quantum: str) -> str | None:
    """The summary's NUMERIC as the page prints it."""
    return None if value is None else str(service._text(value, quantum))


def _by_geo(rows: list[dict]) -> dict[str, dict]:
    return {row["geo_id"]: row for row in rows}


def test_the_summary_row_is_the_number_the_place_page_prints() -> None:
    """Covers: ETL-109"""
    lines = [
        *_rows(DANE, 2023, bump={"B01001_020": 400, "B01001_044": 900}),
        *_rows(WISCONSIN, 2023, scale=40_000, moe=400),
    ]
    for row in lines:
        row["state_fips"] = "55"
    rows = _by_geo(age_summary.summary_rows(SCHEME, lines, dataset_code="acs5"))
    assert set(rows) == {DANE["geo_id"], WISCONSIN["geo_id"]}

    releases = service.releases_from_rows(lines)
    for geo_id, row in rows.items():
        page = service.indicators_for(SCHEME, releases[(geo_id, 2023)])
        assert row["state_fips"] == "55"
        assert row["scheme_id"] == SCHEME.scheme_id
        assert row["scheme_version"] == SCHEME.version
        for group in age_summary.GROUP_IDS:
            served = getattr(page, group)
            assert _as(row[f"{group}_share"], "0.01") == served.share
            assert _as(row[f"{group}_share_moe"], "0.01") == served.share_moe
        for ratio in age_summary.RATIO_IDS:
            served = getattr(page, ratio)
            assert _as(row[ratio], "0.1") == served.value
            assert _as(row[f"{ratio}_moe"], "0.1") == served.moe
        assert row["median_age"] == Decimal(35)
    # A controlled state total carries a zero margin, as the page treats it.
    assert rows[WISCONSIN["geo_id"]]["total_moe"] == 0


def test_a_withheld_line_leaves_its_values_null_with_the_reason() -> None:
    """Covers: ETL-109"""
    lines = _rows(DANE, 2023)
    for row in lines:
        row["state_fips"] = "55"
        if row["metric_code"].endswith("B01001_021"):  # men 67 to 69
            row["estimate_value"] = None
            row["value_status"] = "suppressed"
    (row,) = age_summary.summary_rows(SCHEME, lines, dataset_code="acs5")
    assert row["age_65_plus_share"] is None
    assert row["age_65_plus_unavailable_reason"] == "suppressed_input"
    assert row["old_age_dependency_ratio"] is None
    assert row["old_age_dependency_ratio_unavailable_reason"] == "suppressed_input"
    # Groups that do not hold the line are still served.
    assert row["under_18_share"] is not None
    assert row["child_dependency_ratio"] is not None


def test_a_place_with_no_total_line_gets_no_row() -> None:
    """Covers: ETL-109"""
    lines = [
        r for r in _rows(DANE, 2023) if not r["metric_code"].endswith("B01001_001")
    ]
    assert age_summary.summary_rows(SCHEME, lines, dataset_code="acs5") == []


def test_the_copy_payload_writes_every_column_once_and_nulls_as_null() -> None:
    """Covers: ETL-109"""
    lines = _rows(DANE, 2023)
    rows = age_summary.summary_rows(SCHEME, lines, dataset_code="acs5")
    (line,) = age_summary.copy_payload(rows).read().splitlines()
    fields = line.split("\t")
    assert len(fields) == len(age_summary.COLUMNS)
    assert fields[age_summary.COLUMNS.index("state_fips")] == r"\N"
    assert fields[age_summary.COLUMNS.index("observation_date")] == "2023-01-01"


def test_every_summary_read_names_every_index_column() -> None:
    """Covers: ETL-109 -- each state's read is an index scan on
    (metric_code, geo_level, state_fips, vintage_year) inside one partition."""
    for sql in (age_summary.STATE_LINES_SQL, age_summary.STATELESS_LINES_SQL):
        for condition in (
            "metric_code = ANY(",
            "geo_level = ANY(",
            "vintage_year = %(vintage_year)s",
            "observation_date = %(observation_date)s",
        ):
            assert condition in sql
    assert "state_fips = %(state_fips)s" in age_summary.STATE_LINES_SQL
    assert "state_fips IS NULL" in age_summary.STATELESS_LINES_SQL


# --- The one-indicator map read ----------------------------------------------


class _Result:
    def __init__(self, rows: list[dict], scalar: Any = None) -> None:
        self._rows = rows
        self._scalar = scalar

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def scalar(self):
        return self._scalar


class _SummarySession:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.statements: list[tuple[str, dict]] = []

    def execute(self, query, params=None):
        params = params or {}
        sql = str(query)
        self.statements.append((sql, params))
        if "to_regclass" in sql or "pg_class" in sql:
            return _Result([{"exists": True}], scalar=True)
        matching = [
            r
            for r in self.rows
            if r["dataset_code"] == params["dataset"]
            and r["geo_level"] == params["geo_level"]
            and (
                params["state_fips"] is None or r["state_fips"] == params["state_fips"]
            )
        ]
        if query is service.NEWEST_SUMMARY_RELEASE_QUERY:
            years = [r["vintage_year"] for r in matching]
            return _Result([], scalar=max(years) if years else None)
        if service.SUMMARY_RELATION in sql:
            return _Result(
                [r for r in matching if r["vintage_year"] == params["vintage_year"]]
            )
        raise AssertionError(sql)  # pragma: no cover


def _summary(geo_id: str, level: str, state: str | None, vintage: int, **values):
    return {
        "geo_id": geo_id,
        "geo_level": level,
        "state_fips": state,
        "dataset_code": "acs5",
        "vintage_year": vintage,
        "period_start": f"{vintage - 4}-01-01",
        "period_end": f"{vintage}-12-31",
        "value": None,
        "moe": None,
        "unavailable_reason": None,
        **values,
    }


@pytest.fixture
def map_client(monkeypatch):
    monkeypatch.setattr(service, "require_relation", lambda db, relation: None)

    def make(session: _SummarySession) -> TestClient:
        app.dependency_overrides[get_db_session_dep] = lambda: session
        return TestClient(app)

    yield make
    app.dependency_overrides.clear()


def test_a_county_map_reads_the_grains_newest_release(map_client) -> None:
    """Covers: API-187"""
    session = _SummarySession(
        [
            _summary(
                DANE["geo_id"],
                "COUNTY",
                "55",
                2023,
                value=Decimal("24.567"),
                moe=Decimal("1.04"),
            ),
            _summary(
                "state:55|county:105",
                "COUNTY",
                "55",
                2023,
                unavailable_reason="suppressed_input",
            ),
            _summary(DANE["geo_id"], "COUNTY", "55", 2022, value=Decimal("23")),
        ]
    )
    response = map_client(session).get(
        "/api/v1/age-structure/map",
        params={"indicator": "old_age_dependency_ratio", "geo_level": "COUNTY"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["release"] == "2023"
    assert body["derived"] is True
    assert body["unit"] == "per 100 people aged 18 to 64"
    assert body["label"] == "Old-age dependency ratio"
    assert body["items"] == [
        {
            "geo_id": DANE["geo_id"],
            "value": "24.6",
            "moe": "1.0",
            "unavailable_reason": None,
        },
        {
            "geo_id": "state:55|county:105",
            "value": None,
            "moe": None,
            "unavailable_reason": "suppressed_input",
        },
    ]
    reads = [s for s, _ in session.statements if service.SUMMARY_RELATION in s]
    assert len(reads) == 2, "the newest release, then the one read of its rows"


def test_a_city_map_is_drawn_one_state_at_a_time(map_client) -> None:
    """Covers: API-187"""
    session = _SummarySession(
        [
            _summary(
                MADISON["geo_id"],
                "PLACE",
                "55",
                2023,
                value=Decimal("11.5"),
                moe=Decimal("0.4"),
            )
        ]
    )
    client = map_client(session)
    refused = client.get(
        "/api/v1/age-structure/map",
        params={"indicator": "age_65_plus", "geo_level": "PLACE"},
    )
    assert refused.status_code == 422
    assert "one state at a time" in refused.json()["detail"]
    answered = client.get(
        "/api/v1/age-structure/map",
        params={
            "indicator": "age_65_plus",
            "geo_level": "PLACE",
            "state_fips": "55",
            "release": "2023",
        },
    )
    assert answered.status_code == 200, answered.text
    body = answered.json()
    assert body["unit"] == "percent"
    assert body["items"][0]["value"] == "11.50"


@pytest.mark.parametrize(
    "params",
    [
        {"indicator": "B01001_003", "geo_level": "COUNTY"},
        {"indicator": "sex_ratio", "geo_level": "BLOCK_GROUP"},
        {"indicator": "sex_ratio", "geo_level": "COUNTY", "state_fips": "5x"},
    ],
)
def test_requests_the_map_refuses(map_client, params) -> None:
    """Covers: API-187"""
    response = map_client(_SummarySession([])).get(
        "/api/v1/age-structure/map", params=params
    )
    assert response.status_code == 422


def test_a_grain_with_no_summary_answers_empty_not_zero(map_client) -> None:
    """Covers: API-187"""
    response = map_client(_SummarySession([])).get(
        "/api/v1/age-structure/map",
        params={"indicator": "median_age", "geo_level": "METRO"},
    )
    assert response.status_code == 200
    assert response.json()["release"] is None
    assert response.json()["items"] == []


def test_one_release_reads_eight_states_at_once_and_a_backfill_four() -> None:
    """Covers: ETL-109 -- a release built straight after its publish reads
    warm; several releases read cold, so fewer states at once."""
    assert age_summary.workers_for(1, 8, 4) == 8
    assert age_summary.workers_for(0, 8, 4) == 8
    assert age_summary.workers_for(36, 8, 4) == 4
    assert age_summary.workers_for(36, 2, 4) == 2


def test_the_largest_states_are_read_first() -> None:
    """Covers: ETL-109"""
    assert age_summary.largest_first(["01", "06", "55", "48", "12"]) == [
        "48",
        "06",
        "12",
        "01",
        "55",
    ]
