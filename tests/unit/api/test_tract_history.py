"""API unit tests: one Census tract's history on its 2020 boundary.

Covers: API-187 (``GET /place/tract-history`` says whether a tract's code
        named the same area before 2020 -- unchanged, split, merged, redrawn
        or not recorded -- from the relationship files, and serves each
        measure's published values from 2020 on beside, before 2020, the
        published values of an unchanged tract or the derived comparable
        counts of a redrawn one, each marked derived with its basis; medians
        are never apportioned; every read is scoped to the one tract).
"""

from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.services import tract_history_service as service
from apps.api.services.tract_history_service import classify_boundary, tract_name

pytestmark = [pytest.mark.unit, pytest.mark.api]

# A Travis County tract split in 2020: 2010 tract 18.05 became 18.31 and 18.32.
OLD = "state:48|county:453|tract:001805"
NEW = "state:48|county:453|tract:001831"
SIBLING = "state:48|county:453|tract:001832"
KEPT = "state:48|county:453|tract:001100"
COUNTY = "state:48|county:453"

INCOME = "CENSUS_ACS:acs5:B19013_001"
POPULATION = "CENSUS_ACS:acs5:B01003_001"

GEOGRAPHIES = {
    NEW: {
        "geo_id": NEW,
        "geo_level": "TRACT",
        "geo_name": "Census Tract 18.31",
        "state_fips": "48",
    },
    KEPT: {
        "geo_id": KEPT,
        "geo_level": "TRACT",
        "geo_name": "Census Tract 11",
        "state_fips": "48",
    },
    COUNTY: {
        "geo_id": COUNTY,
        "geo_level": "COUNTY",
        "geo_name": "Travis County",
        "state_fips": "48",
    },
}

CHANGES = [
    # 18.05 -> 18.31 (46% of 18.05's land, all of 18.31's) and 18.32.
    {
        "earlier_geo_id": OLD,
        "later_geo_id": NEW,
        "earlier_land_share": 0.46,
        "later_land_share": 1.0,
    },
    {
        "earlier_geo_id": OLD,
        "later_geo_id": SIBLING,
        "earlier_land_share": 0.54,
        "later_land_share": 1.0,
    },
    {
        "earlier_geo_id": KEPT,
        "later_geo_id": KEPT,
        "earlier_land_share": 1.0,
        "later_land_share": 1.0,
    },
]


def _published(
    geo_id: str, code: str, vintage: int, value: float, moe: float = 100.0
) -> dict:
    return {
        "geo_id": geo_id,
        "metric_code": code,
        "vintage_year": vintage,
        "observation_date": date(vintage, 1, 1),
        "estimate_value": value,
        "margin_of_error": moe,
        "value_status": "valid",
    }


def _comparable(geo_id: str, code: str, vintage: int, value, status: str) -> dict:
    return {
        "geo_id": geo_id,
        "metric_code": code,
        "vintage_year": vintage,
        "observation_date": date(vintage, 1, 1),
        "comparable_value": value,
        "comparable_status": status,
        "weight_basis": "share of land area",
        "source_geo_ids": [OLD],
    }


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _Session:
    """Answers each statement the service declares, and records the params."""

    def __init__(self, published: list[dict], comparable: list[dict]) -> None:
        self.published = published
        self.comparable = comparable
        self.calls: list[tuple[object, dict]] = []

    def execute(self, query, params=None):
        params = params or {}
        self.calls.append((query, params))
        sql = str(query)
        if "to_regclass" in sql or "information_schema" in sql or "pg_class" in sql:
            return _Scalar(True)
        if query is service.GEOGRAPHY_IDENTITY_QUERY:
            return _Result(
                [GEOGRAPHIES[g] for g in params["geo_ids"] if g in GEOGRAPHIES]
            )
        if query is service.CHANGE_QUERY:
            return _Result(
                [r for r in CHANGES if r["later_geo_id"] == params["geo_id"]]
            )
        if query is service.ALSO_BECAME_QUERY:
            return _Result(
                [r for r in CHANGES if r["earlier_geo_id"] in params["earlier_geo_ids"]]
            )
        if query is service.PUBLISHED_QUERY:
            return _Result(
                [
                    r
                    for r in self.published
                    if r["geo_id"] == params["geo_id"]
                    and r["metric_code"] in params["metric_codes"]
                    and r["vintage_year"] >= params["first_vintage"]
                    and r["observation_date"] >= params["first_date"]
                ]
            )
        if query is service.COMPARABLE_QUERY:
            return _Result(
                [
                    r
                    for r in self.comparable
                    if r["geo_id"] == params["geo_id"]
                    and r["metric_code"] in params["metric_codes"]
                ]
            )
        raise AssertionError(
            sql
        )  # pragma: no cover - a statement the test did not expect


class _Scalar:
    def __init__(self, value) -> None:
        self.value = value

    def scalar(self):
        return self.value

    def first(self):
        return (self.value,)


@pytest.fixture(autouse=True)
def _relations_present(monkeypatch):
    monkeypatch.setattr(service, "require_relation", lambda db, name: None)


@pytest.fixture
def client_for():
    def make(session: _Session) -> TestClient:
        app.dependency_overrides[get_db_session_dep] = lambda: session
        return TestClient(app)

    yield make
    app.dependency_overrides.clear()


def test_a_split_tract_serves_its_published_years_and_its_derived_counts(
    client_for,
) -> None:
    """Covers: API-187"""
    session = _Session(
        published=[
            _published(NEW, POPULATION, 2020, 4100),
            _published(NEW, POPULATION, 2024, 4600),
            _published(NEW, INCOME, 2024, 91000, 9000),
            # The same code before 2020 would be another area: never served.
            _published(NEW, INCOME, 2015, 1, 1),
        ],
        comparable=[
            _comparable(NEW, POPULATION, 2015, 3800.3555077067612, "apportioned"),
            _comparable(NEW, POPULATION, 2012, None, "incomplete"),
        ],
    )
    response = client_for(session).get(
        "/api/v1/place/tract-history",
        params={"geo_id": NEW, "metric_codes": f"{POPULATION},{INCOME}"},
    )
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["boundary"]["status"] == "split"
    assert body["boundary"]["earlier"][0]["geo_id"] == OLD
    assert [part["geo_id"] for part in body["boundary"]["also_became"]] == [SIBLING]
    assert "Census Tract 18.05 was split" in body["boundary"]["sentence"]
    assert "46%" in body["boundary"]["sentence"]
    assert "Census Tract 18.32" in body["boundary"]["sentence"]

    population, income = body["series"]
    assert population["metric_code"] == POPULATION
    assert [
        (p["vintage_year"], p["status"], p["derived"]) for p in population["items"]
    ] == [
        (2012, "incomplete", True),
        (2015, "apportioned", True),
        (2020, "published", False),
        (2024, "published", False),
    ]
    assert population["items"][0]["value"] is None
    # An apportioned count is shown whole, not at the view's full precision.
    assert population["items"][1]["value"] == 3800
    assert population["items"][1]["margin_of_error"] is None
    assert population["items"][1]["weight_basis"] == "share of land area"
    assert population["items"][1]["source_geo_ids"] == [OLD]
    assert population["note"] is None
    # A median is not apportioned: its history starts in 2020, and says why.
    assert [p["vintage_year"] for p in income["items"]] == [2024]
    assert "starts in 2020" in income["note"]
    assert "DERIVED" in body["method"]
    assert "starts with the 2009 release" in body["method"]


def test_every_read_is_scoped_to_the_one_tract(client_for) -> None:
    """Covers: API-187"""
    session = _Session(published=[], comparable=[])
    client_for(session).get(
        "/api/v1/place/tract-history",
        params={"geo_id": NEW, "metric_codes": POPULATION},
    )
    statements = {query: params for query, params in session.calls}
    assert statements[service.CHANGE_QUERY] == {"geo_id": NEW}
    assert statements[service.ALSO_BECAME_QUERY] == {"earlier_geo_ids": [OLD]}
    assert statements[service.COMPARABLE_QUERY]["geo_id"] == NEW
    published = statements[service.PUBLISHED_QUERY]
    assert published["geo_id"] == NEW
    # A redrawn tract's published read starts at the 2020 partition.
    assert published["first_date"] == date(2020, 1, 1)
    for query in (
        service.CHANGE_QUERY,
        service.COMPARABLE_QUERY,
        service.PUBLISHED_QUERY,
    ):
        sql = str(query)
        assert "UPPER(" not in sql and "silver_" not in sql
    assert "gold_census.tract_decade_change" in str(service.CHANGE_QUERY)
    assert "gold_census.acs_tract_comparable" in str(service.COMPARABLE_QUERY)


def test_an_unchanged_tract_serves_its_earlier_published_values_as_they_are(
    client_for,
) -> None:
    """Covers: API-187"""
    session = _Session(
        published=[
            _published(KEPT, INCOME, 2012, 52000, 6000),
            _published(KEPT, INCOME, 2024, 70000, 7000),
        ],
        comparable=[_comparable(KEPT, INCOME, 2012, 1, "apportioned")],
    )
    response = client_for(session).get(
        "/api/v1/place/tract-history", params={"geo_id": KEPT, "metric_codes": INCOME}
    )
    body = response.json()
    assert body["boundary"]["status"] == "unchanged"
    assert "kept its boundary" in body["boundary"]["sentence"]
    items = body["series"][0]["items"]
    assert [
        (p["vintage_year"], p["status"], p["derived"], p["value"]) for p in items
    ] == [
        (2012, "unchanged", False, 52000),
        (2024, "published", False, 70000),
    ]
    assert items[0]["margin_of_error"] == 6000
    # No derived read is made for an unchanged tract.
    assert service.COMPARABLE_QUERY not in [query for query, _ in session.calls]


@pytest.mark.parametrize(
    ("params", "status"),
    [
        ({"geo_id": NEW, "metric_codes": "BLS_LAUS:unemployment_rate"}, 422),
        ({"geo_id": NEW, "metric_codes": "CENSUS_ACS:acs1:B19013_001"}, 422),
        (
            {
                "geo_id": NEW,
                "metric_codes": ",".join(
                    f"CENSUS_ACS:acs5:B01001_{n:03d}" for n in range(1, 26)
                ),
            },
            422,
        ),
        ({"geo_id": COUNTY, "metric_codes": INCOME}, 422),
        ({"geo_id": "state:48|county:453|tract:999999", "metric_codes": INCOME}, 404),
    ],
)
def test_a_request_this_route_does_not_answer_is_refused(
    client_for, params, status
) -> None:
    """Covers: API-187"""
    session = _Session(published=[], comparable=[])
    response = client_for(session).get("/api/v1/place/tract-history", params=params)
    assert response.status_code == status, response.text


def test_boundary_classes_name_merges_redraws_and_missing_records() -> None:
    """Covers: API-187"""
    a = "state:48|county:453|tract:000301"
    b = "state:48|county:453|tract:000302"
    merged = classify_boundary(
        NEW,
        [
            {
                "earlier_geo_id": a,
                "later_geo_id": NEW,
                "earlier_land_share": 1.0,
                "later_land_share": 0.6,
            },
            {
                "earlier_geo_id": b,
                "later_geo_id": NEW,
                "earlier_land_share": 0.995,
                "later_land_share": 0.4,
            },
        ],
        [],
    )
    assert merged.status == "merged"
    assert "Census Tract 3.01 and Census Tract 3.02" in merged.sentence

    redrawn = classify_boundary(
        NEW,
        [
            {
                "earlier_geo_id": a,
                "later_geo_id": NEW,
                "earlier_land_share": 0.5,
                "later_land_share": 0.7,
            },
            {
                "earlier_geo_id": b,
                "later_geo_id": NEW,
                "earlier_land_share": 0.2,
                "later_land_share": 0.295,
            },
            # A sliver is a boundary nudge, not a change of area.
            {
                "earlier_geo_id": OLD,
                "later_geo_id": NEW,
                "earlier_land_share": 0.001,
                "later_land_share": 0.005,
            },
        ],
        [],
    )
    assert redrawn.status == "redrawn"
    assert redrawn.slivers_ignored == 1
    assert [part.geo_id for part in redrawn.earlier] == [a, b]

    kept_code = classify_boundary(
        KEPT,
        [
            {
                "earlier_geo_id": KEPT,
                "later_geo_id": KEPT,
                "earlier_land_share": 0.7,
                "later_land_share": 1.0,
            }
        ],
        [
            {
                "earlier_geo_id": KEPT,
                "later_geo_id": SIBLING,
                "earlier_land_share": 0.3,
                "later_land_share": 1.0,
            }
        ],
    )
    assert kept_code.status == "split"
    assert "kept its code but was split" in kept_code.sentence
    assert "Census Tract 18.32" in kept_code.sentence

    missing = classify_boundary(NEW, [], [])
    assert missing.status == "not_recorded"


def test_a_tract_is_named_by_its_number() -> None:
    """Covers: API-187"""
    assert tract_name(OLD) == "Census Tract 18.05"
    assert tract_name(KEPT) == "Census Tract 11"
