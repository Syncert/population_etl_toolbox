"""The geography relationship resource (nearby-and-related-places)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from data_ingestion_toolbox.sql import catalog_queries

pytestmark = pytest.mark.unit


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _Session:
    """Answers the existence probe and the relationship read separately."""

    def __init__(self, own, related):
        self.own = own
        self.related = related
        self.params: list[dict] = []

    def execute(self, query, params=None):
        self.params.append(params or {})
        if query is catalog_queries.GEOGRAPHY_EXISTS_QUERY:
            return _Result([self.own] if self.own else [])
        if query is catalog_queries.GEOGRAPHY_RELATED_QUERY:
            return _Result(self.related)
        raise AssertionError(f"unexpected query: {query}")


def _get(session, path):
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    try:
        return TestClient(app).get(path)
    finally:
        app.dependency_overrides.clear()


DANE = {"geo_id": "state:55|county:025", "geo_level": "COUNTY"}
ROWS = [
    {
        "relationship": "adjacent",
        "geo_id": "state:55|county:021",
        "geo_level": "COUNTY",
        "geo_name": "Columbia County",
        "state_fips": "55",
        "geography_vintage": 2023,
        "overlap_area_m2": None,
        "overlap_weight": None,
        "evidence_source": "census_boundary_adjacency",
    },
    {
        "relationship": "intersects",
        "geo_id": "state:55|place:48000",
        "geo_level": "PLACE",
        "geo_name": "Madison city",
        "state_fips": "55",
        "geography_vintage": 2023,
        "overlap_area_m2": 2.5e8,
        "overlap_weight": 1.0,
        "evidence_source": "census_boundary_intersection",
    },
    {
        "relationship": "part_of",
        "geo_id": "state:55",
        "geo_level": "STATE",
        "geo_name": "Wisconsin",
        "state_fips": "55",
        "geography_vintage": 2023,
        "overlap_area_m2": None,
        "overlap_weight": None,
        "evidence_source": "exact_census_code_hierarchy",
    },
    {
        "relationship": "part_of",
        "geo_id": "us:1",
        "geo_level": "NATIONAL",
        "geo_name": "United States",
        "state_fips": None,
        "geography_vintage": 2023,
        "overlap_area_m2": None,
        "overlap_weight": None,
        "evidence_source": "exact_census_code_hierarchy",
    },
]


def test_a_county_answers_its_state_nation_places_and_neighbours() -> None:
    """Covers: API-164 — every row names its type, vintage, and evidence."""
    session = _Session(DANE, ROWS)
    response = _get(
        session, "/api/v1/catalog/geographies/state%3A55%7Ccounty%3A025/related"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["geo_id"] == "state:55|county:025"
    assert payload["geo_level"] == "COUNTY"
    assert payload["total"] == 4
    assert [(item["relationship"], item["geo_id"]) for item in payload["items"]] == [
        ("adjacent", "state:55|county:021"),
        ("intersects", "state:55|place:48000"),
        ("part_of", "state:55"),
        ("part_of", "us:1"),
    ]
    assert all(
        item["geography_vintage"] and item["evidence_source"]
        for item in payload["items"]
    )
    assert payload["items"][1]["overlap_weight"] == 1.0
    assert session.params == [
        {"geo_id": "state:55|county:025"},
        {"geo_id": "state:55|county:025"},
    ]


def test_an_unknown_geography_is_the_catalogs_stable_404() -> None:
    """Covers: API-164 — refused in the shape an unknown metric is."""
    response = _get(
        _Session(None, []), "/api/v1/catalog/geographies/state%3A99/related"
    )
    assert response.status_code == 404
    assert response.json() == {"detail": "geo_id not found"}


def test_the_relationship_read_matches_by_identity_never_by_name() -> None:
    """Covers: API-164 — the query joins identities and keeps the evidence."""
    rendered = str(catalog_queries.GEOGRAPHY_RELATED_QUERY)
    assert "related.evidence_source" in rendered
    assert "related.geography_vintage" in rendered
    assert "geo_name =" not in rendered and "ILIKE" not in rendered
    # A state's places are reached through its counties, never listed whole.
    assert (
        "NOT (related.relationship = 'contains' AND geography.geo_level = 'PLACE')"
        in rendered
    )
    assert "'part_of'" in rendered
