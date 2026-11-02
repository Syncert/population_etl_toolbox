"""Metro pages: addresses, one resolve read and a state's list (web-metro-pages)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import PUBLIC_CACHE_TARGETS, app
from apps.api.registry import OBSERVATION_DISPATCH
from data_ingestion_toolbox.sql import catalog_queries as cq

pytestmark = [pytest.mark.unit, pytest.mark.api]


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        return "relation"


class _Session:
    def __init__(self, answers):
        self.answers = answers
        self.calls: list[tuple[object, dict]] = []

    def execute(self, query, params=None):
        self.calls.append((query, dict(params or {})))
        for known, rows in self.answers.items():
            if query is known:
                return _Result(rows(params) if callable(rows) else rows)
        # The relation probe every service makes before it reads.
        return _Result([{"exists": True}])

    def params_of(self, query) -> list[dict]:
        return [params for known, params in self.calls if known is query]


def _get(session, path):
    def _override():
        yield session

    app.dependency_overrides[get_db_session_dep] = _override
    try:
        return TestClient(app).get(path)
    finally:
        app.dependency_overrides.clear()


def _metro(geo_id, name, short, state_fips, state_name, population):
    return {
        "geo_id": geo_id,
        "geo_level": "METRO",
        "geo_name": name,
        "short_name": short,
        "state_fips": state_fips,
        "state_name": state_name,
        "population": population,
        "population_year": 2023,
        "population_metric_code": "CENSUS_ACS:acs5:B01003_001",
        "segment_name": name,
    }


METROS = [
    _metro(
        "cbsa:12420",
        "Austin-Round Rock-San Marcos, TX Metro Area",
        "Austin-Round Rock-San Marcos",
        "48",
        "Texas",
        2421115,
    ),
    _metro(
        "cbsa:45500", "Texarkana, TX-AR Metro Area", "Texarkana", "48", "Texas", 147000
    ),
    _metro("cbsa:99901", "Twin, TL Micro Area", "Twin", "70", "Test Land", 1000),
    _metro("cbsa:99902", "Twin TL Micro Area", "Twin", "70", "Test Land", 900),
]

SEGMENTS = [
    {
        "geo_id": "state:48",
        "geo_level": "STATE",
        "state_fips": "48",
        "segment_name": "Texas",
    },
    {
        "geo_id": "state:05",
        "geo_level": "STATE",
        "state_fips": "05",
        "segment_name": "Arkansas",
    },
    {
        "geo_id": "state:48|county:453",
        "geo_level": "COUNTY",
        "state_fips": "48",
        "county_fips": "453",
        "segment_name": "Travis County",
    },
    {
        "geo_id": "state:48|county:491",
        "geo_level": "COUNTY",
        "state_fips": "48",
        "county_fips": "491",
        "segment_name": "Williamson County",
    },
    {
        "geo_id": "state:48|county:037",
        "geo_level": "COUNTY",
        "state_fips": "48",
        "county_fips": "037",
        "segment_name": "Bowie County",
    },
    {
        "geo_id": "state:05|county:091",
        "geo_level": "COUNTY",
        "state_fips": "05",
        "county_fips": "091",
        "segment_name": "Miller County",
    },
    *[{**row, "geo_level": "METRO"} for row in METROS],
]


def _members(params):
    if params["metro_geo_id"] == "cbsa:45500":
        return [
            {
                "geo_id": "state:48|county:037",
                "geo_name": "Bowie County",
                "state_fips": "48",
                "state_name": "Texas",
                "population": 92000,
                "population_year": 2023,
                "geography_vintage": 2023,
                "evidence_source": "census_cbsa_delineation",
            },
            {
                "geo_id": "state:05|county:091",
                "geo_name": "Miller County",
                "state_fips": "05",
                "state_name": "Arkansas",
                "population": 42000,
                "population_year": 2023,
                "geography_vintage": 2023,
                "evidence_source": "census_cbsa_delineation",
            },
        ]
    return [
        {
            "geo_id": "state:48|county:453",
            "geo_name": "Travis County",
            "state_fips": "48",
            "state_name": "Texas",
            "population": 1290188,
            "population_year": 2023,
            "geography_vintage": 2023,
            "evidence_source": "census_cbsa_delineation",
        },
        {
            "geo_id": "state:48|county:491",
            "geo_name": "Williamson County",
            "state_fips": "48",
            "state_name": "Texas",
            "population": 643000,
            "population_year": 2023,
            "geography_vintage": 2023,
            "evidence_source": "census_cbsa_delineation",
        },
    ]


AREAS = [
    {
        "geo_id": "division:7",
        "geo_level": "CENSUS_DIVISION",
        "geo_name": "West South Central",
    },
    {"geo_id": "region:3", "geo_level": "CENSUS_REGION", "geo_name": "South Region"},
    {"geo_id": "us:1", "geo_level": "NATIONAL", "geo_name": "United States"},
]


def _session():
    return _Session(
        {
            cq.METRO_PAGES_QUERY: METROS,
            cq.METRO_MEMBER_COUNTIES_QUERY: _members,
            cq.GEO_SEARCH_SEGMENTS_QUERY: SEGMENTS,
            cq.STATE_CONTAINING_AREAS_QUERY: AREAS,
            cq.METRO_STATE_MEMBERS_QUERY: lambda params: (
                [
                    {"metro_geo_id": "cbsa:12420", "state_fips": "48"},
                    {"metro_geo_id": "cbsa:45500", "state_fips": "48"},
                ]
                if params["state_fips"] == ["48"]
                else [{"metro_geo_id": "cbsa:45500", "state_fips": "05"}]
            ),
        }
    )


def test_a_metro_page_resolves_in_one_read() -> None:
    """Covers: API-187 — the canonical segment names the metro, its counties, states and areas."""
    session = _session()
    response = _get(
        session,
        "/api/v1/catalog/geographies/metros/austin-round-rock-san-marcos-tx-metro-area",
    )
    assert response.status_code == 200
    body = response.json()
    assert body["metro"]["geo_id"] == "cbsa:12420"
    assert (
        body["metro"]["page_path"]
        == "/metro/austin-round-rock-san-marcos-tx-metro-area"
    )
    assert body["metro"]["principal_state_fips"] == "48"
    assert body["delineation_vintage"] == 2023
    assert [county["geo_id"] for county in body["member_counties"]] == [
        "state:48|county:453",
        "state:48|county:491",
    ]
    assert body["member_counties"][0]["page_path"] == "/us/texas/travis-county"
    assert body["states"] == [
        {
            "geo_id": "state:48",
            "state_fips": "48",
            "state_name": "Texas",
            "page_path": "/us/texas",
            "member_counties": 2,
            "principal": True,
        }
    ]
    assert [area["geo_id"] for area in body["principal_state_areas"]] == [
        "division:7",
        "region:3",
        "us:1",
    ]
    # Every relationship read starts from a known key.
    assert session.params_of(cq.METRO_MEMBER_COUNTIES_QUERY) == [
        {"metro_geo_id": "cbsa:12420"}
    ]
    assert session.params_of(cq.STATE_CONTAINING_AREAS_QUERY) == [
        {"state_geo_id": "state:48"}
    ]


def test_a_cbsa_code_resolves_and_names_the_canonical_segment() -> None:
    """Covers: API-187 — the CBSA code is accepted; the metro carries the segment to settle on."""
    body = _get(_session(), "/api/v1/catalog/geographies/metros/12420").json()
    assert body["requested_segment"] == "12420"
    assert body["metro"]["segment"] == "austin-round-rock-san-marcos-tx-metro-area"


def test_a_two_state_metro_lists_both_states_principal_first() -> None:
    """Covers: API-187 — the principal state is the most populous county's, then the others."""
    body = _get(
        _session(), "/api/v1/catalog/geographies/metros/texarkana-tx-ar-metro-area"
    ).json()
    assert [
        (state["state_fips"], state["principal"], state["member_counties"])
        for state in body["states"]
    ] == [
        ("48", True, 1),
        ("05", False, 1),
    ]
    assert body["member_counties"][1]["page_path"] == "/us/arkansas/miller-county"


def test_metros_whose_names_slug_alike_are_addressed_by_code() -> None:
    """Covers: API-187 — a clash falls back to the CBSA code; the slug resolves neither."""
    session = _session()
    assert (
        _get(
            session, "/api/v1/catalog/geographies/metros/twin-tl-micro-area"
        ).status_code
        == 404
    )
    body = _get(session, "/api/v1/catalog/geographies/metros/99901").json()
    assert body["metro"]["page_path"] == "/metro/99901"


def test_an_unknown_or_malformed_segment_is_refused() -> None:
    """Covers: API-187 — 404 for no metro, 422 outside the slug vocabulary."""
    missing = _get(_session(), "/api/v1/catalog/geographies/metros/nowhere-metro-area")
    assert missing.status_code == 404
    assert missing.json() == {"detail": "metro not found"}
    assert (
        _get(
            _session(), "/api/v1/catalog/geographies/metros/Not%20A%20Slug"
        ).status_code
        == 422
    )


def test_a_state_lists_every_metro_reaching_into_it() -> None:
    """Covers: API-187 — a two-state metro is listed in both of its states, by name."""
    texas = _get(_session(), "/api/v1/catalog/geographies/metros?state_fips=48").json()
    assert [item["geo_id"] for item in texas["items"]] == ["cbsa:12420", "cbsa:45500"]
    assert texas["total"] == 2
    arkansas = _get(
        _session(), "/api/v1/catalog/geographies/metros?state_fips=05"
    ).json()
    assert [item["page_path"] for item in arkansas["items"]] == [
        "/metro/texarkana-tx-ar-metro-area"
    ]
    assert (
        _get(_session(), "/api/v1/catalog/geographies/metros?state_fips=4x").status_code
        == 422
    )


def test_metro_reads_are_catalog_reads_shared_by_the_cache() -> None:
    """Covers: API-187 — both routes are public, cacheable catalog reads."""
    for path in (
        "/api/v1/catalog/geographies/metros",
        "/api/v1/catalog/geographies/metros/12420",
    ):
        assert PUBLIC_CACHE_TARGETS.covers(path)


def test_bea_rows_carry_their_derivation() -> None:
    """Covers: API-188 — the BEA dispatch serves `derivation` as a dimension."""
    bea = OBSERVATION_DISPATCH["BEA"]
    assert ("derivation", "derivation") in bea.dimension_expressions


def _related_row(relationship, geo_id, geo_level, geo_name):
    return {
        "relationship": relationship,
        "geo_id": geo_id,
        "geo_level": geo_level,
        "geo_name": geo_name,
        "state_fips": None,
        "geography_vintage": 2023,
        "evidence_source": "census_cbsa_delineation",
        "overlap_area_m2": None,
        "overlap_weight": 1.0,
    }


def test_a_citys_related_metro_carries_its_named_page() -> None:
    """Covers: API-187 — /related names a metro's page, read only when needed."""
    session = _Session(
        {
            cq.GEOGRAPHY_EXISTS_QUERY: [
                {"geo_id": "state:48|place:05000", "geo_level": "PLACE"}
            ],
            cq.GEOGRAPHY_RELATED_QUERY: [
                _related_row(
                    "intersects",
                    "cbsa:12420",
                    "METRO",
                    "Austin-Round Rock-San Marcos, TX Metro Area",
                ),
                _related_row("part_of", "us:1", "NATIONAL", "United States"),
            ],
            cq.METRO_PAGES_QUERY: METROS,
        }
    )
    response = _get(session, "/api/v1/catalog/geographies/state:48|place:05000/related")
    assert response.status_code == 200, response.text
    items = {item["geo_id"]: item for item in response.json()["items"]}
    assert (
        items["cbsa:12420"]["page_path"]
        == "/metro/austin-round-rock-san-marcos-tx-metro-area"
    )
    assert items["us:1"]["page_path"] is None
    assert len(session.params_of(cq.METRO_PAGES_QUERY)) == 1

    # An answer naming no metro reads no metro list.
    plain = _Session(
        {
            cq.GEOGRAPHY_EXISTS_QUERY: [{"geo_id": "state:48", "geo_level": "STATE"}],
            cq.GEOGRAPHY_RELATED_QUERY: [
                _related_row("part_of", "us:1", "NATIONAL", "United States")
            ],
        }
    )
    assert (
        _get(plain, "/api/v1/catalog/geographies/state:48/related").status_code == 200
    )
    assert plain.params_of(cq.METRO_PAGES_QUERY) == []
