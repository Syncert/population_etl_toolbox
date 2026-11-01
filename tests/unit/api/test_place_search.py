"""Finding a place by name or by location (api-place-search)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.api.dependencies import get_db_session_dep
from apps.api.main import PUBLIC_CACHE_TARGETS, app
from apps.api.ratelimit import RateLimitMiddleware
from apps.api.services.place_search import normalise_query
from data_ingestion_toolbox.sql import catalog_queries as cq

pytestmark = pytest.mark.unit


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None


class _Session:
    """Answers each catalog query from canned rows and records its parameters."""

    def __init__(self, answers):
        self.answers = answers
        self.calls: list[tuple[object, dict]] = []

    def execute(self, query, params=None):
        self.calls.append((query, dict(params or {})))
        for known, rows in self.answers.items():
            if query is known:
                return _Result(rows(params) if callable(rows) else rows)
        raise AssertionError(f"unexpected query: {query}")

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


def _row(geo_id, level, name, short, tier=None, **extra):
    row = {
        "geo_id": geo_id,
        "geo_level": level,
        "geo_name": name,
        "short_name": short,
        "state_fips": None,
        "state_name": None,
        "county_geo_id": None,
        "county_name": None,
        "population": None,
        "population_year": None,
        "population_metric_code": None,
    }
    if tier is not None:
        row["match_tier"] = tier
    row.update(extra)
    return row


POP = "CENSUS_ACS:acs5:B01003_001"
TX = {"state_fips": "48", "state_name": "Texas"}
MN = {"state_fips": "27", "state_name": "Minnesota"}

AUSTIN_CITY = _row(
    "state:48|place:05000",
    "PLACE",
    "Austin city",
    "Austin",
    0,
    county_geo_id="state:48|county:453",
    county_name="Travis County",
    population=974447,
    population_year=2023,
    population_metric_code=POP,
    **TX,
)
AUSTIN_COUNTY = _row(
    "state:48|county:015",
    "COUNTY",
    "Austin County",
    "Austin",
    0,
    population=30875,
    population_year=2023,
    population_metric_code=POP,
    **TX,
)
AUSTIN_MN = _row(
    "state:27|place:02908",
    "PLACE",
    "Austin city",
    "Austin",
    0,
    county_geo_id="state:27|county:099",
    county_name="Mower County",
    population=26128,
    population_year=2023,
    population_metric_code=POP,
    **MN,
)
AUSTIN_METRO = _row(
    "cbsa:12420",
    "METRO",
    "Austin-Round Rock-San Marcos, TX",
    "Austin-Round Rock-San Marcos",
    1,
    population=2473275,
    population_year=2023,
    population_metric_code=POP,
    **TX,
)
AUSTINTOWN = _row(
    "state:39|place:03184",
    "PLACE",
    "Austintown CDP",
    "Austintown",
    1,
    state_fips="39",
    state_name="Ohio",
    population=29290,
    population_year=2023,
    population_metric_code=POP,
)
TRAVIS = _row(
    "state:48|county:453",
    "COUNTY",
    "Travis County",
    "Travis",
    population=1326436,
    population_year=2023,
    population_metric_code=POP,
    **TX,
)
MOWER = _row("state:27|county:099", "COUNTY", "Mower County", "Mower", **MN)
AUSTIN_MN_MICRO = _row("cbsa:12380", "METRO", "Austin, MN", "Austin", **MN)

SEGMENTS = [
    {
        "geo_id": "state:48",
        "geo_level": "STATE",
        "state_fips": "48",
        "county_fips": None,
        "place_fips": None,
        "segment_name": "Texas",
    },
    {
        "geo_id": "state:27",
        "geo_level": "STATE",
        "state_fips": "27",
        "county_fips": None,
        "place_fips": None,
        "segment_name": "Minnesota",
    },
    {
        "geo_id": "state:48|county:453",
        "geo_level": "COUNTY",
        "state_fips": "48",
        "county_fips": "453",
        "place_fips": None,
        "segment_name": "Travis County",
    },
    {
        "geo_id": "state:48|county:015",
        "geo_level": "COUNTY",
        "state_fips": "48",
        "county_fips": "015",
        "place_fips": None,
        "segment_name": "Austin County",
    },
    {
        "geo_id": "state:27|county:099",
        "geo_level": "COUNTY",
        "state_fips": "27",
        "county_fips": "099",
        "place_fips": None,
        "segment_name": "Mower County",
    },
    {
        "geo_id": "state:48|place:05000",
        "geo_level": "PLACE",
        "state_fips": "48",
        "county_fips": None,
        "place_fips": "05000",
        "segment_name": "Austin city",
    },
    {
        "geo_id": "state:27|place:02908",
        "geo_level": "PLACE",
        "state_fips": "27",
        "county_fips": None,
        "place_fips": "02908",
        "segment_name": "Austin city",
    },
]


def _austin_session(direct=None):
    by_id = {
        row["geo_id"]: row for row in (TRAVIS, MOWER, AUSTIN_METRO, AUSTIN_MN_MICRO)
    }
    return _Session(
        {
            cq.GEO_SEARCH_QUERY: direct
            if direct is not None
            else [AUSTIN_CITY, AUSTIN_COUNTY, AUSTIN_MN, AUSTIN_METRO, AUSTINTOWN],
            cq.GEO_SEARCH_COUNTY_METROS_QUERY: [
                {"county_geo_id": "state:27|county:099", "metro_geo_id": "cbsa:12380"},
                {"county_geo_id": "state:48|county:453", "metro_geo_id": "cbsa:12420"},
            ],
            cq.GEO_SEARCH_ROWS_QUERY: lambda params: [
                by_id[geo_id] for geo_id in params["geo_ids"] if geo_id in by_id
            ],
            cq.GEO_SEARCH_SEGMENTS_QUERY: SEGMENTS,
        }
    )


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


def test_austin_offers_the_city_then_its_county_and_metro() -> None:
    """Covers: API-171 — "Austin" offers Austin city, Travis County and the Austin metro."""
    session = _austin_session()
    response = _get(session, "/api/v1/catalog/geographies/search?q=Austin")

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["query"] == "austin"
    assert payload["limit"] == 10
    assert [(item["geo_id"], item["match"]) for item in payload["items"]] == [
        ("state:48|place:05000", "exact"),
        ("state:48|county:453", "contains_match"),
        ("cbsa:12420", "prefix"),
        ("state:48|county:015", "exact"),
        ("state:27|place:02908", "exact"),
        ("state:27|county:099", "contains_match"),
        ("cbsa:12380", "contains_match"),
        ("state:39|place:03184", "prefix"),
    ]
    city, travis, metro = payload["items"][:3]
    assert city["page_path"] == "/us/texas/austin-city"
    assert city["county_geo_id"] == "state:48|county:453"
    assert city["county_name"] == "Travis County"
    assert city["state_name"] == "Texas"
    assert city["population"] == 974447
    assert city["population_year"] == 2023
    assert city["population_metric_code"] == POP
    assert travis["matched_via"] == {
        "geo_id": "state:48|place:05000",
        "geo_name": "Austin city",
    }
    assert travis["page_path"] == "/us/texas/travis-county"
    # A metro has no page yet (A-5/D-5): no path, rather than a guessed one.
    assert metro["matched_via"] is None
    assert metro["page_path"] is None

    (bound,) = session.params_of(cq.GEO_SEARCH_QUERY)
    assert bound == {
        "words": " austin",
        "prefix": " austin%",
        "word_prefix": "% austin%",
        "grains": None,
        "state_fips": None,
        "fetch": 20,
    }
    (segments,) = session.params_of(cq.GEO_SEARCH_SEGMENTS_QUERY)
    assert segments == {"state_fips": ["27", "39", "48"]}


def test_a_page_stops_at_the_limit_after_the_containing_rows() -> None:
    """Covers: API-171 — the limit bounds the answer, containing rows included."""
    response = _get(
        _austin_session(), "/api/v1/catalog/geographies/search?q=austin&limit=3"
    )
    assert response.status_code == 200
    assert [item["geo_id"] for item in response.json()["items"]] == [
        "state:48|place:05000",
        "state:48|county:453",
        "cbsa:12420",
    ]


def test_grains_narrow_the_matches_and_the_containing_rows() -> None:
    """Covers: API-171 — a city-only search offers no county or metro beside it."""
    session = _austin_session(direct=[AUSTIN_CITY, AUSTIN_MN])
    response = _get(
        session,
        "/api/v1/catalog/geographies/search?q=austin&grain=place&grain=COUNTY,place",
    )
    assert response.status_code == 200
    assert session.params_of(cq.GEO_SEARCH_QUERY)[0]["grains"] == ["COUNTY", "PLACE"]
    # Counties are allowed, so the containing county still follows each city;
    # metros are not, so their membership is never read.
    assert session.params_of(cq.GEO_SEARCH_COUNTY_METROS_QUERY) == []
    assert [item["geo_id"] for item in response.json()["items"]] == [
        "state:48|place:05000",
        "state:48|county:453",
        "state:27|place:02908",
        "state:27|county:099",
    ]


def test_a_near_miss_is_fuzzy_and_offers_nothing_beside_it() -> None:
    """Covers: API-171 — a misspelling still finds the place, labelled fuzzy."""
    session = _austin_session(direct=[{**AUSTIN_CITY, "match_tier": 3}])
    response = _get(session, "/api/v1/catalog/geographies/search?q=Austn")
    assert response.status_code == 200
    (item,) = response.json()["items"]
    assert item["match"] == "fuzzy"
    assert session.params_of(cq.GEO_SEARCH_ROWS_QUERY) == []


def test_an_unranked_geography_keeps_a_null_population() -> None:
    """Covers: API-171 — a ZCTA with no served population is not ranked as zero."""
    zcta = _row("zcta:78701", "ZCTA", "ZCTA5 78701", "78701", 1)
    session = _Session({cq.GEO_SEARCH_QUERY: [zcta], cq.GEO_SEARCH_SEGMENTS_QUERY: []})
    response = _get(session, "/api/v1/catalog/geographies/search?q=787&state_fips=48")
    assert response.status_code == 200
    (item,) = response.json()["items"]
    assert item["population"] is None
    assert item["page_path"] is None
    assert session.params_of(cq.GEO_SEARCH_QUERY)[0]["state_fips"] == "48"


def test_a_query_is_matched_in_the_projection_word_shape() -> None:
    """Covers: API-171 — accents, ampersands and punctuation fold as the warehouse folds them."""
    assert normalise_query("Española") == "espanola"
    assert normalise_query("Town & Country") == "town and country"
    assert normalise_query("  Coeur d'Alene,  ID ") == "coeur d alene id"
    assert normalise_query("100%_") == "100"


@pytest.mark.parametrize(
    ("query", "detail"),
    [
        ("q=a", "q must hold at least 2 letters or digits"),
        ("q=%27%20-", "q must hold at least 2 letters or digits"),
        (
            "q=austin&grain=CITY",
            "grain must be one of: STATE, COUNTY, METRO, PLACE, ZCTA",
        ),
        (
            "q=austin&grain=PLACE,TRACT",
            "grain must be one of: STATE, COUNTY, METRO, PLACE, ZCTA",
        ),
    ],
)
def test_a_search_outside_the_contract_is_refused(query, detail) -> None:
    """Covers: API-171 — a one-letter query or an unknown grain is a 422 naming the rule."""
    session = _Session({})
    response = _get(session, f"/api/v1/catalog/geographies/search?{query}")
    assert response.status_code == 422
    assert response.json() == {"detail": detail}
    assert session.calls == []


@pytest.mark.parametrize(
    "query",
    [
        "q=austin&state_fips=TX",
        "q=austin&limit=26",
        "q=austin&limit=0",
        "",
        "q=austin&near=1",
    ],
)
def test_malformed_search_parameters_are_refused(query) -> None:
    """Covers: API-171 — a malformed state code, limit, missing query or unknown filter is a 422."""
    session = _Session({})
    response = _get(session, f"/api/v1/catalog/geographies/search?{query}")
    assert response.status_code == 422
    assert session.calls == []


# ---------------------------------------------------------------------------
# Locate
# ---------------------------------------------------------------------------

LOCATED = [
    {
        "geo_id": "state:48|county:453",
        "geo_level": "COUNTY",
        "geo_name": "Travis County",
        "state_fips": "48",
        "county_fips": "453",
        "place_fips": None,
        "boundary_vintage": 2023,
    },
    {
        "geo_id": "state:48|place:05000",
        "geo_level": "PLACE",
        "geo_name": "Austin city",
        "state_fips": "48",
        "county_fips": None,
        "place_fips": "05000",
        "boundary_vintage": 2023,
    },
    {
        "geo_id": "state:48",
        "geo_level": "STATE",
        "geo_name": "Texas",
        "state_fips": "48",
        "county_fips": None,
        "place_fips": None,
        "boundary_vintage": 2023,
    },
    {
        "geo_id": "state:48|county:453|tract:001100",
        "geo_level": "TRACT",
        "geo_name": "Census Tract 11",
        "state_fips": "48",
        "county_fips": "453",
        "place_fips": None,
        "boundary_vintage": 2020,
    },
    {
        "geo_id": "zcta:78701",
        "geo_level": "ZCTA",
        "geo_name": "ZCTA5 78701",
        "state_fips": None,
        "county_fips": None,
        "place_fips": None,
        "boundary_vintage": 2020,
    },
]
METRO = {
    "geo_id": "cbsa:12420",
    "geo_level": "METRO",
    "geo_name": "Austin-Round Rock-San Marcos, TX",
    "boundary_vintage": 2023,
}


def _locate_session(located=LOCATED, metro=(METRO,)):
    return _Session(
        {
            cq.GEO_LOCATE_QUERY: located,
            cq.GEO_LOCATE_METRO_QUERY: list(metro),
            cq.GEO_SEARCH_SEGMENTS_QUERY: SEGMENTS,
        }
    )


def test_a_point_in_a_city_lands_on_the_city() -> None:
    """Covers: API-172 — "use my location" lands on the city, with its county, tract, ZCTA and metro."""
    session = _locate_session()
    response = _get(
        session, "/api/v1/catalog/geographies/locate?lat=30.2650&lon=-97.7470"
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["landing"] == {
        "geo_id": "state:48|place:05000",
        "geo_level": "PLACE",
        "geo_name": "Austin city",
        "boundary_vintage": 2023,
        "page_path": "/us/texas/austin-city",
    }
    assert payload["place"] == payload["landing"]
    assert payload["county"]["page_path"] == "/us/texas/travis-county"
    assert payload["state"]["page_path"] == "/us/texas"
    assert payload["tract"]["geo_id"] == "state:48|county:453|tract:001100"
    # The tract page sits under its county (tract pages, API-190).
    assert payload["tract"]["page_path"] == "/us/texas/travis-county/tract-11"
    assert payload["zcta"]["geo_id"] == "zcta:78701"
    assert payload["metro"]["geo_id"] == "cbsa:12420"
    assert session.params_of(cq.GEO_LOCATE_QUERY) == [{"lat": 30.265, "lon": -97.747}]
    assert session.params_of(cq.GEO_LOCATE_METRO_QUERY) == [
        {"county_geo_id": "state:48|county:453"}
    ]
    # The answer names a tract, never the point.
    assert "30.26" not in response.text
    assert "97.74" not in response.text


def test_a_point_outside_every_city_lands_on_the_county() -> None:
    """Covers: API-172 — unincorporated land lands on its county; a county in no metro has none."""
    rural = [row for row in LOCATED if row["geo_level"] != "PLACE"]
    response = _get(
        _locate_session(located=rural, metro=()),
        "/api/v1/catalog/geographies/locate?lat=30.4&lon=-98.1",
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["place"] is None
    assert payload["metro"] is None
    assert payload["landing"]["geo_id"] == "state:48|county:453"


def test_a_point_no_state_covers_is_not_found() -> None:
    """Covers: API-172 — offshore or abroad is a 404, not an empty answer."""
    response = _get(
        _locate_session(located=[]), "/api/v1/catalog/geographies/locate?lat=0&lon=0"
    )
    assert response.status_code == 404
    assert response.json() == {"detail": "no geography contains this point"}


@pytest.mark.parametrize(
    "query",
    [
        "lat=91&lon=0",
        "lat=0&lon=181",
        "lat=nan&lon=0",
        "lat=0&lon=inf",
        "lat=30",
        "lon=-97",
        "lat=x&lon=1",
    ],
)
def test_coordinates_outside_the_globe_are_refused(query) -> None:
    """Covers: API-172 — a missing, non-finite or out-of-range coordinate is a 422."""
    session = _locate_session()
    response = _get(session, f"/api/v1/catalog/geographies/locate?{query}")
    assert response.status_code == 422
    assert session.calls == []


# ---------------------------------------------------------------------------
# Cacheable and rate limited
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path", ["/api/v1/catalog/geographies/search", "/api/v1/catalog/geographies/locate"]
)
def test_both_routes_are_cached_and_spend_the_catalog_budget(path) -> None:
    """Covers: API-171, API-172 — both reads are public cache targets in the catalog bucket."""
    assert PUBLIC_CACHE_TARGETS.covers(path)
    limiter = RateLimitMiddleware(
        app=None, catalog_per_minute=60, analysis_per_minute=10
    )
    assert limiter._classify(path) == ("catalog", 60)


def test_a_successful_search_is_publicly_cacheable() -> None:
    """Covers: API-171 — a 200 carries the public cache policy, a refusal never does."""
    ok = _get(_austin_session(), "/api/v1/catalog/geographies/search?q=austin")
    assert ok.headers["cache-control"].startswith("public, max-age=")
    refused = _get(_Session({}), "/api/v1/catalog/geographies/search?q=a")
    assert refused.headers["cache-control"] == "no-store"
