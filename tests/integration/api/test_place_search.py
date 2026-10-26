"""Finding a place by name, and the places containing a point (API-171)."""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

STATE = "96"
#: A county square, a city square inside it, and a metro containing the county.
COUNTY = f"state:{STATE}|county:001"
CITY = f"state:{STATE}|place:01000"
METRO = "cbsa:99901"
COUNTY_SQUARE = "MULTIPOLYGON(((-100 30,-99 30,-99 31,-100 31,-100 30)))"
CITY_SQUARE = "MULTIPOLYGON(((-99.8 30.2,-99.6 30.2,-99.6 30.4,-99.8 30.4,-99.8 30.2)))"
POPULATION = "CENSUS_ACS:acs5:B01003_001"


def _geo(cursor, geo_id, level, **fields) -> None:
    cursor.execute(
        """
        INSERT INTO gold_glossary.dim_geo_latest (
            geo_id, geo_level, state_fips, county_fips, place_fips, state_name,
            county_name, place_name, area_name, latitude, longitude, geo_geom,
            geography_state, refreshed_at
        ) VALUES (%s, %s, %s, %s, %s, 'Searchland', %s, %s, %s, %s, %s,
                  ST_GeomFromText(%s, 4326), 'current', NOW())
        ON CONFLICT (geo_id) DO NOTHING
        """,
        (
            geo_id,
            level,
            STATE if level != "METRO" else None,
            fields.get("county_fips"),
            fields.get("place_fips"),
            fields.get("county_name"),
            fields.get("place_name"),
            fields.get("area_name"),
            fields.get("latitude"),
            fields.get("longitude"),
            fields.get("geom"),
        ),
    )


def _population(cursor, geo_id, level, value) -> None:
    cursor.execute(
        """
        INSERT INTO gold_census.mv_acs_latest (
            observation_date, as_of_date, updated_at, geo_id, geo_level, value,
            dataset_code, vintage_year, table_id, variable_code, estimate_value,
            metric_code
        ) VALUES ('2096-01-01', '2096-01-01', NOW(), %s, %s, %s, 'acs5', 2096,
                  'B01003', 'B01003_001', %s, %s)
        """,
        (geo_id, level, value, value, POPULATION),
    )


@pytest.fixture
def searchland(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[TestClient]:
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _geo(cursor, f"state:{STATE}", "STATE")
            _geo(
                cursor,
                COUNTY,
                "COUNTY",
                county_fips="001",
                county_name="Riverbend County",
                geom=COUNTY_SQUARE,
            )
            _geo(
                cursor,
                CITY,
                "PLACE",
                place_fips="01000",
                place_name="Zephyrton city",
                latitude=30.3,
                longitude=-99.7,
                geom=CITY_SQUARE,
            )
            _geo(
                cursor,
                f"state:{STATE}|place:02000",
                "PLACE",
                place_fips="02000",
                place_name="Zephyrton Heights CDP",
                latitude=30.9,
                longitude=-99.1,
            )
            _geo(cursor, METRO, "METRO", area_name="Zephyrton-Riverbend, SL")
            _population(cursor, CITY, "PLACE", 50000)
            _population(cursor, f"state:{STATE}|place:02000", "PLACE", 900)
            _population(cursor, COUNTY, "COUNTY", 80000)
            # The metro has no membership here (that rides the reference
            # bridge), so it ranks with no population, after served ones.
        writer.commit()
    finally:
        writer.close()

    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": settings.host,
            "port": settings.port,
            "user": settings.user,
            "password": settings.password,
            "dbname": settings.database,
        },
        pool_pre_ping=True,
    )

    def override_db() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_census.mv_acs_latest WHERE geo_id LIKE %s",
                    (f"state:{STATE}|%",),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_geo_latest WHERE geo_id LIKE %s OR geo_id IN (%s, %s)",
                    (f"state:{STATE}|%", f"state:{STATE}", METRO),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_a_city_is_found_by_name_with_its_county_beside_it(
    searchland: TestClient,
) -> None:
    """Covers: API-171 -- ranked by match and population; the county follows its city."""
    response = searchland.get(
        "/api/v1/catalog/geographies/search", params={"q": "zephyrton", "limit": 5}
    )
    assert response.status_code == 200, response.text
    items = response.json()["items"]
    ids = [item["geo_id"] for item in items]
    # The metro has no figures of its own; the bigger city outranks the CDP.
    assert ids.index(CITY) < ids.index(f"state:{STATE}|place:02000")
    city = items[ids.index(CITY)]
    assert city["match"] == "prefix"
    assert city["county_name"] == "Riverbend County"
    assert city["population"] == 50000
    county = items[ids.index(CITY) + 1]
    assert county["geo_id"] == COUNTY
    assert county["match"] == "contains_a_match"
    assert county["contains_geo_id"] == CITY


def test_a_search_refuses_a_level_it_does_not_search(searchland: TestClient) -> None:
    """Covers: API-171 -- an unknown level is a 422, and a one-letter query too."""
    assert (
        searchland.get(
            "/api/v1/catalog/geographies/search", params={"q": "ze", "level": "TRACT"}
        ).status_code
        == 422
    )
    assert (
        searchland.get(
            "/api/v1/catalog/geographies/search", params={"q": "z"}
        ).status_code
        == 422
    )


def test_a_point_lands_on_the_city_county_and_state_that_contain_it(
    searchland: TestClient,
) -> None:
    """Covers: API-171 -- smallest first; outside every boundary, nothing."""
    inside = searchland.get(
        "/api/v1/catalog/geographies/locate",
        params={"latitude": 30.3, "longitude": -99.7},
    ).json()["items"]
    assert [item["geo_id"] for item in inside][:2] == [CITY, COUNTY]
    county_only = searchland.get(
        "/api/v1/catalog/geographies/locate",
        params={"latitude": 30.9, "longitude": -99.1},
    ).json()["items"]
    assert [item["geo_id"] for item in county_only] == [COUNTY]
    assert (
        searchland.get(
            "/api/v1/catalog/geographies/locate", params={"latitude": 0, "longitude": 0}
        ).json()["items"]
        == []
    )
