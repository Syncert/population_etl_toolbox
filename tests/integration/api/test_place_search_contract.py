"""Finding a place by name or location against the real projections.

Covers DB-066 (the projection rebuild) and the database-tier halves of
API-170 (search) and API-171 (locate). Every geography here is fictional --
state 96, names no real place has, square boundaries in the open Pacific --
so nothing another suite seeds can match a query or contain a point.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from tests.support.capture_seed import delete_seed_captures, seed_capture
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

POP = "CENSUS_ACS:acs5:B01003_001"
STATE = "state:96"
QUILLON = "state:96|county:001"
AMBROSE = "state:96|county:003"
ZEPHYRTOWN = "state:96|place:98760"
SPRINGS = "state:96|place:98770"
RETIRED = "state:96|place:98780"
TRACT = "state:96|county:001|tract:000100"
METRO = "cbsa:99960"
ALL_IDS = [STATE, QUILLON, AMBROSE, ZEPHYRTOWN, SPRINGS, RETIRED, TRACT, METRO]

#: (geo_id, level, state, county, place, state_name, county_name, place_name,
#:  area_name, state of the row, envelope as xmin, ymin, xmax, ymax)
GEOGRAPHIES = [
    (
        STATE,
        "STATE",
        "96",
        None,
        None,
        "Zephyria",
        None,
        None,
        None,
        "current",
        (-150.0, 10.0, -149.0, 11.0),
    ),
    (
        QUILLON,
        "COUNTY",
        "96",
        "001",
        None,
        "Zephyria",
        "Quillon County",
        None,
        None,
        "current",
        (-150.0, 10.0, -149.5, 11.0),
    ),
    (
        AMBROSE,
        "COUNTY",
        "96",
        "003",
        None,
        "Zephyria",
        "Ambrose County",
        None,
        None,
        "current",
        (-149.5, 10.0, -149.0, 11.0),
    ),
    (
        ZEPHYRTOWN,
        "PLACE",
        "96",
        None,
        "98760",
        "Zephyria",
        None,
        "Zephyrtown city",
        None,
        "current",
        (-149.9, 10.2, -149.45, 10.5),
    ),
    (
        SPRINGS,
        "PLACE",
        "96",
        None,
        "98770",
        "Zephyria",
        None,
        "Zephyr Springs town",
        None,
        "current",
        (-149.3, 10.6, -149.1, 10.7),
    ),
    (
        RETIRED,
        "PLACE",
        "96",
        None,
        "98780",
        "Zephyria",
        None,
        "Zephyrtown Old city",
        None,
        "retired",
        (-149.9, 10.2, -149.6, 10.5),
    ),
    (
        TRACT,
        "TRACT",
        "96",
        "001",
        None,
        "Zephyria",
        "Quillon County",
        None,
        "Census Tract 1",
        "current",
        (-150.0, 10.0, -149.5, 10.5),
    ),
    (
        METRO,
        "METRO",
        None,
        None,
        None,
        None,
        None,
        None,
        "Zephyrtown-Quill, ZZ",
        "current",
        None,
    ),
]

#: (geo_id, vintage, population, value_status)
POPULATIONS = [
    (ZEPHYRTOWN, 2019, 40000, "valid"),
    (ZEPHYRTOWN, 2023, 50000, "valid"),
    (SPRINGS, 2023, None, "absent"),
    (QUILLON, 2023, 80000, "valid"),
    (AMBROSE, 2023, 120000, "valid"),
    (STATE, 2023, 200000, "valid"),
]

#: Reference entities the relationship bridge cites: (geo_id, geo_type, state,
#: county, place, area_code).
ENTITIES = [
    (QUILLON, "county", "96", "001", None, None),
    (AMBROSE, "county", "96", "003", None, None),
    (ZEPHYRTOWN, "place", "96", None, "98760", None),
    (METRO, "metro", None, None, None, "99960"),
]

#: (parent, related, type, overlap_weight)
LINKS = [
    (QUILLON, ZEPHYRTOWN, "intersects", 0.9),
    (AMBROSE, ZEPHYRTOWN, "intersects", 0.1),
    (METRO, QUILLON, "contains", None),
    (METRO, AMBROSE, "contains", None),
]


def _cleanup(cursor, capture_ids) -> None:
    cursor.execute(
        "DELETE FROM gold_glossary.geo_search WHERE geo_id = ANY(%s)", (ALL_IDS,)
    )
    cursor.execute(
        "DELETE FROM gold_census.mv_acs_latest WHERE geo_id = ANY(%s) AND metric_code = %s",
        (ALL_IDS, POP),
    )
    cursor.execute(
        "DELETE FROM silver_ref.bridge_geo_relationship_version WHERE parent_geo_sk IN "
        "(SELECT geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s))",
        (ALL_IDS,),
    )
    cursor.execute(
        "DELETE FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)", (ALL_IDS,)
    )
    cursor.execute(
        "DELETE FROM gold_glossary.dim_geo_latest WHERE geo_id = ANY(%s)", (ALL_IDS,)
    )
    delete_seed_captures(cursor, capture_ids)


@pytest.fixture
def zephyria(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[tuple[TestClient, Callable[[], connection]]]:
    capture_ids = []
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _cleanup(cursor, [])
            for row in GEOGRAPHIES:
                (
                    geo_id,
                    level,
                    state,
                    county,
                    place,
                    state_name,
                    county_name,
                    place_name,
                    area_name,
                    geography_state,
                    envelope,
                ) = row
                cursor.execute(
                    """
                    INSERT INTO gold_glossary.dim_geo_latest (
                        geo_id, geo_level, state_fips, county_fips, place_fips,
                        state_name, county_name, place_name, area_name,
                        geo_geom, boundary_vintage, geography_state, retired_at,
                        refreshed_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        CASE WHEN %s::float8 IS NULL THEN NULL
                             ELSE ST_Multi(ST_MakeEnvelope(%s, %s, %s, %s, 4326)) END,
                        2023, %s,
                        CASE WHEN %s = 'retired' THEN NOW() END, NOW()
                    )
                    """,
                    (
                        geo_id,
                        level,
                        state,
                        county,
                        place,
                        state_name,
                        county_name,
                        place_name,
                        area_name,
                        envelope[0] if envelope else None,
                        *(envelope or (None, None, None, None)),
                        geography_state,
                        geography_state,
                    ),
                )
            for geo_id, vintage, value, status in POPULATIONS:
                cursor.execute(
                    """
                    INSERT INTO gold_census.mv_acs_latest (
                        observation_date, as_of_date, updated_at, geo_id,
                        geo_level, state_fips, value, dataset_code,
                        vintage_year, table_id, variable_code, estimate_value,
                        value_status, metric_code
                    ) VALUES (MAKE_DATE(%s, 1, 1), MAKE_DATE(%s + 1, 12, 1),
                              NOW(), %s, 'PLACE', '96', %s, 'acs5', %s,
                              'B01003', 'B01003_001', %s, %s, %s)
                    """,
                    (vintage, vintage, geo_id, value, vintage, value, status, POP),
                )
            capture = seed_capture(cursor, "CENSUS_GEO")
            capture_ids.append(capture)
            for geo_id, geo_type, state, county, place, area in ENTITIES:
                cursor.execute(
                    """
                    INSERT INTO silver_ref.dim_geo_entity (
                        geo_id, geo_type, state_fips, county_fips, place_fips,
                        area_code, first_seen_version, last_seen_version
                    ) VALUES (%s, %s, %s, %s, %s, %s, 2023, 2023)
                    """,
                    (geo_id, geo_type, state, county, place, area),
                )
            for parent, related, kind, weight in LINKS:
                cursor.execute(
                    """
                    INSERT INTO silver_ref.bridge_geo_relationship_version (
                        parent_geo_sk, related_geo_sk, relationship_type,
                        geography_vintage, overlap_weight, evidence_source,
                        source_snapshot_id
                    )
                    SELECT parent.geo_sk, related.geo_sk, %s, 2023, %s,
                           'place_search_fixture', %s
                    FROM silver_ref.dim_geo_entity AS parent,
                         silver_ref.dim_geo_entity AS related
                    WHERE parent.geo_id = %s AND related.geo_id = %s
                    """,
                    (kind, weight, capture, parent, related),
                )
        writer.commit()
        writer.autocommit = True
        with writer.cursor() as cursor:
            cursor.execute("CALL gold_glossary.refresh_geo_search()")
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
        yield TestClient(app), postgres_connection_factory
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                _cleanup(cursor, capture_ids)
            cleanup.commit()
        finally:
            cleanup.close()


def test_the_projection_rebuilds_from_the_published_geography(zephyria) -> None:
    """Covers: DB-066 — short names, containing county, principal state and labelled population."""
    _, factory = zephyria
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT geo_id, geo_level, geo_name, short_name, name_words,
                       short_words, state_fips, state_name, county_geo_id,
                       county_name, population, population_year,
                       population_metric_code
                FROM gold_glossary.geo_search WHERE geo_id = ANY(%s)
                """,
                (ALL_IDS,),
            )
            columns = [column.name for column in cursor.description]
            rows = {row[0]: dict(zip(columns, row)) for row in cursor.fetchall()}
            cursor.execute(
                "SELECT geo_id FROM gold_glossary.geo_boundary WHERE geo_id = ANY(%s)",
                (ALL_IDS,),
            )
            boundaries = {row[0] for row in cursor.fetchall()}
    finally:
        reader.close()

    # Tracts are not searched by name, and a retired place is not offered.
    assert set(rows) == {STATE, QUILLON, AMBROSE, ZEPHYRTOWN, SPRINGS, METRO}
    city = rows[ZEPHYRTOWN]
    assert city["short_name"] == "Zephyrtown"
    assert city["geo_name"] == "Zephyrtown city"
    assert city["name_words"] == " zephyrtown city"
    assert city["short_words"] == " zephyrtown"
    assert city["county_geo_id"] == QUILLON
    assert city["county_name"] == "Quillon County"
    assert city["state_name"] == "Zephyria"
    assert (
        city["population"],
        city["population_year"],
        city["population_metric_code"],
    ) == (
        50000,
        2023,
        POP,
    )
    assert rows[SPRINGS]["population"] is None
    assert rows[QUILLON]["short_name"] == "Quillon"
    assert rows[METRO]["short_name"] == "Zephyrtown-Quill"
    # The metro's principal state is its most populous county's.
    assert rows[METRO]["state_fips"] == "96"
    assert rows[METRO]["state_name"] == "Zephyria"
    assert rows[STATE]["geo_name"] == "Zephyria"
    # Current boundaries only; a metro has none of its own.
    assert boundaries == {STATE, QUILLON, AMBROSE, ZEPHYRTOWN, SPRINGS, TRACT}


def test_search_answers_the_city_then_its_county_and_metro(zephyria) -> None:
    """Covers: API-170 — the real query ranks, expands and addresses the matches."""
    client, _ = zephyria
    response = client.get(
        "/api/v1/catalog/geographies/search", params={"q": "Zephyrtown"}
    )
    assert response.status_code == 200, response.text
    items = response.json()["items"]
    assert [(item["geo_id"], item["match"]) for item in items[:3]] == [
        (ZEPHYRTOWN, "exact"),
        (QUILLON, "contains_match"),
        (METRO, "prefix"),
    ]
    assert items[0]["page_path"] == "/us/zephyria/zephyrtown-city"
    assert items[1]["page_path"] == "/us/zephyria/quillon-county"
    assert items[1]["matched_via"]["geo_id"] == ZEPHYRTOWN
    assert RETIRED not in {item["geo_id"] for item in items}

    springs = client.get(
        "/api/v1/catalog/geographies/search", params={"q": "zephyr spr"}
    )
    (item,) = [row for row in springs.json()["items"] if row["geo_id"] == SPRINGS]
    assert item["match"] == "prefix"
    assert item["population"] is None

    typo = client.get(
        "/api/v1/catalog/geographies/search",
        params={"q": "Zephyrtwn", "grain": "PLACE"},
    )
    assert typo.status_code == 200
    assert (typo.json()["items"][0]["geo_id"], typo.json()["items"][0]["match"]) == (
        ZEPHYRTOWN,
        "fuzzy",
    )


def test_locate_lands_on_the_city_or_the_county(zephyria) -> None:
    """Covers: API-171 — the real point-in-polygon read and the county's metro."""
    client, _ = zephyria
    town = client.get(
        "/api/v1/catalog/geographies/locate", params={"lat": 10.3, "lon": -149.8}
    )
    assert town.status_code == 200, town.text
    payload = town.json()
    assert payload["landing"]["geo_id"] == ZEPHYRTOWN
    assert payload["landing"]["page_path"] == "/us/zephyria/zephyrtown-city"
    assert payload["county"]["geo_id"] == QUILLON
    assert payload["tract"]["geo_id"] == TRACT
    assert payload["state"]["geo_id"] == STATE
    assert payload["metro"]["geo_id"] == METRO

    rural = client.get(
        "/api/v1/catalog/geographies/locate", params={"lat": 10.9, "lon": -149.2}
    )
    assert rural.status_code == 200
    assert rural.json()["place"] is None
    assert rural.json()["landing"]["geo_id"] == AMBROSE
    assert rural.json()["metro"]["geo_id"] == METRO

    ocean = client.get(
        "/api/v1/catalog/geographies/locate", params={"lat": -60, "lon": -150}
    )
    assert ocean.status_code == 404
