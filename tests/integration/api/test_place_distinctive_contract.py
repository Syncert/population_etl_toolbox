"""Within-parent percentile ranks against the real serving relations (API-165)."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from apps.api.services import distinctive_service
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

STATE = "97"
COUNTIES = [f"{index:03d}" for index in range(1, 14)]
# One county publishes no number (withheld), one publishes no row at all.
VALUES = {
    "001": 50,
    "002": 60,
    "003": 70,
    "004": 80,
    "005": 90,
    "006": 100,
    "007": 110,
    "008": 120,
    "009": 130,
    "010": 140,
    "011": 150,
    "012": None,
}


@pytest.fixture
def ranked_county(
    postgres_connection_factory: Callable[[], connection],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[TestClient, str]]:
    token = uuid4().hex[:8].upper()
    metric = f"CENSUS_ACS:acs5:RANK_{token}"
    variable = f"RANK_{token}E"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO gold_glossary.dim_metric_catalog (
                    metric_code, metric_display_name, source_code,
                    source_object_type, source_object_key,
                    valid_geo_grains, valid_time_grains, physical_lineage
                ) VALUES (%s, 'Rank fixture', 'CENSUS_ACS', 'ACS_VARIABLE', %s,
                          ARRAY['COUNTY'], ARRAY['ANNUAL'], %s::jsonb)
                """,
                (
                    metric,
                    variable,
                    json.dumps(
                        {
                            "schema": "gold_census",
                            "relation": "fact_acs_observation",
                            "key": metric.removeprefix("CENSUS_ACS:"),
                        }
                    ),
                ),
            )
            for county in COUNTIES:
                cursor.execute(
                    """
                    INSERT INTO gold_glossary.dim_geo_latest (geo_id, geo_level, state_fips, county_fips, state_name, county_name, geography_state, refreshed_at)
                    VALUES (%s, 'COUNTY', %s, %s, 'Rank State', %s, 'current', NOW())
                    ON CONFLICT (geo_id) DO NOTHING
                    """,
                    (
                        f"state:{STATE}|county:{county}",
                        STATE,
                        county,
                        f"County {county}",
                    ),
                )
            for county, value in VALUES.items():
                cursor.execute(
                    """
                    INSERT INTO gold_census.mv_acs_latest (
                        observation_date, duration_start, duration_end, time_sk,
                        as_of_date, updated_at, geo_id, geo_level, state_fips,
                        county_fips, state_name, county_name, value, dataset_code,
                        vintage_year, table_id, variable_code, estimate_value,
                        units, metric_code, metric_display_name
                    ) VALUES ('2097-01-01', '2093-01-01', '2097-12-31', 20970101,
                              '2098-01-01', NOW(), %s, 'COUNTY', %s, %s,
                              'Rank State', %s, %s, 'acs5', 2097, 'RANK', %s,
                              %s, 'dollars', %s, 'Rank fixture')
                    """,
                    (
                        f"state:{STATE}|county:{county}",
                        STATE,
                        county,
                        f"County {county}",
                        value,
                        variable,
                        value,
                        metric,
                    ),
                )
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

    monkeypatch.setattr(
        distinctive_service,
        "DISTINCTIVE_MEASURES",
        (metric, "CENSUS_ACS:acs5:NOT_PUBLISHED_ANYWHERE"),
    )
    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        yield TestClient(app), metric
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_census.mv_acs_latest WHERE metric_code = %s",
                    (metric,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code = %s",
                    (metric,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_geo_latest WHERE geo_id LIKE %s",
                    (f"state:{STATE}|%",),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_a_county_is_ranked_among_its_state_one_measure_at_a_time(
    ranked_county: tuple[TestClient, str],
) -> None:
    """Covers: API-165 — a real rank with withheld and missing siblings counted."""
    client, metric = ranked_county
    response = client.get(
        "/api/v1/place/distinctive", params={"geo_id": f"state:{STATE}|county:005"}
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["derived"] is True
    assert payload["parent_scope"] == f"state:{STATE}"
    [ranked] = payload["ranked"]
    assert ranked["metric_code"] == metric
    assert ranked["value"] == 90
    assert ranked["siblings_with_value"] == 10
    assert ranked["siblings_withheld"] == 1
    assert ranked["siblings_missing"] == 1
    assert ranked["siblings_below"] == 4
    assert ranked["percentile_rank"] == pytest.approx(0.4)
    assert "newest_per_geography=true" in ranked["request"]
    assert payload["not_ranked"] == [
        {
            "metric_code": "CENSUS_ACS:acs5:NOT_PUBLISHED_ANYWHERE",
            "reason": "not published in this catalog",
        }
    ]

    missing = client.get(
        "/api/v1/place/distinctive", params={"geo_id": "state:96|county:001"}
    )
    assert missing.status_code == 404
    assert missing.json() == {"detail": "geo_id not found"}
