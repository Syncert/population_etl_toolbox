"""FastAPI routes exercised against the actual gold warehouse schema."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]


@pytest.fixture
def real_api_fixture(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[tuple[TestClient, str, str]]:
    token = uuid4().hex[:12].upper()
    metric_a = f"FRED:TEST_API_{token}_A"
    metric_b = f"FRED:TEST_API_{token}_B"
    series_a = f"TEST_API_{token}_A"
    series_b = f"TEST_API_{token}_B"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO gold_glossary.dim_metric_catalog (
                    metric_code, metric_display_name, source_code,
                    source_object_type, valid_geo_grains, valid_time_grains,
                    dashboard_suitability, do_not_compare_with,
                    recommended_aggregation, owner_team, is_active
                ) VALUES
                    (%s, 'API fixture A', 'FRED', 'FRED_SERIES', ARRAY['NATIONAL'],
                     ARRAY['MONTHLY'], 'PUBLIC_SAFE', ARRAY[]::TEXT[], 'LAST', 'test', TRUE),
                    (%s, 'API fixture B', 'FRED', 'FRED_SERIES', ARRAY['NATIONAL'],
                     ARRAY['MONTHLY'], 'PUBLIC_SAFE', ARRAY[]::TEXT[], 'LAST', 'test', TRUE)
                """,
                (metric_a, metric_b),
            )
            cursor.execute(
                """
                INSERT INTO gold_fred.rpt_fred_observations (
                    source_code, observation_date, duration_start, duration_end,
                    time_sk, as_of_date, updated_at, geo_id, geo_level,
                    series_id, series_title, value, units, frequency,
                    metric_code, metric_display_name, dashboard_suitability
                ) VALUES
                    ('FRED', '2097-01-01', '2097-01-01', '2097-01-31', 20970101,
                     '2097-02-01', NOW(), 'us:1', 'NATIONAL', %s, 'API fixture A',
                     10, 'Index', 'Monthly', %s, 'API fixture A', 'PUBLIC_SAFE'),
                    ('FRED', '2097-02-01', '2097-02-01', '2097-02-28', 20970201,
                     '2097-03-01', NOW(), 'us:1', 'NATIONAL', %s, 'API fixture A',
                     20, 'Index', 'Monthly', %s, 'API fixture A', 'PUBLIC_SAFE'),
                    ('FRED', '2097-02-01', '2097-02-01', '2097-02-28', 20970201,
                     '2097-03-01', NOW(), 'us:1', 'NATIONAL', %s, 'API fixture B',
                     5, 'Index', 'Monthly', %s, 'API fixture B', 'PUBLIC_SAFE')
                """,
                (series_a, metric_a, series_a, metric_a, series_b, metric_b),
            )
            cursor.execute(
                """
                INSERT INTO gold_fred.mv_fred_latest
                SELECT * FROM gold_fred.rpt_fred_observations
                WHERE series_id IN (%s, %s) AND observation_date = '2097-02-01'
                """,
                (series_a, series_b),
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

    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        yield TestClient(app), metric_a, metric_b
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_fred.mv_fred_latest WHERE series_id IN (%s, %s)",
                    (series_a, series_b),
                )
                cursor.execute(
                    "DELETE FROM gold_fred.rpt_fred_observations WHERE series_id IN (%s, %s)",
                    (series_a, series_b),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code IN (%s, %s)",
                    (metric_a, metric_b),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_real_catalog_latest_timeseries_distribution_and_comparison(
    real_api_fixture: tuple[TestClient, str, str],
) -> None:
    """Covers: API-024 — real-schema API calls return exact seeded results."""
    client, metric_a, metric_b = real_api_fixture

    catalog = client.get("/api/catalog/metrics", params={"q": metric_a, "limit": 10})
    assert catalog.status_code == 200
    assert [item["metric_code"] for item in catalog.json()["items"]] == [metric_a]

    latest = client.get("/api/observations/latest", params={"metric_code": metric_a})
    assert latest.status_code == 200
    assert latest.json()["total"] == 1
    assert latest.json()["items"][0]["value"] == "20"

    timeseries = client.get(
        "/api/observations/timeseries",
        params={"metric_code": metric_a, "geo_id": "us:1"},
    )
    assert timeseries.status_code == 200
    assert [item["value"] for item in timeseries.json()["items"]] == ["10", "20"]

    distribution = client.get(
        "/api/distribution/bins", params={"metric_code": metric_a, "bin_count": 5}
    )
    assert distribution.status_code == 200
    assert distribution.json()["total"] == 1
    assert distribution.json()["items"][0]["count"] == 1

    comparison = client.get(
        "/api/comparison",
        params={"metric_code_a": metric_a, "metric_code_b": metric_b},
    )
    assert comparison.status_code == 200
    assert comparison.json()["items"] == [
        {
            "geo_id": "us:1",
            "geo_level": "NATIONAL",
            "state_fips": None,
            "county_fips": None,
            "state_name": None,
            "county_name": None,
            "metric_code_a": metric_a,
            "metric_code_b": metric_b,
            "value_a": 20.0,
            "value_b": 5.0,
            "difference": 15.0,
            "ratio": 4.0,
        }
    ]


@pytest.fixture
def census_bls_api_fixture(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[tuple[TestClient, str, str]]:
    """Seed matching Census and BLS source rows in the actual serving relations."""
    token = uuid4().hex[:10].upper()
    census_metric = f"ACS:acs5:TEST_{token}"
    bls_metric = f"BLS:TEST_{token}"
    census_variable = f"TEST_{token}E"
    bls_series = f"LAUTEST{token}"
    geo_id = f"state:55|county:{token[:3]}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO gold_glossary.dim_metric_catalog (
                    metric_code, metric_display_name, source_code,
                    source_object_type, valid_geo_grains, valid_time_grains,
                    dashboard_suitability, do_not_compare_with,
                    recommended_aggregation, owner_team, is_active
                ) VALUES
                    (%s, 'Census API fixture', 'CENSUS_ACS', 'ACS_VARIABLE',
                     ARRAY['COUNTY'], ARRAY['ANNUAL'], 'PUBLIC_SAFE',
                     ARRAY[]::TEXT[], 'LAST', 'test', TRUE),
                    (%s, 'BLS API fixture', 'BLS', 'BLS_SERIES',
                     ARRAY['COUNTY'], ARRAY['MONTHLY'], 'PUBLIC_SAFE',
                     ARRAY[]::TEXT[], 'LAST', 'test', TRUE)
                """,
                (census_metric, bls_metric),
            )
            cursor.execute(
                """
                INSERT INTO gold_census.rpt_acs_observations (
                    observation_date, duration_start, duration_end, time_sk,
                    as_of_date, updated_at, geo_id, geo_level, state_fips,
                    county_fips, state_name, county_name, value, dataset_code,
                    vintage_year, table_id, variable_code, estimate_value,
                    units, metric_code, metric_display_name, dashboard_suitability
                ) VALUES
                    ('2096-01-01', '2092-01-01', '2096-12-31', 20960101,
                     '2097-01-01', NOW(), %s, 'COUNTY', '55', %s,
                     'Wisconsin', 'API County', 100, 'acs5', 2096, 'TEST', %s,
                     100, 'people', %s, 'Census API fixture', 'PUBLIC_SAFE'),
                    ('2097-01-01', '2093-01-01', '2097-12-31', 20970101,
                     '2098-01-01', NOW(), %s, 'COUNTY', '55', %s,
                     'Wisconsin', 'API County', 110, 'acs5', 2097, 'TEST', %s,
                     110, 'people', %s, 'Census API fixture', 'PUBLIC_SAFE')
                """,
                (
                    geo_id,
                    token[:3],
                    census_variable,
                    census_metric,
                    geo_id,
                    token[:3],
                    census_variable,
                    census_metric,
                ),
            )
            cursor.execute(
                """
                INSERT INTO gold_census.mv_acs_latest
                SELECT * FROM gold_census.rpt_acs_observations
                WHERE metric_code = %s AND observation_date = '2097-01-01'
                """,
                (census_metric,),
            )
            cursor.execute(
                """
                INSERT INTO gold_bls.rpt_bls_observations (
                    observation_date, duration_start, duration_end, time_sk,
                    as_of_date, updated_at, geo_id, geo_level, state_fips,
                    county_fips, state_name, county_name, series_id, program_code,
                    value, value_type, units, metric_code, metric_display_name,
                    dashboard_suitability
                ) VALUES
                    ('2097-01-01', '2097-01-01', '2097-01-31', 20970101,
                     '2097-02-01', NOW(), %s, 'COUNTY', '55', %s,
                     'Wisconsin', 'API County', %s, 'LA', 4, 'RATE', 'percent',
                     %s, 'BLS API fixture', 'PUBLIC_SAFE'),
                    ('2097-02-01', '2097-02-01', '2097-02-28', 20970201,
                     '2097-03-01', NOW(), %s, 'COUNTY', '55', %s,
                     'Wisconsin', 'API County', %s, 'LA', 5, 'RATE', 'percent',
                     %s, 'BLS API fixture', 'PUBLIC_SAFE')
                """,
                (
                    geo_id,
                    token[:3],
                    bls_series,
                    bls_metric,
                    geo_id,
                    token[:3],
                    bls_series,
                    bls_metric,
                ),
            )
            cursor.execute(
                """
                INSERT INTO gold_bls.mv_bls_latest
                SELECT * FROM gold_bls.rpt_bls_observations
                WHERE metric_code = %s AND observation_date = '2097-02-01'
                """,
                (bls_metric,),
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

    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        yield TestClient(app), census_metric, bls_metric
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                for relation in (
                    "gold_census.mv_acs_latest",
                    "gold_census.rpt_acs_observations",
                    "gold_bls.mv_bls_latest",
                    "gold_bls.rpt_bls_observations",
                ):
                    cursor.execute(
                        f"DELETE FROM {relation} WHERE metric_code IN (%s, %s)",
                        (census_metric, bls_metric),
                    )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code IN (%s, %s)",
                    (census_metric, bls_metric),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_real_database_contract_spans_census_bls_and_cross_source_views(
    census_bls_api_fixture: tuple[TestClient, str, str],
) -> None:
    """Covers: API-024 — all source routers and shared views use the real schema."""
    client, census_metric, bls_metric = census_bls_api_fixture

    for source, metric, expected in (
        ("census", census_metric, ["100", "110"]),
        ("bls", bls_metric, ["4", "5"]),
    ):
        catalog = client.get("/api/catalog/metrics", params={"q": metric, "limit": 10})
        assert catalog.status_code == 200
        assert [row["metric_code"] for row in catalog.json()["items"]] == [metric]

        latest = client.get(
            f"/api/{source}/observations/latest", params={"metric_code": metric}
        )
        assert latest.status_code == 200
        assert latest.json()["total"] == 1
        assert latest.json()["items"][0]["source"] in {"CENSUS_ACS", "BLS"}

        history = client.get(
            f"/api/{source}/observations/timeseries",
            params={
                "metric_code": metric,
                "geo_id": latest.json()["items"][0]["geo_id"],
            },
        )
        assert history.status_code == 200
        assert [row["value"] for row in history.json()["items"]] == expected

        common = client.get("/api/observations/latest", params={"metric_code": metric})
        assert common.status_code == 200
        assert common.json()["total"] == 1

        distribution = client.get(
            "/api/distribution/bins",
            params={"metric_code": metric, "bin_count": 1},
        )
        assert distribution.status_code == 200
        assert distribution.json()["total"] == 1

    comparison = client.get(
        "/api/comparison",
        params={"metric_code_a": census_metric, "metric_code_b": bls_metric},
    )
    assert comparison.status_code == 200
    assert comparison.json()["items"][0]["difference"] == 105.0
