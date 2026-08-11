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
