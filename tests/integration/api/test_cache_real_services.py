"""Configured FastAPI cache contracts against disposable PostgreSQL and Redis."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import create_app
from data_ingestion_toolbox.config import Settings
from data_ingestion_toolbox.fred.gold_fred import transform as gold_transform
from tests.support.postgres import PostgresHookStub, PostgresTestConfig
from tests.support.redis import RedisTestConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.api,
    pytest.mark.database,
    pytest.mark.redis,
]


def _redis_config() -> RedisTestConfig:
    configured = RedisTestConfig.from_environment()
    if configured is None:
        pytest.skip("configured cache test requires TEST_REDIS_URL")
    return configured


@pytest.fixture
def configured_cached_api(
    postgres_connection_factory: Callable[[], connection],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[TestClient, Callable[[str], None], str]]:
    """Build the production app once with real database and Redis settings."""
    redis_config = _redis_config()
    redis_client = redis_config.connect()
    redis_client.flushdb()
    redis_client.close()

    token = uuid4().hex[:12].upper()
    series_id = f"CACHE_{token}"
    metric_code = f"FRED:{series_id}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_time (
                    time_sk, date_key, year, quarter, month, day, day_of_week,
                    day_name, month_name, week_of_year, is_weekend,
                    is_month_start, is_month_end, is_quarter_start,
                    is_quarter_end, is_year_start, is_year_end, ingested_at
                ) VALUES (
                    20961201, '2096-12-01', 2096, 4, 12, 1, 7,
                    'Saturday', 'December', 48, TRUE,
                    TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, NOW()
                ) ON CONFLICT (time_sk) DO NOTHING
                """,
            )
            cursor.execute(
                """
                INSERT INTO silver_fred.fact_economic_indicators (
                    time_sk, duration_start, duration_end, observation_date,
                    series_id, domain, value, is_missing, series_title,
                    unit_of_measure, frequency, seasonal_adjustment,
                    source_system, load_batch_id, ingested_at
                ) VALUES (
                    20961201, '2096-12-01', '2096-12-31', '2096-12-01',
                    %s, 'cache-test', 1, FALSE, 'Cache version one',
                    'Index', 'Monthly', 'Not Adjusted', 'FRED', %s, NOW()
                )
                """,
                (series_id, str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()
    hook = PostgresHookStub(postgres_connection_factory)
    gold_transform.refresh_fred_elements(hook)

    postgres_config = PostgresTestConfig.from_environment()
    assert postgres_config is not None
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": postgres_config.host,
            "port": postgres_config.port,
            "user": postgres_config.user,
            "password": postgres_config.password,
            "dbname": postgres_config.database,
        },
        pool_pre_ping=True,
    )

    def database_session() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    monkeypatch.setenv("REDIS_URL", redis_config.url)
    monkeypatch.setenv("API_CACHE_TTL_SECONDS", "1")
    application = create_app(Settings())
    application.dependency_overrides[get_db_session_dep] = database_session

    def refresh_metric(display_name: str) -> None:
        updater = postgres_connection_factory()
        try:
            with updater.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE silver_fred.fact_economic_indicators
                    SET series_title = %s, ingested_at = clock_timestamp()
                    WHERE series_id = %s
                    """,
                    (display_name, series_id),
                )
            updater.commit()
        finally:
            updater.close()
        gold_transform.refresh_fred_elements(hook)

    try:
        with TestClient(application) as client:
            yield client, refresh_metric, metric_code
    finally:
        application.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM gold_glossary.bridge_metric_fred_series b
                    USING gold_fred.dim_fred_series s
                    WHERE b.fred_series_sk = s.fred_series_sk AND s.series_id = %s
                    """,
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code = %s",
                    (metric_code,),
                )
                cursor.execute(
                    "DELETE FROM gold_fred.dim_fred_series WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM silver_fred.fact_economic_indicators WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20961201"
                )
            cleanup.commit()
        finally:
            cleanup.close()
        redis_client = redis_config.connect()
        redis_client.flushdb()
        redis_client.close()


@pytest.mark.slow
def test_configured_app_cache_miss_hit_expiry_and_refresh_policy(
    configured_cached_api: tuple[TestClient, Callable[[str], None], str],
) -> None:
    """Covers: API-019, API-021, API-022 — real cache honors refresh TTL."""
    client, refresh_metric, metric_code = configured_cached_api
    target = "/api/catalog/metrics"
    params = {"q": metric_code, "limit": 10}

    first = client.get(target, params=params)
    second = client.get(target, params=params)
    assert first.status_code == second.status_code == 200
    assert first.headers["x-cache"] == "MISS"
    assert second.headers["x-cache"] == "HIT"
    assert second.content == first.content
    assert first.json()["items"][0]["metric_display_name"] == "Cache version one"

    refresh_metric("Cache version two")
    within_policy = client.get(target, params=params)
    assert within_policy.headers["x-cache"] == "HIT"
    assert within_policy.content == first.content

    time.sleep(1.1)
    after_expiry = client.get(target, params=params)
    assert after_expiry.status_code == 200
    assert after_expiry.headers["x-cache"] == "MISS"
    assert after_expiry.json()["items"][0]["metric_display_name"] == "Cache version two"


def test_configured_app_falls_back_to_database_when_redis_is_unavailable(
    postgres_connection_factory: Callable[[], connection],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-023 — the production app returns DB data without Redis."""
    postgres_config = PostgresTestConfig.from_environment()
    assert postgres_config is not None
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": postgres_config.host,
            "port": postgres_config.port,
            "user": postgres_config.user,
            "password": postgres_config.password,
            "dbname": postgres_config.database,
        },
        pool_pre_ping=True,
    )

    def database_session() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1/15")
    monkeypatch.setenv("API_CACHE_TTL_SECONDS", "1")
    application = create_app(Settings())
    application.dependency_overrides[get_db_session_dep] = database_session
    started = time.perf_counter()
    try:
        with TestClient(application) as client:
            response = client.get("/api/catalog/sources")
    finally:
        application.dependency_overrides.clear()
        engine.dispose()

    assert response.status_code == 200
    assert response.headers["x-cache"] == "MISS"
    assert isinstance(response.json(), list)
    assert time.perf_counter() - started < 1.5
