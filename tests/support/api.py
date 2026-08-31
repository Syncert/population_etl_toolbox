"""Configured production FastAPI clients for service-backed tests."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app, create_app
from data_ingestion_toolbox.config import Settings
from tests.support.postgres import PostgresTestConfig


@contextmanager
def real_api_client() -> Iterator[TestClient]:
    """Serve the production application over the disposable warehouse.

    End-to-end nodes assert through the same FastAPI app the deployment runs,
    with only the session dependency pointed at the test database, so a router,
    schema, or serialization defect is visible where a consumer would see it.
    """
    postgres = PostgresTestConfig.from_environment()
    if postgres is None:
        raise RuntimeError("the real API client requires disposable PostgreSQL")
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": postgres.host,
            "port": postgres.port,
            "user": postgres.user,
            "password": postgres.password,
            "dbname": postgres.database,
        },
        pool_pre_ping=True,
    )

    def database_session() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = database_session
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


@contextmanager
def configured_api_client(
    monkeypatch,
    *,
    redis_url: str = "",
    cache_ttl_seconds: int = 300,
) -> Iterator[TestClient]:
    """Run the production app factory against the configured disposable DB."""
    postgres = PostgresTestConfig.from_environment()
    if postgres is None:
        raise RuntimeError("configured API client requires disposable PostgreSQL")
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": postgres.host,
            "port": postgres.port,
            "user": postgres.user,
            "password": postgres.password,
            "dbname": postgres.database,
        },
        pool_pre_ping=True,
    )

    def database_session() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    monkeypatch.setenv("REDIS_URL", redis_url)
    monkeypatch.setenv("API_CACHE_TTL_SECONDS", str(cache_ttl_seconds))
    application = create_app(Settings())
    application.dependency_overrides[get_db_session_dep] = database_session
    try:
        with TestClient(application) as client:
            yield client
    finally:
        application.dependency_overrides.clear()
        engine.dispose()
