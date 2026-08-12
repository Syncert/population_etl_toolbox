"""Configured production FastAPI clients for service-backed tests."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import create_app
from data_ingestion_toolbox.config import Settings
from tests.support.postgres import PostgresTestConfig


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
