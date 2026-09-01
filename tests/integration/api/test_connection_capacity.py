"""API behavior when its PostgreSQL connection pool is exhausted."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from tests.support.postgres import PostgresTestConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.api,
    pytest.mark.database,
    pytest.mark.slow,
]


def test_api_fails_fast_safely_and_recovers_after_pool_exhaustion() -> None:
    """Covers: RES-008 — pool exhaustion yields sanitized 503 then recovers."""
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
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.1,
    )

    def override_db() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    held_connection = engine.connect()
    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            exhausted = client.get("/api/v1/catalog/sources")
            held_connection.close()
            recovered = client.get("/api/v1/catalog/sources")
        assert exhausted.status_code == 503
        assert exhausted.json() == {
            "detail": "Database service is temporarily unavailable."
        }
        assert settings.password not in exhausted.text
        assert recovered.status_code == 200
    finally:
        if not held_connection.closed:
            held_connection.close()
        app.dependency_overrides.clear()
        engine.dispose()
