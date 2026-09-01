"""Saved analysis configurations against the real app_api schema (API-007).

Covers: API-064 — the bootstrap DDL creates the schema the service expects,
        the full lifecycle (create, read, list, update, delete) round-trips
        through PostgreSQL with owner scoping and optimistic concurrency
        enforced by the database itself, and the warehouse role's read-only
        privileges are untouched by the write path.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.auth import get_app_session_dep, hash_token
from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_SQL = REPOSITORY_ROOT / "sql/bootstrap/002_app_api.sql"


@pytest.fixture
def saved_analysis_api(
    postgres_connection_factory: Callable[[], connection],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[TestClient, str, str, str]]:
    """Apply the real bootstrap DDL, seed two accounts, serve the app."""
    token = uuid4().hex
    other_token = uuid4().hex
    label = f"primary-{token[:8]}"
    other_label = f"secondary-{other_token[:8]}"
    metric_code = f"FRED:SAVED_{token[:10].upper()}"

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            # The production bootstrap statement, run as written: this proves
            # the checked-in DDL creates what the service queries.
            cursor.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
            cursor.execute(
                """
                INSERT INTO gold_glossary.dim_metric_catalog (
                    metric_code, metric_display_name, source_code,
                    source_object_type, source_object_key,
                    valid_geo_grains, valid_time_grains
                ) VALUES (%s, 'Saved analysis fixture', 'FRED', 'FRED_SERIES',
                          %s, ARRAY['NATIONAL'], ARRAY['MONTHLY'])
                """,
                (metric_code, metric_code.split(":", 1)[1]),
            )
            cursor.executemany(
                """
                INSERT INTO app_api.user_account (display_label, token_sha256)
                VALUES (%s, %s)
                """,
                [
                    (label, hash_token(token)),
                    (other_label, hash_token(other_token)),
                ],
            )
        writer.commit()
    finally:
        writer.close()

    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    # Storage is configured for this deployment; the session itself is
    # overridden below, so the URL only has to declare the feature enabled.
    monkeypatch.setenv(
        "APP_API_DATABASE_URL",
        "postgresql+psycopg2://"
        f"{settings.user}:{settings.password}"
        f"@{settings.host}:{settings.port}/{settings.database}",
    )
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

    def session() -> Iterator[Session]:
        with Session(engine) as active:
            yield active

    app.dependency_overrides[get_app_session_dep] = session
    app.dependency_overrides[get_db_session_dep] = session
    try:
        yield TestClient(app), token, other_token, metric_code
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM app_api.user_account WHERE display_label IN (%s, %s)",
                    (label, other_label),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE metric_code = %s",
                    (metric_code,),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_saved_analysis_lifecycle_against_the_real_schema(
    saved_analysis_api: tuple[TestClient, str, str, str],
) -> None:
    """Covers: API-064 — the real DDL, storage, scoping, and concurrency."""
    client, token, other_token, metric_code = saved_analysis_api
    document = {
        "kind": "observations",
        "metric_code": metric_code,
        "scope": "latest",
        "filters": {"geo_level": "NATIONAL"},
        "visualization": {"chart": "line"},
    }

    created = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(token),
        json={"name": "my-view", "document": document},
    )
    assert created.status_code == 201, created.text
    payload = created.json()
    configuration_id = payload["configuration_id"]
    assert payload["version"] == 1
    assert payload["validation"] == {"valid": True, "reason": None}
    assert created.headers["cache-control"] == "private, no-store"

    detail = client.get(
        f"/api/v1/analysis-configurations/{configuration_id}", headers=_auth(token)
    )
    assert detail.status_code == 200
    stored = detail.json()["document"]
    # Every field the caller set survives the JSONB round trip unchanged; the
    # remaining keys are the contract's optional fields, explicitly null.
    assert {key: stored[key] for key in document} == document
    assert detail.json()["validation"]["valid"] is True

    listing = client.get("/api/v1/analysis-configurations", headers=_auth(token))
    assert listing.status_code == 200
    assert listing.json()["total"] == 1
    assert listing.json()["items"][0]["kind"] == "observations"

    # The second account shares the database and sees none of it.
    other_listing = client.get(
        "/api/v1/analysis-configurations", headers=_auth(other_token)
    )
    assert other_listing.json()["total"] == 0
    stolen = client.get(
        f"/api/v1/analysis-configurations/{configuration_id}",
        headers=_auth(other_token),
    )
    assert stolen.status_code == 404
    assert stolen.json() == {"detail": "configuration not found"}

    updated = client.put(
        f"/api/v1/analysis-configurations/{configuration_id}",
        headers=_auth(token),
        json={
            "name": "my-view",
            "document": {**document, "visualization": {"chart": "bar"}},
            "expected_version": 1,
        },
    )
    assert updated.status_code == 200
    assert updated.json()["version"] == 2

    stale = client.put(
        f"/api/v1/analysis-configurations/{configuration_id}",
        headers=_auth(token),
        json={"name": "my-view", "document": document, "expected_version": 1},
    )
    assert stale.status_code == 409
    assert "current version 2" in stale.json()["detail"]

    invalid = client.post(
        "/api/v1/analysis-configurations",
        headers=_auth(token),
        json={
            "name": "invalid",
            "document": {**document, "metric_code": "NO:SUCH:METRIC"},
        },
    )
    assert invalid.status_code == 422
    assert "not a published metric" in invalid.json()["detail"]

    removed = client.delete(
        f"/api/v1/analysis-configurations/{configuration_id}", headers=_auth(token)
    )
    assert removed.status_code == 204
    gone = client.get(
        f"/api/v1/analysis-configurations/{configuration_id}", headers=_auth(token)
    )
    assert gone.status_code == 404


def test_unknown_and_revoked_tokens_are_refused_by_the_real_store(
    saved_analysis_api: tuple[TestClient, str, str, str],
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: API-064 — revocation takes effect against the real table."""
    client, token, _other, _metric = saved_analysis_api

    assert (
        client.get(
            "/api/v1/analysis-configurations", headers=_auth("not-a-real-token")
        ).status_code
        == 401
    )

    assert (
        client.get("/api/v1/analysis-configurations", headers=_auth(token)).status_code
        == 200
    )

    revoker = postgres_connection_factory()
    try:
        with revoker.cursor() as cursor:
            cursor.execute(
                "UPDATE app_api.user_account SET revoked_at = NOW() "
                "WHERE token_sha256 = %s",
                (hash_token(token),),
            )
        revoker.commit()
    finally:
        revoker.close()

    refused = client.get("/api/v1/analysis-configurations", headers=_auth(token))
    assert refused.status_code == 401
    assert refused.json() == {"detail": "a valid bearer token is required"}
