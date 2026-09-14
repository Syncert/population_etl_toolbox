"""Evidence packets against the real app_api schema (ADR-0004).

Covers: API-072 — the checked-in bootstrap DDL creates the packet table the
        service queries; the full lifecycle round-trips through PostgreSQL
        with owner scoping and optimistic concurrency enforced by the
        database itself; a contradiction is refused by the real stack; and
        an incomplete block is stored and reported.
        DB-027 — deleting an account cascades to its packets, and the
        per-owner name uniqueness is a database constraint.
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
def packet_api(
    postgres_connection_factory: Callable[[], connection],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[tuple[TestClient, str, str, str]]:
    """Apply the real bootstrap DDL, seed two accounts and a metric, serve the app."""
    token = uuid4().hex
    other_token = uuid4().hex
    label = f"packet-primary-{token[:8]}"
    other_label = f"packet-secondary-{other_token[:8]}"
    metric_code = f"FRED:PACKET_{token[:10].upper()}"

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
            cursor.execute(
                """
                INSERT INTO gold_glossary.dim_metric_catalog (
                    metric_code, metric_display_name, source_code,
                    source_object_type, source_object_key,
                    valid_geo_grains, valid_time_grains
                ) VALUES (%s, 'Evidence packet fixture', 'FRED', 'FRED_SERIES',
                          %s, ARRAY['NATIONAL'], ARRAY['MONTHLY'])
                """,
                (metric_code, metric_code.split(":", 1)[1]),
            )
            cursor.executemany(
                "INSERT INTO app_api.user_account (display_label, token_sha256) VALUES (%s, %s)",
                [(label, hash_token(token)), (other_label, hash_token(other_token))],
            )
        writer.commit()
    finally:
        writer.close()

    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    monkeypatch.setenv(
        "APP_API_DATABASE_URL",
        "postgresql+psycopg2://"
        f"{settings.user}:{settings.password}@{settings.host}:{settings.port}/{settings.database}",
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
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code = %s",
                    (metric_code,),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _packet(metric_code: str, *, with_empty: bool = False) -> dict:
    query = {
        "kind": "observations",
        "metric_code": metric_code,
        "scope": "latest",
        "filters": {"geo_level": "NATIONAL"},
        "visualization": {},
    }
    envelope = {
        "metric_codes": [metric_code],
        "source_codes": ["FRED"],
        "geo_id": "US",
        "geo_level": "NATIONAL",
        "scope": "latest",
        "release": "",
        "period": "2026-08",
        "units": "Percent",
        "transformation": "none",
        "api_query": f"/api/v1/observations?metric_code={metric_code}",
        "caveats": ["national series"],
    }
    blocks = [
        {
            "block_id": "summary",
            "type": "text",
            "title": "Summary",
            "content": "The need.",
        },
        {
            "block_id": "evidence",
            "type": "analysis",
            "title": "Evidence",
            "envelope": envelope,
            "document": query,
        },
        {
            "block_id": "limits",
            "type": "caveat",
            "title": "Limits",
            "content": "Associations.",
        },
    ]
    if with_empty:
        blocks.append(
            {"block_id": "condition", "type": "analysis", "title": "Condition"}
        )
    return {
        "schema_version": 1,
        "title": "Needs assessment",
        "purpose": "Why",
        "blocks": blocks,
    }


def test_packet_lifecycle_against_the_real_schema(
    packet_api: tuple[TestClient, str, str, str],
) -> None:
    """Covers: API-072 — the real DDL, storage, scoping, validation, concurrency."""
    client, token, other_token, metric_code = packet_api

    created = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(token),
        json={"name": "my-packet", "document": _packet(metric_code, with_empty=True)},
    )
    assert created.status_code == 201, created.text
    payload = created.json()
    packet_id = payload["packet_id"]
    assert payload["version"] == 1
    assert created.headers["cache-control"] == "private, no-store"
    # Incomplete is stored and reported, per block.
    assert payload["validation"]["valid"] is False
    states = {state["block_id"]: state for state in payload["validation"]["blocks"]}
    assert states["evidence"]["valid"] is True
    assert states["condition"]["valid"] is False
    assert "no reproducibility envelope" in states["condition"]["reason"]

    detail = client.get(f"/api/v1/evidence-packets/{packet_id}", headers=_auth(token))
    assert detail.status_code == 200
    stored = detail.json()["document"]
    # Block order and every recorded field survive the JSONB round trip.
    assert [block["block_id"] for block in stored["blocks"]] == [
        "summary",
        "evidence",
        "limits",
        "condition",
    ]
    assert stored["blocks"][1]["envelope"]["caveats"] == ["national series"]
    assert stored["blocks"][1]["document"]["metric_code"] == metric_code

    listing = client.get("/api/v1/evidence-packets", headers=_auth(token))
    assert listing.json()["total"] == 1
    summary = listing.json()["items"][0]
    assert summary["block_count"] == 4 and summary["analytical_block_count"] == 2
    assert "validation" not in summary

    # The second account shares the database and sees none of it.
    assert (
        client.get("/api/v1/evidence-packets", headers=_auth(other_token)).json()[
            "total"
        ]
        == 0
    )
    stolen = client.get(
        f"/api/v1/evidence-packets/{packet_id}", headers=_auth(other_token)
    )
    assert stolen.status_code == 404 and stolen.json() == {"detail": "packet not found"}

    updated = client.put(
        f"/api/v1/evidence-packets/{packet_id}",
        headers=_auth(token),
        json={
            "name": "my-packet",
            "document": _packet(metric_code),
            "expected_version": 1,
        },
    )
    assert updated.status_code == 200 and updated.json()["version"] == 2
    assert updated.json()["validation"]["valid"] is True

    stale = client.put(
        f"/api/v1/evidence-packets/{packet_id}",
        headers=_auth(token),
        json={
            "name": "my-packet",
            "document": _packet(metric_code),
            "expected_version": 1,
        },
    )
    assert stale.status_code == 409 and "current version 2" in stale.json()["detail"]

    # A contradiction is refused by the real stack: the envelope names a
    # measure the query does not ask for.
    contradictory = _packet(metric_code)
    contradictory["blocks"][1]["envelope"]["metric_codes"] = [metric_code, "FRED:OTHER"]
    refused = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(token),
        json={"name": "contradictory", "document": contradictory},
    )
    assert refused.status_code == 422
    assert "does not ask for" in refused.json()["detail"]

    # A query the live contracts refuse is refused, naming the block.
    unknown = _packet(metric_code)
    unknown["blocks"][1]["document"]["metric_code"] = "NO:SUCH:METRIC"
    unknown["blocks"][1]["envelope"]["metric_codes"] = ["NO:SUCH:METRIC"]
    # And the recorded request names the same measure, so the block is
    # internally consistent and the refusal is the live contract's rather than
    # the recorded-request cross-check's (API-129).
    unknown["blocks"][1]["envelope"]["api_query"] = (
        "/api/v1/observations?metric_code=NO:SUCH:METRIC"
    )
    refused = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(token),
        json={"name": "unknown", "document": unknown},
    )
    assert refused.status_code == 422
    assert "block 'evidence'" in refused.json()["detail"]
    assert "not a published metric" in refused.json()["detail"]

    removed = client.delete(
        f"/api/v1/evidence-packets/{packet_id}", headers=_auth(token)
    )
    assert removed.status_code == 204
    assert (
        client.get(
            f"/api/v1/evidence-packets/{packet_id}", headers=_auth(token)
        ).status_code
        == 404
    )


def test_account_deletion_cascades_and_names_are_unique_per_owner(
    packet_api: tuple[TestClient, str, str, str],
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-027 — the database enforces cascade and per-owner uniqueness."""
    client, token, other_token, metric_code = packet_api
    mine = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(token),
        json={"name": "shared-name", "document": _packet(metric_code)},
    )
    theirs = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(other_token),
        json={"name": "shared-name", "document": _packet(metric_code)},
    )
    assert mine.status_code == 201 and theirs.status_code == 201, (
        "uniqueness is per owner"
    )
    duplicate = client.post(
        "/api/v1/evidence-packets",
        headers=_auth(token),
        json={"name": "shared-name", "document": _packet(metric_code)},
    )
    assert duplicate.status_code == 409

    remover = postgres_connection_factory()
    try:
        with remover.cursor() as cursor:
            cursor.execute(
                "DELETE FROM app_api.user_account WHERE token_sha256 = %s",
                (hash_token(other_token),),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM app_api.evidence_packet WHERE packet_id = %s",
                (theirs.json()["packet_id"],),
            )
            assert cursor.fetchone()[0] == 0, "the account's packets went with it"
            cursor.execute(
                "SELECT COUNT(*) FROM app_api.evidence_packet WHERE packet_id = %s",
                (mine.json()["packet_id"],),
            )
            assert cursor.fetchone()[0] == 1, "another owner's packet is untouched"
        remover.commit()
    finally:
        remover.close()
