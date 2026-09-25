"""A stored-work listing counts the rows it returns (API-103).

API-100 gave the *warehouse* engine one snapshot per request and its catalog
row names what it left alone: "the application-storage engine, which writes,
is unchanged". That was right about the engine and incomplete about the
reads. The two listings served off the application engine had the shape
API-100's own failure clause describes -- a `COUNT(*)`, then a `SELECT …
LIMIT … OFFSET …` -- and `app_api` runs at PostgreSQL's default `READ
COMMITTED`, so each statement took its own snapshot.

Flipping this engine would not do: it carries the optimistic-concurrency
`UPDATE`, whose design is that a stale `expected_version` matches no row and
answers 409. Under `REPEATABLE READ` a racing writer gets `could not
serialize access due to concurrent update` instead, which this router
sanitizes into a 503 -- a transient failure in place of the answer the caller
can act on. So the fix is API-084's: the total and the page in one statement,
which has nothing for a commit to land between at any isolation level.

Proved by landing a commit exactly at that seam, against a real PostgreSQL.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from apps.api.auth import get_app_session_dep
from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from tests.support.postgres import PostgresTestConfig
from tests.support.app_accounts import create_account

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_SQL = REPOSITORY_ROOT / "sql/bootstrap/002_app_api.sql"

#: How many rows each listing starts with, so a row arriving mid-read is a
#: visible disagreement rather than the difference between none and one.
SEEDED = 3


class StoredWorkFixture:
    """The served app, plus the seam a concurrent commit is landed in."""

    def __init__(
        self,
        client: TestClient,
        token: str,
        owner_user_id: int,
        metric_code: str,
        engine,
        writer_factory: Callable[[], connection],
    ) -> None:
        self.client = client
        self.token = token
        self.owner_user_id = owner_user_id
        self.metric_code = metric_code
        self._engine = engine
        self._writer_factory = writer_factory
        self.landed = 0

    def auth(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def commit_between_the_count_and_the_page(
        self, table: str, name: str, document: str
    ) -> None:
        """Commit one extra owned row the instant the count has been taken.

        Keyed on the statement rather than on execution order because
        ``require_account`` reads on this same session first: the seam is
        after the statement carrying the `COUNT(*)`, which is where the old
        implementation's second statement followed and where the fixed
        implementation has already finished reading.
        """

        def after_cursor_execute(conn, cursor, statement, *_args) -> None:
            if self.landed or "COUNT(*)" not in statement or table not in statement:
                return
            self.landed += 1
            writer = self._writer_factory()
            try:
                with writer.cursor() as inner:
                    inner.execute(
                        f"INSERT INTO app_api.{table} "
                        "(owner_user_id, name, version, document) "
                        "VALUES (%s, %s, 1, CAST(%s AS JSONB))",
                        (self.owner_user_id, name, document),
                    )
                writer.commit()
            finally:
                writer.close()

        event.listen(self._engine, "after_cursor_execute", after_cursor_execute)
        self._listener = after_cursor_execute

    def stop_landing(self) -> None:
        event.remove(self._engine, "after_cursor_execute", self._listener)


@pytest.fixture
def stored_work(
    postgres_connection_factory: Callable[[], connection],
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[StoredWorkFixture]:
    """The real bootstrap DDL, one account, and SEEDED rows in each table."""
    token = uuid4().hex
    label = f"listing-{token[:8]}"
    metric_code = f"FRED:LISTING_{token[:10].upper()}"

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
                ) VALUES (%s, 'Stored work fixture', 'FRED', 'FRED_SERIES',
                          %s, ARRAY['NATIONAL'], ARRAY['MONTHLY'])
                """,
                (metric_code, metric_code.split(":", 1)[1]),
            )
            owner_user_id = create_account(cursor, label, token)
        writer.commit()
    finally:
        writer.close()

    settings = PostgresTestConfig.from_environment()
    assert settings is not None
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
        yield StoredWorkFixture(
            TestClient(app),
            token,
            owner_user_id,
            metric_code,
            engine,
            postgres_connection_factory,
        )
    finally:
        app.dependency_overrides.clear()
        engine.dispose()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                # The account cascades to both tables, so the rows this test
                # committed behind the API's back go with it.
                cursor.execute(
                    "DELETE FROM app_api.user_account WHERE display_label = %s",
                    (label,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog "
                    "WHERE metric_code = %s",
                    (metric_code,),
                )
            cleanup.commit()
        finally:
            cleanup.close()


def _analysis_document(metric_code: str) -> dict:
    return {
        "kind": "observations",
        "metric_code": metric_code,
        "scope": "latest",
        "filters": {"geo_level": "NATIONAL"},
        "visualization": {"chart": "line"},
    }


def _packet_document(metric_code: str) -> dict:
    return {
        "schema_version": 1,
        "title": "Needs assessment",
        "purpose": "Why",
        "blocks": [
            {
                "block_id": "summary",
                "type": "text",
                "title": "Summary",
                "content": "The need.",
            }
        ],
    }


@pytest.mark.parametrize(
    ("resource", "table", "document_for"),
    [
        (
            "analysis-configurations",
            "saved_analysis_configuration",
            _analysis_document,
        ),
        ("evidence-packets", "evidence_packet", _packet_document),
    ],
)
def test_a_listing_counts_the_rows_it_returns(
    stored_work: StoredWorkFixture, resource: str, table: str, document_for
) -> None:
    """Covers: API-103 — the total and the page are one reading.

    A row committed by another request at the seam between the total and the
    page was counted by neither statement or by both, never by one: under
    `READ COMMITTED` the second statement took a fresh snapshot, so the page
    could carry a row the total did not count.
    """
    document = document_for(stored_work.metric_code)
    for index in range(SEEDED):
        created = stored_work.client.post(
            f"/api/v1/{resource}",
            headers=stored_work.auth(),
            json={"name": f"seeded-{index}", "document": document},
        )
        assert created.status_code == 201, created.text

    stored_work.commit_between_the_count_and_the_page(
        table, "zz-arrives-mid-read", json.dumps(document)
    )
    try:
        listing = stored_work.client.get(
            f"/api/v1/{resource}", headers=stored_work.auth()
        )
    finally:
        stored_work.stop_landing()

    assert listing.status_code == 200, listing.text
    body = listing.json()
    assert stored_work.landed == 1, (
        "the concurrent commit never landed, so this test proved nothing "
        "about the seam it exists to close"
    )
    assert len(body["items"]) == body["total"], (
        f"{resource} answered total {body['total']} beside "
        f"{len(body['items'])} items: the page and the total were taken from "
        "two different snapshots"
    )
    assert body["total"] == SEEDED

    # Non-vacuity from the other side: the commit was real, and the next
    # request -- a new session, a new transaction -- reads it.
    after = stored_work.client.get(f"/api/v1/{resource}", headers=stored_work.auth())
    assert after.json()["total"] == SEEDED + 1
    assert len(after.json()["items"]) == SEEDED + 1


@pytest.mark.parametrize(
    ("resource", "table", "document_for"),
    [
        (
            "analysis-configurations",
            "saved_analysis_configuration",
            _analysis_document,
        ),
        ("evidence-packets", "evidence_packet", _packet_document),
    ],
)
def test_an_empty_page_still_reports_the_true_total(
    stored_work: StoredWorkFixture, resource: str, table: str, document_for
) -> None:
    """Covers: API-103 — the total is the caller's count, not the page's.

    The one answer a single-statement total can get wrong: with no page row
    to carry it, an `offset` past the end must still report how many rows the
    caller owns, or the client is told its work is gone.
    """
    document = document_for(stored_work.metric_code)
    for index in range(SEEDED):
        created = stored_work.client.post(
            f"/api/v1/{resource}",
            headers=stored_work.auth(),
            json={"name": f"seeded-{index}", "document": document},
        )
        assert created.status_code == 201, created.text

    listing = stored_work.client.get(
        f"/api/v1/{resource}?offset={SEEDED + 10}", headers=stored_work.auth()
    )
    assert listing.status_code == 200, listing.text
    assert listing.json()["items"] == []
    assert listing.json()["total"] == SEEDED


def test_a_racing_update_still_answers_conflict_not_unavailable(
    stored_work: StoredWorkFixture,
) -> None:
    """Covers: API-103 — the write path's isolation is deliberately unchanged.

    This is why the engine was not simply flipped to `REPEATABLE READ`: the
    optimistic-concurrency `UPDATE` is designed so that a stale
    `expected_version` matches no row and answers 409. Under `REPEATABLE
    READ` the loser of the race raises a serialization failure instead, which
    the router sanitizes into a 503 the caller cannot act on.
    """
    document = _analysis_document(stored_work.metric_code)
    created = stored_work.client.post(
        "/api/v1/analysis-configurations",
        headers=stored_work.auth(),
        json={"name": "raced", "document": document},
    )
    assert created.status_code == 201, created.text
    configuration_id = created.json()["configuration_id"]

    first = stored_work.client.put(
        f"/api/v1/analysis-configurations/{configuration_id}",
        headers=stored_work.auth(),
        json={"name": "raced", "document": document, "expected_version": 1},
    )
    assert first.status_code == 200, first.text

    # The second writer read version 1 and is a full version behind.
    stale = stored_work.client.put(
        f"/api/v1/analysis-configurations/{configuration_id}",
        headers=stored_work.auth(),
        json={"name": "raced", "document": document, "expected_version": 1},
    )
    assert stale.status_code == 409, stale.text
    assert "current version 2" in stale.json()["detail"]
