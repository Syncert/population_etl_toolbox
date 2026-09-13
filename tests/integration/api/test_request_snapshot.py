"""The API's warehouse reads within one request see one warehouse.

Covers: API-100 — a request reads one snapshot.

API-084 fixed this once, for one statement: a range and its counts taken in
two executions let a serving refresh commit between them and describe two
different sets of rows. Every paged read has the same shape -- a `COUNT(*)`
and then a `SELECT … LIMIT … OFFSET …` -- and under PostgreSQL's
`READ COMMITTED` each statement takes its own snapshot.

The commit that does it is a chunked rebuild of an ordinary table, not a view
swap: see DB-041, which holds the serving relations to that shape so this
reason stays checkable.

Proved here against a real PostgreSQL rather than by asserting a
configuration string: a second connection commits between two reads on an API
session, and the session must not see it.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection
from sqlalchemy import text

from apps.api import database as api_database
from tests.support.postgres import PostgresTestConfig

pytestmark = [pytest.mark.integration, pytest.mark.database, pytest.mark.api]

_RELATION = "public.api_snapshot_probe"


@pytest.fixture
def probe_relation(
    postgres_connection_factory: Callable[[], connection],
) -> Callable[[], connection]:
    """A table this test owns, created and dropped by it."""
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {_RELATION}")
            cursor.execute(f"CREATE TABLE {_RELATION} (id INT PRIMARY KEY)")
            cursor.execute(f"INSERT INTO {_RELATION} (id) VALUES (1)")
        writer.commit()
    finally:
        writer.close()
    try:
        yield postgres_connection_factory
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {_RELATION}")
            cleanup.commit()
        finally:
            cleanup.close()


@pytest.fixture
def api_session(monkeypatch: pytest.MonkeyPatch):
    """A session from the real API engine, against the test warehouse."""
    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg2://"
        f"{settings.user}:{settings.password}"
        f"@{settings.host}:{settings.port}/{settings.database}",
    )
    api_database.dispose_engine()
    sessions = api_database.get_db_session()
    session = next(sessions)
    try:
        yield session
    finally:
        session.close()
        api_database.dispose_engine()


def test_two_reads_in_one_request_see_one_warehouse(
    probe_relation, api_session
) -> None:
    """Covers: API-100 — a commit between two reads is not half-visible.

    The shape of every paged read: a total, then a page. Under `READ
    COMMITTED` the second statement takes a fresh snapshot, so a serving
    refresh committing in between answers a `total` counted over one set of
    rows and a page taken from another.
    """
    before = int(
        api_session.execute(text(f"SELECT COUNT(*) FROM {_RELATION}")).scalar()
    )
    assert before == 1

    writer = probe_relation()
    try:
        with writer.cursor() as cursor:
            cursor.execute(f"INSERT INTO {_RELATION} (id) VALUES (2)")
        writer.commit()
    finally:
        writer.close()

    rows = api_session.execute(text(f"SELECT id FROM {_RELATION} ORDER BY id")).all()
    assert len(rows) == before, (
        "the second read in this request saw a commit the first did not: a "
        "page and its total can describe different sets of rows"
    )


def test_the_next_request_sees_the_commit(probe_relation, api_session) -> None:
    """Covers: API-100 — the snapshot ends with the request, not the process.

    A snapshot held across requests would serve a warehouse that stops
    advancing. It is the session's transaction, and the session is closed per
    request, so the next one reads current rows.
    """
    assert (
        int(api_session.execute(text(f"SELECT COUNT(*) FROM {_RELATION}")).scalar())
        == 1
    )

    writer = probe_relation()
    try:
        with writer.cursor() as cursor:
            cursor.execute(f"INSERT INTO {_RELATION} (id) VALUES (3)")
        writer.commit()
    finally:
        writer.close()

    # What ends a request here: the session is closed and a new one opened,
    # exactly as `get_db_session` does between requests.
    api_session.close()
    assert (
        int(api_session.execute(text(f"SELECT COUNT(*) FROM {_RELATION}")).scalar())
        == 2
    )
