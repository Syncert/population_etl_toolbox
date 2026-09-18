"""Two writers, one name, one row.

Covers: API-148 -- a name taken in a race is a conflict, against a real
        unique constraint rather than a stub that raises on cue.

The unit tier proves the service turns an `IntegrityError` into a `409`. What
it cannot prove is that PostgreSQL raises one here: that depends on
`UNIQUE (owner_user_id, name)` existing in `sql/bootstrap/002_app_api.sql` and
on both writers reaching the insert before either commits. This races two real
connections against the real schema.
"""

from __future__ import annotations

import pathlib
import threading
from collections.abc import Callable

import psycopg2
import pytest
from psycopg2.extensions import connection

pytestmark = [pytest.mark.integration, pytest.mark.database]

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[3]
APP_API_SQL = REPOSITORY_ROOT / "sql/bootstrap/002_app_api.sql"

_INSERT = """
INSERT INTO app_api.saved_analysis_configuration (owner_user_id, name, document)
VALUES (%s, %s, %s::jsonb)
RETURNING configuration_id
"""


@pytest.fixture
def an_account(postgres_connection_factory: Callable[[], connection]) -> int:
    """One account to own the racing writes, and its cleanup."""
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            cursor.execute(APP_API_SQL.read_text(encoding="utf-8"))
            cursor.execute(
                """
                INSERT INTO app_api.user_account (display_label, token_sha256)
                VALUES ('race-probe', repeat('a', 64))
                ON CONFLICT (token_sha256) DO UPDATE SET display_label = 'race-probe'
                RETURNING user_account_id
                """
            )
            owner = int(cursor.fetchone()[0])
    finally:
        database.close()

    try:
        yield owner
    finally:
        cleanup = postgres_connection_factory()
        cleanup.autocommit = True
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM app_api.saved_analysis_configuration "
                    "WHERE owner_user_id = %s",
                    (owner,),
                )
                cursor.execute(
                    "DELETE FROM app_api.user_account WHERE user_account_id = %s",
                    (owner,),
                )
        finally:
            cleanup.close()


def test_two_writers_racing_one_name_leave_one_row(
    postgres_connection_factory: Callable[[], connection],
    an_account: int,
) -> None:
    """Covers: API-148 — the constraint decides, and it decides once.

    Both threads are held at a barrier until each has opened a transaction, so
    neither can win by simply arriving first: they are both inside the window
    the service's check-then-insert leaves open.
    """
    name = "raced-configuration"
    ready = threading.Barrier(2, timeout=20)
    outcomes: list[tuple[str, str]] = []
    lock = threading.Lock()

    def writer() -> None:
        database = postgres_connection_factory()
        try:
            with database.cursor() as cursor:
                # Open the transaction before the barrier, so the wait happens
                # with both writers already inside one.
                cursor.execute("SELECT 1")
                ready.wait()
                try:
                    cursor.execute(_INSERT, (an_account, name, '{"probe": true}'))
                    cursor.fetchone()
                    database.commit()
                    with lock:
                        outcomes.append(("created", ""))
                except psycopg2.errors.UniqueViolation as conflict:
                    database.rollback()
                    with lock:
                        outcomes.append(
                            ("conflict", conflict.diag.constraint_name or "")
                        )
        finally:
            database.close()

    threads = [threading.Thread(target=writer) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
        assert not thread.is_alive(), "a racing writer did not finish"

    kinds = sorted(kind for kind, _ in outcomes)
    assert kinds == ["conflict", "created"], (
        f"expected exactly one create and one conflict, got {outcomes}"
    )

    conflicting = [detail for kind, detail in outcomes if kind == "conflict"]
    assert conflicting and "name" in conflicting[0], (
        "the refusal did not come from the owner/name unique constraint, so "
        f"this raced something else: {conflicting}"
    )

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM app_api.saved_analysis_configuration "
                "WHERE owner_user_id = %s AND name = %s",
                (an_account, name),
            )
            assert cursor.fetchone()[0] == 1, (
                "the race left more or fewer than one row, so the constraint "
                "is not what decided the winner"
            )
    finally:
        reader.close()
