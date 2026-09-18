"""What the refresh procedures say reaches the run's log.

Covers: DB-058 -- the procedures `RAISE NOTICE` their per-chunk row counts and,
        since DB-056, a `cleared_partitions=` marker saying whether the chunk
        truncated its partition or fell back to deleting. All of it landed in
        psycopg2's `connection.notices`, a list nothing read.

        Found by writing an operator instruction -- "each chunk logs
        `cleared_partitions=1`; a `0` means that chunk deleted rather than
        truncated" -- into `BETA_RESET_REINGESTION.md` section 7, then running
        a real re-serve and grepping the Airflow log for it. Zero occurrences.
        The instruction was unfollowable, and the signal it pointed at had
        never been visible to anyone.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.utility.gold_schema import _forward_procedure_notices

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_a_notice_raised_by_a_procedure_is_logged(
    postgres_connection_factory: Callable[[], connection],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: DB-058 — a NOTICE becomes a log line, not a discarded list entry."""
    log = logging.getLogger("test.chunk.notices")
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute("DO $$ BEGIN RAISE NOTICE 'cleared_partitions=1'; END $$")
        with caplog.at_level(logging.INFO, logger="test.chunk.notices"):
            _forward_procedure_notices(database, log)
    finally:
        database.close()

    assert any("cleared_partitions=1" in record.message for record in caplog.records), (
        "the procedure's notice did not reach the log, so an operator watching "
        "a re-serve cannot tell a chunk that truncated its partition from one "
        "that deleted the range instead"
    )


def test_a_warning_is_logged_as_a_warning(
    postgres_connection_factory: Callable[[], connection],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: DB-058 — the one line that must not look like progress.

    The refresh raises exactly one warning: that the relation is not
    partitioned and the chunk deleted rather than truncated. In a twenty-chunk
    run at INFO level it would scroll past as another status line.
    """
    log = logging.getLogger("test.chunk.notices.warn")
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute("DO $$ BEGIN RAISE WARNING 'not partitioned'; END $$")
        with caplog.at_level(logging.INFO, logger="test.chunk.notices.warn"):
            _forward_procedure_notices(database, log)
    finally:
        database.close()

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("not partitioned" in record.message for record in warnings), (
        f"the refusal was not logged as a warning: "
        f"{[(r.levelname, r.message) for r in caplog.records]}"
    )


def test_the_notices_are_cleared_so_a_chunk_does_not_report_the_last_one(
    postgres_connection_factory: Callable[[], connection],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: DB-058 — twenty chunks, twenty reports, not a growing repeat.

    `connection.notices` accumulates for the life of the connection. Without
    clearing it, chunk two would re-log chunk one's lines and chunk twenty
    would log all twenty -- and every one of them would claim to be about the
    year currently being served.
    """
    log = logging.getLogger("test.chunk.notices.clear")
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute("DO $$ BEGIN RAISE NOTICE 'first chunk'; END $$")
        _forward_procedure_notices(database, log)

        with database.cursor() as cursor:
            cursor.execute("DO $$ BEGIN RAISE NOTICE 'second chunk'; END $$")
        with caplog.at_level(logging.INFO, logger="test.chunk.notices.clear"):
            _forward_procedure_notices(database, log)
    finally:
        database.close()

    messages = [record.message for record in caplog.records]
    assert any("second chunk" in message for message in messages)
    assert not any("first chunk" in message for message in messages), (
        f"the previous chunk's notice was reported again under this chunk: {messages}"
    )
