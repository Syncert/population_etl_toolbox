"""Unit contracts for database retry classification."""

from __future__ import annotations

import psycopg2
import pytest

from data_ingestion_toolbox.utility.retry import is_retryable_database_error

pytestmark = pytest.mark.unit


class _DatabaseError(Exception):
    def __init__(self, pgcode: str | None) -> None:
        super().__init__(pgcode)
        self.pgcode = pgcode


@pytest.mark.parametrize(
    "error",
    [
        psycopg2.OperationalError("connection lost"),
        psycopg2.InterfaceError("connection closed"),
        psycopg2.errors.SerializationFailure("serialization failure"),
        psycopg2.errors.DeadlockDetected("deadlock"),
        _DatabaseError("40001"),
        _DatabaseError("40P01"),
        _DatabaseError("08006"),
    ],
)
def test_retryable_database_failures_are_classified(error: BaseException) -> None:
    """Covers: RES-004 — transient database failures are retry eligible."""
    assert is_retryable_database_error(error) is True


@pytest.mark.parametrize("pgcode", [None, "23505", 8006])
def test_terminal_database_failures_are_not_retried(pgcode: object) -> None:
    """Covers: RES-004 — terminal database failures are not retried."""
    assert is_retryable_database_error(_DatabaseError(pgcode)) is False
