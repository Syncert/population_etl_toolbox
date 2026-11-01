"""Telling an operator's stop from a fault, and failing a task without a retry.

An operator stops a runaway statement with `pg_cancel_backend` (QueryCanceled
"due to user request") or `pg_terminate_backend` (AdminShutdown). Neither is a
fault to retry: in November 2026 an operator stopped a 3 h 15 m ACS read and
Airflow started it again at once. A statement timeout is also a QueryCanceled,
but it is the task's own limit, so it stays an ordinary, retryable error.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg2


def stopped_by_operator(exc: BaseException) -> bool:
    """Whether ``exc`` (or what caused it) is a statement an operator stopped."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, psycopg2.errors.AdminShutdown):
            return True
        if isinstance(current, psycopg2.errors.QueryCanceled):
            return "statement timeout" not in str(current)
        current = current.__cause__ or current.__context__
    return False


@contextmanager
def fail_without_retry_when_stopped(what: str, resume: str) -> Iterator[None]:
    """Re-raise an operator's stop inside the block as ``AirflowFailException``.

    ``what`` names the work ("the ACS serving refresh") and ``resume`` says
    what clearing the task does, so the failed task's log tells the operator
    both. Any other error propagates unchanged and keeps its retries.
    """
    try:
        yield
    except Exception as exc:
        if not stopped_by_operator(exc):
            raise
        from airflow.exceptions import AirflowFailException

        raise AirflowFailException(
            f"An operator stopped {what}; not retrying. {resume}"
        ) from exc
