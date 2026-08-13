"""Bounded retry policies used by production database transaction boundaries."""

from __future__ import annotations

import psycopg2
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

DATABASE_RETRY_ATTEMPTS = 3


def is_retryable_database_error(error: BaseException) -> bool:
    """Classify connection, serialization, and deadlock failures as transient."""
    if isinstance(
        error,
        (
            psycopg2.OperationalError,
            psycopg2.InterfaceError,
            psycopg2.errors.SerializationFailure,
            psycopg2.errors.DeadlockDetected,
        ),
    ):
        return True
    code = getattr(error, "pgcode", None)
    return code in {"40001", "40P01"} or (
        isinstance(code, str) and code.startswith("08")
    )


retry_database_transaction = retry(
    reraise=True,
    stop=stop_after_attempt(DATABASE_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=0.1, min=0.1, max=1.0),
    retry=retry_if_exception(is_retryable_database_error),
)
