"""The API-owned database engine, with declared limits (API-006).

The API used to share ``data_ingestion_toolbox.db``'s bare engine, which
carried SQLAlchemy defaults only: a 30-second wait on an exhausted pool, no
server-side statement timeout, no connect timeout, and no disposal on
shutdown. Those budgets belong to the API deployment, not to the ETL package,
so they are declared here and configured through ``Settings``:

- ``pool_timeout`` makes pool exhaustion fail fast into the sanitized 503
  instead of queueing requests behind a saturated pool.
- ``statement_timeout`` is the cancellation contract: a runaway query is
  cancelled server-side rather than holding a connection indefinitely.
- ``connect_timeout`` bounds how long an unreachable database can stall a
  request before the same sanitized 503.
- ``isolation_level`` is ``REPEATABLE READ``: every read in one request sees
  one snapshot, so a page and its total cannot describe different sets of
  rows (API-100).
- ``dispose_engine`` runs in the application's shutdown hook so connections
  are returned before the process exits (graceful shutdown).

ETL connections are owned elsewhere and deliberately keep their own budgets;
a 15-second statement timeout that is right for a bounded public read would
kill a legitimate warehouse refresh.
"""

from __future__ import annotations

import os
from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from data_ingestion_toolbox.config import Settings, get_settings

_engine = None
_session_factory: sessionmaker | None = None


class DatabaseNotConfigured(RuntimeError):
    """No database URL is configured for this deployment.

    A distinct type because it must answer the same sanitized 503 as any other
    unavailability rather than escaping as an unhandled error. The readiness
    probe in particular has to *report* an unservable process; a probe that
    raises tells orchestration nothing it can act on.
    """


def _build_engine(settings: Settings):
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        raise DatabaseNotConfigured(
            "DATABASE_URL environment variable is not set. "
            "Configure it before starting the API server."
        )
    connect_args: dict[str, object] = {
        "connect_timeout": settings.db_connect_timeout_seconds,
    }
    if settings.db_statement_timeout_ms > 0:
        connect_args["options"] = (
            f"-c statement_timeout={settings.db_statement_timeout_ms}"
        )
    return create_engine(
        database_url,
        # One snapshot per request. API-084 fixed this once, for one
        # statement: a range and its counts taken in two executions let a
        # `REFRESH MATERIALIZED VIEW CONCURRENTLY` commit between them and
        # describe two different sets of rows. Every paged read has that
        # shape -- a `COUNT(*)` and then a `SELECT … LIMIT … OFFSET …` -- and
        # under PostgreSQL's default `READ COMMITTED` each statement takes its
        # own snapshot, so a `total` can be counted over one set of rows and
        # the page taken from another.
        #
        # `REPEATABLE READ` takes one snapshot for the whole transaction, and
        # this role is read-only, so it has no write conflict to lose to and
        # nothing to serialize beyond that. The snapshot is the session's
        # transaction and the session is closed per request, so it ends there
        # rather than serving a warehouse that stops advancing (API-100).
        isolation_level="REPEATABLE READ",
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout_seconds,
        pool_recycle=settings.db_pool_recycle_seconds,
        connect_args=connect_args,
    )


def get_api_engine(settings: Settings | None = None):
    """The process-wide API engine, built lazily from the active settings."""
    global _engine, _session_factory
    if _engine is None:
        _engine = _build_engine(settings or get_settings())
        _session_factory = sessionmaker(bind=_engine, autocommit=False, autoflush=False)
    return _engine


def get_db_session() -> Generator[Session, None, None]:
    """Yield a session from the API engine, closed after use."""
    get_api_engine()
    assert _session_factory is not None
    session: Session = _session_factory()
    try:
        yield session
    finally:
        session.close()


def dispose_engine() -> None:
    """Return every pooled connection; called from the shutdown hook."""
    global _engine, _session_factory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _session_factory = None
