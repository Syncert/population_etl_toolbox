"""Unit contracts for the API-owned database engine and its declared limits.

Covers: API-016 (lazy, explicit, request-scoped database sessions),
        API-058 (the engine carries the declared budgets — fail-fast pool
        timeout, server-side statement timeout, connect timeout — and is
        disposed on shutdown).
"""

from __future__ import annotations

import pytest

from apps.api import database
from data_ingestion_toolbox.config import Settings

pytestmark = [pytest.mark.unit, pytest.mark.api]


def _fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(database, "_engine", None)
    monkeypatch.setattr(database, "_session_factory", None)


def test_database_engine_requires_explicit_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-016 — absent database configuration fails explicitly."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    _fresh(monkeypatch)

    with pytest.raises(RuntimeError, match="DATABASE_URL environment variable"):
        database.get_api_engine(Settings())


def test_engine_carries_the_declared_budgets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-058 — pool, statement, and connect limits come from settings."""
    recorded: dict = {}

    def create_engine(url, **kwargs):
        recorded["url"] = url
        recorded.update(kwargs)
        return object()

    monkeypatch.setenv("DATABASE_URL", "postgresql://fixture.invalid/test")
    monkeypatch.setenv("API_DB_POOL_SIZE", "3")
    monkeypatch.setenv("API_DB_MAX_OVERFLOW", "4")
    monkeypatch.setenv("API_DB_POOL_TIMEOUT_SECONDS", "2")
    monkeypatch.setenv("API_DB_CONNECT_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("API_DB_STATEMENT_TIMEOUT_MS", "1234")
    monkeypatch.setattr(database, "create_engine", create_engine)
    _fresh(monkeypatch)

    database.get_api_engine(Settings())

    assert recorded["pool_pre_ping"] is True
    assert recorded["pool_size"] == 3
    assert recorded["max_overflow"] == 4
    assert recorded["pool_timeout"] == 2, "pool exhaustion must fail fast"
    assert recorded["connect_args"]["connect_timeout"] == 1
    assert recorded["connect_args"]["options"] == "-c statement_timeout=1234", (
        "a runaway query must be cancelled server-side"
    )


def test_statement_timeout_zero_disables_the_option(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-058 — 0 disables the timeout instead of passing a bad option."""
    recorded: dict = {}

    def create_engine(url, **kwargs):
        recorded.update(kwargs)
        return object()

    monkeypatch.setenv("DATABASE_URL", "postgresql://fixture.invalid/test")
    monkeypatch.setenv("API_DB_STATEMENT_TIMEOUT_MS", "0")
    monkeypatch.setattr(database, "create_engine", create_engine)
    _fresh(monkeypatch)

    database.get_api_engine(Settings())
    assert "options" not in recorded["connect_args"]


def test_engine_is_created_once_and_reused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-016 — configured engine setup is lazy and reused."""
    engine = object()
    calls: list[str] = []

    def create_engine(url, **kwargs):
        calls.append(url)
        return engine

    monkeypatch.setenv("DATABASE_URL", "postgresql://fixture.invalid/test")
    monkeypatch.setattr(database, "create_engine", create_engine)
    _fresh(monkeypatch)

    assert database.get_api_engine(Settings()) is engine
    assert database.get_api_engine(Settings()) is engine
    assert calls == ["postgresql://fixture.invalid/test"]


def test_database_session_is_closed_when_consumer_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-016 — request-scoped database sessions always close."""

    class Session:
        closed = False

        def close(self) -> None:
            self.closed = True

    session = Session()
    monkeypatch.setattr(database, "_engine", object())
    monkeypatch.setattr(database, "_session_factory", lambda: session)

    dependency = database.get_db_session()
    assert next(dependency) is session
    dependency.close()
    assert session.closed is True


def test_dispose_returns_connections_and_resets_the_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-058 — shutdown disposal returns every pooled connection."""

    class Engine:
        disposed = False

        def dispose(self) -> None:
            self.disposed = True

    engine = Engine()
    monkeypatch.setattr(database, "_engine", engine)
    monkeypatch.setattr(database, "_session_factory", object())

    database.dispose_engine()

    assert engine.disposed is True
    assert database._engine is None
    assert database._session_factory is None
