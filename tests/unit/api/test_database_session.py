"""Unit contracts for lazy API database session creation."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox import db

pytestmark = [pytest.mark.unit, pytest.mark.api]


def test_database_engine_requires_explicit_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-016 — absent database configuration fails explicitly."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(db, "_engine", None)
    monkeypatch.setattr(db, "_SessionLocal", None)

    with pytest.raises(RuntimeError, match="DATABASE_URL environment variable"):
        db._get_engine()


def test_database_engine_is_created_once_and_reused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-016 — configured engine setup is lazy and reused."""
    engine = object()
    engine_calls: list[tuple[str, bool]] = []
    sessionmaker_calls: list[dict] = []

    def create_engine(url: str, *, pool_pre_ping: bool):
        engine_calls.append((url, pool_pre_ping))
        return engine

    def sessionmaker(**kwargs):
        sessionmaker_calls.append(kwargs)
        return object()

    monkeypatch.setenv("DATABASE_URL", "postgresql://fixture.invalid/test")
    monkeypatch.setattr(db, "_engine", None)
    monkeypatch.setattr(db, "_SessionLocal", None)
    monkeypatch.setattr(db, "create_engine", create_engine)
    monkeypatch.setattr(db, "sessionmaker", sessionmaker)

    assert db._get_engine() is engine
    assert db._get_engine() is engine
    assert engine_calls == [("postgresql://fixture.invalid/test", True)]
    assert sessionmaker_calls == [
        {"bind": engine, "autocommit": False, "autoflush": False}
    ]


def test_database_session_is_closed_when_consumer_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-016 — request-scoped database sessions always close."""

    class Session:
        closed = False

        def close(self) -> None:
            self.closed = True

    session = Session()
    monkeypatch.setattr(db, "_engine", object())
    monkeypatch.setattr(db, "_SessionLocal", lambda: session)

    dependency = db.get_db_session()
    assert next(dependency) is session
    dependency.close()
    assert session.closed is True
