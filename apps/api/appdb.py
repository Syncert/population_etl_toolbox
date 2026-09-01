"""The API-owned application-storage engine (ADR-0003, API-007).

Saved analysis configurations live in ``app_api``, written through a
separately configured engine and a separate database role
(``api_app_writer``). The warehouse engine in ``apps.api.database`` keeps its
read-only serving role, so a warehouse read and an application write can never
share a transaction, a connection, or a privilege by accident -- which is the
whole point of giving user-owned data its own boundary.

Storage is optional. When ``APP_API_DATABASE_URL`` is unset the saved-analysis
routes are still served (the contract stays stable across deployments) but
answer a clear 503: an unconfigured feature is a deployment fact, not a
caller error, and refusing to authenticate is the only honest response when
credentials cannot be verified at all.
"""

from __future__ import annotations

import os
from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from data_ingestion_toolbox.config import Settings, get_settings

APP_STORAGE_UNCONFIGURED_DETAIL = (
    "saved analysis configuration storage is not configured for this deployment"
)

_engine = None
_session_factory: sessionmaker | None = None


def app_storage_url() -> str:
    return os.environ.get("APP_API_DATABASE_URL", "")


def app_storage_configured() -> bool:
    return bool(app_storage_url())


def get_app_engine(settings: Settings | None = None):
    """The process-wide application-storage engine, built lazily."""
    global _engine, _session_factory
    if _engine is None:
        url = app_storage_url()
        if not url:
            raise RuntimeError("APP_API_DATABASE_URL is not set")
        configured = settings or get_settings()
        _engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_size=configured.db_pool_size,
            max_overflow=configured.db_max_overflow,
            pool_timeout=configured.db_pool_timeout_seconds,
            pool_recycle=configured.db_pool_recycle_seconds,
            connect_args={
                "connect_timeout": configured.db_connect_timeout_seconds,
            },
        )
        _session_factory = sessionmaker(bind=_engine, autocommit=False, autoflush=False)
    return _engine


def get_app_session() -> Generator[Session, None, None]:
    """Yield an application-storage session, closed after use."""
    get_app_engine()
    assert _session_factory is not None
    session: Session = _session_factory()
    try:
        yield session
    finally:
        session.close()


def dispose_app_engine() -> None:
    """Return every pooled application connection; called at shutdown."""
    global _engine, _session_factory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _session_factory = None
