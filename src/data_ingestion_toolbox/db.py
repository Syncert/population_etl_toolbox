"""Database session factory for the ETL toolbox.

Provides a SQLAlchemy session generator consumed by FastAPI dependency injection
via ``apps.api.dependencies.get_db_session_dep``.  The real connection is
established lazily from ``DATABASE_URL``; in unit tests the dependency is
overridden so this module is never called.
"""

from __future__ import annotations

import os
from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

_engine = None
_SessionLocal: sessionmaker | None = None


def _get_engine():
    global _engine, _SessionLocal
    if _engine is None:
        database_url = os.environ.get("DATABASE_URL", "")
        if not database_url:
            raise RuntimeError(
                "DATABASE_URL environment variable is not set. "
                "Configure it before starting the API server."
            )
        _engine = create_engine(database_url, pool_pre_ping=True)
        _SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False)
    return _engine


def get_db_session() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session and ensure it is closed after use."""
    _get_engine()
    assert _SessionLocal is not None
    db: Session = _SessionLocal()
    try:
        yield db
    finally:
        db.close()
