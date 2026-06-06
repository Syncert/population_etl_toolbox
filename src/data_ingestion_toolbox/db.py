from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from data_ingestion_toolbox.config import get_settings

_settings = get_settings()
_engine = create_engine(_settings.sqlalchemy_url, pool_pre_ping=True)
_SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False)


def get_engine():
    return _engine


def get_session_factory():
    return _SessionLocal


def get_db_session() -> Iterator[Session]:
    db = _SessionLocal()
    try:
        yield db
    finally:
        db.close()
