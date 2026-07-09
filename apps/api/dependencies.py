import logging

from fastapi import HTTPException

from data_ingestion_toolbox.db import get_db_session

logger = logging.getLogger(__name__)


def get_db_session_dep():
    yield from get_db_session()


def db_service_unavailable(exc: Exception) -> HTTPException:
    logger.exception("Database service unavailable", exc_info=exc)
    return HTTPException(
        status_code=503,
        detail="Database service is temporarily unavailable.",
    )
