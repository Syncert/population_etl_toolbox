import logging

from fastapi import HTTPException
from fastapi.responses import JSONResponse

from data_ingestion_toolbox.db import get_db_session

logger = logging.getLogger(__name__)

#: The single sanitized text every unavailability answers with. Callers must
#: not be able to tell a pool exhaustion from a missing relation from a
#: credential failure -- each of those would leak deployment state.
SERVICE_UNAVAILABLE_DETAIL = "Database service is temporarily unavailable."


def get_db_session_dep():
    yield from get_db_session()


def db_service_unavailable(exc: Exception) -> HTTPException:
    logger.exception("Database service unavailable", exc_info=exc)
    return HTTPException(status_code=503, detail=SERVICE_UNAVAILABLE_DETAIL)


def serving_contract_unavailable(exc: Exception) -> JSONResponse:
    """Answer a missing serving contract the way a database outage is answered.

    A relation the API declares a dependency on is absent, which is an
    infrastructure fault rather than anything the caller did. The relation name
    goes to the server log, where an operator can act on it; the response carries
    the same sanitized text as every other unavailability so it cannot be used to
    probe which warehouse objects exist.
    """
    logger.exception("Serving contract unavailable", exc_info=exc)
    return JSONResponse(
        status_code=503,
        content={"detail": SERVICE_UNAVAILABLE_DETAIL},
    )
