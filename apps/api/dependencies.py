import logging

from fastapi import HTTPException, Request
from fastapi.dependencies.utils import get_flat_params
from fastapi.params import Query
from fastapi.responses import JSONResponse

from apps.api.database import get_db_session

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


def declared_query_parameters(route: object) -> frozenset[str]:
    """The query-parameter names one route accepts, read from the route.

    Derived from the route's own solved dependency tree, which is what
    actually binds a request -- including parameters a shared dependency
    contributes rather than the endpoint signature. A list maintained beside
    the routers would be a second declaration to keep in step, and the reason
    an unknown parameter goes unnoticed is that nothing compares two
    declarations for a living.
    """
    dependant = getattr(route, "dependant", None)
    if dependant is None:
        return frozenset()
    return frozenset(
        field.alias
        for field in get_flat_params(dependant)
        if isinstance(field.field_info, Query)
    )


def reject_undeclared_query_parameters(request: Request) -> None:
    """Refuse a request carrying a query parameter its route does not declare.

    The serving registry already promises this of a filter a source does not
    declare -- "A request using a parameter absent here is rejected with an
    explanation, never silently ignored" -- and it was true only for names the
    route had heard of. FastAPI binds the parameters a signature names and
    discards the rest, so a misspelling reached no validation at all:
    ``geo_levels=COUNTY`` was answered with every grain, at 200, with a
    ``total`` that reads as a complete answer to the question the caller
    thought they asked (API-093).

    This API gives the same idea three spellings across its routes --
    ``adjustment_status`` and ``adjustment``, ``year_from``/``year_to`` and
    ``year_start``/``year_end`` -- so sending one route's name to another is
    an ordinary mistake, not an exotic one.

    The accepted names go in the message because the caller cannot see the
    signature; they are already published in ``/openapi.json``, so naming them
    here discloses nothing that document does not. For the same reason this
    runs before authentication on the private routes: the refusal describes
    the request's shape, not the resource's contents.
    """
    route = request.scope.get("route")
    declared = declared_query_parameters(route)
    unknown = sorted(set(request.query_params.keys()) - declared)
    if not unknown:
        return
    accepted = ", ".join(sorted(declared)) or "none"
    raise HTTPException(
        status_code=422,
        detail=(
            f"query parameters not accepted by this route: {', '.join(unknown)}; "
            f"accepted: {accepted}"
        ),
    )
