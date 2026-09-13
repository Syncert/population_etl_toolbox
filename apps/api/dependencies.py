import logging
import re

from fastapi import HTTPException, Request
from fastapi.dependencies.utils import get_flat_params
from fastapi.params import Query
from fastapi.responses import JSONResponse

from apps.api.database import get_db_session
from apps.api.registry import grain_refusal

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


#: Request parameters whose value space is closed, and how a value is checked.
#:
#: A filter whose vocabulary or shape is closed must refuse a value outside it
#: rather than bind it and answer an empty page (API-122). `geo_level` and
#: `geo_type` name a grain: the vocabulary is `registry.GEO_GRAINS`, published
#: per metric as `valid_geo_grains`, and the guide promises a grain read from
#: the catalog can be sent straight back. `state_fips` and `county_fips` have a
#: closed *shape* rather than a closed set -- the warehouse's own CHECK
#: constraints are `^[0-9]{2}$` and `^[0-9]{3}$` -- so a well-formed code that
#: names no state answers nothing (a fact about the warehouse) while `ZZ` is
#: refused (a fact about the request).
#:
#: `geo_id` is deliberately absent. Its shape is source-dependent -- `us:1`,
#: `state:NN`, `state:NN|county:NNN`, `state:NN|place:NNNNN`, and `agency:<ORI>`
#: for FBI UCR, whose tail is a provider string -- so a shape rule here would
#: be a second declaration of something the reference layer owns.
_FIPS_SHAPES: dict[str, tuple[re.Pattern[str], str]] = {
    "state_fips": (re.compile(r"\A[0-9]{2}\Z"), "two digits"),
    "county_fips": (re.compile(r"\A[0-9]{3}\Z"), "three digits"),
}
#: `geo_level` is the grain every shared route takes. `geo_type` is absent on
#: purpose: only `/cdc/observations` declares it, and that route already
#: refuses it against the three grains CDC actually publishes (API-116) --
#: a narrower and therefore more accurate refusal than this rule's five-word
#: vocabulary would give. Listing it here would replace an exact message with
#: a vaguer one.
_GRAIN_PARAMETERS = ("geo_level",)
CLOSED_PARAMETERS: frozenset[str] = frozenset(_GRAIN_PARAMETERS) | frozenset(
    _FIPS_SHAPES
)


def reject_values_outside_a_closed_set(request: Request) -> None:
    """Refuse a grain that is not one, and a FIPS code that is not one.

    Runs for every route from the application's own dependency list, so a
    route added later is covered without being named here -- the same reason
    `reject_undeclared_query_parameters` is mounted there.

    An empty value is treated as absent, because every service already treats
    it that way: `if state_fips:` is falsy, so the filter is not applied. A
    saved analysis document records `state_fips: ""` for a source that
    declares no state filter (API-117, WEB-075), and replaying one must not
    become a 422.
    """
    route = request.scope.get("route")
    declared = declared_query_parameters(route)
    for parameter in sorted(CLOSED_PARAMETERS & declared):
        value = request.query_params.get(parameter)
        if not value:
            continue
        shape = _FIPS_SHAPES.get(parameter)
        if shape is not None:
            pattern, expected = shape
            if not pattern.match(value):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"{parameter} must be {expected}; a well-formed code "
                        f"that names no geography answers an empty page, and "
                        f"this is not a well-formed code"
                    ),
                )
            continue
        refusal = grain_refusal(parameter, value)
        if refusal:
            raise HTTPException(status_code=422, detail=refusal)
