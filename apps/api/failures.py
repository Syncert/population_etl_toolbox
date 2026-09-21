"""What each served route can refuse with, declared once.

The guide's Errors table is the contract; this module is the one place that
says it in the document, so the two cannot drift (API-121). Each bundle below
is a reviewed statement about a *group* of routes -- what the shared
middleware and dependencies behind them can answer -- and a route adds the
failures only it can raise (a 404 for an identifier it resolves, a 409 for a
name it holds unique, a 413 for a body it parses).

Nothing here changes a status or a body. Every sentence is the one the
application already sends, imported from where it is raised rather than
restated, so a reworded refusal cannot leave the contract describing the old
one.
"""

from __future__ import annotations

from typing import Any, Union

from apps.api.dependencies import SERVICE_UNAVAILABLE_DETAIL
from apps.api.middleware import REQUEST_TOO_LARGE_DETAIL
from apps.api.ratelimit import RATE_LIMITED_DETAIL
from apps.api.schemas.errors import ErrorDetail, HTTPValidationError

#: One reviewed description per status. The 422 entry is the only one with two
#: bodies, and which one a client gets says who refused the request: the API
#: itself (a sentence) or validation before the endpoint ran (a list).
_DESCRIPTIONS: dict[int, str] = {
    401: (
        "Missing, malformed, unknown, or revoked bearer token. Identical for "
        "every case by design."
    ),
    403: (
        "Authenticated, and not authorized for this act. Account deletion is "
        "the only route that answers it: it requires a recent sign-in, and a "
        "long-lived session is not that."
    ),
    404: (
        "Unknown identifier, or a resource you do not own -- indistinguishable "
        "on purpose."
    ),
    409: "A version conflict, or a name you already use.",
    413: REQUEST_TOO_LARGE_DETAIL,
    422: (
        "A refused request. One the API decided -- an unsupported filter, a "
        "contradictory scope, an incompatible comparison, a reversed range, an "
        "invalid document -- answers a sentence under `detail`. One refused "
        "before the endpoint ran -- a missing, malformed, or out-of-bounds "
        "parameter, or a body that fails schema validation -- answers a list."
    ),
    429: RATE_LIMITED_DETAIL,
    503: SERVICE_UNAVAILABLE_DETAIL,
}

#: The two bodies a 422 can carry, as one declared union.
_UNPROCESSABLE_MODEL = Union[ErrorDetail, HTTPValidationError]

ResponseDeclarations = dict[int | str, dict[str, Any]]


def failures(*statuses: int) -> ResponseDeclarations:
    """Declare the given statuses, each with the body it actually answers."""
    declared: ResponseDeclarations = {}
    for status in statuses:
        model = _UNPROCESSABLE_MODEL if status == 422 else ErrorDetail
        declared[status] = {"model": model, "description": _DESCRIPTIONS[status]}
    return declared


#: Every route, including the health resource and the private stores: the
#: strict-parameter dependency (API-093) is applied application-wide and
#: refuses an undeclared query parameter with a 422 before any route runs.
EVERY_ROUTE_FAILURES: ResponseDeclarations = failures(422)

#: The public analytical reads: rate limited, and answered 503 when the
#: database or a required serving contract is unavailable.
WAREHOUSE_READ_FAILURES: ResponseDeclarations = failures(422, 429, 503)

#: The API-owned private stores. Authenticated, so a 401; rate limited; and
#: 503 when storage is unavailable or unconfigured.
PRIVATE_STORE_FAILURES: ResponseDeclarations = failures(401, 422, 429, 503)

#: A route whose caller is authenticated but not sufficiently recently
#: (ADR-0005 s5). Distinct from a 401, and the distinction is the useful part:
#: the caller's credential is good, and signing in again is the remedy.
FRESH_SIGN_IN: ResponseDeclarations = failures(403)

#: A route that resolves an identifier.
NOT_FOUND: ResponseDeclarations = failures(404)

#: A route that holds a name unique, or writes against a read version.
CONFLICT: ResponseDeclarations = failures(409)

#: A route that parses a request body. The body limit is refused before any
#: router sees the request, so only routes that accept a body can answer it.
BODY_LIMIT: ResponseDeclarations = failures(413)
