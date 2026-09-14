"""The bodies a refused request answers with.

The consumer guide opens by promising that "everything here is served by the
checked-in application and pinned by the reviewed OpenAPI snapshot ... so a
change to anything below appears in review as a snapshot diff". Its Errors
table promised 401, 404, 409, 413, 422, 429 and 503; the published document
declared exactly 200, 201, 204 and 422, with FastAPI's generated
``HTTPValidationError`` as the one error schema. A client generated from
``/openapi.json`` therefore typed `422.detail` as an array and had no branch
for a 404 or a 429 at all, and the guide's table could change without a
snapshot diff -- the opposite of what the guide says (API-121).

``ErrorDetail`` is the shape every failure the API raises by hand already
sends: a single sentence under ``detail``. ``HTTPValidationError`` and
``ValidationError`` restate the body FastAPI sends for a request refused
before the endpoint ran, under the same names and with the same fields it
generates, so a client reading either document sees one contract. They are
declared here rather than inherited because every route now declares its 422
explicitly, and a route that declares one gets no generated default.
"""

from __future__ import annotations

from typing import Any, Union

from pydantic import BaseModel, ConfigDict, Field


class ErrorDetail(BaseModel):
    """A refusal the API can state in one sentence.

    The same shape for every status: a 401 that says nothing about which part
    of the credential was wrong, a 404 that cannot distinguish "unknown" from
    "not yours", a 503 that never names a warehouse relation.
    """

    model_config = ConfigDict(extra="forbid")

    detail: str = Field(..., description="Why the request was refused.")


class ValidationError(BaseModel):
    """One entry of a pre-endpoint validation failure.

    Mirrors FastAPI's own generated entry exactly: ``loc`` is the path to what
    was refused, ``msg`` says why, ``type`` is the stable machine-readable
    reason, and ``input``/``ctx`` are diagnostic.
    """

    loc: list[Union[str, int]]
    msg: str
    type: str
    input: Any = None
    ctx: dict[str, Any] = Field(default_factory=dict)


class HTTPValidationError(BaseModel):
    """A request refused before the endpoint ran: ``detail`` is a list."""

    detail: list[ValidationError] = Field(default_factory=list)
