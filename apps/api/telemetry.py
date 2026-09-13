"""Request correlation and structured operational telemetry (API-006).

Every request gets a correlation id — the caller's ``X-Request-ID`` when it
looks like an id, a fresh one otherwise — echoed on the response and attached
to one structured completion line: method, route path, status, duration, and
the cache disposition. That line is the operational signal the plan asks for
(latency, error, cache behaviour) without logging response datasets.

What is deliberately absent from the log line matters as much as what is in
it: no query-string values (parameter values are user input and never belong
in logs by default), no headers, no body, and nothing derived from the
database URL. The sanitized-failure logging in ``apps.api.dependencies``
already keeps credentials out of responses; this module keeps request logs to
route-shaped facts only.

This middleware is also the correlation boundary for a failure nothing else
catches. Starlette builds ``ServerErrorMiddleware`` outside every middleware
an application adds, so a 500 raised by an endpoint never passed through the
``send`` below: the response carried no ``X-Request-ID``, the completion line
reported ``status=0``, and the body was Starlette's plain-text default where
every other failure this API answers is a JSON ``detail``. The id existed, and
was in the log, and the only person who could not see it was the one opening
a ticket about the failure (API-088). So an unhandled exception is answered
here: sanitized to the caller, correlated, and logged whole with its
traceback for the operator.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from apps.api.middleware import SECURITY_HEADERS

logger = logging.getLogger("apps.api.request")

Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

#: The single sanitized text an unhandled failure answers with. It says
#: nothing about the exception -- not its type, not its message, not where it
#: came from -- for the same reason
#: ``apps.api.dependencies.SERVICE_UNAVAILABLE_DETAIL`` says nothing about the
#: database: an error body is not a place to publish deployment state.
INTERNAL_FAILURE_DETAIL = "The API failed to complete this request."

_INTERNAL_FAILURE_BODY = json.dumps({"detail": INTERNAL_FAILURE_DETAIL}).encode("utf-8")


def route_shape(scope: dict[str, Any]) -> str:
    """The path with each path parameter's value replaced by its name.

    The completion line is the operational signal this module exists to
    produce, and latency, error rate and cache behaviour are per-route facts.
    Logging the request path made them uncomputable: `/evidence-packets/12345`
    and `/evidence-packets/12346` are the same route, and one `path` value per
    packet leaves nothing to group by. It also put a private identifier on
    disk -- the one the web client keeps out of the address bar -- when this
    module's own rule already says parameter values do not belong in logs.
    That rule had been applied to the query string and not to the path
    (API-089).

    Replacement is by whole segment. A value that is a substring of a longer
    segment is left alone; a literal segment that happens to equal a
    parameter's value is replaced too, which can only remove an identifier and
    lower cardinality, never add either -- reconstructing which segment the
    router actually matched would mean re-deriving its own match here.

    A request that matched no route has no template, and its path is what
    makes a 404 actionable, so it is logged as it arrived. Every route this
    API serves that takes an identifier does match, so no identifier of a
    served resource lands there.
    """
    path = str(scope.get("path", "-"))
    parameters = scope.get("path_params") or {}
    if not parameters:
        return path
    names_by_value = {str(value): name for name, value in parameters.items()}
    return "/".join(
        "{" + names_by_value[segment] + "}" if segment in names_by_value else segment
        for segment in path.split("/")
    )


def _incoming_request_id(scope: dict[str, Any]) -> str | None:
    for name, value in scope.get("headers") or ():
        if name == b"x-request-id":
            candidate = value.decode("latin-1", errors="replace")
            if _REQUEST_ID_PATTERN.fullmatch(candidate):
                return candidate
            return None
    return None


class RequestTelemetryMiddleware:
    def __init__(self, app) -> None:
        self.app = app

    async def __call__(
        self, scope: dict[str, Any], receive: Receive, send: Send
    ) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        request_id = _incoming_request_id(scope) or uuid.uuid4().hex
        started = time.perf_counter()
        status = 0
        cache_state = "-"
        started_response = False

        async def send_with_request_id(message: Message) -> None:
            nonlocal status, cache_state, started_response
            if message.get("type") == "http.response.start":
                started_response = True
                status = int(message.get("status", 0))
                headers = list(message.get("headers", []))
                for name, value in headers:
                    if name == b"x-cache":
                        cache_state = value.decode("latin-1", errors="replace")
                headers.append((b"x-request-id", request_id.encode("latin-1")))
                message["headers"] = headers
            await send(message)

        try:
            try:
                await self.app(scope, receive, send_with_request_id)
            except Exception:
                # Logged whole, with the id the caller was given, so a search
                # from that id reaches this stack. The caller gets none of it.
                logger.exception(
                    "api_request_failed method=%s path=%s request_id=%s",
                    scope.get("method", "-"),
                    route_shape(scope),
                    request_id,
                )
                if started_response:
                    # The response is already on the wire; there is no status
                    # to correct and nothing safe to append. The failure is in
                    # the log above, under the id the caller holds.
                    raise
                status = 500
                await send_with_request_id(
                    {
                        "type": "http.response.start",
                        "status": 500,
                        # The declared set, read from where it is declared:
                        # this response is created outside
                        # `SecurityHeadersMiddleware` and so is the one
                        # response it cannot reach.
                        "headers": [
                            (b"content-type", b"application/json"),
                            (
                                b"content-length",
                                str(len(_INTERNAL_FAILURE_BODY)).encode("latin-1"),
                            ),
                            *SECURITY_HEADERS,
                        ],
                    }
                )
                await send(
                    {"type": "http.response.body", "body": _INTERNAL_FAILURE_BODY}
                )
        finally:
            duration_ms = (time.perf_counter() - started) * 1000.0
            logger.info(
                "api_request method=%s path=%s status=%d duration_ms=%.1f "
                "cache=%s request_id=%s",
                scope.get("method", "-"),
                route_shape(scope),
                status,
                duration_ms,
                cache_state,
                request_id,
            )
