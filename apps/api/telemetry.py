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
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

logger = logging.getLogger("apps.api.request")

Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


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

        async def send_with_request_id(message: Message) -> None:
            nonlocal status, cache_state
            if message.get("type") == "http.response.start":
                status = int(message.get("status", 0))
                headers = list(message.get("headers", []))
                for name, value in headers:
                    if name == b"x-cache":
                        cache_state = value.decode("latin-1", errors="replace")
                headers.append((b"x-request-id", request_id.encode("latin-1")))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            duration_ms = (time.perf_counter() - started) * 1000.0
            logger.info(
                "api_request method=%s path=%s status=%d duration_ms=%.1f "
                "cache=%s request_id=%s",
                scope.get("method", "-"),
                scope.get("path", "-"),
                status,
                duration_ms,
                cache_state,
                request_id,
            )
