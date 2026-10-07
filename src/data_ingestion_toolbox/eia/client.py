"""Transport for EIA API v2.

The key is a query parameter, so this module is the only place a URL with
the key is built, and nothing it raises or returns carries one: an error
names the route and status only, and a response is checked not to echo the
key before it is kept. Calls are spaced; ``429`` and server errors retry
with backoff, and any other client error fails at once (``401``/``403`` as a
refused key).
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import EIA_API_BASE_URL, EiaConfig


class EiaFetchError(RuntimeError):
    """A sanitized API failure: the route and status only, never the key."""

    def __init__(self, route: str, *, code: str, status: int | None = None) -> None:
        self.route = route
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"EIA API request failed ({code}{status_text}) at {route}")


class EiaPayloadError(EiaFetchError):
    """A successful response that is not the registered answer."""


@dataclass(frozen=True)
class EiaResponse:
    route: str
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int


def page_rows(raw_bytes: bytes, route: str) -> tuple[int, list[dict[str, Any]]]:
    """(total rows the query matches, this page's rows), or EiaPayloadError."""
    try:
        document = json.loads(raw_bytes)
        response = document["response"]
        rows = response["data"]
        total = int(response["total"])
    except (ValueError, KeyError, TypeError) as exc:
        raise EiaPayloadError(route, code="unexpected_answer") from exc
    if not isinstance(rows, list):
        raise EiaPayloadError(route, code="unexpected_answer")
    return total, rows


class EiaClient:
    """Sequential, spaced calls carrying the key as a query parameter."""

    def __init__(
        self,
        config: EiaConfig,
        *,
        client: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        key = config.eia_api_key
        if not key.strip() or key != key.strip():
            raise EiaFetchError("EIA_API_KEY", code="missing_api_key")
        self._key = key
        self._config = config
        self._own = client is None
        self._client = client or httpx.Client(
            timeout=config.timeout_seconds, follow_redirects=True
        )
        self._sleep = sleep
        self._clock = clock
        self._last: float | None = None

    def close(self) -> None:
        if self._own:
            self._client.close()

    def _space(self) -> None:
        if self._last is not None:
            wait = self._config.min_spacing_seconds - (self._clock() - self._last)
            if wait > 0:
                self._sleep(wait)
        self._last = self._clock()

    def get(
        self,
        route: str,
        params: Sequence[tuple[str, str]],
        *,
        on_retry: Callable[[BaseException], None] | None = None,
    ) -> EiaResponse:
        """One answer from ``<base>/<route>``; ``params`` never hold the key."""
        if any(name == "api_key" for name, _ in params):
            raise EiaFetchError(route, code="key_in_parameters")
        query = [("api_key", self._key), *params]
        final_status: int | None = None
        final_error: BaseException | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            self._space()
            try:
                response = self._client.get(
                    f"{EIA_API_BASE_URL}/{route}",
                    params=query,
                    headers={"User-Agent": self._config.user_agent},
                )
            except httpx.HTTPError as exc:
                # The exception's text can carry the URL, and the URL the key.
                final_error = EiaFetchError(route, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code == 200:
                    if self._key.encode() in raw_bytes:
                        raise EiaPayloadError(route, code="answer_echoes_key")
                    return EiaResponse(route, raw_bytes, headers, 200)
                if response.status_code in (401, 403):
                    raise EiaFetchError(
                        route, code="key_refused", status=response.status_code
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise EiaFetchError(
                        route, code="non_retryable_http", status=response.status_code
                    )
                final_error = EiaFetchError(
                    route, code="retryable_http", status=response.status_code
                )
            if attempt < self._config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                self._sleep(min(self._config.min_spacing_seconds * 2**attempt, 300.0))
        raise EiaFetchError(route, code="retry_exhausted", status=final_status) from None
