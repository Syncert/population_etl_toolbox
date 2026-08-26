"""Deterministic httpx client doubles for CDC Socrata client tests."""

from __future__ import annotations

import httpx

TOKEN = "CDC:unit-test-token"
TOKEN_LEN = 24


class ScriptedSocrataClient:
    """Context-managed httpx double returning or raising scripted outcomes.

    ``get()`` records every call so tests can assert on URL, headers, and
    query parameters without any real network I/O.
    """

    def __init__(self, outcomes: list[httpx.Response | BaseException]) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0
        self.requests: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.closed = False

    def __enter__(self) -> "ScriptedSocrataClient":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def _next(self) -> httpx.Response:
        if self.calls >= len(self.outcomes):
            raise AssertionError("ScriptedSocrataClient exhausted its outcomes")
        outcome = self.outcomes[self.calls]
        self.calls += 1
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def get(self, *args: object, **kwargs: object) -> httpx.Response:
        self.requests.append((args, kwargs))
        return self._next()

    def close(self) -> None:
        self.closed = True


def socrata_response(
    status_code: int,
    payload: object | None = None,
    *,
    headers: dict[str, str] | None = None,
    raw: bytes | None = None,
) -> httpx.Response:
    """Build a canned Socrata response; ``raw`` overrides the JSON payload."""
    request = httpx.Request("GET", "https://data.cdc.gov/api/views/unit-test")
    if raw is not None:
        kwargs: dict[str, object] = {"content": raw}
    elif payload is None:
        kwargs = {"content": b""}
    else:
        kwargs = {"json": payload}
    if headers:
        kwargs["headers"] = headers
    return httpx.Response(status_code, request=request, **kwargs)
