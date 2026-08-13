"""Deterministic HTTP client doubles that preserve production retry wrappers."""

from __future__ import annotations

from collections.abc import Iterable

import httpx


class SequencedHttpClient:
    """Context-managed client returning or raising scripted outcomes."""

    def __init__(self, outcomes: Iterable[httpx.Response | BaseException]) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0
        self.requests: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def __enter__(self) -> "SequencedHttpClient":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def _next(self) -> httpx.Response:
        if self.calls >= len(self.outcomes):
            raise AssertionError("HTTP double exhausted its scripted outcomes")
        outcome = self.outcomes[self.calls]
        self.calls += 1
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def get(self, *args: object, **kwargs: object) -> httpx.Response:
        self.requests.append((args, kwargs))
        return self._next()

    def post(self, *args: object, **kwargs: object) -> httpx.Response:
        self.requests.append((args, kwargs))
        return self._next()


def response(status_code: int, payload: object | None = None) -> httpx.Response:
    """Create a response that supports both JSON and raise_for_status."""
    request = httpx.Request("GET", "https://source.example.test/data")
    if payload is None:
        return httpx.Response(status_code, request=request, content=b"")
    return httpx.Response(status_code, request=request, json=payload)


def invalid_json_response() -> httpx.Response:
    """Create a successful response with deliberately invalid JSON."""
    return httpx.Response(
        200,
        request=httpx.Request("GET", "https://source.example.test/data"),
        content=b"not-json",
        headers={"content-type": "application/json"},
    )
