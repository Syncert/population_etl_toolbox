"""Small helpers for scheduled external-source contract tests."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Callable, TypeVar

import httpx
import pytest

T = TypeVar("T")


@dataclass(frozen=True)
class ExternalResult:
    source: str
    status: str
    latency_seconds: float
    failure_class: str | None = None


def require_external_key(name: str, value: str | None) -> str:
    """Return a configured key or skip without revealing its value."""
    if not value or not value.strip():
        pytest.skip(f"{name} is not configured for external contract tests")
    return value.strip()


def classify_external_failure(error: BaseException) -> str:
    """Separate transient upstream availability from contract regressions."""
    if isinstance(error, (httpx.TimeoutException, httpx.NetworkError)):
        return "upstream-unavailable"
    if isinstance(error, httpx.HTTPStatusError) and (
        error.response.status_code == 429 or error.response.status_code >= 500
    ):
        return "upstream-unavailable"
    return "contract-regression"


def observe_external_call(
    source: str,
    operation: Callable[[], T],
    *,
    logger: logging.Logger,
) -> tuple[T, ExternalResult]:
    """Run and classify one external request while recording sanitized telemetry."""
    started = time.monotonic()
    try:
        value = operation()
    except BaseException as exc:
        result = ExternalResult(
            source=source,
            status="failed",
            latency_seconds=time.monotonic() - started,
            failure_class=classify_external_failure(exc),
        )
        logger.warning(
            "external source=%s status=%s latency_seconds=%.3f failure_class=%s",
            result.source,
            result.status,
            result.latency_seconds,
            result.failure_class,
        )
        raise
    result = ExternalResult(
        source=source,
        status="ok",
        latency_seconds=time.monotonic() - started,
    )
    logger.info(
        "external source=%s status=%s latency_seconds=%.3f",
        result.source,
        result.status,
        result.latency_seconds,
    )
    return value, result
