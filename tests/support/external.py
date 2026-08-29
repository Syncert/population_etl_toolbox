"""Small helpers for scheduled external-source contract tests."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Callable, TypeVar

import httpx
import pytest

from data_ingestion_toolbox.fbi_ucr.config import (
    API_KEY_ENVIRONMENT_VARIABLE as FBI_CDE_API_KEY_VARIABLE,
)
from data_ingestion_toolbox.usda_nass.config import (
    API_KEY_ENVIRONMENT_VARIABLE as USDA_NASS_API_KEY_VARIABLE,
)

T = TypeVar("T")

#: Credentials the scheduled external-contract run must have before it can
#: claim to cover every source. The two provider-required keys are read from
#: their adapters so a renamed variable cannot silently drop a source from the
#: tier. CDC reads anonymously, so its optional rate-limit token is not here.
REQUIRED_SCHEDULED_CREDENTIALS = (
    "CENSUS_API_KEY",
    "BLS_API_KEY",
    "FRED_API_KEY",
    FBI_CDE_API_KEY_VARIABLE,
    USDA_NASS_API_KEY_VARIABLE,
)


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


def validate_scheduled_credentials(environment: Mapping[str, str]) -> None:
    """Fail a credentialed scheduled run without exposing secret values."""
    missing = [
        name
        for name in REQUIRED_SCHEDULED_CREDENTIALS
        if not environment.get(name, "").strip()
    ]
    if missing:
        raise RuntimeError(
            "missing required scheduled external credentials: " + ", ".join(missing)
        )


def _is_unavailable_status(status: object) -> bool:
    return isinstance(status, int) and (status == 429 or status >= 500)


def classify_external_failure(error: BaseException) -> str:
    """Separate transient upstream availability from contract regressions.

    Adapters that wrap transport failures in their own sanitized error type
    stay classifiable through the provider-neutral ``status``/``code``
    attributes, so a live 429, 5xx, or exhausted retry budget is never
    reported as an implementation regression.
    """
    if isinstance(error, (httpx.TimeoutException, httpx.NetworkError)):
        return "upstream-unavailable"
    if isinstance(error, httpx.HTTPStatusError) and _is_unavailable_status(
        error.response.status_code
    ):
        return "upstream-unavailable"
    if _is_unavailable_status(getattr(error, "status", None)):
        return "upstream-unavailable"
    if getattr(error, "code", None) in {"retry_exhausted", "retryable_http"}:
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


def main() -> None:
    validate_scheduled_credentials(os.environ)


if __name__ == "__main__":
    main()
