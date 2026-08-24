"""Lossless HTTP capture for registered Census PEP bulk releases."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import httpx
import polars as pl

from data_ingestion_toolbox.capture import (
    CaptureControl,
    CaptureReceipt,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.census_pep.config import CONFIG, PEPRelease
from data_ingestion_toolbox.census_pep.registry import PEPRegistry

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


# ---------------------------------------------------------------------------
# Release selection
# ---------------------------------------------------------------------------


def _select_releases(
    *,
    dataset_codes: tuple[str, ...] | None = None,
    vintage_years: tuple[int, ...] | None = None,
) -> list[PEPRelease]:
    """Resolve a deterministic set of registered releases before I/O."""
    registry = PEPRegistry(CONFIG)
    requested_datasets = (
        tuple(sorted(set(dataset_codes)))
        if dataset_codes is not None
        else tuple(sorted(registry.datasets))
    )
    unknown = set(requested_datasets) - set(registry.datasets)
    if unknown:
        raise ValueError("unknown PEP dataset: " + ", ".join(sorted(unknown)))

    if vintage_years is None:
        releases = [
            release
            for dataset_code in requested_datasets
            if (release := registry.get_current_release(dataset_code)) is not None
        ]
    else:
        requested_vintages = tuple(sorted(set(vintage_years)))
        releases = [
            release
            for dataset_code in requested_datasets
            for vintage_year in requested_vintages
            if (release := registry.get_release(dataset_code, vintage_year)) is not None
        ]
        expected_count = len(requested_datasets) * len(requested_vintages)
        if len(releases) != expected_count:
            found = {
                (release.dataset_code, release.vintage_year) for release in releases
            }
            missing = [
                f"{dataset_code}/{vintage_year}"
                for dataset_code in requested_datasets
                for vintage_year in requested_vintages
                if (dataset_code, vintage_year) not in found
            ]
            raise ValueError("no registered PEP releases for: " + ", ".join(missing))

    if not releases:
        raise ValueError("no registered PEP releases match the requested scope")
    return sorted(
        releases,
        key=lambda release: (release.dataset_code, release.vintage_year),
    )


# ---------------------------------------------------------------------------
# HTTP client with retry / rate-limit handling
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PEPHTTPResponse:
    """Source response values required by immutable raw capture."""

    payload: bytes
    status_code: int
    response_headers: dict[str, str]


_RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503})


def _fetch_with_retry(
    url: str,
    *,
    max_retries: int = 3,
    base_delay: float = 5.0,
    on_retry: Callable[[Exception], None] | None = None,
) -> PEPHTTPResponse:
    """Fetch *url* with exponential-backoff retry.

    Retries are bounded to transport failures and explicitly retryable HTTP
    statuses. Other 4xx responses fail immediately.
    """
    last_exc: Exception | None = None
    with httpx.Client(
        timeout=CONFIG.request_timeout,
        follow_redirects=True,
    ) as client:
        for attempt in range(1, max_retries + 1):
            try:
                response = client.get(url)
                response.raise_for_status()
                return PEPHTTPResponse(
                    payload=response.content,
                    status_code=response.status_code,
                    response_headers=dict(response.headers),
                )
            except httpx.HTTPStatusError as exc:
                last_exc = exc
                status_code = exc.response.status_code
                if status_code not in _RETRYABLE_STATUS_CODES:
                    raise RuntimeError(
                        f"Census PEP returned non-retryable HTTP {status_code} for {url}"
                    ) from exc
                if attempt < max_retries:
                    if on_retry is not None:
                        on_retry(exc)
                    retry_after = exc.response.headers.get("retry-after")
                    try:
                        delay = (
                            float(retry_after)
                            if retry_after
                            else base_delay * 2 ** (attempt - 1)
                        )
                    except ValueError:
                        delay = base_delay * 2 ** (attempt - 1)
                    logger.warning(
                        "Retryable Census PEP HTTP %s on %s; retrying in %ss "
                        "(attempt %s/%s)",
                        status_code,
                        url,
                        delay,
                        attempt,
                        max_retries,
                    )
                    time.sleep(delay)
            except httpx.RequestError as exc:
                last_exc = exc
                logger.warning(
                    "Request error fetching %s (attempt %s/%s): %s",
                    url,
                    attempt,
                    max_retries,
                    exc,
                )
                if attempt < max_retries:
                    if on_retry is not None:
                        on_retry(exc)
                    time.sleep(base_delay * 2 ** (attempt - 1))
    raise RuntimeError(
        f"Failed to fetch {url} after {max_retries} attempts"
    ) from last_exc


# ---------------------------------------------------------------------------
# Capture orchestration
# ---------------------------------------------------------------------------


def _ingest_release(
    hook: PostgresHook,
    release: PEPRelease,
    run_id: UUID,
) -> CaptureReceipt:
    """Capture one registered PEP bulk release into ``raw_capture``.

    Returns a :class:`CaptureReceipt` identifying the persisted payload.
    """
    conn_factory = hook.get_conn

    ctrl = CaptureControl(conn_factory, source_code=CONFIG.source_code)
    request_parameters = {
        "dataset_code": release.dataset_code,
        "vintage_year": release.vintage_year,
        "product_code": release.product_code,
    }

    req = ctrl.start_request(
        run_id=run_id,
        endpoint=release.data_url,
        parameters=request_parameters,
        max_attempts=3,
    )

    def record_retry(error: Exception) -> None:
        ctrl.record_request_retry(req.request_id, error=error)

    try:
        response = _fetch_with_retry(
            release.data_url,
            max_retries=3,
            base_delay=5.0,
            on_retry=record_retry,
        )
        capture = ResponseCapture(
            capture_id=uuid4(),
            request_id=req.request_id,
            run_id=run_id,
            source_code=CONFIG.source_code,
            endpoint=release.data_url,
            request_parameters=request_parameters,
            retrieved_at=datetime.now(timezone.utc),
            http_status=response.status_code,
            response_headers=response.response_headers,
            media_type=release.media_type,
            payload=response.payload,
            payload_schema_version=release.schema_version,
            source_revision=release.product_code,
        )
        receipt = persist_response_capture(conn_factory, capture)
    except Exception as exc:
        ctrl.finish_request(req.request_id, status="error", error=exc)
        raise RuntimeError(f"Capture failed for {release.product_code}: {exc}") from exc

    ctrl.finish_request(req.request_id, status="success")
    logger.info(
        "Census PEP captured: dataset=%s vintage=%s product=%s checksum=%s",
        release.dataset_code,
        release.vintage_year,
        release.product_code,
        receipt.payload_checksum,
    )
    return receipt


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def ingest_census_pep(
    dataset_codes: tuple[str, ...] | None = None,
    vintage_years: tuple[int, ...] | None = None,
) -> int:
    """Fetch and capture an exact registered Census PEP release scope.

    Parameters
    ----------
    dataset_codes:
        Stable registered products. Defaults to every supported product.
    vintage_years:
        Explicit release vintages for replay/backfill. When omitted, selects
        the latest published release for each requested product.

    Returns
    -------
    int
        Number of distinct payloads captured.
    """
    releases = _select_releases(
        dataset_codes=dataset_codes,
        vintage_years=vintage_years,
    )

    hook = _get_hook()
    conn_factory = hook.get_conn
    ctrl = CaptureControl(conn_factory, source_code=CONFIG.source_code)
    run_id = ctrl.start_run(
        watermark={
            "releases": [
                {
                    "dataset_code": release.dataset_code,
                    "vintage_year": release.vintage_year,
                    "product_code": release.product_code,
                }
                for release in releases
            ]
        }
    )
    captured = 0
    failures: list[str] = []

    logger.info(
        "[CENSUS_PEP] Starting ingestion for %d registered releases",
        len(releases),
    )

    for release in releases:
        try:
            _ingest_release(hook, release, run_id)
            captured += 1
        except Exception as exc:
            failures.append(release.product_code)
            logger.error(
                "[CENSUS_PEP] Failed to capture %s: %s",
                release.product_code,
                exc,
            )

    if failures:
        error = RuntimeError(
            f"{len(failures)} of {len(releases)} PEP releases failed: "
            + ", ".join(failures)
        )
        ctrl.finish_run(run_id, status="error", error=error)
        raise error

    ctrl.finish_run(run_id, status="success")
    logger.info("[CENSUS_PEP] Ingestion complete: %d payloads captured", captured)
    return captured


def get_pep_api_columns() -> pl.DataFrame:
    """Return column metadata from the PEP silver table for API discovery.

    Queries the ``silver_pep.pep_column_metadata`` table and returns the
    column definitions as a Polars DataFrame.  This supports the API
    discovery layer that exposes available PEP variables and their
    semantics to downstream consumers.

    Returns
    -------
    pl.DataFrame
        Columns: ``variable_code``, ``variable_label``, ``concept``,
        ``universe``, ``data_type``, ``is_numeric``, ``is_geometry``.
    """
    hook = _get_hook()
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT variable_code, variable_label, concept, universe,
                   data_type, is_numeric, is_geometry
            FROM silver_pep.pep_column_metadata
            ORDER BY variable_code
            """
        )
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    return pl.DataFrame(rows, schema=columns, orient="row")
