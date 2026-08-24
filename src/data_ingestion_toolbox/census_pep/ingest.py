"""
Census PEP (Population Estimates) adapter — HTTP capture layer.

Fetches annual population estimates from the U.S. Census Bureau API
and persists raw response payloads for offline replay.

API reference: https://www.census.gov/data/datasets/time-series-democ-pep.html
"""

from __future__ import annotations

import logging
import time
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
from data_ingestion_toolbox.census_pep.config import CONFIG

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


# ---------------------------------------------------------------------------
# URL construction
# ---------------------------------------------------------------------------

_PEP_API_URL = "https://api.census.gov/data/{year}/pep/{file_type}.json"
_SUPPORTED_FILE_TYPES = ("ansfile", "intlfile")
_DEFAULT_YEARS = range(2020, 2027)  # years available via the PEP API


def _build_urls(years: range | None = None, file_types: tuple[str, ...] | None = None) -> list[str]:
    """Build the list of Census PEP API URLs to fetch."""
    years = years or _DEFAULT_YEARS
    file_types = file_types or _SUPPORTED_FILE_TYPES
    urls: list[str] = []
    for year in years:
        for ft in file_types:
            urls.append(_PEP_API_URL.format(year=year, file_type=ft))
    return urls


# ---------------------------------------------------------------------------
# HTTP client with retry / rate-limit handling
# ---------------------------------------------------------------------------

def _fetch_with_retry(
    url: str,
    *,
    max_retries: int = 3,
    base_delay: float = 5.0,
) -> bytes:
    """Fetch *url* with exponential-backoff retry.

    Census API enforces a rate limit (approximately one request per second).
    When a 429 or transient network error occurs the function backs off
    and retries up to *max_retries* times before raising.
    """
    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.get(url)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("retry-after", base_delay * 2))
                    logger.warning(
                        "Census PEP rate limit (%s) on %s; retrying in %ss (attempt %s/%s)",
                        response.status_code,
                        url,
                        retry_after,
                        attempt,
                        max_retries,
                    )
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                return response.content
        except httpx.HTTPStatusError as exc:
            last_exc = exc
            logger.warning(
                "HTTP %s fetching %s (attempt %s/%s): %s",
                exc.response.status_code,
                url,
                attempt,
                max_retries,
                exc,
            )
            if attempt < max_retries:
                time.sleep(base_delay * (2 ** (attempt - 1)))
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
                time.sleep(base_delay * (2 ** (attempt - 1)))
    raise RuntimeError(
        f"Failed to fetch {url} after {max_retries} attempts"
    ) from last_exc


# ---------------------------------------------------------------------------
# Capture orchestration
# ---------------------------------------------------------------------------

def _ingest_url(
    hook: PostgresHook,
    url: str,
    run_id: UUID,
) -> CaptureReceipt:
    """Capture one PEP API response into raw_capture.

    Returns a :class:`CaptureReceipt` identifying the persisted payload.
    """
    conn_factory = lambda: hook.get_conn()  # noqa: E731

    ctrl = CaptureControl(conn_factory, source_code=CONFIG.source_code)
    parsed = url.rsplit("/", 1)[-1]  # e.g. "2023/pep/ansfile.json" -> parts
    parts = parsed.split("/")
    year_str = parts[0] if len(parts) > 0 else "unknown"
    file_type = parts[-1].replace(".json", "") if parts else "unknown"

    req = ctrl.start_request(
        run_id=run_id,
        endpoint=url,
        parameters={},
        max_attempts=3,
    )

    payload = b""
    status_code = 0
    response_headers: dict[str, object] = {}
    start = datetime.now(timezone.utc)

    try:
        payload = _fetch_with_retry(url, max_retries=3, base_delay=5.0)
        status_code = 200  # _fetch_with_retry raises on error
        response_headers = {"content-type": "application/json"}
    except Exception as exc:
        ctrl.finish_request(req.request_id, status="error", error=exc)
        raise RuntimeError(f"Capture failed for {url}: {exc}") from exc
    finally:
        ctrl.finish_request(req.request_id, status="success")

    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=req.request_id,
        run_id=run_id,
        source_code=CONFIG.source_code,
        endpoint=url,
        request_parameters={},
        retrieved_at=start,
        http_status=status_code,
        response_headers=response_headers,
        media_type="application/json",
        payload=payload,
        payload_schema_version="1.0",
        source_revision=year_str,
    )

    receipt = persist_response_capture(conn_factory, capture)
    logger.info(
        "Census PEP captured: url=%s year=%s file_type=%s checksum=%s",
        url,
        year_str,
        file_type,
        receipt.payload_checksum,
    )
    return receipt


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def ingest_census_pep(
    years: range | None = None,
    file_types: tuple[str, ...] | None = None,
) -> int:
    """Fetch and capture Census PEP annual/international files for *years*.

    Parameters
    ----------
    years:
        Inclusive range of calendar years to ingest (default: 2020-2026).
    file_types:
        Which PEP file types to fetch.  Supported values are ``"ansfile"``
        (annual domestic) and ``"intlfile"`` (international).

    Returns
    -------
    int
        Number of distinct payloads captured.
    """
    hook = _get_hook()
    years = years or _DEFAULT_YEARS
    file_types = file_types or _SUPPORTED_FILE_TYPES

    run_id = uuid4()
    conn_factory = lambda: hook.get_conn()  # noqa: E731
    ctrl = CaptureControl(conn_factory, source_code=CONFIG.source_code)
    ctrl.start_run(watermark={"years": list(years), "file_types": list(file_types)})

    urls = _build_urls(years, file_types)
    captured = 0

    logger.info("[CENSUS_PEP] Starting ingestion: years=%s-%s, file_types=%s", min(years), max(years), file_types)

    for url in urls:
        try:
            _ingest_url(hook, url, run_id)
            captured += 1
        except Exception as exc:
            logger.error("[CENSUS_PEP] Failed to capture %s: %s", url, exc)

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
