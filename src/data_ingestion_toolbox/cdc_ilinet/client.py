"""Transport for FluView Interactive's download service (gis.cdc.gov/flu2/).

Two requests, the same the FluView page sends: a GET of the init document
(seasons, regions and jurisdictions) and a POST of the download body
(``AppVersion``, ``DatasourceDT``, ``RegionTypeId``, ``SubRegionsDT``,
``SeasonsDT``), which answers a zip holding ``ILINet.csv``. A client error
fails without retrying; ``429`` and server errors retry with backoff. Error
text names the path and status only. The init is kept only when it is a JSON
object with ``seasons``; a download only when it is a zip. The pipeline asks
with its own plain client identity; a refusal is reported, never worked
around.
"""

from __future__ import annotations

import io
import json
import time
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from data_ingestion_toolbox.capture import allowlisted_response_headers

from .config import APP_VERSION, DOWNLOAD_PATH, INIT_PATH, CdcIlinetConfig
from .registry import RegionType


class CdcIlinetFetchError(RuntimeError):
    """A sanitized transport failure: the path and status only."""

    def __init__(self, endpoint: str, *, code: str, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.code = code
        self.status = status
        status_text = f"; HTTP {status}" if status is not None else ""
        super().__init__(f"FluView request failed ({code}{status_text}) at {endpoint}")


class CdcIlinetPayloadError(CdcIlinetFetchError):
    """A successful response whose bytes are not the registered shape."""


@dataclass(frozen=True)
class IlinetResponse:
    endpoint: str
    parameters: Mapping[str, object]
    raw_bytes: bytes
    response_headers: Mapping[str, str]
    http_status: int
    media_type: str


@dataclass(frozen=True)
class InitDocument:
    #: (season id, label, has state data), newest first as served.
    seasons: tuple[tuple[int, str, bool], ...]
    #: (state id, name, CDC's state FIPS text) of every reporting jurisdiction.
    states: tuple[tuple[int, str, str], ...]
    #: (HHS region id, name).
    hhs_regions: tuple[tuple[int, str], ...]


def read_init(raw_bytes: bytes, endpoint: str = INIT_PATH) -> InitDocument:
    """The seasons and regions of an init document, or CdcIlinetPayloadError."""
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CdcIlinetPayloadError(endpoint, code="invalid_json") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("seasons"), list):
        raise CdcIlinetPayloadError(endpoint, code="expected_init_document")
    try:
        seasons = tuple(
            (
                int(item["seasonid"]),
                str(item.get("label", "")),
                bool(item.get("hasstatedata")),
            )
            for item in payload["seasons"]
            if item.get("enabled", 1)
        )
        states = tuple(
            (
                int(item["stateid"]),
                str(item["statename"]).strip(),
                str(item.get("statefips") or "").strip(),
            )
            for item in payload.get("states", [])
        )
        regions = tuple(
            (int(item["hhsregionid"]), str(item["hhsregionname"]).strip())
            for item in payload.get("hhsregion", [])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise CdcIlinetPayloadError(endpoint, code="unreadable_init_document") from exc
    if not seasons:
        raise CdcIlinetPayloadError(endpoint, code="no_seasons")
    return InitDocument(seasons, states, regions)


def download_body(region_type: RegionType, init: InitDocument) -> dict[str, object]:
    """The page's own download body for one region type and every season it offers."""
    if region_type.key == "national":
        sub_regions = [{"ID": 0, "Name": ""}]
        seasons = [season for season in init.seasons]
    elif region_type.key == "hhs_region":
        sub_regions = [
            {"ID": rid, "Name": str(rid)} for rid, _name in sorted(init.hhs_regions)
        ]
        seasons = [season for season in init.seasons]
    else:
        sub_regions = [
            {"ID": sid, "Name": str(sid)} for sid, _name, _fips in sorted(init.states)
        ]
        seasons = [season for season in init.seasons if season[2]]
    return {
        "AppVersion": APP_VERSION,
        "DatasourceDT": [{"ID": 1, "Name": "ILINet"}],
        "RegionTypeId": region_type.region_type_id,
        "SubRegionsDT": sub_regions,
        "SeasonsDT": [
            {"ID": sid, "Name": str(sid)} for sid, _label, _state in sorted(seasons)
        ],
    }


def ilinet_csv(raw_bytes: bytes, endpoint: str = DOWNLOAD_PATH) -> bytes:
    """The ``ILINet.csv`` member of a download, or CdcIlinetPayloadError."""
    try:
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as archive:
            names = [
                name for name in archive.namelist() if name.lower() == "ilinet.csv"
            ]
            if len(names) != 1:
                raise CdcIlinetPayloadError(endpoint, code="expected_ilinet_member")
            return archive.read(names[0])
    except zipfile.BadZipFile as exc:
        raise CdcIlinetPayloadError(endpoint, code="expected_zip") from exc


def _request(
    method: str,
    path: str,
    *,
    params: Mapping[str, object] | None,
    body: Mapping[str, object] | None,
    config: CdcIlinetConfig,
    client: Any | None,
    check: Callable[[bytes, str], None],
    media_type: str,
    on_retry: Callable[[BaseException], None] | None,
    sleep: Callable[[float], None],
) -> IlinetResponse:
    url = f"{config.base_url}{path}"
    own_client = client is None
    active = client or httpx.Client(timeout=config.timeout_seconds)
    final_status: int | None = None
    final_error: BaseException | None = None
    headers = {"User-Agent": config.user_agent}
    try:
        for attempt in range(1, config.max_attempts + 1):
            try:
                if method == "GET":
                    response = active.get(
                        url,
                        params=dict(params or {}),
                        headers={**headers, "Accept": "application/json"},
                    )
                else:
                    response = active.post(url, json=dict(body or {}), headers=headers)
            except httpx.HTTPError as exc:
                final_error = CdcIlinetFetchError(path, code=type(exc).__name__)
            else:
                final_status = response.status_code
                raw_bytes = response.content
                kept_headers = allowlisted_response_headers(dict(response.headers))
                response.close()
                if response.status_code < 400:
                    check(raw_bytes, path)
                    return IlinetResponse(
                        path,
                        dict(params or body or {}),
                        raw_bytes,
                        kept_headers,
                        response.status_code,
                        media_type,
                    )
                if response.status_code != 429 and response.status_code < 500:
                    raise CdcIlinetFetchError(
                        path, code="non_retryable_http", status=response.status_code
                    )
                final_error = CdcIlinetFetchError(
                    path, code="retryable_http", status=response.status_code
                )
            if attempt < config.max_attempts:
                if on_retry is not None and final_error is not None:
                    on_retry(final_error)
                sleep(min(config.min_spacing_seconds * 2 ** (attempt - 1) + 1.0, 120.0))
        raise CdcIlinetFetchError(
            path, code="retry_exhausted", status=final_status
        ) from final_error
    finally:
        if own_client:
            active.close()


def fetch_init(
    *,
    config: CdcIlinetConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> IlinetResponse:
    """Fetch FluView's init document (seasons, regions, jurisdictions)."""
    runtime = config or CdcIlinetConfig()
    return _request(
        "GET",
        INIT_PATH,
        params={"appVersion": APP_VERSION},
        body=None,
        config=runtime,
        client=client,
        check=lambda raw, path: read_init(raw, path) and None,
        media_type="application/json",
        on_retry=on_retry,
        sleep=sleep,
    )


def fetch_download(
    body: Mapping[str, object],
    *,
    config: CdcIlinetConfig | None = None,
    client: Any | None = None,
    on_retry: Callable[[BaseException], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> IlinetResponse:
    """Fetch one ILINet download (a zip holding ``ILINet.csv``)."""
    runtime = config or CdcIlinetConfig()
    return _request(
        "POST",
        DOWNLOAD_PATH,
        params=None,
        body=body,
        config=runtime,
        client=client,
        check=lambda raw, path: ilinet_csv(raw, path) and None,
        media_type="application/zip",
        on_retry=on_retry,
        sleep=sleep,
    )
