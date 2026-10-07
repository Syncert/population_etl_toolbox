"""Areas a provider defines itself, loaded into the shared reference.

Some providers publish figures for areas that are not Census geography: a
BLS CPI metro whose definition is BLS's own, an EIA Petroleum Administration
for Defense District, a BEA state's nonmetropolitan portion. Each becomes a
``provider_area`` entity identified as ``area:<provider>:<code>``, with the
provider's name for it, read from the provider's own published list -- never
typed into an adapter. A provider area is not matched to a CBSA or a city by
its name; where the provider publishes the area's members, they are related
by code, and otherwise the area stands alone.

Each list is captured before it is parsed, and the parse reads the stored
bytes, as every other reference load does.
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

import httpx

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    load_captured_payload,
    persist_response_capture,
)
from data_ingestion_toolbox.silver_ref import geography_pipeline
from data_ingestion_toolbox.silver_ref.geography_contract import canonical_geo_id
from data_ingestion_toolbox.silver_ref.geography_pipeline import (
    HTTP_MAX_ATTEMPTS,
    GeographyRecord,
    GeographyRepository,
)

PARSER_VERSION = "provider-area-list-v1"

#: A current BLS CPI metropolitan area: `S`, a two-digit region/division
#: pair, and a letter (`S12A` New York). The `A`-prefixed codes are areas
#: BLS discontinued, and the size classes (`S000`, `N100`, `D200`) are
#: population groupings rather than places; neither is a geography.
BLS_CPI_METRO = re.compile(r"^S[1-4][0-9][A-Z]$")

#: download.bls.gov refuses clients that do not name themselves; this is the
#: adapter's own identifying string (``bls/metadata.py``).
BLS_HEADERS = {
    "User-Agent": "population_toolbox/1.0 (contact: your_email@example.com)",
    "Accept": "text/plain,*/*",
}


@dataclass(frozen=True)
class ProviderArea:
    code: str
    name: str


@dataclass(frozen=True)
class ProviderAreaList:
    """Where one provider publishes its area list, and how to read it."""

    provider: str
    source_code: str
    #: One or more published files; the provider's areas are their union.
    urls: tuple[str, ...]
    headers: Mapping[str, str]
    parse: Callable[[bytes], list[ProviderArea]]


def parse_bls_cpi_areas(payload: bytes) -> list[ProviderArea]:
    """BLS's current CPI metropolitan areas from ``cu.area``."""
    text = payload.decode("utf-8")
    rows = list(csv.DictReader(io.StringIO(text), delimiter="\t"))
    if not rows or not {"area_code", "area_name"} <= {
        (key or "").strip() for key in rows[0]
    }:
        raise ValueError("the BLS area list lacks area_code and area_name")
    areas = []
    for row in rows:
        values = {(key or "").strip(): (value or "").strip() for key, value in row.items()}
        if BLS_CPI_METRO.fullmatch(values["area_code"]):
            areas.append(ProviderArea(values["area_code"], values["area_name"]))
    if not areas:
        raise ValueError("the BLS area list names no current CPI metro area")
    return areas


#: A BEA area that is not Census geography: the nation's nonmetropolitan
#: portion (`00999`) and each state's metropolitan (`ss998`) and
#: nonmetropolitan (`ss999`) portion.
BEA_PORTION = re.compile(r"^(00999|[0-8][0-9]99[89])$")


def parse_bea_portions(payload: bytes) -> list[ProviderArea]:
    """BEA's own portions from a regional price parity zip's every-area CSV."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile as exc:
        raise ValueError("the BEA file is not a zip") from exc
    members = [
        name for name in archive.namelist() if name.endswith(".csv") and "__" not in name
    ]
    if len(members) != 1:
        raise ValueError("the BEA zip holds no single every-area CSV")
    rows = csv.reader(io.StringIO(archive.read(members[0]).decode("latin-1")))
    header = next(rows, [])
    if [name.strip() for name in header[:2]] != ["GeoFIPS", "GeoName"]:
        raise ValueError("the BEA file lacks GeoFIPS and GeoName")
    areas: dict[str, str] = {}
    for row in rows:
        if len(row) < 2:
            continue
        code = row[0].strip().strip('"').strip()
        if BEA_PORTION.fullmatch(code):
            # BEA marks the nation's portion with a footnote asterisk.
            areas.setdefault(code, row[1].strip().rstrip("*").strip())
    return [ProviderArea(code, name) for code, name in sorted(areas.items())]


BEA_HEADERS = {
    "User-Agent": "population-etl-toolbox BEA regional ingestion (public-data warehouse)"
}

PROVIDER_AREA_LISTS: dict[str, ProviderAreaList] = {
    "bls_cpi": ProviderAreaList(
        provider="bls_cpi",
        source_code="BLS",
        urls=("https://download.bls.gov/pub/time.series/cu/cu.area",),
        headers=BLS_HEADERS,
        parse=parse_bls_cpi_areas,
    ),
    # The portions BEA publishes price parities for (grocery-and-gasoline-prices).
    "bea": ProviderAreaList(
        provider="bea",
        source_code="BEA",
        urls=(
            "https://apps.bea.gov/regional/zip/PARPP.zip",
            "https://apps.bea.gov/regional/zip/MARPP.zip",
        ),
        headers=BEA_HEADERS,
        parse=parse_bea_portions,
    ),
}


def provider_area_records(
    provider: str, areas: list[ProviderArea], *, vintage: int
) -> list[GeographyRecord]:
    """One reference record per area, identified by the provider's code."""
    records = []
    for area in areas:
        geo_id = canonical_geo_id("provider_area", area_code=area.code, provider=provider)
        records.append(
            GeographyRecord(
                "provider_area",
                geo_id,
                area.code,
                None,
                None,
                None,
                area.name,
                vintage,
                area_code=f"{provider}:{area.code}",
            )
        )
    return records


def sync_provider_areas(provider: str) -> dict[str, int]:
    """Capture one provider's area list(s) and load their areas."""
    area_list = PROVIDER_AREA_LISTS[provider]
    hook = geography_pipeline._get_hook()
    factory = hook.get_conn
    control = CaptureControl(factory, source_code=area_list.source_code)
    retrieved_at = datetime.now(timezone.utc)
    run_id = control.start_run(
        watermark={"provider_area_list": provider, "retrieved": retrieved_at.date().isoformat()}
    )
    try:
        areas: dict[str, ProviderArea] = {}
        last_capture = None
        with httpx.Client(
            follow_redirects=True, timeout=120, headers=dict(area_list.headers)
        ) as client:
            for url in area_list.urls:
                parameters = {"provider_area_list": provider, "file": url.rsplit("/", 1)[-1]}
                request = control.start_request(
                    run_id=run_id,
                    endpoint=url,
                    parameters=parameters,
                    max_attempts=HTTP_MAX_ATTEMPTS,
                )
                try:
                    response = geography_pipeline._download_with_retry(
                        client, url, control=control, request_id=request.request_id
                    )
                except BaseException as exc:
                    control.finish_request(request.request_id, status="failed", error=exc)
                    raise
                capture_id = uuid4()
                persist_response_capture(
                    factory,
                    ResponseCapture(
                        capture_id=capture_id,
                        request_id=request.request_id,
                        run_id=run_id,
                        source_code=area_list.source_code,
                        endpoint=url,
                        request_parameters=parameters,
                        retrieved_at=retrieved_at,
                        http_status=response.status_code,
                        response_headers=response.headers,
                        media_type=response.headers.get("content-type", "text/plain"),
                        payload=response.content,
                        payload_schema_version=PARSER_VERSION,
                        source_revision=retrieved_at.date().isoformat(),
                    ),
                )
                control.finish_request(request.request_id, status="captured")
                payload = load_captured_payload(factory, capture_id)
                try:
                    for area in area_list.parse(payload):
                        areas.setdefault(area.code, area)
                except BaseException as exc:
                    control.quarantine(
                        capture_id=capture_id,
                        run_id=run_id,
                        parser_version=PARSER_VERSION,
                        error_code="provider_area_list_replay_failed",
                        error=exc,
                    )
                    raise
                last_capture = capture_id
        loaded = GeographyRepository(factory).load_attributes(
            provider_area_records(provider, list(areas.values()), vintage=retrieved_at.year),
            capture_id=last_capture,
        )
        control.finish_run(run_id, status="success")
        return {"provider_areas": loaded}
    except BaseException as exc:
        control.finish_run(run_id, status="failed", error=exc)
        raise
