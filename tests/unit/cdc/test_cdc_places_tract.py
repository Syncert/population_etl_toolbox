"""Offline contracts for the CDC PLACES tract release.

Covers: ETL-134
"""

from __future__ import annotations

import json
from uuid import uuid4

import httpx
import pytest

from data_ingestion_toolbox.capture import CaptureReceipt
from data_ingestion_toolbox.cdc.capture import CdcCaptureError, capture_asset_release
from data_ingestion_toolbox.cdc.metadata import (
    MetadataDecision,
    decide_metadata,
    parse_metadata,
)
from data_ingestion_toolbox.cdc.registry import PLACES_TRACT_ASSET
from data_ingestion_toolbox.cdc.silver_cdc.places_county import _geo
from data_ingestion_toolbox.cdc.silver_cdc.replay import parse_page
from tests.support import cdc_places_tract as tract
from tests.unit.cdc._doubles import MemoryPageLedger

pytestmark = pytest.mark.unit


class _Control:
    """Records the control calls a capture makes; no database."""

    def __init__(self) -> None:
        self.requests: list[dict] = []

    def start_run(self, **_kwargs: object):  # noqa: ANN201
        return uuid4()

    def start_request(self, **kwargs: object):  # noqa: ANN201
        self.requests.append(kwargs)
        return type("Request", (), {"request_id": uuid4()})()

    def finish_request(self, *_args: object, **_kwargs: object) -> None:
        return None

    def record_request_retry(self, *_args: object, **_kwargs: object) -> None:
        return None

    def set_run_watermark(self, *_args: object, **_kwargs: object) -> None:
        return None

    def finish_run(self, *_args: object, **_kwargs: object) -> None:
        return None

    def quarantine(self, **_kwargs: object) -> None:
        return None


def _persist(_factory: object, capture: object) -> CaptureReceipt:
    return CaptureReceipt(capture.capture_id, capture.payload_checksum)  # type: ignore[attr-defined]


def _capture(client: tract.FixtureClient, ledger: MemoryPageLedger):  # noqa: ANN202
    return capture_asset_release(
        lambda: None,
        PLACES_TRACT_ASSET,
        config=tract.config(),
        client=client,
        control=_Control(),
        persist_capture=_persist,
        page_ledger=ledger,
    )


def test_the_view_document_matches_the_registered_tract_contract() -> None:
    """Covers: ETL-134 -- the 2025 tract release's columns are the registered ones."""
    metadata = parse_metadata(tract.metadata_bytes(), PLACES_TRACT_ASSET)
    assert metadata.socrata_id == "cwsq-ngmh"
    assert (
        decide_metadata(PLACES_TRACT_ASSET, metadata, None) is MetadataDecision.INGEST
    )
    assert PLACES_TRACT_ASSET.bulk and PLACES_TRACT_ASSET.geography_vintage == 2020


def test_an_eleven_digit_code_is_the_tract_it_names_and_nothing_else_is() -> None:
    """Covers: ETL-134 -- tract codes resolve on 2020 tracts; a county code is unsupported."""
    assert _geo("55025000100", "places_tract") == (
        "tract",
        "state:55|county:025|tract:000100",
    )
    assert _geo("55025", "places_tract") == ("unsupported", None)
    assert _geo("59", "places_tract") == ("unsupported", None)
    assert _geo("55025", "places_county") == ("county", "state:55|county:025")


def test_every_dane_row_parses_with_its_limits_and_year() -> None:
    """Covers: ETL-134 -- 4,920 rows, none set aside, limits and BRFSS year kept."""
    stored = 0
    years: set[int] = set()
    for offset in tract.PAGE_OFFSETS:
        rows = json.loads(tract.page_bytes(offset))
        result = parse_page(
            PLACES_TRACT_ASSET,
            capture_id=uuid4(),
            offset=offset,
            rows=rows,
            release_watermark="1765560443",
        )
        assert result.quarantined == ()
        assert result.observations[0].source_row_index == offset
        stored += len(result.observations)
        years |= {item.period_start for item in result.observations}
        assert all(item.geo_type == "tract" for item in result.observations)
        assert all(
            item.confidence_lower <= item.value <= item.confidence_upper
            for item in result.observations
            if item.value is not None
        )
    assert stored == tract.ROW_COUNT
    assert years == {2022, 2023}


def test_a_bulk_read_uses_large_pages_records_each_and_checks_the_count() -> None:
    """Covers: ETL-134 -- every page recorded; the total must equal count(*)."""
    ledger = MemoryPageLedger()
    client = tract.FixtureClient()
    release = _capture(client, ledger)
    assert (release.row_count, release.counted_row_count) == (4920, 4920)
    assert [(page["offset"], page["row_count"]) for page in ledger.pages] == [
        (0, 2000),
        (2000, 2000),
        (4000, 920),
    ]
    assert client.calls[-1][1] == {"$select": "count(*)"}
    with pytest.raises(CdcCaptureError) as refused:
        _capture(tract.FixtureClient(count=5000), MemoryPageLedger())
    assert refused.value.code == "paged_total_differs"


def test_a_resumed_read_reuses_the_full_pages_and_reads_from_the_next() -> None:
    """Covers: ETL-134 -- captured pages are not asked for again."""
    earlier = uuid4()
    captured = uuid4()
    ledger = MemoryPageLedger(resume=(earlier, [(0, 2000, captured)]))
    client = tract.FixtureClient()
    release = _capture(client, ledger)
    offsets = [
        int(params["$offset"]) for _url, params in client.calls if "$offset" in params
    ]
    assert offsets == [2000, 4000]
    assert release.page_capture_ids[0] == captured
    assert ledger.pages[0]["resumed_from_run_id"] == earlier
    assert release.row_count == 4920


def test_a_server_error_mid_read_fails_the_run_without_a_release() -> None:
    """Covers: ETL-134 -- an exhausted retry raises; nothing is published."""
    ledger = MemoryPageLedger()
    client = tract.FixtureClient({2000: httpx.Response(503, content=b"busy")})
    with pytest.raises(Exception, match="retry_exhausted"):
        _capture(client, ledger)
    assert [page["offset"] for page in ledger.pages] == [0]
