"""Offline contracts for the HHS hospital capacity adapter.

Covers: ETL-133
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.hhs_hospital_capacity.client import (
    HhsFetchError,
    HhsPayloadError,
    fetch_metadata,
    fetch_page,
    page_parameters,
    rows_updated_at,
)
from data_ingestion_toolbox.hhs_hospital_capacity.config import (
    HhsHospitalCapacityConfig,
)
from data_ingestion_toolbox.hhs_hospital_capacity.registry import (
    MEASURES,
    SELECT_COLUMNS,
)
from data_ingestion_toolbox.hhs_hospital_capacity.silver_hhs_hospital_capacity.parse import (
    parse_page,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "hhs_hospital_capacity"
UW_HOSPITAL = "520098"


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


class _ScriptedClient:
    def __init__(self, responses: list[httpx.Response]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict, dict[str, str]]] = []

    def get(self, url: str, *, params: dict, headers: dict[str, str]) -> httpx.Response:
        self.calls.append((url, dict(params), dict(headers)))
        response = self.responses.pop(0)
        return httpx.Response(
            response.status_code,
            content=response.content,
            request=httpx.Request("GET", url),
        )


def _config(**overrides: object) -> HhsHospitalCapacityConfig:
    return HhsHospitalCapacityConfig(
        min_spacing_seconds=0, max_attempts=2, page_size=400, **overrides
    )


def test_a_page_asks_for_the_registered_columns_in_one_stable_order() -> None:
    """Covers: ETL-133 -- every page is the same $select and $order at its offset."""
    parameters = page_parameters(offset=800, page_size=400)
    assert parameters["$select"].split(",") == list(SELECT_COLUMNS)
    assert parameters["$order"] == "hospital_pk,collection_week,:id"
    assert (parameters["$limit"], parameters["$offset"]) == (400, 800)
    assert "is_corrected" not in SELECT_COLUMNS
    with pytest.raises(ValueError):
        page_parameters(offset=-1, page_size=400)


def test_the_dane_pages_parse_every_hospital_week_as_published() -> None:
    """Covers: ETL-133 -- 1,044 hospital weeks across three pages, none set aside."""
    seen: set[tuple[str, date]] = set()
    rows = []
    first = 1
    for offset in (0, 400, 800):
        page = parse_page(
            _fixture(f"anag_dane_offset_{offset}.json"),
            first_row_index=first,
            seen=seen,
        )
        assert page.quarantined == ()
        rows.extend(page.rows)
        first += page.row_count
    assert len(rows) == 1044
    assert {row.state_fips for row in rows} == {"55"}
    assert {row.fips_code for row in rows} == {"55025"}
    week = [row for row in rows if row.collection_week == date(2020, 8, 23)]
    assert sum(
        row.figures["inpatient_beds"].value
        for row in week
        if row.figures["inpatient_beds"].status == "valid"
    ) == Decimal("1302.1")
    assert set(rows[0].figures) == {measure.measure_id for measure in MEASURES}


def test_suppression_is_not_zero_and_a_bad_row_is_set_aside_alone() -> None:
    """Covers: ETL-133 -- -999999 is suppressed, an omitted cell missing; bad rows alone."""
    base = json.loads(_fixture("anag_dane_offset_0.json"))[0]
    suppressed = dict(base, inpatient_beds_7_day_avg="-999999.0")
    suppressed.pop("total_icu_beds_7_day_avg", None)
    repeated = dict(base)
    unreadable = dict(base, collection_week="2020-13-40T00:00:00.000")
    not_a_number = dict(base, hospital_pk="999999", inpatient_beds_7_day_avg="12x")
    no_hospital = dict(base)
    no_hospital.pop("hospital_pk")
    payload = json.dumps(
        [suppressed, repeated, unreadable, not_a_number, no_hospital]
    ).encode()
    page = parse_page(payload, first_row_index=1, seen=set())
    (kept,) = page.rows
    assert kept.figures["inpatient_beds"].status == "suppressed"
    assert kept.figures["inpatient_beds"].value is None
    assert kept.figures["total_icu_beds"].status == "missing"
    assert kept.figures["total_icu_beds"].value is None
    assert [(item.source_row_index, item.error_code) for item in page.quarantined] == [
        (2, "repeated_facility_week"),
        (3, "unreadable_week"),
        (4, "unreadable_value"),
        (5, "unreadable_facility"),
    ]
    assert (
        parse_page(b"<html>", first_row_index=1, seen=set()).quarantined[0].error_code
        == "invalid_json"
    )


def test_a_client_error_is_asked_once_and_a_server_error_is_retried() -> None:
    """Covers: ETL-133 -- 403 fails at once naming its status; 503 then 200 is kept."""
    client = _ScriptedClient([httpx.Response(403)])
    with pytest.raises(HhsFetchError) as refused:
        fetch_page(0, config=_config(), client=client, sleep=lambda _s: None)
    assert refused.value.status == 403
    assert len(client.calls) == 1
    headers = client.calls[0][2]
    assert headers["User-Agent"].startswith("population-etl-toolbox")
    client = _ScriptedClient([httpx.Response(503), httpx.Response(200, content=b"[]")])
    retries: list[BaseException] = []
    response = fetch_page(
        0,
        config=_config(),
        client=client,
        on_retry=retries.append,
        sleep=lambda _s: None,
    )
    assert (response.row_count, len(client.calls), len(retries)) == (0, 2, 1)
    client = _ScriptedClient([httpx.Response(200, content=b'{"a": 1}')])
    with pytest.raises(HhsPayloadError) as shape:
        fetch_page(0, config=_config(), client=client, sleep=lambda _s: None)
    assert shape.value.code == "expected_json_list"


def test_the_view_document_says_when_the_rows_last_changed() -> None:
    """Covers: ETL-133 -- the view document is checked and its rowsUpdatedAt read."""
    view = _fixture("anag_view.json")
    client = _ScriptedClient([httpx.Response(200, content=view)])
    response = fetch_metadata(config=_config(), client=client, sleep=lambda _s: None)
    assert rows_updated_at(response.raw_bytes) == json.loads(view)["rowsUpdatedAt"]
    client = _ScriptedClient([httpx.Response(200, content=b"[]")])
    with pytest.raises(HhsPayloadError):
        fetch_metadata(config=_config(), client=client, sleep=lambda _s: None)


def test_configuration_imports_without_io_or_credential() -> None:
    """Covers: ETL-133 -- no key, and the client says who it is plainly."""
    config = HhsHospitalCapacityConfig()
    assert config.base_url == "https://healthdata.gov"
    assert "@" not in config.user_agent
    assert "Mozilla" not in config.user_agent
