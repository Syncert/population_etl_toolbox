"""Offline contracts for the Census Building Permits Survey adapter.

Covers: ETL-057
"""

from __future__ import annotations

import importlib
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.census_bps.client import (
    BpsFetchError,
    BpsPayloadError,
    fetch_file,
    read_rows,
)
from data_ingestion_toolbox.census_bps.config import BpsConfig
from data_ingestion_toolbox.census_bps.registry import (
    ANNUAL,
    MONTHLY,
    BpsSlice,
    metric_key,
    recent_periods,
    registered_periods,
    slices_for,
)
from data_ingestion_toolbox.census_bps.silver_census_bps.parse import parse_file

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "census_bps"
COUNTY_MONTH = BpsSlice("county", MONTHLY, 2024, 3)
COUNTY_YEAR = BpsSlice("county", ANNUAL, 2024, 12)
STATE_MONTH = BpsSlice("state", MONTHLY, 2024, 3)
PLACE_YEAR = BpsSlice("place", ANNUAL, 2024, 12, "south")


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


class _Client:
    def __init__(self, outcomes: list[httpx.Response]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.calls.append(url)
        return self.outcomes.pop(0)

    def close(self) -> None:
        return None


def _response(status: int, content: bytes = b"") -> httpx.Response:
    return httpx.Response(
        status,
        content=content,
        request=httpx.Request("GET", "https://www2.census.gov/econ/bps"),
    )


def test_configuration_imports_without_io() -> None:
    """Covers: ETL-057 — importing reads nothing and needs no credential."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.census_bps"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.census_bps.config")
    assert module.BpsConfig().recent_months == 6
    assert (module.FIRST_YEAR, module.FIRST_PLACE_YEAR) == (2000, 2007)


def test_the_registry_names_every_file_it_requests() -> None:
    """Covers: ETL-057 — county and state monthly; county, state and place annual."""
    assert [item.path for item in slices_for(MONTHLY, 2024, 3)] == [
        "/County/co2403c.txt",
        "/State/st2403c.txt",
    ]
    annual = slices_for(ANNUAL, 2024)
    assert [item.path for item in annual] == [
        "/County/co2412y.txt",
        "/State/st2412y.txt",
        "/Place/Midwest Region/mw2024a.txt",
        "/Place/Northeast Region/ne2024a.txt",
        "/Place/South Region/so2024a.txt",
        "/Place/West Region/we2024a.txt",
    ]
    assert [item.slice_key for item in slices_for(ANNUAL, 2006)] == ["county", "state"]
    with pytest.raises(ValueError, match="registered from 2000"):
        slices_for(MONTHLY, 1999, 1)
    with pytest.raises(ValueError, match="not a month"):
        slices_for(MONTHLY, 2024, 13)
    assert (
        metric_key("units", "5_plus_units", "monthly") == "units:5_plus_units:monthly"
    )
    assert recent_periods(date(2026, 2, 10), 3) == [
        (MONTHLY, 2025, 11),
        (MONTHLY, 2025, 12),
        (MONTHLY, 2026, 1),
        (ANNUAL, 2025, 12),
    ]
    history = registered_periods(date(2001, 3, 1))
    assert history[:2] == [(MONTHLY, 2000, 1), (MONTHLY, 2000, 2)]
    assert history[-3:] == [(ANNUAL, 2000, 12), (MONTHLY, 2001, 1), (MONTHLY, 2001, 2)]


def test_an_unpublished_month_is_empty_and_a_client_error_fails_once() -> None:
    """Covers: ETL-057 — 404 is an empty file; 403 is not retried."""
    client = _Client([_response(404)])
    empty = fetch_file(COUNTY_MONTH, config=BpsConfig(max_attempts=2), client=client)
    assert not empty.published and empty.raw_bytes == b""
    assert client.calls == ["https://www2.census.gov/econ/bps/County/co2403c.txt"]
    client = _Client([_response(403)])
    with pytest.raises(BpsFetchError) as raised:
        fetch_file(COUNTY_MONTH, config=BpsConfig(max_attempts=3), client=client)
    assert raised.value.code == "non_retryable_http" and len(client.calls) == 1


def test_a_changed_layout_is_a_payload_error() -> None:
    """Covers: ETL-057 — the two header rows and the column count are checked."""
    with pytest.raises(BpsPayloadError, match="unexpected_header"):
        read_rows(b"<html>moved</html>", COUNTY_MONTH.path, COUNTY_MONTH)
    # A place file read as a county file has the wrong number of columns.
    with pytest.raises(BpsPayloadError, match="unexpected_layout"):
        read_rows(_fixture("Place_South_so2024a.txt"), COUNTY_MONTH.path, COUNTY_MONTH)


def test_a_county_month_keeps_the_estimate_beside_what_was_reported() -> None:
    """Covers: ETL-057 — units, buildings and valuation per structure type, reported kept."""
    parsed = parse_file(_fixture("County_co2403c.txt"), item=COUNTY_MONTH)
    assert parsed.quarantined == ()
    # Delaware's three counties and Autauga County; the `000` rows are counted out.
    assert (parsed.in_scope_row_count, parsed.out_of_scope_row_count) == (
        4,
        parsed.row_count - 4,
    )
    assert parsed.out_of_scope_row_count > 0
    assert len(parsed.observations) == 4 * 4 * 3
    kent = {
        (item.measure_id, item.structure_type): item
        for item in parsed.observations
        if item.geo_id == "state:10|county:001"
    }
    assert kent[("units", "1_unit")].value == Decimal("81")
    assert kent[("valuation", "1_unit")].value == Decimal("12451035")
    assert kent[("units", "2_units")].value == Decimal("8")
    assert kent[("units", "1_unit")].period_start == date(2024, 3, 1)
    assert kent[("units", "1_unit")].period_end == date(2024, 3, 31)
    autauga = {
        (item.measure_id, item.structure_type): item
        for item in parsed.observations
        if item.geo_id == "state:01|county:001"
    }
    # Imputation: 15 units estimated, 15 reported -- both kept, never merged.
    assert (
        autauga[("units", "1_unit")].value,
        autauga[("units", "1_unit")].reported_value,
    ) == (Decimal("15"), Decimal("15"))


def test_state_files_publish_no_valuation_and_the_nation_is_its_own_row() -> None:
    """Covers: ETL-057 — the state file's thousands of dollars are not mixed with dollars."""
    parsed = parse_file(_fixture("State_st2403c.txt"), item=STATE_MONTH)
    assert {item.measure_id for item in parsed.observations} == {"buildings", "units"}
    assert {item.geo_id for item in parsed.observations} == {
        "us:1",
        "state:01",
        "state:10",
    }
    nation = [
        item
        for item in parsed.observations
        if item.geo_id == "us:1"
        and item.measure_id == "units"
        and item.structure_type == "5_plus_units"
    ]
    assert nation[0].value == Decimal("34835")


def test_an_annual_place_file_keeps_unreported_places_unreported() -> None:
    """Covers: ETL-057 — a place that reported no month is not_reported, never zero."""
    parsed = parse_file(_fixture("Place_South_so2024a.txt"), item=PLACE_YEAR)
    assert parsed.quarantined == ()
    # 19 Delaware rows: two unincorporated remainders and one county-level row are out.
    assert (
        parsed.row_count,
        parsed.in_scope_row_count,
        parsed.out_of_scope_row_count,
    ) == (19, 16, 3)
    newark = [
        item for item in parsed.observations if item.geo_id == "state:10|place:50670"
    ]
    assert newark and all(
        item.value_status == "not_reported" and item.value is None for item in newark
    )
    assert all(item.months_reported == 0 for item in newark)
    dover = {
        (item.measure_id, item.structure_type): item
        for item in parsed.observations
        if item.geo_id == "state:10|place:21200"
    }
    assert dover[("units", "5_plus_units")].value == Decimal("108")
    assert dover[("units", "1_unit")].period_start == date(2024, 1, 1)
    assert dover[("units", "1_unit")].geo_source_label == "Dover"


def test_rows_that_cannot_be_read_are_quarantined_and_absence_writes_nothing() -> None:
    """Covers: ETL-057 — a wrong date or code is set aside; a missing county is no zero."""
    raw = _fixture("County_co2403c.txt").split(b"\n")
    header, rows = raw[:3], [line for line in raw[3:] if line.strip()]
    kent = next(line for line in rows if line.startswith(b"202403,10,001"))
    payload = (
        b"\n".join(
            header
            + [
                kent.replace(b"202403", b"202402", 1),
                kent.replace(b",10,001,", b",1A,001,", 1),
                kent[:40],
            ]
        )
        + b"\n"
    )
    parsed = parse_file(payload, item=COUNTY_MONTH)
    assert [
        (item.source_row_index, item.error_code) for item in parsed.quarantined
    ] == [
        (1, "unexpected_period"),
        (2, "unreadable_geography"),
        (3, "ragged_row"),
    ]
    assert parsed.observations == ()
    # Kent County is absent from this file: nothing is written for it.
    without_kent = (
        b"\n".join(
            header + [line for line in rows if not line.startswith(b"202403,10,001")]
        )
        + b"\n"
    )
    absent = parse_file(without_kent, item=COUNTY_MONTH)
    assert not [
        item for item in absent.observations if item.geo_id == "state:10|county:001"
    ]
    assert parse_file(b"", item=COUNTY_YEAR).row_count == 0
