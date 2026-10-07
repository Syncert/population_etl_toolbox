"""Offline contracts for the BLS QCEW adapter.

Covers: ETL-056
"""

from __future__ import annotations

import importlib
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.bls_qcew.client import (
    QcewFetchError,
    QcewPayloadError,
    fetch_slice,
    read_header,
)
from data_ingestion_toolbox.bls_qcew.config import QcewConfig
from data_ingestion_toolbox.bls_qcew.registry import (
    INDUSTRIES,
    SECTORS,
    TOTAL,
    get_industry,
    metric_key,
    period_bounds,
    registered_periods,
    slice_path,
)
from data_ingestion_toolbox.bls_qcew.silver_bls_qcew.parse import parse_slice

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "bls_qcew"


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


class _Client:
    def __init__(self, outcomes: list[httpx.Response | BaseException]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, dict[str, str]]] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.calls.append((url, dict(headers)))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def close(self) -> None:
        return None


def _response(status: int, content: bytes = b"") -> httpx.Response:
    return httpx.Response(
        status,
        content=content,
        headers={"content-type": "text/csv", "set-cookie": "x"},
        request=httpx.Request("GET", "https://data.bls.gov/cew/data/api"),
    )


def _config() -> QcewConfig:
    return QcewConfig(min_spacing_seconds=0, max_attempts=3)


def test_configuration_imports_without_io() -> None:
    """Covers: ETL-056 — importing reads nothing and needs no credential."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.bls_qcew"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.bls_qcew.config")
    assert module.QcewConfig().postgres_conn_id == "public_data"
    assert module.FIRST_YEAR == 2014


def test_the_registry_names_every_slice_it_requests() -> None:
    """Covers: ETL-056 — industries, ownerships and periods are declared."""
    assert TOTAL.ownerships == ("0", "5")
    assert all(industry.ownerships == ("5",) for industry in SECTORS)
    assert len(INDUSTRIES) == 22
    assert slice_path(2024, "1", get_industry("31-33")) == "/2024/1/industry/31_33.csv"
    assert slice_path(2023, "a", TOTAL) == "/2023/a/industry/10.csv"
    with pytest.raises(ValueError, match="begins in 2014"):
        slice_path(2013, "1", TOTAL)
    with pytest.raises(ValueError, match="unregistered QCEW period"):
        slice_path(2024, "5", TOTAL)
    with pytest.raises(KeyError):
        get_industry("1011")
    assert metric_key("employment", "62", "5") == "employment:62:5"
    periods = registered_periods(2025, 2)
    assert periods[:5] == [
        (2014, "1"),
        (2014, "2"),
        (2014, "3"),
        (2014, "4"),
        (2014, "a"),
    ]
    assert periods[-3:] == [(2024, "a"), (2025, "1"), (2025, "2")]
    assert (2025, "a") not in periods


def test_periods_are_calendar_spans() -> None:
    """Covers: ETL-056 — a quarter, a month of it, and a year."""
    assert period_bounds(2024, "1") == (date(2024, 1, 1), date(2024, 3, 31))
    assert period_bounds(2024, "1", 1) == (date(2024, 2, 1), date(2024, 2, 29))
    assert period_bounds(2024, "4", 2) == (date(2024, 12, 1), date(2024, 12, 31))
    assert period_bounds(2023, "a") == (date(2023, 1, 1), date(2023, 12, 31))


def test_an_unpublished_period_is_empty_and_a_client_error_fails_once() -> None:
    """Covers: ETL-056 — 404 is an empty slice; 403 is not retried; 503 is."""
    client = _Client([_response(404)])
    empty = fetch_slice(2026, "4", TOTAL, config=_config(), client=client)
    assert empty.raw_bytes == b"" and not empty.published
    assert client.calls[0][0].endswith("/2026/4/industry/10.csv")
    assert "population-etl-toolbox" in client.calls[0][1]["User-Agent"]

    client = _Client([_response(403)])
    with pytest.raises(QcewFetchError) as raised:
        fetch_slice(2024, "1", TOTAL, config=_config(), client=client)
    assert raised.value.code == "non_retryable_http" and len(client.calls) == 1

    retries: list[BaseException] = []
    client = _Client(
        [_response(503), _response(200, _fixture("2024_1_industry_10.csv"))]
    )
    captured = fetch_slice(
        2024,
        "1",
        TOTAL,
        config=_config(),
        client=client,
        on_retry=retries.append,
        sleep=lambda _: None,
    )
    assert captured.raw_bytes == _fixture("2024_1_industry_10.csv")
    assert "set-cookie" not in {name.lower() for name in captured.response_headers}
    assert len(retries) == 1


def test_a_changed_layout_is_a_payload_error() -> None:
    """Covers: ETL-056 — the header is checked against the registered layout."""
    with pytest.raises(QcewPayloadError, match="missing_columns"):
        read_header(b'"area_fips","own_code"\n', "/2024/1/industry/10.csv", "1")
    # The annual layout is not the quarterly one.
    with pytest.raises(QcewPayloadError, match="missing_columns"):
        read_header(_fixture("2023_a_industry_10.csv"), "/2023/a/industry/10.csv", "1")
    client = _Client([_response(200, b"<html>maintenance</html>")])
    with pytest.raises(QcewPayloadError):
        fetch_slice(2024, "1", TOTAL, config=_config(), client=client)


def test_a_quarter_slice_parses_its_registered_rows_and_counts_the_rest() -> None:
    """Covers: ETL-056 — MSAs, other ownerships and unknown-county areas are counted, not loaded."""
    parsed = parse_slice(
        _fixture("2024_1_industry_10.csv"), year=2024, period="1", industry=TOTAL
    )
    assert parsed.quarantined == ()
    assert (
        parsed.row_count,
        parsed.in_scope_row_count,
        parsed.out_of_scope_row_count,
    ) == (47, 12, 35)
    # Six values per row: establishments, three months of employment, wages, weekly wage.
    assert len(parsed.observations) == 12 * 6
    geo_ids = {item.geo_id for item in parsed.observations}
    assert geo_ids == {
        "us:1",
        "state:10",
        "state:01|county:001",
        "state:10|county:001",
        "state:10|county:003",
        "state:10|county:005",
    }
    assert not any(item.geo_source_code.endswith("999") for item in parsed.observations)
    kent = {
        (item.measure_id, item.month_index): item
        for item in parsed.observations
        if item.geo_id == "state:10|county:001" and item.own_code == "0"
    }
    assert kent[("establishments", 0)].value == Decimal("5775")
    assert kent[("employment", 2)].value == Decimal("70393")
    assert kent[("employment", 2)].period_start == date(2024, 3, 1)
    assert kent[("avg_weekly_wage", 0)].value == Decimal("1073")
    assert kent[("total_wages", 0)].period_end == date(2024, 3, 31)


def test_a_withheld_cell_is_never_a_zero() -> None:
    """Covers: ETL-056 — disclosure `N` is withheld with the provider's text."""
    parsed = parse_slice(
        _fixture("2024_1_industry_10.csv"), year=2024, period="1", industry=TOTAL
    )
    withheld = [item for item in parsed.observations if item.value_status == "withheld"]
    assert withheld
    for item in withheld:
        assert item.value is None
        assert item.disclosure_code == "N"
        assert item.value_source is not None


def test_sector_and_annual_slices_use_their_own_measures() -> None:
    """Covers: ETL-056 — a sector is private-only; annual averages are their own measures."""
    sector = parse_slice(
        _fixture("2024_1_industry_62.csv"),
        year=2024,
        period="1",
        industry=get_industry("62"),
    )
    assert {item.own_code for item in sector.observations} == {"5"}
    assert {item.industry_code for item in sector.observations} == {"62"}
    assert sector.in_scope_row_count == 6
    annual = parse_slice(
        _fixture("2023_a_industry_10.csv"), year=2023, period="a", industry=TOTAL
    )
    assert {item.measure_id for item in annual.observations} == {
        "annual_avg_establishments",
        "annual_avg_employment",
        "total_annual_wages",
        "annual_avg_weekly_wage",
    }
    assert {(item.period_start, item.period_end) for item in annual.observations} == {
        (date(2023, 1, 1), date(2023, 12, 31))
    }


def test_rows_outside_the_slice_are_quarantined_with_reasons() -> None:
    """Covers: ETL-056 — a wrong period, industry or area is set aside, not loaded."""
    lines = _fixture("2024_1_industry_10.csv").split(b"\r\n")
    header, rows = lines[0], [line for line in lines[1:] if line]
    kent = next(line for line in rows if line.startswith(b'"10001","0"'))
    wrong_year = kent.replace(b'"2024","1"', b'"2023","1"')
    wrong_industry = kent.replace(b'"10","70"', b'"62","70"')
    wrong_area = kent.replace(b'"10001"', b'"1000A"')
    payload = b"\r\n".join([header, wrong_year, wrong_industry, wrong_area]) + b"\r\n"
    parsed = parse_slice(payload, year=2024, period="1", industry=TOTAL)
    assert [
        (item.source_row_index, item.error_code) for item in parsed.quarantined
    ] == [
        (1, "unexpected_period"),
        (2, "unexpected_industry"),
        (3, "unreadable_geography"),
    ]
    assert parsed.observations == ()
    rejected = parse_slice(b"not,a,qcew,file\n", year=2024, period="1", industry=TOTAL)
    assert [
        (item.source_row_index, item.error_code) for item in rejected.quarantined
    ] == [(0, "missing_columns")]
    assert parse_slice(b"", year=2026, period="4", industry=TOTAL).row_count == 0
