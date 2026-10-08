"""Offline contracts for the BEA regional accounts adapter.

Covers: ETL-058
"""

from __future__ import annotations

import importlib
import io
import sys
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.bea.client import (
    BeaFetchError,
    BeaPayloadError,
    every_area_member,
    fetch_table,
)
from data_ingestion_toolbox.bea.config import BeaConfig
from data_ingestion_toolbox.bea.registry import TABLES, get_table, metric_key
from data_ingestion_toolbox.bea.silver_bea.parse import parse_table

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "bea"


def _fixture(code: str) -> bytes:
    return (FIXTURES / f"{code}.zip").read_bytes()


def _rezip(code: str, transform) -> bytes:
    source = zipfile.ZipFile(io.BytesIO(_fixture(code)))
    name = source.namelist()[0]
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as target:
        target.writestr(name, transform(source.read(name)))
    return out.getvalue()


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
        request=httpx.Request("GET", "https://apps.bea.gov/regional/zip"),
    )


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-058 — the bulk files need no key, so there is none to leak."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.bea"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.bea.config")
    fields = set(module.BeaConfig.model_fields)
    assert not {field for field in fields if "key" in field or "token" in field}
    assert module.BEA_BULK_BASE_URL == "https://apps.bea.gov/regional/zip"


def test_the_registry_names_every_table_and_line() -> None:
    """Covers: ETL-058 — chained and current dollars are distinct metric identities."""
    assert [table.code for table in TABLES] == [
        "CAINC1",
        "CAINC4",
        "CAINC5N",
        "CAGDP1",
        "CAGDP2",
    ]
    gdp = get_table("CAGDP1")
    assert gdp.lines == {"1": "chained_dollars", "3": "current_dollars"}
    assert metric_key("CAGDP1", "1") != metric_key("CAGDP1", "3")
    assert get_table("CAINC1").path == "/CAINC1.zip"
    with pytest.raises(KeyError):
        get_table("CAINC30")


def test_a_client_error_fails_once_and_a_bad_container_is_refused() -> None:
    """Covers: ETL-058 — 404 fails without retry; a non-zip or wrong member is a payload error."""
    client = _Client([_response(404)])
    with pytest.raises(BeaFetchError) as raised:
        fetch_table(
            get_table("CAINC1"), config=BeaConfig(max_attempts=3), client=client
        )
    assert raised.value.code == "non_retryable_http" and len(client.calls) == 1
    with pytest.raises(BeaPayloadError, match="not_a_zip"):
        every_area_member(b"<html>", "/CAINC1.zip", get_table("CAINC1"))
    with pytest.raises(BeaPayloadError, match="every_area_member_missing"):
        every_area_member(_fixture("CAGDP1"), "/CAINC1.zip", get_table("CAINC1"))
    retries: list[BaseException] = []
    client = _Client([_response(503), _response(200, _fixture("CAINC1"))])
    fetched = fetch_table(
        get_table("CAINC1"),
        config=BeaConfig(max_attempts=2),
        client=client,
        on_retry=retries.append,
        sleep=lambda _: None,
    )
    assert fetched.raw_bytes == _fixture("CAINC1") and len(retries) == 1


def test_a_table_parses_its_registered_lines_with_the_release_date() -> None:
    """Covers: ETL-058 — every year of every registered line, the release from the footer."""
    parsed = parse_table(_fixture("CAINC1"), table=get_table("CAINC1"))
    assert parsed.release_date == date(2026, 2, 5)
    assert parsed.member == "CAINC1__ALL_AREAS_1969_2024.csv"
    assert parsed.quarantined == ()
    kent = {
        (item.line_code, item.year): item
        for item in parsed.observations
        if item.geo_id == "state:10|county:001"
    }
    assert kent[("3", 2024)].value == Decimal("55474")
    assert kent[("3", 2024)].unit == "Dollars"
    assert kent[("1", 1969)].year == 1969
    assert {item.geo_type for item in parsed.observations} == {
        "nation",
        "state",
        "county",
    }


def test_regions_and_combined_areas_are_counted_not_loaded() -> None:
    """Covers: ETL-058 — a BEA region or combined area is BEA's own geography, never a county."""
    parsed = parse_table(_fixture("CAINC1"), table=get_table("CAINC1"))
    assert not [
        item
        for item in parsed.observations
        if item.geo_source_code in {"98000", "51901", "15901"}
    ]
    assert parsed.out_of_scope_row_count > 0
    assert parsed.in_scope_row_count + parsed.out_of_scope_row_count == parsed.row_count


def test_provider_codes_keep_their_status_and_never_a_number() -> None:
    """Covers: ETL-058 — (D), (NA), (NM) and (L) are statuses, not zeros."""
    gdp = parse_table(_fixture("CAGDP2"), table=get_table("CAGDP2"))
    withheld = [item for item in gdp.observations if item.value_status == "withheld"]
    assert withheld and all(
        item.value is None and item.value_source == "(D)" for item in withheld
    )

    def recode(content: bytes) -> bytes:
        lines = content.split(b"\n")
        for index, line in enumerate(lines):
            if line.lstrip().startswith(b'"10001"') and b",CAINC1,3," in line:
                cells = line.split(b",")
                cells[-1] = b"(NA)"
                cells[-2] = b"(NM)"
                cells[-3] = b"(L)"
                cells[-4] = b"n/a"
                lines[index] = b",".join(cells)
        return b"\n".join(lines)

    parsed = parse_table(_rezip("CAINC1", recode), table=get_table("CAINC1"))
    kent = {
        item.year: item
        for item in parsed.observations
        if item.geo_id == "state:10|county:001" and item.line_code == "3"
    }
    assert (
        kent[2024].value_status,
        kent[2023].value_status,
        kent[2022].value_status,
        kent[2021].value_status,
    ) == (
        "not_available",
        "not_meaningful",
        "below_threshold",
        "missing",
    )
    assert all(kent[year].value is None for year in (2021, 2022, 2023, 2024))


def test_unreadable_rows_and_a_missing_release_are_quarantined() -> None:
    """Covers: ETL-058 — a wrong table, a bad code, a short row; no footer refuses the file."""

    def corrupt(content: bytes) -> bytes:
        lines = content.split(b"\n")
        kent = [
            index
            for index, line in enumerate(lines)
            if line.lstrip().startswith(b'"10001"')
        ]
        lines[kent[0]] = lines[kent[0]].replace(b",CAINC1,", b",CAINC9,")
        lines[kent[1]] = lines[kent[1]].replace(b'"10001"', b'"1000X"')
        lines[kent[2]] = b",".join(lines[kent[2]].split(b",")[:20])
        return b"\n".join(lines)

    parsed = parse_table(_rezip("CAINC1", corrupt), table=get_table("CAINC1"))
    assert sorted(item.error_code for item in parsed.quarantined) == [
        "ragged_row",
        "unexpected_table",
        "unreadable_geography",
    ]
    assert not [
        item for item in parsed.observations if item.geo_id == "state:10|county:001"
    ]

    no_footer = parse_table(
        _rezip(
            "CAINC1", lambda content: content.replace(b"Last updated:", b"Updated:")
        ),
        table=get_table("CAINC1"),
    )
    assert [item.error_code for item in no_footer.quarantined] == [
        "release_date_missing"
    ]
    assert no_footer.observations == ()
