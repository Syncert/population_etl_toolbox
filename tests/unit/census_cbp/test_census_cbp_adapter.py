"""Offline contracts for the County Business Patterns adapter.

Covers: ETL-062
"""

from __future__ import annotations

import importlib
import io
import sys
import zipfile
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.census_cbp.client import (
    CbpFetchError,
    CbpPayloadError,
    fetch_file,
    file_member,
)
from data_ingestion_toolbox.census_cbp.config import CbpConfig
from data_ingestion_toolbox.census_cbp.registry import (
    get_file,
    metric_key,
    registered_files,
)
from data_ingestion_toolbox.census_cbp.silver_census_cbp.parse import parse_file

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "census_cbp"
COUNTY_2023 = get_file("county", 2023)
COUNTY_2016 = get_file("county", 2016)
KENT = "state:10|county:001"


def _fixture(name: str) -> bytes:
    return (FIXTURES / f"{name}.zip").read_bytes()


def _rezip(name: str, transform) -> bytes:
    source = zipfile.ZipFile(io.BytesIO(_fixture(name)))
    member = source.namelist()[0]
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as target:
        target.writestr(member, transform(source.read(member)))
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
        status, content=content, request=httpx.Request("GET", "https://www2.census.gov")
    )


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-062 — the files need no key, so there is none to leak."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.census_cbp"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.census_cbp.config")
    fields = set(module.CbpConfig.model_fields)
    assert not {field for field in fields if "key" in field or "token" in field}


def test_the_registry_names_three_levels_a_year() -> None:
    """Covers: ETL-062 — county, state and nation files from 2016 to 2023."""
    files = registered_files()
    assert len(files) == 8 * 3
    assert COUNTY_2023.path == "/2023/cbp23co.zip"
    assert get_file("nation", 2016).member == "cbp16us.txt"
    assert metric_key("emp", "72----") == "emp:72"
    assert metric_key("est", "------") == "est:total"
    with pytest.raises(KeyError):
        get_file("county", 2015)


def test_a_client_error_fails_once_and_a_wrong_member_is_refused() -> None:
    """Covers: ETL-062 — 404 fails without retry; a state zip is not a county file."""
    client = _Client([_response(404)])
    with pytest.raises(CbpFetchError) as raised:
        fetch_file(COUNTY_2023, config=CbpConfig(max_attempts=3), client=client)
    assert raised.value.code == "non_retryable_http" and len(client.calls) == 1
    with pytest.raises(CbpPayloadError, match="member_missing"):
        file_member(_fixture("cbp23st"), COUNTY_2023.path, COUNTY_2023)
    with pytest.raises(CbpPayloadError, match="not_a_zip"):
        file_member(b"<html>", COUNTY_2023.path, COUNTY_2023)
    retries: list[BaseException] = []
    client = _Client([_response(503), _response(200, _fixture("cbp23co"))])
    fetched = fetch_file(
        COUNTY_2023,
        config=CbpConfig(max_attempts=2),
        client=client,
        on_retry=retries.append,
        sleep=lambda _: None,
    )
    assert fetched.raw_bytes == _fixture("cbp23co") and len(retries) == 1


def test_a_county_file_keeps_its_sectors_flags_and_units() -> None:
    """Covers: ETL-062 — four measures per sector; the noise flag beside the value."""
    parsed = parse_file(_fixture("cbp23co"), item=COUNTY_2023)
    assert parsed.quarantined == ()
    kent = {
        (obs.measure, obs.naics_key): obs
        for obs in parsed.observations
        if obs.geo_id == KENT
    }
    total = kent[("emp", "total")]
    assert (total.value, total.noise_flag, total.value_status, total.value_source) == (
        Decimal("61078"),
        "G",
        "valid",
        "G:61078",
    )
    assert (kent[("ap", "72")].value, kent[("ap", "72")].noise_flag) == (
        Decimal("217407"),
        "G",
    )
    assert (kent[("est", "total")].value, kent[("est", "total")].noise_flag) == (
        Decimal("4971"),
        None,
    )
    assert {key for _measure, key in kent} >= {"total", "72", "62", "44"}
    assert {obs.geo_type for obs in parsed.observations} == {"county"}


def test_the_statewide_row_and_detailed_industries_are_counted_not_loaded() -> None:
    """Covers: ETL-062 — county 999 is the Bureau's statewide row; six-digit NAICS is out of scope."""
    parsed = parse_file(_fixture("cbp23co"), item=COUNTY_2023)
    assert not [
        obs for obs in parsed.observations if obs.geo_source_code.endswith("999")
    ]
    assert parsed.in_scope_row_count == 63
    assert parsed.in_scope_row_count + parsed.out_of_scope_row_count == parsed.row_count
    state = parse_file(_fixture("cbp23st"), item=get_file("state", 2023))
    assert state.in_scope_row_count == 21 and {
        obs.geo_id for obs in state.observations
    } == {"state:10"}
    nation = parse_file(_fixture("cbp23us"), item=get_file("nation", 2023))
    assert {obs.geo_id for obs in nation.observations} == {"us:1"}


def test_a_withheld_cell_is_never_the_zero_the_file_writes() -> None:
    """Covers: ETL-062 — 2016's D cells keep their status and size range, with no number."""
    parsed = parse_file(_fixture("cbp16co"), item=COUNTY_2016)
    withheld = [obs for obs in parsed.observations if obs.value_status == "withheld"]
    assert withheld and all(
        obs.value is None and obs.value_source == "D:0" for obs in withheld
    )
    sussex_mining = next(
        obs
        for obs in withheld
        if obs.geo_id == "state:10|county:005"
        and obs.naics_key == "21"
        and obs.measure == "emp"
    )
    assert sussex_mining.employment_range == "B"
    establishments = next(
        obs
        for obs in parsed.observations
        if obs.geo_id == "state:10|county:005"
        and obs.naics_key == "21"
        and obs.measure == "est"
    )
    assert (establishments.value_status, establishments.value) == (
        "valid",
        Decimal("3"),
    )


def test_unreadable_rows_and_a_wrong_container_are_quarantined() -> None:
    """Covers: ETL-062 — a short row, a bad state, an unknown flag; a non-zip refuses the file."""

    def corrupt(content: bytes) -> bytes:
        lines = content.split(b"\n")
        kent = [
            index
            for index, line in enumerate(lines)
            if line.startswith(b'"10","001","') and b"----" in line
        ]
        lines[kent[0]] = b",".join(lines[kent[0]].split(b",")[:6])
        lines[kent[1]] = lines[kent[1]].replace(b'"10","001"', b'"1X","001"', 1)
        lines[kent[2]] = (
            lines[kent[2]]
            .replace(b'"G"', b'"Q"', 1)
            .replace(b'"H"', b'"Q"', 1)
            .replace(b'"J"', b'"Q"', 1)
        )
        return b"\n".join(lines)

    parsed = parse_file(_rezip("cbp23co", corrupt), item=COUNTY_2023)
    assert sorted(item.error_code for item in parsed.quarantined) == [
        "ragged_row",
        "unexpected_flag",
        "unreadable_geography",
    ]
    refused = parse_file(b"not a zip", item=COUNTY_2023)
    assert [item.error_code for item in refused.quarantined] == [
        "not_a_zip"
    ] and refused.observations == ()
