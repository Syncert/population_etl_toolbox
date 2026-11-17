"""Offline contracts for the NOAA nClimDiv county adapter.

Covers: ETL-126
"""

from __future__ import annotations

import importlib
import sys
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.noaa_climdiv.client import (
    ClimdivFetchError,
    ClimdivFile,
    ClimdivPayloadError,
    check_county_file,
    fetch_county_file,
    listed_files,
)
from data_ingestion_toolbox.noaa_climdiv.config import ClimdivConfig
from data_ingestion_toolbox.noaa_climdiv.registry import (
    ELEMENTS_BY_FILE,
    NCEI_STATE_TO_FIPS,
)
from data_ingestion_toolbox.noaa_climdiv.silver_noaa_climdiv.parse import (
    ParsedFile,
    parse_county_file,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "noaa_climdiv"


def _fixture(element: str) -> bytes:
    return (FIXTURES / f"climdiv-{element}cy-v1.0.0-20261105").read_bytes()


def _parse(raw: bytes, element: str = "pcpn") -> tuple[list, ParsedFile]:
    parsed = ParsedFile()
    values = list(
        parse_county_file(raw, ELEMENTS_BY_FILE[element], last_year=2026, parsed=parsed)
    )
    return values, parsed


def test_configuration_imports_with_no_io_and_no_credential() -> None:
    """Covers: ETL-126 — importing the configuration reads nothing."""
    sys.modules.pop("data_ingestion_toolbox.noaa_climdiv.config", None)
    module = importlib.import_module("data_ingestion_toolbox.noaa_climdiv.config")
    config = module.ClimdivConfig()
    assert not any("key" in name or "token" in name for name in config.model_dump())


def test_the_listing_names_the_newest_file_of_each_registered_element() -> None:
    """Covers: ETL-126 — newest stamp per element; other files are ignored."""
    listing = (
        b'<a href="climdiv-pcpncy-v1.0.0-20261005">x</a>'
        b'<a href="climdiv-pcpncy-v1.0.0-20261105">x</a>'
        b'<a href="climdiv-tmpccy-v1.0.0-20261105">x</a>'
        b'<a href="climdiv-pdsidv-v1.0.0-20261105">x</a>'
        b'<a href="climdiv-pcpnst-v1.0.0-20261105">x</a>'
        b'<a href="county-readme.txt">x</a>'
    )
    assert listed_files(listing) == [
        ClimdivFile("climdiv-pcpncy-v1.0.0-20261105", "pcpn", "1.0.0", "20261105"),
        ClimdivFile("climdiv-tmpccy-v1.0.0-20261105", "tmpc", "1.0.0", "20261105"),
    ]


def test_ncei_state_codes_are_not_fips_codes() -> None:
    """Covers: ETL-126 — NCEI's 01-50 table is crosswalked, never read as FIPS."""
    assert NCEI_STATE_TO_FIPS["07"] == "10"  # Delaware
    assert NCEI_STATE_TO_FIPS["47"] == "55"  # Wisconsin
    assert NCEI_STATE_TO_FIPS["50"] == "02"  # Alaska
    assert len(NCEI_STATE_TO_FIPS) == len(set(NCEI_STATE_TO_FIPS.values())) == 50


def test_a_body_that_is_not_the_elements_county_file_is_refused() -> None:
    """Covers: ETL-126 — another element's file or an HTML page fails capture."""
    pcpn = ClimdivFile("climdiv-pcpncy-v1.0.0-20261105", "pcpn", "1.0.0", "20261105")
    check_county_file(_fixture("pcpn"), pcpn)
    with pytest.raises(ClimdivPayloadError, match="unexpected_layout"):
        check_county_file(_fixture("tmpc"), pcpn)
    with pytest.raises(ClimdivPayloadError, match="unexpected_layout"):
        check_county_file(b"<html>moved</html>", pcpn)
    with pytest.raises(ClimdivPayloadError, match="not_ascii"):
        check_county_file("07001011895 é".encode(), pcpn)


def test_a_404_fails_without_retry() -> None:
    """Covers: ETL-126 — a missing file is not retried."""
    calls: list[str] = []

    class _Client:
        def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
            calls.append(url)
            return httpx.Response(404, request=httpx.Request("GET", url))

    client = _Client()
    file = ClimdivFile("climdiv-pcpncy-v1.0.0-20261105", "pcpn", "1.0.0", "20261105")
    with pytest.raises(ClimdivFetchError, match="non_retryable_http"):
        fetch_county_file(
            file,
            config=ClimdivConfig(max_attempts=3),
            client=client,
            sleep=lambda _: None,
        )
    assert len(calls) == 1


@pytest.mark.parametrize("element", ["pcpn", "tmpc", "tmax", "tmin", "hddc", "cddc"])
def test_every_fixture_reads_with_the_missing_months_kept_apart(element: str) -> None:
    """Covers: ETL-126 — NCEI's missing markers are `missing` with no number."""
    values, parsed = _parse(_fixture(element), element)
    assert (parsed.line_count, parsed.quarantined) == (10, [])
    assert len(values) == 120
    missing = [value for value in values if value.value_status == "missing"]
    # November and December 2026 for both counties.
    assert {(value.year, value.month) for value in missing} == {(2026, 11), (2026, 12)}
    assert len(missing) == 4
    assert all(value.value is None for value in missing)
    assert all(value.missing_reason == "not_yet_published" for value in missing)


def test_values_keep_ncei_text_beside_the_number() -> None:
    """Covers: ETL-126 — the number is NCEI's text, typed; the text is kept."""
    values, _parsed = _parse(_fixture("pcpn"))
    january = values[0]
    assert (january.ncei_state_code, january.state_fips, january.county_fips) == (
        "07",
        "10",
        "001",
    )
    assert (january.year, january.month, january.value, january.value_source) == (
        1895,
        1,
        Decimal("4.51"),
        "4.51",
    )


def test_an_unreadable_line_is_quarantined_alone() -> None:
    """Covers: ETL-126 — each refused line has its own reason; the rest still read."""
    lines = _fixture("pcpn").decode().splitlines()
    good = lines[0]
    broken = [
        good[:11] + "  -1.00" + good[18:],  # negative precipitation
        "99" + lines[1][2:],  # not an NCEI state code
        good[:5] + "02" + good[7:],  # another element
        good[:7] + "1894" + good[11:],  # before the series starts
        good[:11] + "   abc " + good[18:],  # not a number
        good[:11] + " 100.00" + good[18:],  # above the readme's range
        good[:40],  # short
        lines[2],
        lines[2],  # repeated
    ]
    values, parsed = _parse(("\n".join(broken) + "\n").encode())
    assert [(item.line, item.error_code) for item in parsed.quarantined] == [
        (1, "negative_value"),
        (2, "unknown_state_code"),
        (3, "wrong_element"),
        (4, "unreadable_year"),
        (5, "unreadable_value"),
        (6, "out_of_range"),
        (7, "unexpected_layout"),
        (9, "duplicate_line"),
    ]
    assert {value.line for value in values} == {8}


def test_temperatures_may_be_negative_but_stay_in_the_readme_range() -> None:
    """Covers: ETL-126 — a below-zero temperature is a value; -60 F is not."""
    good = _fixture("tmin").decode().splitlines()[0]
    cold = good[:11] + " -12.30" + good[18:]
    colder = good[:11] + " -60.00" + good[18:]
    values, parsed = _parse((cold + "\n" + colder + "\n").encode(), "tmin")
    assert values[0].value == Decimal("-12.30")
    assert [item.error_code for item in parsed.quarantined] == ["out_of_range"]
