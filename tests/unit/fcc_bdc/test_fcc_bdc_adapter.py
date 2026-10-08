"""Offline contracts for the FCC broadband availability adapter.

Covers: ETL-071
"""

from __future__ import annotations

import csv
import importlib
import io
import sys
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.fcc_bdc.client import (
    BdcClient,
    BdcFetchError,
    BdcPayloadError,
    listing_files,
)
from data_ingestion_toolbox.fcc_bdc.config import BdcConfig
from data_ingestion_toolbox.fcc_bdc.registry import (
    AS_OF_DATES,
    CENSUS_PLACE,
    OTHER_GEOGRAPHIES,
    SummaryFile,
)
from data_ingestion_toolbox.fcc_bdc.silver_fcc_bdc.parse import parse_summary

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "fcc_bdc"
TOKEN = "fixture-token-not-a-real-secret"
NATIONAL = SummaryFile(
    date(2025, 12, 31),
    OTHER_GEOGRAPHIES,
    None,
    1820956,
    "bdc_us_fixed_broadband_summary_by_geography_D25_29sep2026",
)
DELAWARE_PLACES = SummaryFile(
    date(2025, 12, 31),
    CENSUS_PLACE,
    "10",
    1820975,
    "bdc_10_fixed_broadband_summary_by_geography_place_D25_29sep2026",
)


def _member(file_id: int) -> tuple[str, str]:
    archive = zipfile.ZipFile(io.BytesIO((FIXTURES / f"{file_id}.zip").read_bytes()))
    member = archive.infolist()[0]
    return member.filename, archive.read(member).decode("utf-8")


def _zip(name: str, rows: list[list[str]]) -> bytes:
    out = io.StringIO()
    csv.writer(out, lineterminator="\n").writerows(rows)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, out.getvalue())
    return buffer.getvalue()


def test_configuration_imports_without_io_and_reads_credentials_only_on_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-071 — importing reads nothing; ``from_environment`` reads both credentials."""
    monkeypatch.setenv("FCC_BDC_USERNAME", "someone@example.invalid")
    monkeypatch.setenv("FCC_BDC_API_TOKEN", TOKEN)
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.fcc_bdc"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.fcc_bdc.config")
    assert module.BdcConfig().fcc_bdc_api_token == ""
    loaded = module.BdcConfig.from_environment()
    assert (loaded.fcc_bdc_username, loaded.fcc_bdc_api_token) == (
        "someone@example.invalid",
        TOKEN,
    )
    assert [as_of.month for as_of in AS_OF_DATES] == [12, 12]


def test_the_listing_names_the_registered_files_and_revisions() -> None:
    """Covers: ETL-071 — the national file and each state's place file, fixed broadband only."""
    files = listing_files(
        (FIXTURES / "listAvailabilityData_2025-12-31.json").read_bytes(),
        date(2025, 12, 31),
        frozenset({OTHER_GEOGRAPHIES, CENSUS_PLACE}),
    )
    assert [
        (item.kind, item.state_fips, item.file_id, item.revision) for item in files
    ] == [
        ("other_geographies", None, 1820956, "29sep2026"),
        ("place", "10", 1820975, "29sep2026"),
        ("place", "11", 1820967, "29sep2026"),
    ]
    with pytest.raises(BdcPayloadError, match="unexpected_answer"):
        listing_files(b"<html>", date(2025, 12, 31), frozenset({OTHER_GEOGRAPHIES}))


def test_the_national_file_keeps_total_residential_rows_by_code() -> None:
    """Covers: ETL-071 — nation, states and counties kept; CBSA, urban and business rows counted, not kept."""
    _name, text = _member(1820956)
    parsed = parse_summary((FIXTURES / "1820956.zip").read_bytes(), item=NATIONAL)
    assert (parsed.row_count, len(parsed.rows), parsed.quarantined) == (930, 21, ())
    assert {row.geography_type for row in parsed.rows} == {"nation", "state", "county"}
    kent = next(
        row
        for row in parsed.rows
        if row.geo_id == "state:10|county:001" and row.technology == "Any Technology"
    )
    assert (kent.total_units, kent.shares[5], kent.value_status) == (
        85249,
        Decimal("0.636547056"),
        "valid",
    )
    assert "CBSA (MSA)" in text


def test_a_place_is_padded_and_must_be_in_its_files_state() -> None:
    """Covers: ETL-071 — a seven-digit place id is the state plus place code; another state's place is refused."""
    parsed = parse_summary(
        (FIXTURES / "1820975.zip").read_bytes(), item=DELAWARE_PLACES
    )
    dover = next(
        row
        for row in parsed.rows
        if row.geography_id == "1021200" and row.technology == "Any Technology"
    )
    assert dover.geo_id == "state:10|place:21200"
    name, text = _member(1820975)
    header, *rows = list(csv.reader(io.StringIO(text)))
    first = next(
        row
        for row in rows
        if row[0] == "Total" and row[6] == "R" and row[7] == "Any Technology"
    )
    elsewhere = [*first]
    elsewhere[2] = "1150000"
    short = [*first]
    short[2] = "650000"
    parsed = parse_summary(_zip(name, [header, elsewhere, short]), item=DELAWARE_PLACES)
    assert [q.error_code for q in parsed.quarantined] == [
        "state_mismatch",
        "state_mismatch",
    ]


def test_shares_keep_zero_refuse_the_impossible_and_have_none_without_units() -> None:
    """Covers: ETL-071 — 0 is a reported zero; >1 or a word is refused; no units means no share."""
    name, text = _member(1820956)
    header, *rows = list(csv.reader(io.StringIO(text)))
    kept = [
        row
        for row in rows
        if row[0] == "Total"
        and row[6] == "R"
        and row[1] in ("National", "State", "County")
        and row[7] == "Any Technology"
    ]
    zero, over, word, empty, no_units, repeat = (list(row) for row in kept[:6])
    zero[13] = "0.000000000"
    over[13] = "1.2"
    word[13] = "n/a"
    empty[13] = ""
    no_units[5] = "0"
    parsed = parse_summary(
        _zip(name, [header, zero, over, word, empty, no_units, repeat, repeat]),
        item=NATIONAL,
    )
    assert sorted(q.error_code for q in parsed.quarantined) == [
        "duplicate_row",
        "share_out_of_range",
        "unreadable_value",
    ]
    by_geo = {row.geography_id: row for row in parsed.rows}
    assert (
        by_geo[zero[2]].shares[5] == Decimal("0")
        and by_geo[zero[2]].value_status == "valid"
    )
    assert (by_geo[empty[2]].value_status, by_geo[empty[2]].missing_reason) == (
        "missing",
        "blank",
    )
    assert by_geo[no_units[2]].shares == (None,) * 6
    assert by_geo[no_units[2]].missing_reason == "no_units"
    assert (
        parse_summary(b"<html>", item=NATIONAL).quarantined[0].error_code == "not_a_zip"
    )


class _Scripted:
    def __init__(self, *responses: httpx.Response) -> None:
        self.responses = list(responses)
        self.headers: list[dict[str, str]] = []

    def get(
        self, url: str, *, params: dict[str, str], headers: dict[str, str]
    ) -> httpx.Response:
        self.headers.append(headers)
        return self.responses.pop(0)


def _config(**overrides: object) -> BdcConfig:
    return BdcConfig(
        fcc_bdc_username="someone@example.invalid",
        fcc_bdc_api_token=TOKEN,
        min_spacing_seconds=0,
        max_attempts=2,
        **overrides,
    )


def test_the_client_sends_credentials_as_headers_and_never_names_them() -> None:
    """Covers: ETL-071 — username and hash_value headers; refused, missing, 429 and 404 told apart."""
    request = httpx.Request("GET", "https://example.test")
    with pytest.raises(BdcFetchError, match="missing_credentials"):
        BdcClient(BdcConfig())
    scripted = _Scripted(
        httpx.Response(429, request=request),
        httpx.Response(200, content=b'{"data":[]}', request=request),
    )
    client = BdcClient(_config(), client=scripted, sleep=lambda _seconds: None)
    assert client.get("listAsOfDates").http_status == 200
    assert {
        (headers["username"], headers["hash_value"]) for headers in scripted.headers
    } == {("someone@example.invalid", TOKEN)}
    for status, code in ((401, "credentials_refused"), (404, "non_retryable_http")):
        refused = BdcClient(
            _config(),
            client=_Scripted(httpx.Response(status, request=request)),
            sleep=lambda _seconds: None,
        )
        with pytest.raises(BdcFetchError) as raised:
            refused.get("listAsOfDates")
        assert (raised.value.code, raised.value.status) == (code, status)
        assert TOKEN not in str(raised.value)


def test_calls_are_spaced_to_the_apis_limit() -> None:
    """Covers: ETL-071 — 10 calls a minute: consecutive calls wait out the spacing."""
    request = httpx.Request("GET", "https://example.test")
    slept: list[float] = []
    clock = iter([0.0, 1.0, 6.5])
    client = BdcClient(
        BdcConfig(
            fcc_bdc_username="someone@example.invalid",
            fcc_bdc_api_token=TOKEN,
            max_attempts=1,
        ),
        client=_Scripted(
            httpx.Response(200, content=b"{}", request=request),
            httpx.Response(200, content=b"{}", request=request),
        ),
        sleep=slept.append,
        clock=lambda: next(clock),
    )
    client.get("listAsOfDates")
    client.get("listAsOfDates")
    assert slept == [pytest.approx(5.5)]
