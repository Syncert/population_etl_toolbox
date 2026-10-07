"""Offline contracts for the NCES Common Core of Data adapter.

Covers: ETL-070
"""

from __future__ import annotations

import csv
import importlib
import io
import sys
import zipfile
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.nces_ccd.client import (
    CcdFetchError,
    CcdPayloadError,
    check_file,
    fetch_file,
)
from data_ingestion_toolbox.nces_ccd.config import CcdConfig
from data_ingestion_toolbox.nces_ccd.registry import (
    OPERATING_STATUSES,
    get_file,
    registered_files,
    version_rank,
)
from data_ingestion_toolbox.nces_ccd.silver_nces_ccd.parse import parse_file

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "nces_ccd"
GEOCODE = get_file("geocode:2024-2025")
DIRECTORY = get_file("directory:2024-2025")
STAFF = get_file("staff:2024-2025")
LUNCH = get_file("lunch:2024-2025")
GOLD_SQL = (
    Path(__file__).resolve().parents[3]
    / "src/data_ingestion_toolbox/nces_ccd/gold_nces_ccd/DDL/gold_nces_ccd.sql"
)


def _fixture(item) -> bytes:  # noqa: ANN001
    return (FIXTURES / f"{item.stem}.zip").read_bytes()


def _member(item) -> str:  # noqa: ANN001
    return (
        zipfile.ZipFile(io.BytesIO(_fixture(item))).read(item.member).decode("latin-1")
    )


def _zip(item, text: str) -> bytes:  # noqa: ANN001
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(item.member, text.encode("latin-1"))
    return buffer.getvalue()


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-070 — static public files; no key, token or email."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.nces_ccd"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.nces_ccd.config")
    assert not {
        field
        for field in module.CcdConfig.model_fields
        if "key" in field or "token" in field or "email" in field
    }
    years = {item.school_year for item in registered_files()}
    assert years == {"2023-2024", "2024-2025"}


def test_release_versions_order_and_operating_statuses_match_the_views() -> None:
    """Covers: ETL-070 — 0a < 1a < 1b < 2a; the gold views count the registered statuses."""
    assert (
        version_rank("0a")
        < version_rank("1a")
        < version_rank("1b")
        < version_rank("2a")
    )
    assert version_rank("edge") == 0
    sql = GOLD_SQL.read_text(encoding="utf-8")
    for status in OPERATING_STATUSES:
        assert f"'{status}'" in sql


def test_the_reviewed_files_parse_with_leading_zeros_kept() -> None:
    """Covers: ETL-070 — every fixture row reads; identifiers stay twelve- and seven-character strings."""
    geocode = parse_file(_fixture(GEOCODE), item=GEOCODE)
    assert (geocode.row_count, len(geocode.locations), geocode.quarantined) == (
        555,
        555,
        (),
    )
    bie = next(row for row in geocode.locations if row.ncessch == "590002500172")
    assert (bie.operating_state_fips, bie.state_fips, bie.county_fips) == (
        "59",
        "38",
        "38079",
    )
    directory = parse_file(_fixture(DIRECTORY), item=DIRECTORY)
    assert len(directory.directory) == 555 and directory.quarantined == ()
    assert all(
        len(row.leaid) == 7 and row.ncessch.startswith(row.leaid)
        for row in directory.directory
    )
    lunch = parse_file(_fixture(LUNCH), item=LUNCH)
    assert (
        lunch.row_count == 2710
        and len(lunch.counts) == 2168
        and lunch.quarantined == ()
    )
    assert {row.measure for row in lunch.counts} == {
        "frpl_eligible",
        "free_lunch_eligible",
        "reduced_price_lunch_eligible",
        "direct_certification",
    }


def test_withheld_counts_carry_no_number() -> None:
    """Covers: ETL-070 — Not reported, Missing and Suppressed are statuses, never zero."""
    staff = parse_file(_fixture(STAFF), item=STAFF)
    withheld = [row for row in staff.counts if row.value_status != "valid"]
    assert withheld and all(row.value is None for row in withheld)
    assert {
        (row.dms_flag, row.value_status, row.missing_reason) for row in withheld
    } == {
        ("Missing", "missing", "missing"),
        ("Not reported", "missing", "not_reported"),
        ("Suppressed", "suppressed", "suppressed"),
    }
    fte = next(
        row
        for row in staff.counts
        if row.value is not None and row.value != row.value.to_integral_value()
    )
    assert isinstance(fte.value, Decimal)


def test_bad_rows_are_quarantined_alone() -> None:
    """Covers: ETL-070 — a bad identifier, year, flag, blank Reported value or repeat is refused."""
    reader = csv.DictReader(io.StringIO(_member(STAFF)))
    columns = list(reader.fieldnames or [])
    reported = [row for row in reader if row["DMS_FLAG"] == "Reported"]

    def with_cell(row: dict[str, str], column: str, value: str) -> dict[str, str]:
        return {**row, column: value}

    damaged = [
        with_cell(reported[0], "NCESSCH", "1000123"),
        with_cell(reported[1], "SCHOOL_YEAR", "2023-2024"),
        with_cell(reported[2], "DMS_FLAG", "Imputed"),
        with_cell(reported[3], "TEACHERS", ""),
        with_cell(reported[4], "TEACHERS", "-3"),
        reported[5],
        reported[5],
        reported[6],
    ]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(damaged)
    parsed = parse_file(_zip(STAFF, out.getvalue()), item=STAFF)
    assert sorted(q.error_code for q in parsed.quarantined) == [
        "duplicate_row",
        "reported_without_value",
        "unknown_flag",
        "unreadable_identifier",
        "unreadable_value",
        "wrong_school_year",
    ]
    assert [row.ncessch for row in parsed.counts] == [
        reported[5]["NCESSCH"],
        reported[6]["NCESSCH"],
    ]


def test_a_geocode_county_outside_its_state_is_refused() -> None:
    """Covers: ETL-070 — CNTY must be a five-digit code inside STFIP; names are never read."""
    first = _member(GEOCODE).split("\r\n")[0].split("|")
    first[9] = "24003"
    parsed = parse_file(_zip(GEOCODE, "|".join(first) + "\r\n"), item=GEOCODE)
    assert [q.error_code for q in parsed.quarantined] == ["unreadable_county"]


def test_a_payload_that_is_not_the_registered_file_is_refused() -> None:
    """Covers: ETL-070 — HTML, a foreign header or an empty member never reach capture."""
    with pytest.raises(CcdPayloadError, match="member_missing"):
        check_file(b"<html>moved</html>", STAFF)
    with pytest.raises(CcdPayloadError, match="unexpected_header"):
        check_file(_zip(STAFF, "A,B\n1,2\n"), STAFF)
    with pytest.raises(CcdPayloadError, match="empty_member"):
        check_file(_zip(GEOCODE, ""), GEOCODE)
    with pytest.raises(CcdPayloadError, match="unexpected_header"):
        check_file(_zip(GEOCODE, "a|b\r\n"), GEOCODE)
    for item in (GEOCODE, DIRECTORY, STAFF, LUNCH):
        check_file(_fixture(item), item)


class _Scripted:
    def __init__(self, *responses: httpx.Response) -> None:
        self.responses = list(responses)

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        return self.responses.pop(0)


def test_server_errors_retry_and_client_errors_do_not() -> None:
    """Covers: ETL-070 — 503 retries then succeeds; 404 fails at once with the path only."""
    request = httpx.Request("GET", "https://example.test")
    config = CcdConfig(min_spacing_seconds=0, max_attempts=2)
    response = fetch_file(
        STAFF,
        config=config,
        client=_Scripted(
            httpx.Response(503, request=request),
            httpx.Response(200, content=_fixture(STAFF), request=request),
        ),
        sleep=lambda _seconds: None,
    )
    assert response.http_status == 200
    with pytest.raises(CcdFetchError) as raised:
        fetch_file(
            STAFF,
            config=config,
            client=_Scripted(httpx.Response(404, request=request)),
            sleep=lambda _seconds: None,
        )
    assert (raised.value.code, raised.value.status) == ("non_retryable_http", 404)
    assert "https://" not in str(raised.value)
