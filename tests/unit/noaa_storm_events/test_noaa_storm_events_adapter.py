"""Offline contracts for the NOAA Storm Events adapter.

Covers: ETL-127
"""

from __future__ import annotations

import gzip
import importlib
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.noaa_storm_events.client import (
    SourceFile,
    StormEventsPayloadError,
    check_details_file,
    check_zone_file,
    listed_details_files,
    listed_zone_files,
)
from data_ingestion_toolbox.noaa_storm_events.registry import (
    NATION_STATE_FIPS,
    STATE_FIPS_TO_USPS,
    era_for_year,
    name_key,
    type_key,
    zone_file_valid_from,
)
from data_ingestion_toolbox.noaa_storm_events.silver_noaa_storm_events.parse import (
    ParsedDetails,
    ParsedZones,
    parse_damage,
    parse_details_file,
    parse_zone_file,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "noaa_storm_events"
DETAILS_2024 = "StormEvents_details-ftp_v1.0_d2024_c20260728.csv.gz"


def _details(name: str) -> bytes:
    return gzip.decompress((FIXTURES / name).read_bytes())


def test_configuration_imports_with_no_io_and_no_credential() -> None:
    """Covers: ETL-127 — importing the configuration reads nothing."""
    sys.modules.pop("data_ingestion_toolbox.noaa_storm_events.config", None)
    module = importlib.import_module("data_ingestion_toolbox.noaa_storm_events.config")
    config = module.StormEventsConfig()
    assert not any("key" in name or "token" in name for name in config.model_dump())


def test_the_listing_names_the_newest_stamp_of_every_year() -> None:
    """Covers: ETL-127 — newest `c` stamp per year; fatalities and other files ignored."""
    listing = (
        b'<a href="StormEvents_details-ftp_v1.0_d2024_c20260628.csv.gz">x</a>'
        b'<a href="StormEvents_details-ftp_v1.0_d2024_c20260728.csv.gz">x</a>'
        b'<a href="StormEvents_details-ftp_v1.0_d1950_c20260323.csv.gz">x</a>'
        b'<a href="StormEvents_fatalities-ftp_v1.0_d2024_c20260728.csv.gz">x</a>'
        b'<a href="StormEvents_details-ftp_v2.0_d2024_c20260801.csv.gz">x</a>'
        b'<a href="?C=M;O=A">sort</a>'
    )
    files = listed_details_files(listing)
    assert [(file.data_year, file.stamp) for file in files] == [
        (1950, "20260323"),
        (2024, "20260728"),
    ]
    assert files[1].url.endswith("/csvfiles/" + DETAILS_2024)


def test_zone_files_are_read_from_their_names_and_an_unknown_month_is_refused() -> None:
    """Covers: ETL-127 — `bp16ap26` is valid from 2026-04-16; unknown letters never guessed."""
    page = (
        b'<a href="/source/gis/Shapefiles/County/bp16ap26.dbx">2026</a>'
        b'<a href="/source/gis/Shapefiles/County/bp18mr25.dbx">2025</a>'
        b'<a href="/source/gis/Shapefiles/County/bp01zz25.dbx">?</a>'
        b'<a href="/source/gis/Shapefiles/County/c_16ap26.zip">shapefile</a>'
    )
    files = listed_zone_files(page)
    assert [(file.name, file.stamp) for file in files] == [
        ("bp18mr25.dbx", "20250318"),
        ("bp16ap26.dbx", "20260416"),
    ]
    assert zone_file_valid_from("bp16ap26.dbx") == date(2026, 4, 16)
    with pytest.raises(ValueError):
        zone_file_valid_from("bp01zz25.dbx")


def test_damage_text_is_read_as_published_and_a_blank_is_not_zero() -> None:
    """Covers: ETL-127 — K, M, B and bare dollars; blank not reported; other text unreadable."""
    assert parse_damage("10.00K").usd == Decimal("10000.00")
    assert parse_damage("1.5M").usd == Decimal("1500000.0")
    assert parse_damage("2B").usd == Decimal("2000000000")
    assert parse_damage("250").usd == Decimal("250")
    assert parse_damage("0.00K").usd == Decimal("0.00")
    blank = parse_damage("")
    assert (blank.usd, blank.status, blank.published) == (None, "not_reported", None)
    odd = parse_damage("K")
    assert (odd.usd, odd.status, odd.published) == (None, "unreadable", "K")
    assert parse_damage("about 5K").status == "unreadable"


def test_zone_names_match_without_case_punctuation_or_a_trailing_county() -> None:
    """Covers: ETL-127 — the only loosening of the zone-name match."""
    assert name_key("Northern Marinette County") == name_key("NORTHERN MARINETTE")
    assert name_key("St. Croix") == name_key("ST. CROIX") == "ST CROIX"
    assert name_key("Green Lake") == "GREEN LAKE"
    # A different word is a different zone.
    assert name_key("Kent Coastal") != name_key("Kent")
    assert type_key("Cold/Wind Chill") == "cold_wind_chill"
    assert type_key("Hurricane (Typhoon)") == "hurricane_typhoon"


def test_recording_eras_and_the_state_tables() -> None:
    """Covers: ETL-127 — eras by year; the nation is the 50 states and DC."""
    assert era_for_year(1950).event_types == ("Tornado",)
    assert era_for_year(1995).event_types == ("Tornado", "Thunderstorm Wind", "Hail")
    assert era_for_year(1996).event_types is None
    assert len(NATION_STATE_FIPS) == 51
    assert "72" in STATE_FIPS_TO_USPS and "72" not in NATION_STATE_FIPS


def test_the_reviewed_details_file_reads_every_row() -> None:
    """Covers: ETL-127 — 34 events; zone and county types; typed figures."""
    parsed = ParsedDetails()
    events = list(
        parse_details_file(_details(DETAILS_2024), file_year=2024, parsed=parsed)
    )
    assert (parsed.row_count, len(events), parsed.quarantined) == (34, 34, [])
    assert {event.cz_type for event in events} == {"C", "Z"}
    tornado = next(event for event in events if event.event_type == "Tornado")
    assert (tornado.state_fips, tornado.cz_fips, tornado.cz_name) == (
        "55",
        "025",
        "DANE",
    )
    assert tornado.event_type_key == "tornado"
    assert all(len(event.row_hash) == 32 for event in events)


def test_an_unreadable_or_repeated_row_is_quarantined_alone() -> None:
    """Covers: ETL-127 — a wrong year, an unknown place type and a repeated id."""
    text = _details(DETAILS_2024).decode()
    header, first, second, *rest = text.splitlines(keepends=True)
    bad_type = first.replace(",Z,", ",X,", 1)
    corpus = (header + bad_type + second + second + "".join(rest)).encode()
    parsed = ParsedDetails()
    events = list(parse_details_file(corpus, file_year=2024, parsed=parsed))
    assert [item.error_code for item in parsed.quarantined] == [
        "unknown_cz_type",
        "duplicate_event_id",
    ]
    assert len(events) == 33
    wrong_year = ParsedDetails()
    assert list(parse_details_file(corpus, file_year=2023, parsed=wrong_year)) == []
    # The year is read first, so every row of another year is refused for it.
    assert {item.error_code for item in wrong_year.quarantined} == {"year_mismatch"}


def test_a_file_with_another_header_is_refused_whole() -> None:
    """Covers: ETL-127 — layout drift refuses the file before any row reads."""
    parsed = ParsedDetails()
    assert list(parse_details_file(b"A,B\n1,2\n", file_year=2024, parsed=parsed)) == []
    assert [(item.line, item.error_code) for item in parsed.quarantined] == [
        (0, "unexpected_header")
    ]
    file = SourceFile(
        "details", DETAILS_2024, "https://x/" + DETAILS_2024, 2024, "20260728"
    )
    check_details_file((FIXTURES / DETAILS_2024).read_bytes(), file)
    with pytest.raises(StormEventsPayloadError):
        check_details_file(b"not gzip", file)
    with pytest.raises(StormEventsPayloadError):
        check_details_file(gzip.compress(b"A,B\n"), file)


def test_zone_county_rows_read_verbatim() -> None:
    """Covers: ETL-127 — eleven fields; a split zone polygon is one row."""
    raw = (FIXTURES / "bp16ap26.dbx").read_bytes()
    zone = SourceFile(
        "zone_county", "bp16ap26.dbx", "https://x/bp16ap26.dbx", None, "20260416"
    )
    check_zone_file(raw, zone)
    parsed = ParsedZones()
    rows = list(parse_zone_file(raw + raw.splitlines(keepends=True)[0], parsed=parsed))
    assert len(rows) == 7 and parsed.line_count == 8
    kent = next(row for row in rows if row.state == "DE" and row.zone == "002")
    assert (kent.zone_name, kent.county_fips5) == ("Kent", "10001")
    with pytest.raises(StormEventsPayloadError):
        check_zone_file(b"a,b,c\n", zone)
