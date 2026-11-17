"""CDC FluView ILINet adapter contracts: init, download bodies and CSV parsing.

Covers: ETL-136
"""

from __future__ import annotations

import io
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.cdc_ilinet.client import (
    CdcIlinetPayloadError,
    download_body,
    ilinet_csv,
    read_init,
)
from data_ingestion_toolbox.cdc_ilinet.registry import REGION_TYPES
from data_ingestion_toolbox.cdc_ilinet.silver_cdc_ilinet.load import region_placements
from data_ingestion_toolbox.cdc_ilinet.silver_cdc_ilinet.parse import (
    mmwr_week_start,
    parse_download,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "cdc_ilinet"
REGION_TYPE = {region_type.key: region_type for region_type in REGION_TYPES}


def _csv(name: str) -> bytes:
    return ilinet_csv((FIXTURES / name).read_bytes())


def _init():
    return read_init((FIXTURES / "fluview_init.json").read_bytes())


@pytest.mark.parametrize(
    ("year", "week", "start"),
    [
        (2019, 40, date(2019, 9, 29)),
        (2025, 1, date(2024, 12, 29)),
        (2024, 52, date(2024, 12, 22)),
        (2020, 53, date(2020, 12, 27)),
        (1997, 40, date(1997, 9, 28)),
    ],
)
def test_mmwr_weeks_start_on_cdc_sundays(year: int, week: int, start: date) -> None:
    """Covers: ETL-136 -- MMWR week 1 holds the year's first Thursday."""
    assert mmwr_week_start(year, week) == start


def test_a_year_without_week_53_is_refused() -> None:
    """Covers: ETL-136."""
    with pytest.raises(ValueError):
        mmwr_week_start(2024, 53)


def test_download_bodies_repeat_the_page_request() -> None:
    """Covers: ETL-136 -- states ask only for seasons with state data."""
    init = _init()
    national = download_body(REGION_TYPE["national"], init)
    assert national["RegionTypeId"] == 3
    assert national["SubRegionsDT"] == [{"ID": 0, "Name": ""}]
    assert [item["ID"] for item in national["SeasonsDT"]] == [49, 64, 65]
    states = download_body(REGION_TYPE["state"], init)
    assert states["RegionTypeId"] == 5
    assert [item["ID"] for item in states["SeasonsDT"]] == [64, 65]
    assert {item["ID"] for item in states["SubRegionsDT"]} == {
        sid for sid, _name, _fips in init.states
    }
    assert download_body(REGION_TYPE["hhs_region"], init)["RegionTypeId"] == 1
    assert national["DatasourceDT"] == [{"ID": 1, "Name": "ILINet"}]


def test_init_documents_without_seasons_are_refused() -> None:
    """Covers: ETL-136."""
    with pytest.raises(CdcIlinetPayloadError):
        read_init(b'{"states": []}')
    with pytest.raises(CdcIlinetPayloadError):
        read_init(b"<html>")


def test_a_download_that_is_not_the_registered_zip_is_refused() -> None:
    """Covers: ETL-136."""
    with pytest.raises(CdcIlinetPayloadError) as raised:
        ilinet_csv(b"<html>maintenance</html>")
    assert raised.value.code == "expected_zip"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("WHO_NREVSS.csv", "x")
    with pytest.raises(CdcIlinetPayloadError) as raised:
        ilinet_csv(buffer.getvalue())
    assert raised.value.code == "expected_ilinet_member"


def test_state_rows_keep_cdc_text_and_x_as_not_published() -> None:
    """Covers: ETL-136 -- Wisconsin 2025 week 1 as CDC published it."""
    parsed = parse_download(_csv("ilinet_state.zip"), region_type="state")
    assert (parsed.row_count, len(parsed.rows), parsed.quarantined) == (10, 10, ())
    assert parsed.text_encoding == "utf-8"
    row = next(r for r in parsed.rows if (r.region, r.mmwr_week) == ("Wisconsin", 1))
    assert row.week_start == date(2024, 12, 29)
    assert row.figures["unweighted_ili_pct"].value == Decimal("3.04037")
    assert row.figures["ili_visits_total"].value == 961
    assert row.figures["reporting_providers"].value == 65
    assert row.figures["patient_visits_total"].value == 31608
    weighted = row.figures["weighted_ili_pct"]
    assert (weighted.source, weighted.value, weighted.status) == (
        "X",
        None,
        "not_published",
    )


def test_national_rows_name_the_nation_and_hhs_blanks_are_missing() -> None:
    """Covers: ETL-136."""
    national = parse_download(_csv("ilinet_national.zip"), region_type="national")
    assert {row.region for row in national.rows} == {"National"}
    assert len(national.rows) == 3
    hhs = parse_download(_csv("ilinet_hhs_region.zip"), region_type="hhs_region")
    row = hhs.rows[0]
    assert row.figures["ili_visits_age_25_64"].status == "missing"
    assert row.figures["ili_visits_age_25_49"].status == "valid"


def test_rows_of_another_region_type_or_unreadable_cells_are_set_aside() -> None:
    """Covers: ETL-136."""
    raw = _csv("ilinet_state.zip").decode()
    lines = raw.splitlines()
    lines.append("HHS Regions,Region 1,2025,1,1,1,1,1,1,1,1,1,1,1,1")
    lines.append("States,Ohio,2025,x,1,1,1,1,1,1,1,1,1,1,1")
    lines.append("States,Iowa,2025,1,n/a,1,1,1,1,1,1,1,1,1,1")
    lines.append(lines[2])
    parsed = parse_download("\n".join(lines).encode(), region_type="state")
    assert [item.error_code for item in parsed.quarantined] == [
        "unexpected_region_type",
        "unreadable_week",
        "unreadable_value",
        "repeated_region_week",
    ]
    assert parsed.row_count == len(parsed.rows) + len(parsed.quarantined)


def test_a_changed_title_or_header_sets_the_file_aside() -> None:
    """Covers: ETL-136."""
    raw = _csv("ilinet_national.zip")
    titled = parse_download(
        b"OTHER TITLE\n" + raw.split(b"\n", 1)[1], region_type="national"
    )
    assert [item.error_code for item in titled.quarantined] == ["unregistered_layout"]
    headed = parse_download(
        raw.replace(b"ILITOTAL", b"ILI TOTAL"), region_type="national"
    )
    assert [item.error_code for item in headed.quarantined] == ["unregistered_layout"]
    assert headed.rows == ()


def test_cp1252_files_are_read_and_reported() -> None:
    """Covers: ETL-136."""
    raw = _csv("ilinet_state.zip").replace(
        b"Puerto Rico", "Puerto Rico’".encode("cp1252")
    )
    parsed = parse_download(raw, region_type="state")
    assert parsed.text_encoding == "cp1252"
    assert "Puerto Rico’" in {row.region for row in parsed.rows}


def test_regions_are_placed_by_cdc_state_fips() -> None:
    """Covers: ETL-136 -- separately reported cities and HHS regions are not served."""
    init = _init()
    placed = {
        (row[0], row[1]): row[2:]
        for row in region_placements(
            init.states,
            {
                ("national", "National"),
                ("hhs_region", "Region 5"),
                ("state", "Wisconsin"),
                ("state", "New York"),
                ("state", "New York City"),
                ("state", "Los Angeles"),
                ("state", "Atlantis"),
            },
        )
    }
    assert placed[("national", "National")][:3] == ("us:1", "nation", "resolved")
    assert placed[("state", "Wisconsin")] == ("state:55", "state", "resolved", None)
    assert placed[("state", "New York")][:3] == ("state:36", "state", "resolved")
    assert placed[("state", "New York City")][2] == "unsupported"
    assert placed[("state", "Los Angeles")][2] == "unsupported"
    assert placed[("hhs_region", "Region 5")][2] == "unsupported"
    assert placed[("state", "Atlantis")][2] == "unsupported"
