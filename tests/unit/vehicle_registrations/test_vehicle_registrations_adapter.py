"""Vehicle registrations adapter contracts: index links, Report 28 and MV-1 parsing.

Covers: ETL-138
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.vehicle_registrations.client import (
    VehicleRegistrationsPayloadError,
    _check_pdf,
    _check_xlsx,
    report28_links,
)
from data_ingestion_toolbox.vehicle_registrations.registry import (
    WISDOT_COUNTIES,
    county_geo_id,
)
from data_ingestion_toolbox.vehicle_registrations.silver_vehicle_registrations.parse_mv1 import (
    parse_mv1,
)
from data_ingestion_toolbox.vehicle_registrations.silver_vehicle_registrations.parse_rpt28 import (
    parse_rpt28,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "vehicle_registrations"


@pytest.fixture(scope="module")
def report() -> object:
    return parse_rpt28((FIXTURES / "rpt-28-cal-25-dane-and-state.pdf").read_bytes())


def _values(line) -> dict[str, int | None]:  # noqa: ANN001
    return {column: cell.value for column, cell in line.cells.items()}


def test_index_pages_link_each_calendar_year_report() -> None:
    """Covers: ETL-138 — the fiscal-year report and other reports are not read."""
    page = (FIXTURES / "veh-info.html").read_bytes()
    assert report28_links(page) == {2025: "/Documents/dmv/shared/rpt-28-cal-25.pdf"}
    assert report28_links((FIXTURES / "veh-info-prev.html").read_bytes()) == {}


def test_a_body_that_is_not_the_registered_kind_is_refused() -> None:
    """Covers: ETL-138."""
    with pytest.raises(VehicleRegistrationsPayloadError) as raised:
        _check_pdf(b"<html>maintenance</html>", "/x.pdf")
    assert raised.value.code == "expected_pdf"
    with pytest.raises(VehicleRegistrationsPayloadError):
        _check_xlsx(b"%PDF-1.4", "/x.xlsx")


def test_wisdot_county_codes_place_each_county_by_its_fips_code() -> None:
    """Covers: ETL-138 — WisDOT's codes are alphabetical with Menominee 73."""
    assert len(WISDOT_COUNTIES) == 72
    assert WISDOT_COUNTIES["13"] == ("DANE", "55025")
    assert WISDOT_COUNTIES["73"] == ("MENOMINEE", "55078")
    assert WISDOT_COUNTIES["71"] == ("WOOD", "55141")
    assert county_geo_id("55025") == "state:55|county:025"


def test_dane_county_sum_is_read_as_printed(report) -> None:  # noqa: ANN001
    """Covers: ETL-138 — Dane's Table 1 Sum and Table 2 line, every column."""
    assert report.data_year == 2025
    expected = {
        "AUCY": 56,
        "AUTO": 152_184,
        "CYCL": 19_503,
        "TRLR": 39_988,
        "TRUK": 318_345,
        "SUM": 530_076,
    }
    (table_one,) = [
        line
        for line in report.lines
        if line.table_part == "cvt"
        and line.county_code == "13"
        and line.row_kind == "sum"
    ]
    assert _values(table_one) == expected
    (table_two,) = [
        line
        for line in report.lines
        if line.table_part == "county" and line.county_code == "13"
    ]
    assert table_two.row_kind == "county" and _values(table_two) == expected
    assert sum(v for k, v in expected.items() if k != "SUM") == expected["SUM"]


def test_dane_cities_keep_their_cvt_code_and_kind_and_blank_cells(report) -> None:  # noqa: ANN001
    """Covers: ETL-138 — the City and the Town of Madison are two lines."""
    madison = {
        (line.cvt_code, line.cvt_kind): line
        for line in report.lines
        if line.county_code == "13" and line.cvt_name == "MADISON"
    }
    city = madison[("73", "C")]
    assert _values(city) == {
        "AUCY": 10,
        "AUTO": 70_108,
        "CYCL": 6_613,
        "TRLR": 9_920,
        "TRUK": 122_249,
        "SUM": 208_900,
    }
    town = madison[("16", "T")]
    assert town.cells["AUCY"].status == "blank" and town.cells["AUCY"].value is None
    assert town.cells["AUCY"].source == ""
    assert town.cells["SUM"].value == 177


def test_dane_lines_add_to_its_sum_and_counties_to_the_state(report) -> None:  # noqa: ANN001
    """Covers: ETL-138 — the three printed totals agree, Dodge aside.

    The fixture stops on the page Dane ends on, which also prints the first
    of Dodge County's lines and the Sum WisDOT prints at the foot of each
    of a county's pages, so only Dodge's lines fall short of its Sum here.
    """
    dane = [
        line
        for line in report.lines
        if line.table_part == "cvt"
        and line.county_code == "13"
        and line.row_kind != "sum"
    ]
    assert sum(line.cells["SUM"].value for line in dane) == 530_076
    (state,) = [
        line
        for line in report.lines
        if line.table_part == "county" and line.row_kind == "sum"
    ]
    assert state.county_code == "" and state.cells["SUM"].value == 6_384_450
    assert state.cells["AUTO"].value == 1_517_380
    counties = [
        line
        for line in report.lines
        if line.table_part == "county" and line.row_kind != "sum"
    ]
    assert len(counties) == 73  # 72 counties and the UNKNOWN line
    assert sum(line.cells["SUM"].value for line in counties) == 6_384_450
    assert {item[0:2] for item in report.disagreements} == {
        ("cvt_lines_to_county_sum", "14")
    }
    assert report.quarantined == ()
    assert report.row_count == len(report.lines) + report.repeated_count


def test_menominee_prints_no_autocycle_column() -> None:
    """Covers: ETL-138 — a column a county's table does not print has no cell."""
    parsed = parse_rpt28((FIXTURES / "rpt-28-cal-25-dane-and-state.pdf").read_bytes())
    (menominee,) = [
        line
        for line in parsed.lines
        if line.table_part == "county" and line.county_code == "73"
    ]
    assert menominee.cells["AUCY"].status == "blank"
    assert menominee.cells["SUM"].value == 2_086


def _sum_line(parsed, county_code: str):  # noqa: ANN001, ANN202
    (line,) = [
        line
        for line in parsed.lines
        if line.table_part == "cvt"
        and line.county_code == county_code
        and line.row_kind == "sum"
    ]
    return line


@pytest.mark.parametrize(
    ("name", "year", "expected"),
    [
        # Text WisDOT painted over with white boxes (2021) or drew invisible
        # (2024) would read as headings "WOOD" and the like and lines that no
        # pattern reads; only the text a reader sees is read.
        (
            "rpt-28-cal-21-painted-over.pdf",
            2021,
            {
                "AUCY": 9,
                "AUTO": 18_963,
                "CYCL": 4_864,
                "TRLR": 10_445,
                "TRUK": 46_677,
                "SUM": 80_958,
            },
        ),
        (
            "rpt-28-cal-24-hidden-text.pdf",
            2024,
            {
                "AUCY": 11,
                "AUTO": 17_041,
                "CYCL": 4_331,
                "TRLR": 11_471,
                "TRUK": 50_787,
                "SUM": 83_641,
            },
        ),
    ],
)
def test_text_hidden_under_the_printed_page_is_not_read(
    name: str, year: int, expected: dict[str, int]
) -> None:
    """Covers: ETL-138 — Chippewa County's Sum, with nothing set aside."""
    parsed = parse_rpt28((FIXTURES / name).read_bytes())
    assert parsed.data_year == year
    assert parsed.quarantined == ()
    assert _values(_sum_line(parsed, "09")) == expected
    assert {line.county_name for line in parsed.lines} == {
        "",
        "BURNETT",
        "CALUMET",
        "CHIPPEWA",
        "CLARK",
    }


def test_2020_tables_split_over_two_pages_are_joined() -> None:
    """Covers: ETL-138 — TRUK and the Sum on the page after the other types."""
    parsed = parse_rpt28((FIXTURES / "rpt-28-cal-20-split-tables.pdf").read_bytes())
    assert parsed.data_year == 2020
    assert parsed.quarantined == ()
    assert _values(_sum_line(parsed, "36")) == {
        "AUCY": 12,
        "AUTO": 28_349,
        "CYCL": 6_815,
        "TRLR": 8_986,
        "TRUK": 52_882,
        "SUM": 97_044,
    }
    # Adams's heading names the county without its code.
    assert _values(_sum_line(parsed, "01"))["SUM"] == 31_657
    # Marathon's Sum printed at the foot of Manitowoc's pages repeats the Sum
    # under its own table, and is counted once.
    assert _values(_sum_line(parsed, "37"))["SUM"] == 157_891
    assert parsed.repeated_count == 2
    assert parsed.row_count == len(parsed.lines) + parsed.repeated_count


def test_2020_out_of_state_lines_and_no_county_heading_are_kept() -> None:
    """Covers: ETL-138 — "County Name: NA" is the vehicles in no county."""
    parsed = parse_rpt28((FIXTURES / "rpt-28-cal-20-split-tables.pdf").read_bytes())
    no_county = [
        line
        for line in parsed.lines
        if line.table_part == "cvt" and line.county_code == ""
    ]
    assert {line.row_kind for line in no_county} == {"out_of_state", "sum"}
    (adams_out,) = [
        line
        for line in parsed.lines
        if line.county_code == "01" and line.row_kind == "out_of_state"
    ]
    assert _values(adams_out)["SUM"] == 37


def test_a_payload_that_is_not_report_28_is_set_aside() -> None:
    """Covers: ETL-138."""
    parsed = parse_rpt28(b"%PDF-1.4 not really")
    assert [item.error_code for item in parsed.quarantined] == ["not_a_pdf"]
    assert parsed.lines == ()


def test_mv1_wisconsin_row_matches_the_published_total() -> None:
    """Covers: ETL-138 — 2024, every column as FHWA stores it."""
    parsed = parse_mv1((FIXTURES / "mv1-2024.xlsx").read_bytes())
    assert parsed.quarantined == () and parsed.row_count == 52
    by_area = {row.area_name: row for row in parsed.rows}
    assert len(by_area) == 52 and by_area["Dist. of Col."].fips == "11"
    wisconsin = by_area["Wisconsin"]
    assert (wisconsin.fips, wisconsin.source_label, wisconsin.footnote_marks) == (
        "55",
        "Wisconsin (2)",
        ("2",),
    )
    figures = {(f.vehicle_class, f.ownership): f.value for f in wisconsin.figures}
    assert figures[("automobiles", "total")] == 1_626_951
    assert figures[("trucks", "total")] == 3_918_981
    assert figures[("motorcycles", "total")] == 279_308
    assert figures[("all_motor_vehicles", "total")] == 5_836_776
    assert figures[("all_motor_vehicles", "total")] == sum(
        figures[(group, "total")]
        for group in ("automobiles", "buses", "trucks", "motorcycles")
    )
    total = by_area["Total"]
    assert (
        total.fips == ""
        and {(f.vehicle_class, f.ownership): f.value for f in total.figures}[
            ("all_motor_vehicles", "total")
        ]
        == 297_525_836
    )
    marks = dict(parsed.footnotes)
    assert marks["2"].startswith("State did not report one or more publicly-owned")


def test_mv1_estimates_keep_the_decimals_fhwa_stored() -> None:
    """Covers: ETL-138 — 2023 is FHWA's estimate, kept as stored, never rounded."""
    parsed = parse_mv1((FIXTURES / "mv1-2023.xlsx").read_bytes())
    wisconsin = next(row for row in parsed.rows if row.area_name == "Wisconsin")
    automobiles = next(
        f
        for f in wisconsin.figures
        if (f.vehicle_class, f.ownership) == ("automobiles", "total")
    )
    assert automobiles.source == "1683880.4848862195"
    assert automobiles.value == Decimal("1683880.4848862195")
    assert ("", "Sums may not equal to totals due to rounding.") in parsed.footnotes


def test_an_mv1_workbook_in_another_layout_is_set_aside() -> None:
    """Covers: ETL-138."""
    assert [item.error_code for item in parse_mv1(b"PK\x03\x04junk").quarantined] == [
        "unregistered_layout"
    ]
