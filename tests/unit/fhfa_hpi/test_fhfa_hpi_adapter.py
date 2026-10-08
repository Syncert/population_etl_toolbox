"""Offline contracts for the FHFA annual House Price Index adapter.

Covers: ETL-064
"""

from __future__ import annotations

import importlib
import io
import re
import sys
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.fhfa_hpi.client import HpiPayloadError, check_workbook
from data_ingestion_toolbox.fhfa_hpi.registry import COUNTY_FILE
from data_ingestion_toolbox.fhfa_hpi.silver_fhfa_hpi.parse import parse_file
from data_ingestion_toolbox.utility.workbook import WorkbookError, read_sheet

pytestmark = pytest.mark.unit

FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "fhfa_hpi" / "hpi_at_county.xlsx"
)
SHEET = "xl/worksheets/sheet1.xml"


def _fixture() -> bytes:
    return FIXTURE.read_bytes()


def _with_sheet(edit) -> bytes:  # noqa: ANN001
    """The fixture workbook with its sheet XML passed through ``edit``."""
    source = zipfile.ZipFile(io.BytesIO(_fixture()))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as target:
        for info in source.infolist():
            data = source.read(info)
            if info.filename == SHEET:
                data = edit(data.decode("utf-8")).encode("utf-8")
            target.writestr(info, data)
    return out.getvalue()


def _observations(raw: bytes) -> dict[tuple[str, int, str], object]:
    return {
        (obs.fips_code, obs.year, obs.measure): obs
        for obs in parse_file(raw, item=COUNTY_FILE).observations
    }


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-064 — the workbook needs no key, so there is none to leak."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.fhfa_hpi"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.fhfa_hpi.config")
    assert not {
        field
        for field in module.HpiConfig.model_fields
        if "key" in field or "token" in field
    }
    assert "neither endorsed nor certified by FHFA" in module.FHFA_NOTICE


def test_the_workbook_reader_keeps_stored_text_and_numbers() -> None:
    """Covers: ETL-064 — a text FIPS keeps its zero; a number is the file's own digits."""
    rows = dict(read_sheet(_fixture(), "county"))
    assert rows[6][3].value == "FIPS code"
    kinds = {
        (cells[1].value, cells[3].kind, cells[3].value)
        for number, cells in rows.items()
        if number > 6
    }
    assert ("AL", "text", "01001") in kinds and ("DE", "number", "10001") in kinds
    with pytest.raises(WorkbookError, match="sheet_missing"):
        list(read_sheet(_fixture(), "ZIP5"))
    with pytest.raises(WorkbookError, match="not_a_workbook"):
        list(read_sheet(b"<html>", "county"))


def test_the_fixture_replays_with_its_vintage_gaps_and_first_years() -> None:
    """Covers: ETL-064 — vintage, values, missing reasons and the first year's change."""
    parsed = parse_file(_fixture(), item=COUNTY_FILE)
    assert parsed.vintage.isoformat() == "2026-03-31"
    assert (
        parsed.quarantined == ()
        and parsed.county_count == 7
        and parsed.row_count == 302
    )
    rows = _observations(_fixture())
    sussex = rows[("10005", 2025, "hpi_base_2000")]
    assert (sussex.value, sussex.value_status, sussex.geo_id) == (
        Decimal("307.11"),
        "valid",
        "state:10|county:005",
    )
    kent = rows[("10001", 2025, "hpi")]
    assert (kent.value_source, kent.value) == ("600.16999999999996", Decimal("600.17"))
    first = rows[("10001", 1979, "annual_change_pct")]
    assert (first.value, first.value_status, first.missing_reason) == (
        None,
        "not_applicable",
        "first_recorded_year",
    )
    gap = rows[("01115", 1981, "hpi")]
    assert (gap.value, gap.value_status, gap.missing_reason) == (
        None,
        "missing",
        "provider_missing",
    )
    after_gap = rows[("01115", 1982, "annual_change_pct")]
    assert (after_gap.value_status, after_gap.missing_reason) == (
        "not_applicable",
        "prior_year_missing",
    )
    chugach = rows[("02063", 2010, "hpi_base_2000")]
    assert (chugach.value, chugach.value_status, chugach.missing_reason) == (
        None,
        "missing",
        "base_year_unavailable",
    )
    assert rows[("02063", 2025, "hpi")].missing_reason == "provider_missing"
    assert rows[("09110", 2025, "hpi")].geo_id == "state:09|county:110"
    assert all(
        obs.value is None for obs in rows.values() if obs.value_status != "valid"
    )


def test_an_integer_fips_resolves_to_its_zero_padded_text_form() -> None:
    """Covers: ETL-064 — FIPS 1001 stored as a number is 01001."""

    def as_number(sheet: str) -> str:
        return re.sub(
            r'(<c r="C\d+" s="13") t="s"><v>\d+</v>',
            lambda m: m.group(1) + "><v>1001</v>",
            sheet,
            count=1,
        )

    rows = _observations(_with_sheet(as_number))
    renumbered = [obs for obs in rows.values() if obs.fips_source == "1001"]
    assert renumbered and {obs.fips_code for obs in renumbered} == {"01001"}
    assert {obs.geo_id for obs in renumbered} == {"state:01|county:001"}


def test_malformed_rows_and_workbooks_are_quarantined() -> None:
    """Covers: ETL-064 — a bad code, a text value or a repeated row is quarantined; a wrong header refuses the file."""

    def damage(sheet: str) -> str:
        sheet = sheet.replace('<c r="D8" s="5"><v>', '<c r="D8" s="5" t="str"><v>x', 1)
        sheet = re.sub(
            r'(<c r="F9" s="7")><v>[^<]*</v>', r'\1 t="str"><v>n/a</v>', sheet, count=1
        )
        row10 = re.search(r'<row r="10" .*?</row>', sheet, flags=re.S).group(0)
        duplicate = re.sub(r'r="(\D*)10"', r'r="\g<1>999"', row10)
        return sheet.replace("</sheetData>", duplicate + "</sheetData>")

    parsed = parse_file(_with_sheet(damage), item=COUNTY_FILE)
    assert sorted((q.source_row_index, q.error_code) for q in parsed.quarantined) == [
        (8, "unreadable_row"),
        (9, "unreadable_row"),
        (999, "duplicate_row"),
    ]
    wrong = parse_file(
        _with_sheet(
            lambda sheet: sheet.replace(
                '<c r="C6" s="4" t="s"><v>9</v>', '<c r="C6" s="4" t="str"><v>GEOID</v>'
            )
        ),
        item=COUNTY_FILE,
    )
    assert [(q.source_row_index, q.error_code) for q in wrong.quarantined] == [
        (0, "unexpected_header")
    ]
    with pytest.raises(HpiPayloadError, match="unexpected_header"):
        check_workbook(
            _with_sheet(
                lambda sheet: sheet.replace('<c r="C6" s="4" t="s"><v>9</v></c>', "")
            ),
            COUNTY_FILE,
        )
    check_workbook(_fixture(), COUNTY_FILE)
    undated = parse_file(
        _with_sheet(
            lambda sheet: re.sub(
                r'<row r="4" .*?</row>', "", sheet, count=1, flags=re.S
            )
        ),
        item=COUNTY_FILE,
    )
    assert [q.error_code for q in undated.quarantined] == ["vintage_missing"]
