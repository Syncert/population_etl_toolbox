"""The standard-library ``.xls`` reader against SOI's own workbooks.

Covers: ETL-059
"""

from __future__ import annotations

from pathlib import Path

import pytest

from data_ingestion_toolbox.utility.xls import XlsFormatError, read_first_sheet

pytestmark = pytest.mark.unit

MEMBERS = (
    Path(__file__).resolve().parents[2] / "fixtures" / "irs_migration" / "state_members"
)


def test_a_workbook_reads_its_title_text_and_numbers_as_stored() -> None:
    """Covers: ETL-059 — shared strings, text codes and numbers come back cell for cell."""
    rows = read_first_sheet((MEMBERS / "co0304DEi.xls").read_bytes())
    assert len(rows) == 317 and all(len(row) == 9 for row in rows)
    assert rows[0][0] == "2003-2004 County to County Migration Inflow"
    assert rows[8] == [
        "10",
        "000",
        "96",
        "000",
        "DE",
        "Total Mig - US & For",
        18621.0,
        34538.0,
        849929.0,
    ]


def test_codes_stored_as_numbers_and_footnotes_are_kept() -> None:
    """Covers: ETL-059 — the 2010-2011 workbooks store codes as numbers and end with footnotes."""
    rows = read_first_sheet((MEMBERS / "co1011ide.xls").read_bytes())
    assert rows[6][:6] == [10.0, 0.0, 96.0, 0.0, "DE", "Total Mig - US & For"]
    assert rows[-3][0].startswith("d - Data has been suppressed")
    early = read_first_sheet((MEMBERS / "co934dei.xls").read_bytes())
    assert early[8][:6] == [10.0, 1.0, 0.0, 1.0, "De", "Kent (Total Migrant)"]


def test_bytes_that_are_not_a_workbook_are_refused() -> None:
    """Covers: ETL-059 — never a partial sheet."""
    with pytest.raises(XlsFormatError):
        read_first_sheet(b"not a workbook")
    with pytest.raises(XlsFormatError):
        read_first_sheet((MEMBERS / "co0304DEi.xls").read_bytes()[:4096])
