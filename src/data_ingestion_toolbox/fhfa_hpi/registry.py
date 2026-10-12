"""The registered FHFA annual House Price Index files and measures.

Read from FHFA's datasets page and the county workbook on 2026-10-06. The
annual ("developmental") all-transactions indexes are static downloads at
``https://www.fhfa.gov/hpi/download/annual/``; the county file is
``hpi_at_county.xlsx`` with one sheet, ``county``: five preamble rows (a
title, a blank row, the notes, ``Last updated: <Month> <D>, <YYYY>.`` and
``Not Seasonally Adjusted (NSA)``), then the header below and one row per
county and year. The ``FIPS code`` cell is text where it has a leading zero
and a number elsewhere, so it is read as text and left-padded.

The index is nominal and not seasonally adjusted. ``HPI`` is 100 in the
first year a county is recorded; the 1990- and 2000-based columns rescale
the same series, so the annual change is the same in all three. A county
too thin to index is either not reported until recording starts or left
empty (the notes say ``"."``; the current file writes an empty cell). A
missing 1990 or 2000 index leaves that base column empty for the series.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

FHFA_BASE_URL = "https://www.fhfa.gov/hpi/download/annual"

COUNTY = "county"

#: The header row, exactly as the workbook writes it.
COUNTY_HEADER: tuple[str, ...] = (
    "State",
    "County",
    "FIPS code",
    "Year",
    "Annual Change (%)",
    "HPI",
    "HPI with 1990 base",
    "HPI with 2000 base",
)

#: (measure, header column, unit, label). Silver keeps all four; gold
#: publishes the annual change and the 2000-based index, whose base is the
#: same year for every county.
MEASURES: tuple[tuple[str, str, str, str], ...] = (
    (
        "annual_change_pct",
        "Annual Change (%)",
        "percent",
        "Annual change in the house price index",
    ),
    (
        "hpi",
        "HPI",
        "index, first recorded year = 100",
        "House price index, first recorded year = 100",
    ),
    (
        "hpi_base_1990",
        "HPI with 1990 base",
        "index, 1990 = 100",
        "House price index, 1990 = 100",
    ),
    (
        "hpi_base_2000",
        "HPI with 2000 base",
        "index, 2000 = 100",
        "House price index, 2000 = 100",
    ),
)
PUBLISHED_MEASURES: tuple[str, ...] = ("annual_change_pct", "hpi_base_2000")

#: The workbook's "missing" mark, per its notes.
MISSING_MARKS: frozenset[str] = frozenset({"", "."})

_LAST_UPDATED = re.compile(r"Last updated:\s*([A-Za-z]+ \d{1,2}, \d{4})")


def last_updated_text(cell: str) -> str | None:
    """``March 31, 2026`` from the preamble's "Last updated" cell, or ``None``."""
    match = _LAST_UPDATED.search(cell)
    return match.group(1) if match else None


@dataclass(frozen=True)
class HpiFile:
    kind: str
    path: str
    sheet: str
    header: tuple[str, ...]

    @property
    def key(self) -> str:
        return self.kind


COUNTY_FILE = HpiFile(COUNTY, "/hpi_at_county.xlsx", "county", COUNTY_HEADER)


def registered_files() -> tuple[HpiFile, ...]:
    return (COUNTY_FILE,)


def get_file(kind: str) -> HpiFile:
    for item in registered_files():
        if item.kind == kind:
            return item
    raise KeyError(f"{kind} is not a registered FHFA HPI file")
