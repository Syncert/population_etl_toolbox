"""Aggregate one captured LODES file from blocks to counties.

Pure: gzipped bytes in, county sums and quarantine records out. A block
code's first two characters are its state and its first five its state and
county, so a county is never inferred from a name. A block whose code is
not fifteen digits, or whose state is not the file's, is quarantined.
"""

from __future__ import annotations

import csv
import gzip
import io
from collections import defaultdict
from dataclasses import dataclass, field

from ..registry import (
    OD_AUX,
    OD_MAIN,
    RAC,
    RAC_COLUMNS,
    STATE_FIPS,
    WAC,
    WAC_COLUMNS,
    LodesFile,
)


@dataclass(frozen=True)
class LodesQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class AreaTotals:
    """RAC or WAC: county code -> column -> sum, and blocks per county."""

    totals: dict[str, dict[str, int]]
    blocks: dict[str, int]
    quarantined: tuple[LodesQuarantine, ...]
    row_count: int


@dataclass(frozen=True)
class FlowTotals:
    """OD: (home county, work county) -> jobs."""

    flows: dict[tuple[str, str], int]
    quarantined: tuple[LodesQuarantine, ...]
    row_count: int
    totals: dict[str, int] = field(default_factory=dict)


def _rows(payload: bytes) -> tuple[list[str], csv.reader]:
    reader = csv.reader(io.StringIO(gzip.decompress(payload).decode("latin-1")))
    header = [column.strip() for column in next(reader, [])]
    return header, reader


def _block(code: str) -> bool:
    return len(code) == 15 and code.isdigit()


def parse_area(payload: bytes, *, item: LodesFile) -> AreaTotals:
    """Sum a RAC or WAC file's blocks to counties."""
    if item.family not in (RAC, WAC):
        raise ValueError(f"{item.family} is not a residence or workplace file")
    header, reader = _rows(payload)
    key = "h_geocode" if item.family == RAC else "w_geocode"
    columns = RAC_COLUMNS if item.family == RAC else WAC_COLUMNS
    if not header or header[0] != key or not set(columns) <= set(header):
        return AreaTotals(
            {},
            {},
            (
                LodesQuarantine(
                    0, "unexpected_header", "the header is not the registered layout"
                ),
            ),
            0,
        )
    positions = {column: header.index(column) for column in columns}
    state = STATE_FIPS[item.state]
    totals: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    blocks: dict[str, int] = defaultdict(int)
    quarantined: list[LodesQuarantine] = []
    rows = 0
    for index, cells in enumerate(reader, start=1):
        if not any(cell.strip() for cell in cells):
            continue
        rows += 1
        if len(cells) != len(header):
            quarantined.append(
                LodesQuarantine(
                    index,
                    "ragged_row",
                    f"row has {len(cells)} fields, header {len(header)}",
                )
            )
            continue
        code = cells[0].strip()
        if not _block(code) or code[:2] != state:
            quarantined.append(
                LodesQuarantine(
                    index,
                    "unreadable_block",
                    f"{code!r} is not a block of state {state}",
                )
            )
            continue
        try:
            values = {
                column: int(cells[position]) for column, position in positions.items()
            }
        except ValueError:
            quarantined.append(
                LodesQuarantine(index, "unreadable_value", "a count is not an integer")
            )
            continue
        county = code[:5]
        blocks[county] += 1
        for column, value in values.items():
            totals[county][column] += value
    return AreaTotals(
        {county: dict(sums) for county, sums in totals.items()},
        dict(blocks),
        tuple(quarantined),
        rows,
    )


def parse_flows(payload: bytes, *, item: LodesFile) -> FlowTotals:
    """Sum an OD file's block pairs to county pairs (segment S000)."""
    if item.family not in (OD_MAIN, OD_AUX):
        raise ValueError(f"{item.family} is not an origin-destination file")
    header, reader = _rows(payload)
    if header[:3] != ["w_geocode", "h_geocode", "S000"]:
        return FlowTotals(
            {},
            (
                LodesQuarantine(
                    0, "unexpected_header", "the header is not the registered layout"
                ),
            ),
            0,
        )
    state = STATE_FIPS[item.state]
    flows: dict[tuple[str, str], int] = defaultdict(int)
    quarantined: list[LodesQuarantine] = []
    rows = 0
    for index, cells in enumerate(reader, start=1):
        if not any(cell.strip() for cell in cells):
            continue
        rows += 1
        if len(cells) != len(header):
            quarantined.append(
                LodesQuarantine(
                    index,
                    "ragged_row",
                    f"row has {len(cells)} fields, header {len(header)}",
                )
            )
            continue
        work, home = cells[0].strip(), cells[1].strip()
        # The work block is always in the file's state; the home block is in
        # it for `main` and in another state for `aux`.
        home_in_state = home[:2] == state
        if (
            not (_block(work) and _block(home))
            or work[:2] != state
            or home_in_state != (item.family == OD_MAIN)
        ):
            quarantined.append(
                LodesQuarantine(
                    index,
                    "unreadable_block",
                    f"{home!r} -> {work!r} is not a {item.family} pair",
                )
            )
            continue
        try:
            jobs = int(cells[2])
        except ValueError:
            quarantined.append(
                LodesQuarantine(index, "unreadable_value", "S000 is not an integer")
            )
            continue
        flows[(home[:5], work[:5])] += jobs
    return FlowTotals(dict(flows), tuple(quarantined), rows)
