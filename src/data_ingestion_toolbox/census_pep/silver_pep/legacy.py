"""Offline replay of the Census PEP tables published before the CSV era.

The Bureau's pre-2000 county series are printed tables and fixed-width cell
files rather than the wide "all data" CSVs the later decades use, so they
need their own readers. Two layouts cover every product registered here:

``census-pep-fixed-width-table-v1``
    The 1970s and 1980s intercensal county tables (``e7079co.txt``,
    ``e8089co.txt``). A printed table, paginated one block per state and
    half-decade, with the measure kind and the year read from each block's
    own two-line header rather than from fixed offsets -- the columns shift
    between blocks, and Alaska's wider name column shifts them again.

``census-pep-fixed-width-cells-v1``
    The 1990s county race-by-Hispanic-origin annual series
    (``co-99-10.txt``). One record per county and year, holding eight
    population cells and no total; the total is their sum, and the product
    records that derivation so a summed value is never presented as one the
    Bureau printed.

Both readers keep the source text of every value they publish, so a reader
can check the value against the bytes that were captured.
"""

from __future__ import annotations

import re
from typing import Any

from data_ingestion_toolbox.census_pep.config import (
    CENSUS_COUNT_VARIABLE,
    CONFIG,
    PEPRelease,
)
from data_ingestion_toolbox.normalization import NumericParseError, parse_decimal

#: The printed tables label each value column as a census enumeration or an
#: estimate. That label is the measure: the decennial count opens the decade
#: in April, the estimates follow each July.
_COLUMN_KIND_MEASURES = {
    "Census": CENSUS_COUNT_VARIABLE,
    "Estimate": "POPESTIMATE",
}

_TABLE_HEADER = re.compile(r"^FIPS\b")
_TABLE_SUBHEADER = re.compile(r"^\s*Code\b")
_TABLE_ROW = re.compile(r"^(\d{5})\s+(.*)$")
_NUMERIC_TOKEN = re.compile(r"^-?[\d,]+$")
_YEAR_TOKEN = re.compile(r"^(19|20)\d{2}$")

#: ``co-99-10.txt``: year, blank, state, county, then eight nine-character
#: cells, exactly as the published layout describes them.
_CELL_ROW = re.compile(r"^(\d{4}) (\d{2})(\d{3})")
_CELL_COUNT = 8
_CELL_WIDTH = 9
_CELL_START = 10


class PepLegacyPayloadError(ValueError):
    """A captured legacy PEP payload violates its published layout."""


def _decode(payload: bytes, *, text_encoding: str) -> list[str]:
    if not payload:
        raise PepLegacyPayloadError("PEP legacy capture is empty")
    try:
        text = payload.decode(text_encoding)
    except UnicodeDecodeError as exc:
        raise PepLegacyPayloadError(
            f"PEP legacy capture is not valid {text_encoding} text"
        ) from exc
    if not text.strip():
        raise PepLegacyPayloadError("PEP legacy capture is empty")
    return text.splitlines()


def _summary_level(fips: str) -> tuple[str, str, str | None]:
    """Map a five-digit table code onto a summary level and its FIPS parts.

    ``00000`` is the nation, ``SS000`` a state and ``SSCCC`` a county, which
    is how the printed tables carry all three in one column.
    """
    state, county = fips[:2], fips[2:]
    if fips == "00000":
        return "010", state, None
    if county == "000":
        return "040", state, None
    return "050", state, county


def _value(value_source: str) -> tuple[Any, str]:
    stripped = value_source.strip()
    if not stripped:
        return None, "blank"
    try:
        value = parse_decimal(stripped.replace(",", ""))
    except NumericParseError:
        return None, "invalid"
    if value is None:
        return None, "invalid"
    return value, "valid"


def _block_columns(header: str, subheader: str) -> list[tuple[str, int]]:
    """The (measure, year) of each value column in one printed block."""
    kinds = [token for token in header.split()[1:]]
    years = [int(token) for token in subheader.split() if _YEAR_TOKEN.fullmatch(token)]
    if not kinds or len(kinds) != len(years):
        raise PepLegacyPayloadError(
            "PEP legacy table block header does not describe its columns: "
            f"{kinds!r} against {years!r}"
        )
    columns: list[tuple[str, int]] = []
    for kind, year in zip(kinds, years):
        measure = _COLUMN_KIND_MEASURES.get(kind)
        if measure is None:
            raise PepLegacyPayloadError(
                f"PEP legacy table block declares an unknown column kind: {kind}"
            )
        columns.append((measure, year))
    return columns


def _split_row(remainder: str) -> tuple[str, list[str]]:
    """Split a row's text into its area name and its trailing values."""
    tokens = remainder.split()
    values: list[str] = []
    while tokens and _NUMERIC_TOKEN.fullmatch(tokens[-1]):
        values.insert(0, tokens.pop())
    return " ".join(tokens), values


def parse_legacy_table_values(
    payload: bytes,
    *,
    release: PEPRelease,
) -> list[dict[str, Any]]:
    """Read a printed intercensal county table into revision records."""
    dataset = CONFIG.datasets[release.dataset_code]
    lines = _decode(payload, text_encoding=dataset.text_encoding)

    parsed: list[dict[str, Any]] = []
    columns: list[tuple[str, int]] = []
    index = 0
    while index < len(lines):
        line = lines[index]

        # A block header restates which years the next rows carry. The
        # preamble and the page furniture between blocks carry no rows, so
        # anything that is not a header or a row is simply passed over.
        if _TABLE_HEADER.match(line) and index + 1 < len(lines):
            subheader = lines[index + 1]
            if _TABLE_SUBHEADER.match(subheader):
                columns = _block_columns(line, subheader)
                index += 2
                continue

        match = _TABLE_ROW.match(line)
        if match is None:
            index += 1
            continue
        if not columns:
            raise PepLegacyPayloadError(
                "PEP legacy table row appears before any block header"
            )

        fips, remainder = match.group(1), match.group(2)
        name, values = _split_row(remainder)
        row_index = index

        # A long area name wraps onto the next line, taking the values with
        # it: the Virginia independent cities do this. The record is the two
        # lines together, not a row with no values followed by an orphan.
        if not values and index + 1 < len(lines):
            continuation_name, continuation_values = _split_row(lines[index + 1])
            if continuation_values:
                name = f"{name} {continuation_name}".strip()
                values = continuation_values
                index += 1

        if len(values) != len(columns):
            raise PepLegacyPayloadError(
                f"PEP legacy table row {fips} carries {len(values)} values "
                f"where its block declares {len(columns)}"
            )

        summary_level, state_fips, county_fips = _summary_level(fips)
        for column_index, ((metric_code, observation_year), value_source) in enumerate(
            zip(columns, values)
        ):
            if not (
                release.observation_start_year
                <= observation_year
                <= release.observation_end_year
            ):
                raise PepLegacyPayloadError(
                    "PEP legacy table column is outside release range: "
                    f"{observation_year}"
                )
            value, value_status = _value(value_source)
            parsed.append(
                _record(
                    release=release,
                    source_row_index=row_index,
                    source_column_index=column_index,
                    source_header=f"{metric_code}{observation_year}",
                    observation_year=observation_year,
                    metric_code=metric_code,
                    summary_level=summary_level,
                    state_fips=state_fips,
                    county_fips=county_fips,
                    name=name,
                    value_source=value_source,
                    value=value,
                    value_status=value_status,
                )
            )
        index += 1

    if not parsed:
        raise PepLegacyPayloadError("PEP legacy table contains no data rows")
    return parsed


def parse_legacy_cell_values(
    payload: bytes,
    *,
    release: PEPRelease,
) -> list[dict[str, Any]]:
    """Read the 1990s race-by-origin county file into revision records.

    The file publishes eight population cells per county and year and no
    total. The total is their sum; ``value_source`` keeps the exact cell
    text it was summed from, so the derivation is checkable against the
    captured bytes rather than asserted.
    """
    dataset = CONFIG.datasets[release.dataset_code]
    lines = _decode(payload, text_encoding=dataset.text_encoding)

    parsed: list[dict[str, Any]] = []
    for row_index, line in enumerate(lines):
        match = _CELL_ROW.match(line)
        if match is None:
            continue
        observation_year = int(match.group(1))
        if not (
            release.observation_start_year
            <= observation_year
            <= release.observation_end_year
        ):
            raise PepLegacyPayloadError(
                f"PEP legacy cell row is outside release range: {observation_year}"
            )

        cells = [
            line[
                _CELL_START + _CELL_WIDTH * position : _CELL_START
                + _CELL_WIDTH * (position + 1)
            ]
            for position in range(_CELL_COUNT)
        ]
        value_source = "".join(cells)
        total = 0
        value_status = "valid"
        for cell in cells:
            cell_value, cell_status = _value(cell)
            if cell_status != "valid":
                value_status = cell_status
                break
            total += int(cell_value)

        parsed.append(
            _record(
                release=release,
                source_row_index=row_index,
                source_column_index=0,
                source_header="RACE_BY_HISPANIC_ORIGIN_CELLS",
                observation_year=observation_year,
                metric_code="POPESTIMATE",
                summary_level="050",
                state_fips=match.group(2),
                county_fips=match.group(3),
                name=None,
                value_source=value_source,
                value=total if value_status == "valid" else None,
                value_status=value_status,
            )
        )

    if not parsed:
        raise PepLegacyPayloadError("PEP legacy cell file contains no data rows")
    return parsed


def _record(
    *,
    release: PEPRelease,
    source_row_index: int,
    source_column_index: int,
    source_header: str,
    observation_year: int,
    metric_code: str,
    summary_level: str,
    state_fips: str,
    county_fips: str | None,
    name: str | None,
    value_source: str,
    value: Any,
    value_status: str,
) -> dict[str, Any]:
    """One revision record in the shape the silver replay insert expects."""
    return {
        "source_row_index": source_row_index,
        "source_column_index": source_column_index,
        "source_header": source_header,
        "dataset_code": release.dataset_code,
        "release_vintage": release.vintage_year,
        "product_code": release.product_code,
        "observation_year": observation_year,
        "metric_code": metric_code,
        "unit": "persons",
        "summary_level": summary_level,
        "region_code_source": None,
        "division_code_source": None,
        "state_fips_source": state_fips,
        "county_fips_source": county_fips,
        "place_fips_source": None,
        "county_subdivision_source": None,
        "consolidated_city_source": None,
        "functional_status_source": None,
        "name_source": name,
        "state_name_source": None,
        "value_source": value_source,
        "value": value,
        "value_status": value_status,
    }
