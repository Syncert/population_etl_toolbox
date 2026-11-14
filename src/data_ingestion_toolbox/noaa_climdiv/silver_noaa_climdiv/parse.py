"""Parse a captured nClimDiv county file into monthly values.

Pure: bytes in, typed rows and quarantine records out. Each line is one
county, element and year with twelve monthly values (``county-readme.txt``).
A value equal to the element's registered missing marker is a month NCEI has
not published yet: kept as ``missing`` with no number, never zero. A line
that does not read -- a state code outside NCEI's table, another element's
code, a year before 1895, a value that is not a number, a negative value
where none can occur -- is quarantined alone, never published.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from ..registry import (
    FIRST_YEAR,
    NCEI_STATE_TO_FIPS,
    ClimdivElement,
)

#: county-readme.txt: temperature -50.00 to 140.00 degrees Fahrenheit,
#: precipitation 00.00 to 99.99 inches. Degree days have no stated range.
_TEMPERATURE_RANGE = (Decimal("-50"), Decimal("140"))
_PRECIPITATION_RANGE = (Decimal("0"), Decimal("99.99"))


@dataclass(frozen=True)
class MonthValue:
    line: int
    ncei_state_code: str
    state_fips: str
    county_fips: str
    year: int
    month: int
    value_source: str
    value: Decimal | None
    value_status: str
    missing_reason: str | None


@dataclass(frozen=True)
class ClimdivQuarantine:
    line: int
    error_code: str
    error_summary: str


@dataclass
class ParsedFile:
    line_count: int = 0
    quarantined: list[ClimdivQuarantine] = field(default_factory=list)


class _LineError(ValueError):
    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


def _bounds(element: ClimdivElement) -> tuple[Decimal, Decimal] | None:
    if element.unit == "inches":
        return _PRECIPITATION_RANGE
    if element.can_be_negative:
        return _TEMPERATURE_RANGE
    return None


def _parse_line(
    text: str, number: int, element: ClimdivElement, last_year: int
) -> list[MonthValue]:
    if len(text.rstrip()) != 95 or text[95:].strip():
        raise _LineError(
            "unexpected_layout",
            f"line {number} is {len(text.rstrip())} characters, not 95",
        )
    state, county, code, year_text = text[0:2], text[2:5], text[5:7], text[7:11]
    if state not in NCEI_STATE_TO_FIPS:
        raise _LineError("unknown_state_code", f"line {number} state code {state!r}")
    if not county.isdigit():
        raise _LineError("unreadable_county", f"line {number} county {county!r}")
    if code != element.element_code:
        raise _LineError(
            "wrong_element",
            f"line {number} element {code!r}, not {element.element_code}",
        )
    if not year_text.isdigit() or not FIRST_YEAR <= int(year_text) <= last_year:
        raise _LineError("unreadable_year", f"line {number} year {year_text!r}")
    year = int(year_text)
    bounds = _bounds(element)
    values: list[MonthValue] = []
    for month in range(1, 13):
        start = 11 + 7 * (month - 1)
        stored = text[start : start + 7].strip()
        try:
            number_value = Decimal(stored)
        except InvalidOperation as exc:
            raise _LineError(
                "unreadable_value",
                f"line {number} month {month} {stored!r} is not a number",
            ) from exc
        if number_value == element.missing_marker:
            values.append(
                MonthValue(
                    number,
                    state,
                    NCEI_STATE_TO_FIPS[state],
                    county,
                    year,
                    month,
                    stored,
                    None,
                    "missing",
                    "not_yet_published",
                )
            )
            continue
        if not element.can_be_negative and number_value < 0:
            raise _LineError(
                "negative_value",
                f"line {number} month {month} is {stored}, which cannot be negative",
            )
        if bounds is not None and not bounds[0] <= number_value <= bounds[1]:
            raise _LineError(
                "out_of_range",
                f"line {number} month {month} is {stored}, outside {bounds[0]} to {bounds[1]}",
            )
        values.append(
            MonthValue(
                number,
                state,
                NCEI_STATE_TO_FIPS[state],
                county,
                year,
                month,
                stored,
                number_value,
                "valid",
                None,
            )
        )
    return values


def parse_county_file(
    raw_bytes: bytes, element: ClimdivElement, *, last_year: int, parsed: ParsedFile
) -> Iterator[MonthValue]:
    """Yield every monthly value of every readable line; record the rest in ``parsed``.

    ``last_year`` is the newest year a line may carry: the file's release
    year. The values are yielded rather than listed: a county file holds
    about five million.
    """
    try:
        text = raw_bytes.decode("ascii")
    except UnicodeDecodeError:
        parsed.quarantined.append(
            ClimdivQuarantine(0, "not_ascii", "the file is not ASCII")
        )
        return
    seen: set[tuple[str, str, int]] = set()
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        parsed.line_count += 1
        try:
            values = _parse_line(line, number, element, last_year)
            key = (values[0].ncei_state_code, values[0].county_fips, values[0].year)
            if key in seen:
                raise _LineError("duplicate_line", f"line {number} repeats {key}")
            seen.add(key)
        except _LineError as error:
            parsed.quarantined.append(
                ClimdivQuarantine(number, error.code, str(error)[:200])
            )
            continue
        yield from values
