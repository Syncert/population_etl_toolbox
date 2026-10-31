"""The Return A master file record, field by field.

Positions are the FBI's own, from ``Ret A Rec Descrip.pdf`` in
``reta-help.zip`` (kept under ``tests/fixtures/fbi_ucr/return_a/``), counted
from 1 as the guide counts them. A record is 7,385 characters: a 305-character
agency header, then twelve 590-character month blocks, January first.

Each month block holds four cards of 28 five-character offense fields:
card 0 unfounded, card 1 actual offenses, card 2 cleared by arrest, card 3
cleared involving only people under 18. Card types say whether a month's card
is a normal return (5), an adjustment (2), not available (4) or not updated
(0). Only a normal return or an adjustment is a reported month; anything else
is a month with no report, never a zero.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

from .overpunch import decode_signed, decode_unsigned

RECORD_LENGTH = 7385
HEADER_LENGTH = 305
MONTH_LENGTH = 590
FIELDS_PER_CARD = 28
FIELD_WIDTH = 5

#: Card types that carry a month's counts.
REPORTED_CARD_TYPES = frozenset({"5", "2"})
#: Every card type the guide documents; anything else fails the record.
KNOWN_CARD_TYPES = frozenset({"0", "2", "4", "5", " "})

#: The guide's 28 offense fields, numbered as it numbers them.
OFFENSE_FIELDS: tuple[str, ...] = (
    "murder",
    "manslaughter",
    "rape_total",
    "rape_by_force",
    "attempted_rape",
    "robbery_total",
    "robbery_gun",
    "robbery_knife",
    "robbery_other_weapon",
    "robbery_strong_arm",
    "assault_total",
    "assault_gun",
    "assault_knife",
    "assault_other_weapon",
    "assault_hands_feet",
    "simple_assault",
    "burglary_total",
    "burglary_forcible_entry",
    "burglary_no_forcible_entry",
    "burglary_attempted",
    "larceny_total",
    "motor_vehicle_theft_total",
    "auto_theft",
    "truck_bus_theft",
    "other_vehicle_theft",
    "grand_total",
    "larceny_under_50",
    "unused",
)
_FIELD = {name: index for index, name in enumerate(OFFENSE_FIELDS)}


class ReturnARecordError(ValueError):
    """A record that does not have the documented shape."""


def _at(line: str, first: int, last: int) -> str:
    """The guide's 1-based, inclusive positions."""
    return line[first - 1 : last]


@dataclass(frozen=True)
class CountySlice:
    """One county the agency's population lies in, with its share of it."""

    fbi_county_code: int
    population: int
    msa_code: str


@dataclass(frozen=True)
class MonthCards:
    """One month block: whether it is a report, and its offense cards."""

    month: int
    included_in: int | None
    actual_card_type: str
    cleared_card_type: str
    actual: tuple[int | None, ...]
    cleared: tuple[int | None, ...]

    @property
    def is_reported(self) -> bool:
        """A normal return or an adjustment, held in its own month."""
        if self.included_in not in (None, 0, self.month):
            return False
        return self.actual_card_type in REPORTED_CARD_TYPES

    @property
    def clearances_reported(self) -> bool:
        if self.included_in not in (None, 0, self.month):
            return False
        return self.cleared_card_type in REPORTED_CARD_TYPES


def _value(card: tuple[int | None, ...], name: str) -> int:
    value = card[_FIELD[name]]
    return value or 0


def violent_total(card: tuple[int | None, ...]) -> int:
    """Murder, rape, robbery and aggravated assault, as the CDE's violent crime total sums them.

    The file's "Assault Total" includes simple assault (field 16), which is
    not a violent crime; aggravated assault is the total less it. Negligent
    manslaughter (field 2) is not a violent crime either.
    """
    aggravated = _value(card, "assault_total") - _value(card, "simple_assault")
    return (
        _value(card, "murder")
        + _value(card, "rape_total")
        + _value(card, "robbery_total")
        + aggravated
    )


def property_total(card: tuple[int | None, ...]) -> int:
    """Burglary, larceny-theft and motor vehicle theft, the CDE's property crime total."""
    return (
        _value(card, "burglary_total")
        + _value(card, "larceny_total")
        + _value(card, "motor_vehicle_theft_total")
    )


@dataclass(frozen=True)
class ReturnARecord:
    """One agency's year, as the master file holds it."""

    line_number: int
    ori7: str
    population_group: str
    data_year: int
    covered_by: str | None
    months_reported: int
    agency_name: str
    county_slices: tuple[CountySlice, ...]
    months: tuple[MonthCards, ...]

    @property
    def ori(self) -> str:
        """The nine-character ORI the CDE uses: the seven here plus ``00``."""
        return f"{self.ori7}00"

    @property
    def state_code(self) -> str:
        return self.ori7[:2]

    @property
    def population(self) -> int:
        """The agency's population: its county slices summed (the guide's note)."""
        return sum(item.population for item in self.county_slices)

    @property
    def county_codes(self) -> tuple[int, ...]:
        return tuple(item.fbi_county_code for item in self.county_slices)


def _slice(line: str, first: int) -> CountySlice | None:
    population = decode_unsigned(_at(line, first, first + 8))
    county = _at(line, first + 9, first + 11).strip()
    if not county or not county.isdigit() or int(county) == 0:
        if population:
            raise ReturnARecordError(
                f"population {population} without a county at {first}"
            )
        return None
    return CountySlice(
        fbi_county_code=int(county),
        population=population or 0,
        msa_code=_at(line, first + 12, first + 14).strip(),
    )


def _card(block: str, card: int) -> tuple[int | None, ...]:
    start = 17 + card * FIELDS_PER_CARD * FIELD_WIDTH
    return tuple(
        decode_signed(block[start + index * 5 : start + index * 5 + 5])
        for index in range(FIELDS_PER_CARD)
    )


def _month(block: str, month: int) -> MonthCards:
    types = block[8:13]
    for card_type in types[:4]:
        if card_type not in KNOWN_CARD_TYPES:
            raise ReturnARecordError(f"unknown card type {card_type!r} in {month}")
    included = block[0:2].strip()
    return MonthCards(
        month=month,
        included_in=int(included) if included.isdigit() else None,
        actual_card_type=types[1],
        cleared_card_type=types[2],
        actual=_card(block, 1),
        cleared=_card(block, 2),
    )


def parse_record(line: str, line_number: int) -> ReturnARecord:
    """One record, or :class:`ReturnARecordError` if its shape is wrong.

    A wrong length, an unknown card type or a non-numeric count fails the
    whole record: a shifted column must never be read as a value.
    """
    if len(line) != RECORD_LENGTH:
        raise ReturnARecordError(
            f"line {line_number}: {len(line)} characters, not {RECORD_LENGTH}"
        )
    if line[0] != "1":
        raise ReturnARecordError(f"line {line_number}: not a Return A record")
    ori7 = _at(line, 4, 10)
    if not (ori7[:2].isalpha() and ori7[:2].isupper()):
        raise ReturnARecordError(f"line {line_number}: invalid ORI {ori7!r}")
    try:
        months_reported = decode_unsigned(_at(line, 42, 43)) or 0
        slices = tuple(
            item
            for item in (_slice(line, 45), _slice(line, 60), _slice(line, 75))
            if item is not None
        )
        months = tuple(
            _month(
                line[
                    HEADER_LENGTH + index * MONTH_LENGTH : HEADER_LENGTH
                    + (index + 1) * MONTH_LENGTH
                ],
                index + 1,
            )
            for index in range(12)
        )
    except ValueError as error:
        raise ReturnARecordError(f"line {line_number} ({ori7}): {error}") from error
    year = int(_at(line, 14, 15))
    covered_by = _at(line, 24, 30).strip() or None
    return ReturnARecord(
        line_number=line_number,
        ori7=ori7,
        population_group=_at(line, 11, 12).strip(),
        data_year=year + (1900 if year >= 60 else 2000),
        covered_by=covered_by,
        months_reported=months_reported,
        agency_name=_at(line, 121, 144).strip(),
        county_slices=slices,
        months=months,
    )


def iter_records(text: str) -> Iterator[ReturnARecord]:
    """Every record of one file's text, in file order."""
    for number, line in enumerate(text.split("\n"), start=1):
        line = line.rstrip("\r")
        if line:
            yield parse_record(line, number)
