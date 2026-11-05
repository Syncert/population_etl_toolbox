"""Parse captured Census government finance payloads (offline).

Pure functions over the captured bytes: no network and no database. A
record whose government, item or geography cannot be read is quarantined
with a reason; an amount that is not a number is kept as ``missing`` with
the provider's own text, never coerced to zero.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ..registry import (
    CENSUS_STATE_TO_FIPS,
    GOVERNMENT_TYPES,
    GOVS_LEVELS,
    QTAX_STATE_CATEGORY,
    STATE_TOTAL_LEVELS,
    USPS_TO_FIPS,
    CensusEdition,
    DirectoryLayout,
    StateTotalLayout,
    UnitArchive,
    UnitFileLayout,
    api_expected_header,
    govs_measure,
    qtax_measure,
)

_ITEM_CODE = re.compile(r"^[0-9A-Z]{3}$")
_QUARTER = re.compile(r"^(\d{4})-?Q([1-4])$")
_FIPS_STATES = frozenset(CENSUS_STATE_TO_FIPS.values())
_DOCUMENT_SUFFIXES = (".pdf", ".doc", ".docx", ".xls", ".xlsx", "/")


@dataclass(frozen=True)
class Quarantined:
    source_row_index: int
    error_code: str
    error_summary: str
    member: str = ""


@dataclass(frozen=True)
class UnitAmount:
    source_row_index: int
    government_code: str
    type_code: str
    fips_state: str
    #: From the code itself where the edition's codes carry the FIPS county;
    #: otherwise the directory places the government.
    fips_county: str | None
    item_code: str
    amount_source: str
    amount: Decimal | None
    value_status: str
    data_year: int | None
    imputation_flag: str | None


@dataclass(frozen=True)
class UnitGovernment:
    source_row_index: int
    government_code: str
    type_code: str
    government_type: str
    name: str
    county_name: str | None
    fips_state: str
    fips_county: str | None
    population: int | None
    fiscal_year_ending: str | None


@dataclass(frozen=True)
class StateTotal:
    source_row_index: int
    fips_state: str
    level_code: str
    item_code: str
    amount: Decimal | None
    coefficient_of_variation: Decimal | None


@dataclass(frozen=True)
class ParsedArchive:
    members: tuple[str, ...]
    amounts: tuple[UnitAmount, ...]
    governments: tuple[UnitGovernment, ...]
    state_totals: tuple[StateTotal, ...]
    quarantined: tuple[Quarantined, ...]
    row_count: int


@dataclass(frozen=True)
class ApiObservation:
    source_row_index: int
    measure_id: str
    government_level: str
    fiscal_year: int
    quarter: int | None
    geo_type: str
    geo_source_code: str
    geo_id: str
    value_source: str | None
    value: Decimal | None
    value_status: str
    source_record_id: str
    source_record: dict[str, Any]


@dataclass(frozen=True)
class ParsedApiSlice:
    observations: tuple[ApiObservation, ...]
    quarantined: tuple[Quarantined, ...]
    #: Rows outside the registered measures (another aggregate, government
    #: type or tax category): read and set aside, not errors.
    skipped: int
    row_count: int


def decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        number = Decimal(text)
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def member_matching(names: Iterable[str], pattern: str) -> str | None:
    """The one data member whose lower-case base name matches ``pattern``."""
    expression = re.compile(pattern)
    candidates = sorted(
        name
        for name in names
        if expression.search(name.lower().rsplit("/", 1)[-1])
        and not name.lower().endswith(_DOCUMENT_SUFFIXES)
    )
    return candidates[0] if candidates else None


def _record_id(*parts: object) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode("utf-8")).hexdigest()


def _place(code: str, scheme: str) -> tuple[str, str, str | None] | None:
    """(type, FIPS state, FIPS county or None) from a government code."""
    if scheme == "fips":
        if len(code) != 12 or not code.isdigit():
            return None
        state, type_code, county = code[:2], code[2], code[3:6]
        if state not in _FIPS_STATES or type_code not in GOVERNMENT_TYPES:
            return None
        return type_code, state, None if county == "000" else county
    if len(code) != 14 or not code.isdigit():
        return None
    state, type_code = code[:2], code[2]
    if state not in CENSUS_STATE_TO_FIPS or type_code not in GOVERNMENT_TYPES:
        return None
    return type_code, CENSUS_STATE_TO_FIPS[state], None


def parse_unit_lines(
    lines: Iterable[str], layout: UnitFileLayout, *, member: str = ""
) -> tuple[list[UnitAmount], list[Quarantined], int]:
    """Every (government, item, amount) record of a unit file."""
    amounts: list[UnitAmount] = []
    quarantined: list[Quarantined] = []
    count = 0
    for index, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        count += 1
        if len(line.rstrip("\r\n")) < layout.min_length:
            quarantined.append(
                Quarantined(index, "short_record", "record is shorter than the layout", member)
            )
            continue
        government = layout.government_code.read(line)
        placed = _place(government, layout.state_scheme)
        if placed is None:
            quarantined.append(
                Quarantined(
                    index,
                    "unreadable_government",
                    "government code is not a registered state, type and unit",
                    member,
                )
            )
            continue
        item = layout.item_code.read(line).upper()
        if not _ITEM_CODE.match(item):
            quarantined.append(
                Quarantined(index, "unreadable_item", "item code is not three characters", member)
            )
            continue
        type_code, fips_state, fips_county = placed
        source = layout.amount.read(line)
        amount = decimal_or_none(source)
        year_text = layout.year.read(line) if layout.year else ""
        flag = layout.imputation_flag.read(line) if layout.imputation_flag else ""
        amounts.append(
            UnitAmount(
                index,
                government,
                type_code,
                fips_state,
                fips_county,
                item,
                source,
                amount,
                "valid" if amount is not None else "missing",
                int(year_text) if year_text.isdigit() else None,
                flag or None,
            )
        )
    return amounts, quarantined, count


def parse_directory_lines(
    lines: Iterable[str], layout: DirectoryLayout, scheme: str, *, member: str = ""
) -> tuple[list[UnitGovernment], list[Quarantined], int]:
    """Every government of a directory file, with its FIPS state and county."""
    governments: list[UnitGovernment] = []
    quarantined: list[Quarantined] = []
    count = 0
    for index, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        count += 1
        if len(line.rstrip("\r\n")) < layout.min_length:
            quarantined.append(
                Quarantined(index, "short_record", "record is shorter than the layout", member)
            )
            continue
        government = layout.government_code.read(line)
        placed = _place(government, scheme)
        if placed is None:
            quarantined.append(
                Quarantined(
                    index,
                    "unreadable_government",
                    "government code is not a registered state, type and unit",
                    member,
                )
            )
            continue
        type_code, fips_state, fips_county = placed
        if layout.fips_state is not None:
            stated = layout.fips_state.read(line)
            if stated and stated != fips_state:
                quarantined.append(
                    Quarantined(
                        index,
                        "state_code_disagrees",
                        "FIPS state disagrees with the government code",
                        member,
                    )
                )
                continue
        if layout.fips_county is not None:
            stated_county = layout.fips_county.read(line)
            fips_county = (
                stated_county
                if len(stated_county) == 3 and stated_county.isdigit() and stated_county != "000"
                else None
            )
        if type_code == "0":
            fips_county = None
        population_text = layout.population.read(line) if layout.population else ""
        fiscal_year_ending = (
            layout.fiscal_year_ending.read(line) if layout.fiscal_year_ending else ""
        )
        governments.append(
            UnitGovernment(
                index,
                government,
                type_code,
                GOVERNMENT_TYPES[type_code],
                layout.name.read(line),
                layout.county_name.read(line) or None,
                fips_state,
                fips_county,
                int(population_text) if population_text.isdigit() else None,
                fiscal_year_ending or None,
            )
        )
    return governments, quarantined, count


def parse_state_total_lines(
    lines: Iterable[str], layout: StateTotalLayout, *, member: str = ""
) -> tuple[list[StateTotal], list[Quarantined], int]:
    """Census's totals by state and level for the three levels the replay checks."""
    totals: list[StateTotal] = []
    quarantined: list[Quarantined] = []
    count = 0
    for index, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        count += 1
        if len(line.rstrip("\r\n")) < layout.min_length:
            quarantined.append(
                Quarantined(index, "short_record", "record is shorter than the layout", member)
            )
            continue
        state = layout.state_code.read(line)
        if layout.state_scheme == "census_alpha" and state != "00":
            state = CENSUS_STATE_TO_FIPS.get(state, "")
        level = layout.level_code.read(line)
        item = layout.item_code.read(line).upper()
        if state != "00" and state not in _FIPS_STATES:
            quarantined.append(
                Quarantined(index, "unreadable_geography", "state code is not a FIPS state", member)
            )
            continue
        if not _ITEM_CODE.match(item):
            quarantined.append(
                Quarantined(index, "unreadable_item", "item code is not three characters", member)
            )
            continue
        if level not in STATE_TOTAL_LEVELS:
            continue
        totals.append(
            StateTotal(
                index,
                state,
                level,
                item,
                decimal_or_none(layout.amount.read(line)),
                decimal_or_none(layout.coefficient_of_variation.read(line)),
            )
        )
    return totals, quarantined, count


def parse_archive(edition: CensusEdition, archive: UnitArchive, payload: bytes) -> ParsedArchive:
    """Read every registered member of one captured unit-file zip."""
    try:
        bundle = zipfile.ZipFile(io.BytesIO(payload))
    except zipfile.BadZipFile:
        return ParsedArchive(
            (), (), (), (), (Quarantined(0, "not_a_zip", "capture is not a zip archive"),), 0
        )
    with bundle:
        names = bundle.namelist()
        members: list[str] = []
        quarantined: list[Quarantined] = []
        amounts: list[UnitAmount] = []
        governments: list[UnitGovernment] = []
        totals: list[StateTotal] = []
        rows = 0

        def lines_of(pattern: str | None, what: str) -> tuple[str, list[str]] | None:
            if pattern is None:
                return None
            member = member_matching(names, pattern)
            if member is None:
                quarantined.append(Quarantined(0, "missing_member", f"zip lacks the {what} file"))
                return None
            members.append(member)
            return member, bundle.read(member).decode("latin-1").splitlines()

        read = lines_of(archive.data_member, "unit")
        if read:
            found, bad, count = parse_unit_lines(read[1], edition.data_layout, member=read[0])
            amounts.extend(found)
            quarantined.extend(bad)
            rows += count
        read = lines_of(archive.directory_member, "directory")
        if read:
            found_g, bad, count = parse_directory_lines(
                read[1], edition.directory_layout, edition.data_layout.state_scheme, member=read[0]
            )
            governments.extend(found_g)
            quarantined.extend(bad)
        if edition.state_total_layout is not None:
            read = lines_of(archive.state_total_member, "state totals")
            if read:
                found_t, bad, count = parse_state_total_lines(
                    read[1], edition.state_total_layout, member=read[0]
                )
                totals.extend(found_t)
                quarantined.extend(bad)
    return ParsedArchive(
        tuple(members),
        tuple(amounts),
        tuple(governments),
        tuple(totals),
        tuple(quarantined),
        rows,
    )


def _api_rows(payload: bytes, dataset_id: str) -> tuple[list[str], list[list[Any]]] | Quarantined:
    from ..client import GovFinPayloadError, validate_api_payload

    try:
        rows = validate_api_payload(payload, dataset_id, api_expected_header(dataset_id))
    except GovFinPayloadError as exc:
        return Quarantined(0, exc.code, str(exc))
    return [str(name).lower() for name in rows[0]], rows[1:]


def _geography(record: dict[str, Any], slice_key: str) -> tuple[str, str, str] | None:
    if slice_key == "us":
        return "nation", "1", "us:1"
    code = str(record.get("state") or "")
    if code not in _FIPS_STATES:
        return None
    return "state", code, f"state:{code}"


def parse_govs(payload: bytes, *, period: int, slice_key: str) -> ParsedApiSlice:
    """Census's published state and national totals from ``govslocalfin``."""
    if not payload:
        return ParsedApiSlice((), (), 0, 0)
    read = _api_rows(payload, "asslgf")
    if isinstance(read, Quarantined):
        return ParsedApiSlice((), (read,), 0, 0)
    header, rows = read
    observations: list[ApiObservation] = []
    quarantined: list[Quarantined] = []
    skipped = 0
    for index, values in enumerate(rows, start=1):
        record = dict(zip(header, values))
        measure_id = govs_measure(str(record.get("agg_desc") or ""))
        level = GOVS_LEVELS.get(str(record.get("govtype") or ""))
        if measure_id is None or level is None:
            skipped += 1
            continue
        year = str(record.get("time") or record.get("year") or "")
        if year != str(period):
            quarantined.append(
                Quarantined(index, "unexpected_year", f"row is for {year}, not {period}")
            )
            continue
        place = _geography(record, slice_key)
        if place is None:
            quarantined.append(
                Quarantined(index, "unreadable_geography", "state is not a FIPS state")
            )
            continue
        geo_type, code, geo_id = place
        source = record.get("amount")
        value = decimal_or_none(source)
        observations.append(
            ApiObservation(
                index,
                measure_id,
                level,
                period,
                None,
                geo_type,
                code,
                geo_id,
                None if source is None else str(source),
                value,
                "valid" if value is not None else "missing",
                _record_id(
                    "asslgf", measure_id, level, period, geo_id,
                    json.dumps(record, sort_keys=True),
                ),  # fmt: skip
                record,
            )
        )
    return ParsedApiSlice(tuple(observations), tuple(quarantined), skipped, len(rows))


def parse_qtax(payload: bytes, *, period: int, slice_key: str) -> ParsedApiSlice:
    """State tax collections by type and quarter from QTAX."""
    if not payload:
        return ParsedApiSlice((), (), 0, 0)
    read = _api_rows(payload, "qtax")
    if isinstance(read, Quarantined):
        return ParsedApiSlice((), (read,), 0, 0)
    header, rows = read
    observations: list[ApiObservation] = []
    quarantined: list[Quarantined] = []
    skipped = 0
    for index, values in enumerate(rows, start=1):
        record = dict(zip(header, values))
        measure_id = qtax_measure(str(record.get("data_type_code") or ""))
        if (
            str(record.get("category_code") or "") != QTAX_STATE_CATEGORY
            or measure_id is None
            or str(record.get("seasonally_adj") or "").lower() != "no"
        ):
            skipped += 1
            continue
        quarter_text = str(record.get("time") or "")
        quarter_match = _QUARTER.match(quarter_text)
        if quarter_match is None or int(quarter_match.group(1)) != period:
            quarantined.append(
                Quarantined(
                    index, "unexpected_quarter", f"row is for {quarter_text!r}, not {period}"
                )
            )
            continue
        place = _geography(record, slice_key)
        geo_level_code = str(record.get("geo_level_code") or "").upper()
        if place is None or (
            slice_key == "state" and USPS_TO_FIPS.get(geo_level_code) != place[1]
        ):
            quarantined.append(
                Quarantined(index, "unreadable_geography", "state code and name disagree")
            )
            continue
        geo_type, code, geo_id = place
        source = record.get("cell_value")
        value = decimal_or_none(source)
        observations.append(
            ApiObservation(
                index,
                measure_id,
                "state",
                period,
                int(quarter_match.group(2)),
                geo_type,
                code,
                geo_id,
                None if source is None else str(source),
                value,
                "valid" if value is not None else "missing",
                _record_id("qtax", measure_id, quarter_text, geo_id, json.dumps(record, sort_keys=True)),
                record,
            )
        )
    return ParsedApiSlice(tuple(observations), tuple(quarantined), skipped, len(rows))
