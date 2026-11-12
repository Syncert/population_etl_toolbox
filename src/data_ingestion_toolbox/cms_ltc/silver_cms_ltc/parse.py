"""Parse one captured CMS CSV into facility rows and their figures.

The header must hold every registered column, or the whole file is refused
(row 0). Each data row is one facility row of the release. A row without a
six-character certification number, with a report key already read in the
same file, or with a registered figure that is neither blank nor a whole
number is set aside alone with its reason. A blank figure is ``missing``,
never zero. A cost report's fiscal dates are read from ``MM/DD/YYYY``; a
blank date is kept blank, and an unreadable one sets the row aside.
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from datetime import date, datetime

from ..client import decode_csv
from ..registry import USPS_TO_FIPS, Series

_CCN = re.compile(r"^[0-9A-Z]{6}$")
_WHOLE = re.compile(r"^[0-9]+$")


@dataclass(frozen=True)
class FacilityRow:
    source_row_index: int
    report_key: str
    ccn: str
    facility_name: str
    street_address: str | None
    city: str | None
    state_code: str
    state_fips: str | None
    zip_code: str | None
    county_name: str | None
    fiscal_year_begin: date | None
    fiscal_year_end: date | None
    ownership_type: str | None
    hospital_based: str | None
    certification_date: str | None
    #: measure_id -> (published text, value or None).
    counts: dict[str, tuple[str, int | None]] = field(default_factory=dict)


@dataclass(frozen=True)
class Quarantined:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedFile:
    rows: tuple[FacilityRow, ...]
    quarantined: tuple[Quarantined, ...]
    row_count: int
    #: The encoding the file was read in (``client.TEXT_ENCODINGS``), or
    #: None when it read in neither.
    encoding: str | None = None


def _text(record: dict[str, str], column: str | None) -> str | None:
    if column is None:
        return None
    value = (record.get(column) or "").strip()
    return value or None


def _us_date(value: str | None) -> date | None:
    if value is None:
        return None
    return datetime.strptime(value, "%m/%d/%Y").date()


def parse_file(series: Series, payload: bytes) -> ParsedFile:
    """Every facility row of one captured file, or why a row was set aside."""
    try:
        text, encoding = decode_csv(payload)
    except UnicodeDecodeError:
        return ParsedFile(
            (),
            (Quarantined(0, "not_text", "file is neither UTF-8 nor Windows-1252"),),
            0,
        )
    reader = csv.DictReader(io.StringIO(text, newline=""))
    header = {column.strip() for column in reader.fieldnames or ()}
    missing = sorted(series.required_columns - header)
    if missing:
        return ParsedFile(
            (),
            (Quarantined(0, "unexpected_header", f"missing columns: {missing}"),),
            0,
            encoding,
        )
    rows: list[FacilityRow] = []
    quarantined: list[Quarantined] = []
    seen: set[str] = set()
    row_count = 0
    for index, raw in enumerate(reader, start=1):
        row_count += 1
        record = {str(key).strip(): value for key, value in raw.items() if key}
        ccn = _text(record, series.ccn_column) or ""
        key = _text(record, series.report_key_column) or ""
        if not _CCN.match(ccn):
            quarantined.append(
                Quarantined(index, "unreadable_ccn", f"certification number {ccn!r}")
            )
            continue
        if not key or key in seen:
            quarantined.append(
                Quarantined(index, "repeated_report", f"report key {key!r} read twice")
            )
            continue
        counts: dict[str, tuple[str, int | None]] = {}
        unreadable = None
        for measure in series.measures:
            published = (record.get(measure.column) or "").strip()
            if published and not _WHOLE.match(published):
                unreadable = measure.column
                break
            counts[measure.measure_id] = (
                published,
                int(published) if published else None,
            )
        if unreadable is not None:
            quarantined.append(
                Quarantined(index, "unreadable_value", f"{unreadable} is not a count")
            )
            continue
        try:
            begin = _us_date(_text(record, series.fiscal_begin_column))
            end = _us_date(_text(record, series.fiscal_end_column))
        except ValueError:
            quarantined.append(
                Quarantined(index, "unreadable_date", "fiscal year date unreadable")
            )
            continue
        state = (_text(record, series.state_column) or "").upper()
        seen.add(key)
        rows.append(
            FacilityRow(
                source_row_index=index,
                report_key=key,
                ccn=ccn,
                facility_name=_text(record, series.name_column) or "",
                street_address=_text(record, series.street_column),
                city=_text(record, series.city_column),
                state_code=state,
                state_fips=USPS_TO_FIPS.get(state),
                zip_code=_text(record, series.zip_column),
                county_name=_text(record, series.county_column),
                fiscal_year_begin=begin,
                fiscal_year_end=end,
                ownership_type=_text(record, "Ownership Type"),
                hospital_based=_text(record, "Hospital Based"),
                certification_date=_text(record, "Certification Date"),
                counts=counts,
            )
        )
    return ParsedFile(tuple(rows), tuple(quarantined), row_count, encoding)


#: Words a county's name carries in the Census reference but CMS may omit.
_COUNTY_WORDS = (
    " CITY AND BOROUGH",
    " CENSUS AREA",
    " MUNICIPALITY",
    " BOROUGH",
    " COUNTY",
    " PARISH",
    " CITY",
)


def normalise_county_name(name: str) -> str:
    """Upper case, periods and apostrophes dropped, spaces collapsed."""
    cleaned = name.upper().replace(".", " ").replace("'", "")
    return " ".join(cleaned.split())


def county_base(name: str) -> str:
    """A reference county's name without its County/Parish/Borough/city word."""
    normalised = normalise_county_name(name)
    for word in _COUNTY_WORDS:
        if normalised.endswith(word):
            return normalised[: -len(word)]
    return normalised


def place_county(
    county_name: str | None,
    state_fips: str | None,
    counties: dict[str, list[tuple[str, int, str]]],
) -> tuple[str, str | None, int | None]:
    """(status, geo_id, geo_sk) for a cost report's county name.

    ``counties`` maps a state FIPS code to its counties as
    ``(geo_id, geo_sk, reference name)``. A name matches a county whose own
    name, or whose name without its County/Parish/Borough/city word, equals
    it; only a single match places the facility.
    """
    if not county_name or not state_fips:
        return "unmapped", None, None
    wanted = normalise_county_name(county_name)
    matches = {
        (geo_id, geo_sk)
        for geo_id, geo_sk, name in counties.get(state_fips, ())
        if wanted in (normalise_county_name(name), county_base(name))
    }
    if len(matches) == 1:
        ((geo_id, geo_sk),) = matches
        return "resolved", geo_id, geo_sk
    return ("ambiguous" if matches else "unmapped"), None, None
