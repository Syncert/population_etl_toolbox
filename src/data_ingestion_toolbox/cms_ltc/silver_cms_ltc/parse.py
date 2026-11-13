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
import unicodedata
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


def compact_county_name(name: str) -> str:
    """The normalised name with accents folded and every space removed
    (``DONA ANA`` and ``Doña Ana``; ``BLACKHAWK`` and ``Black Hawk``)."""
    folded = unicodedata.normalize("NFKD", normalise_county_name(name))
    return "".join(
        character
        for character in folded
        if not unicodedata.combining(character) and not character.isspace()
    )


def place_county_with_match(
    county_name: str | None,
    state_fips: str | None,
    counties: dict[str, list[tuple[str, int, str]]],
) -> tuple[str, str | None, int | None, str | None]:
    """(status, geo_id, geo_sk, how it matched) for a cost report's county name.

    ``counties`` maps a state FIPS code to its counties as
    ``(geo_id, geo_sk, reference name)``. Three deterministic steps, each
    used only when the one before found no county, and each placing the
    facility only on a single match:

    1. ``exact``: the name equals a county's own name, or its name without
       its County/Parish/Borough/city word, once case, periods and
       apostrophes are set aside.
    2. ``spelling``: the same, with accents folded and spaces removed.
    3. ``only_county``: the state has exactly one county (the District of
       Columbia), whatever name the report gives.

    Two matches at a step leave the facility ``ambiguous`` and stop: a name
    such as ST. LOUIS that is both a county and an independent city is never
    decided between them.
    """
    if not county_name or not state_fips:
        return "unmapped", None, None, None
    candidates = counties.get(state_fips, ())
    steps = (
        (
            "exact",
            normalise_county_name,
            lambda name: (normalise_county_name(name), county_base(name)),
        ),
        (
            "spelling",
            compact_county_name,
            lambda name: (
                compact_county_name(name),
                compact_county_name(county_base(name)),
            ),
        ),
    )
    for kind, normalise, forms in steps:
        wanted = normalise(county_name)
        matches = {
            (geo_id, geo_sk)
            for geo_id, geo_sk, name in candidates
            if wanted in forms(name)
        }
        if len(matches) == 1:
            ((geo_id, geo_sk),) = matches
            return "resolved", geo_id, geo_sk, kind
        if matches:
            return "ambiguous", None, None, None
    if len(candidates) == 1:
        geo_id, geo_sk, _name = candidates[0]
        return "resolved", geo_id, geo_sk, "only_county"
    return "unmapped", None, None, None


def place_county(
    county_name: str | None,
    state_fips: str | None,
    counties: dict[str, list[tuple[str, int, str]]],
) -> tuple[str, str | None, int | None]:
    """(status, geo_id, geo_sk) for a cost report's county name (see
    :func:`place_county_with_match`)."""
    status, geo_id, geo_sk, _kind = place_county_with_match(
        county_name, state_fips, counties
    )
    return status, geo_id, geo_sk
