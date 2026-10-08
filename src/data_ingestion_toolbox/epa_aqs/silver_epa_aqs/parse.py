"""Parse one captured AirData annual monitor file into monitor-year rows.

Pure: bytes in, typed rows and quarantine records out. Only the registered
pollutant standards at U.S. monitors are in scope; every other row is
counted, not loaded.
Each kept row keeps its event type, completeness and certification beside
the statistic; an empty statistic is missing, never zero. A county is the
row's own FIPS codes, never its name.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from ..client import AqsPayloadError, csv_text
from ..registry import AirDataFile, pollutant_for

_STATE_CODES = frozenset(f"{code:02d}" for code in (*range(1, 57), 60, 66, 69, 72, 78))
#: AQS's codes for monitors outside the United States (80 is Mexico): kept
#: in the raw capture, never read as a state.
FOREIGN_STATE_CODES = frozenset({"80"})
_EVENT_TYPES = frozenset(
    {"No Events", "Events Included", "Events Excluded", "Concurred Events Excluded"}
)


@dataclass(frozen=True)
class MonitorObservation:
    source_row_index: int
    monitor_id: str
    state_fips: str
    county_fips: str
    geo_id: str
    site_number: str
    parameter_code: str
    poc: int
    measure: str
    pollutant_standard: str
    sample_duration: str
    event_type: str
    completeness: str
    certification: str
    observation_count: int
    units: str
    value_source: str
    value: Decimal | None
    value_status: str
    date_of_last_change: date | None
    source_record_id: str


@dataclass(frozen=True)
class AqsQuarantine:
    source_row_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedFile:
    observations: tuple[MonitorObservation, ...]
    quarantined: tuple[AqsQuarantine, ...]
    row_count: int
    in_scope_row_count: int


class _RowError(ValueError):
    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


def parse_file(raw_bytes: bytes, *, item: AirDataFile) -> ParsedFile:
    try:
        text = csv_text(raw_bytes, item)
    except AqsPayloadError as error:
        return ParsedFile(
            (), (AqsQuarantine(0, error.code, f"file refused: {error.code}"),), 0, 0
        )
    reader = csv.DictReader(io.StringIO(text, newline=""))
    observations: list[MonitorObservation] = []
    quarantined: list[AqsQuarantine] = []
    seen: set[tuple[str, ...]] = set()
    row_count = in_scope = 0
    for row in reader:
        row_count += 1
        number = reader.line_num
        pollutant = pollutant_for(
            (row.get("Parameter Code") or "").strip(),
            (row.get("Pollutant Standard") or "").strip(),
        )
        if (
            pollutant is None
            or (row.get("State Code") or "").strip() in FOREIGN_STATE_CODES
        ):
            continue
        in_scope += 1
        try:
            if None in row:
                raise _RowError(
                    "ragged_row", "the row has more columns than the header"
                )
            state, county = row["State Code"].strip(), row["County Code"].strip()
            if state not in _STATE_CODES or len(county) != 3 or not county.isdigit():
                raise _RowError(
                    "unreadable_fips", f"FIPS {state!r}{county!r} is not a county code"
                )
            site = row["Site Num"].strip()
            if len(site) != 4 or not site.isdigit():
                raise _RowError(
                    "unreadable_site", f"Site Num {site!r} is not four digits"
                )
            if int(row["Year"]) != item.year:
                raise _RowError(
                    "wrong_year", f"Year {row['Year']!r} is not the file's {item.year}"
                )
            event = row["Event Type"].strip()
            if event not in _EVENT_TYPES:
                raise _RowError(
                    "unknown_event_type", f"Event Type {event!r} is not registered"
                )
            completeness = row["Completeness Indicator"].strip()
            if completeness not in ("Y", "N"):
                raise _RowError(
                    "unreadable_value",
                    f"Completeness Indicator {completeness!r} is not Y or N",
                )
            poc = int(row["POC"])
            observations_count = int(row["Observation Count"])
            stored = (row[pollutant.statistic_column] or "").strip()
            try:
                value = Decimal(stored) if stored else None
            except InvalidOperation as exc:
                raise _RowError(
                    "unreadable_value",
                    f"{pollutant.statistic_column} {stored!r} is not a number",
                ) from exc
            changed_text = row["Date of Last Change"].strip()
            changed = date.fromisoformat(changed_text) if changed_text else None
        except _RowError as error:
            quarantined.append(AqsQuarantine(number, error.code, str(error)[:200]))
            continue
        except ValueError as exc:
            quarantined.append(
                AqsQuarantine(number, "unreadable_value", str(exc)[:200])
            )
            continue
        monitor_id = f"{state}{county}-{site}-{pollutant.parameter_code}-{poc}"
        key = (
            monitor_id,
            row["Sample Duration"].strip(),
            pollutant.pollutant_standard,
            event,
        )
        if key in seen:
            quarantined.append(
                AqsQuarantine(
                    number, "duplicate_row", f"{monitor_id} {event} appears twice"
                )
            )
            continue
        seen.add(key)
        observations.append(
            MonitorObservation(
                source_row_index=number,
                monitor_id=monitor_id,
                state_fips=state,
                county_fips=county,
                geo_id=f"state:{state}|county:{county}",
                site_number=site,
                parameter_code=pollutant.parameter_code,
                poc=poc,
                measure=pollutant.measure,
                pollutant_standard=pollutant.pollutant_standard,
                sample_duration=row["Sample Duration"].strip(),
                event_type=event,
                completeness=completeness,
                certification=row["Certification Indicator"].strip(),
                observation_count=observations_count,
                units=row["Units of Measure"].strip(),
                value_source=stored,
                value=value,
                value_status="valid" if value is not None else "missing",
                date_of_last_change=changed,
                source_record_id=hashlib.sha256(
                    "|".join(("epa_aqs", str(item.year), *key)).encode()
                ).hexdigest(),
            )
        )
    return ParsedFile(tuple(observations), tuple(quarantined), row_count, in_scope)
