"""Parse the captured normals archive into stations and their annual normals.

Pure: bytes in, typed rows and quarantine records out. Each station file
has one row. A variable the station does not measure has no column and no
row; a withheld value (``M``, ``V``, ``Y``) keeps its flag and no number; an
``X`` (nonzero, rounded to zero) keeps the published zero with its flag, so
it is never read as a true zero. A sentinel number (``-9999`` and kin) with
no withholding flag quarantines the station file rather than becoming a
value. Coordinates are kept for county assignment;
the station's name is never used for identity.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from ..client import NormalsPayloadError, station_files
from ..registry import (
    COMPLETENESS_FLAGS,
    KEPT_FLAGS,
    SENTINEL_VALUES,
    STATION_COLUMNS,
    VARIABLES,
    WITHHELD_FLAGS,
)


@dataclass(frozen=True)
class Station:
    member_index: int
    station_id: str
    latitude: Decimal
    longitude: Decimal
    elevation_m: Decimal | None
    name: str


@dataclass(frozen=True)
class NormalsObservation:
    member_index: int
    station_id: str
    variable: str
    measure: str
    value_source: str
    value: Decimal | None
    value_status: str
    missing_reason: str | None
    measurement_flag: str | None
    completeness_flag: str | None
    years: int | None


@dataclass(frozen=True)
class NormalsQuarantine:
    member_index: int
    error_code: str
    error_summary: str


@dataclass(frozen=True)
class ParsedArchive:
    stations: tuple[Station, ...]
    observations: tuple[NormalsObservation, ...]
    quarantined: tuple[NormalsQuarantine, ...]
    member_count: int


class _RowError(ValueError):
    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


def _decimal(text: str, field: str) -> Decimal:
    try:
        return Decimal(text.strip())
    except InvalidOperation as exc:
        raise _RowError(
            "unreadable_value", f"{field} {text!r} is not a number"
        ) from exc


def parse_archive(raw_bytes: bytes) -> ParsedArchive:
    stations: list[Station] = []
    observations: list[NormalsObservation] = []
    quarantined: list[NormalsQuarantine] = []
    seen: set[str] = set()
    count = 0
    try:
        for name, content in station_files(raw_bytes):
            count += 1
            index = count
            try:
                rows = list(csv.DictReader(io.StringIO(content.decode("utf-8"))))
                if len(rows) != 1:
                    raise _RowError(
                        "unexpected_rows", f"{name} has {len(rows)} rows, not one"
                    )
                row = rows[0]
                if not STATION_COLUMNS <= set(row) or None in row:
                    raise _RowError(
                        "unexpected_header", f"{name} lacks the station columns"
                    )
                station_id = row["STATION"].strip()
                if not station_id or f"{station_id}.csv" != name.rsplit("/", 1)[-1]:
                    raise _RowError(
                        "unreadable_station", f"{name} names station {station_id!r}"
                    )
                if station_id in seen:
                    raise _RowError("duplicate_station", f"{station_id} appears twice")
                latitude = _decimal(row["LATITUDE"], "LATITUDE")
                longitude = _decimal(row["LONGITUDE"], "LONGITUDE")
                if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                    raise _RowError(
                        "unreadable_value", f"{station_id} coordinates are out of range"
                    )
                elevation = row["ELEVATION"].strip()
                station = Station(
                    index,
                    station_id,
                    latitude,
                    longitude,
                    _decimal(elevation, "ELEVATION") if elevation else None,
                    row["NAME"].strip(),
                )
                kept: list[NormalsObservation] = []
                for variable in VARIABLES:
                    if variable.variable not in row:
                        continue
                    stored = (row[variable.variable] or "").strip()
                    flag = (row.get(f"meas_flag_{variable.variable}") or "").strip()
                    completeness = (
                        row.get(f"comp_flag_{variable.variable}") or ""
                    ).strip()
                    years_text = (row.get(f"years_{variable.variable}") or "").strip()
                    if flag not in WITHHELD_FLAGS and flag not in KEPT_FLAGS:
                        raise _RowError(
                            "unknown_flag",
                            f"{station_id} {variable.variable} flag {flag!r}",
                        )
                    if completeness and completeness not in COMPLETENESS_FLAGS:
                        raise _RowError(
                            "unknown_flag",
                            f"{station_id} {variable.variable} completeness {completeness!r}",
                        )
                    if flag in WITHHELD_FLAGS:
                        status, reason = WITHHELD_FLAGS[flag]
                        value = None
                    elif stored:
                        status, reason, value = (
                            "valid",
                            None,
                            _decimal(stored, variable.variable),
                        )
                        if value in SENTINEL_VALUES:
                            raise _RowError(
                                "sentinel_value",
                                f"{station_id} {variable.variable} is {stored} without a withholding flag",
                            )
                    else:
                        status, reason, value = "missing", "blank", None
                    kept.append(
                        NormalsObservation(
                            index,
                            station_id,
                            variable.variable,
                            variable.measure,
                            stored,
                            value,
                            status,
                            reason,
                            flag or None,
                            completeness or None,
                            int(years_text) if years_text.isdigit() else None,
                        )
                    )
            except _RowError as error:
                quarantined.append(
                    NormalsQuarantine(index, error.code, str(error)[:200])
                )
                continue
            except UnicodeDecodeError:
                quarantined.append(
                    NormalsQuarantine(index, "undecodable", f"{name} is not UTF-8")
                )
                continue
            seen.add(station.station_id)
            stations.append(station)
            observations.extend(kept)
    except NormalsPayloadError as error:
        return ParsedArchive(
            (),
            (),
            (NormalsQuarantine(0, error.code, f"archive refused: {error.code}"),),
            0,
        )
    return ParsedArchive(
        tuple(stations), tuple(observations), tuple(quarantined), count
    )
