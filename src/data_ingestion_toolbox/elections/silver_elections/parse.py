"""Parse captured election files into typed rows.

Pure: bytes in, typed rows and quarantine records out.

* A congressional term is kept when it ends on or after 1969-01-01. A term
  whose party changed during it (``party_affiliations``) is one span per
  affiliation, so a seat's party on any day is the span covering it.
* A governor term is one Wikidata position statement. Its party is the
  holder's party statement in force on the term's first day (a dated
  statement covering it, else the newest that began before it, else an
  undated one); a holder with no party statement is ``OTH`` with no text.
* A result row is MEDSL's own: one candidate, ballot line and mode in one
  area. Votes that are not a whole number are quarantined, never read as
  zero; an empty total is unknown. A county row with no county code (a
  statewide write-in line, overseas ballots) is quarantined and counted.
* An Algara & Amlani row is one county and contest with the Democratic and
  Republican nominees' votes and the county's total. It becomes one row per
  major-party nominee who stood, plus "(other candidates combined)" for any
  votes left over, so a county's total is still the sum of its rows. Only
  election years from 1990 are read. A county code retired since then is
  read as its successor (``registry.COUNTY_SUCCESSOR``); a merged city's
  votes are added into its county before anything is served.
* A district-to-county answer is the Census Bureau's: one row per county
  piece of one district with its census population. A retired county code
  is read as its successor and its population added into that county's
  piece; Census's delegate district ``98`` (the District of Columbia) is
  read as district 0, the number MEDSL gives a single seat.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

from data_ingestion_toolbox.utility.rdata import RDataError, read_data_frames

from ..registry import (
    CENSUS_DELEGATE_DISTRICT,
    COUNTY_PRESIDENT,
    COUNTY_PRESIDENT_HISTORY,
    COUNTY_SENATE,
    COUNTY_SENATE_HISTORY,
    COUNTY_STREAMS,
    COUNTY_SUCCESSOR,
    FIPS_STATE,
    FIRST_TERM_DAY,
    HOUSE,
    SENATE,
    STATE_FIPS,
    STATE_PRESIDENT,
    STATEWIDE_TOTAL_STREAMS,
    WIKIDATA_PARTIES,
    district_plan,
    party_code,
    result_dataset,
)

_FIRST_DAY = date.fromisoformat(FIRST_TERM_DAY)
_QID = re.compile(r"/entity/(Q[0-9]+)$")
_RESULT_OFFICES = {
    COUNTY_PRESIDENT: "president",
    STATE_PRESIDENT: "president",
    SENATE: "us_senate",
    HOUSE: "us_house",
    COUNTY_PRESIDENT_HISTORY: "president",
    COUNTY_SENATE_HISTORY: "us_senate",
    COUNTY_SENATE: "us_senate",
}


@dataclass(frozen=True)
class SeatTerm:
    record_index: int
    term_key: str
    office: str
    state_code: str
    state_fips: str | None
    district: int | None
    senate_class: int | None
    holder_id: str
    holder_name: str
    party_text: str | None
    party_code: str
    caucus_code: str | None
    start_date: date
    end_date: date | None
    how: str | None
    source_record_id: str

    @property
    def geo_id(self) -> str | None:
        return f"state:{self.state_fips}" if self.state_fips else None


@dataclass(frozen=True)
class CandidateResult:
    record_index: int
    result_key: str
    office: str
    election_year: int
    stage: str
    special: bool
    state_code: str
    state_fips: str | None
    county_fips: str | None
    district: int | None
    candidate: str
    party_text: str | None
    party_code: str
    writein: bool
    mode: str
    candidate_votes: int
    total_votes: int | None
    geo_level: str
    source_record_id: str

    @property
    def geo_id(self) -> str | None:
        if self.geo_level == "county" and self.county_fips:
            return f"state:{self.county_fips[:2]}|county:{self.county_fips[2:]}"
        if self.geo_level == "state" and self.state_fips:
            return f"state:{self.state_fips}"
        return None


@dataclass(frozen=True)
class DistrictCountyPiece:
    """The population of one county's piece of one district in one plan."""

    record_index: int
    plan_key: str
    congress: int
    census_year: int
    state_fips: str
    state_code: str
    district: int
    county_fips: str
    population: int
    source_county_fips: str
    source_record_id: str

    @property
    def geo_id(self) -> str:
        return f"state:{self.county_fips[:2]}|county:{self.county_fips[2:]}"


@dataclass(frozen=True)
class Quarantine:
    record_index: int
    error_code: str
    error_summary: str


class _RowError(ValueError):
    def __init__(self, code: str, summary: str) -> None:
        self.code = code
        super().__init__(summary)


def _sha(*parts: object) -> str:
    return hashlib.sha256(
        "|".join("" if p is None else str(p) for p in parts).encode()
    ).hexdigest()


def _day(text: Any, field: str) -> date:
    try:
        return date.fromisoformat(str(text)[:10])
    except ValueError as exc:
        raise _RowError("unreadable_date", f"{field} {text!r} is not a date") from exc


def _known_day(text: str | None) -> date | None:
    """A Wikidata qualifier date, or None where Wikidata has none to give.

    Wikidata writes an "unknown value" as a blank-node link
    (``http://www.wikidata.org/.well-known/genid/...``) where a date would be.
    """
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Congressional terms.
# ---------------------------------------------------------------------------


def parse_legislators(
    raw_bytes: bytes, *, offset: int = 0
) -> tuple[list[SeatTerm], list[Quarantine]]:
    """Seat-term spans from one congress-legislators file."""
    people = json.loads(raw_bytes.decode("utf-8"))
    terms: list[SeatTerm] = []
    quarantined: list[Quarantine] = []
    index = offset
    for person in people:
        identity = person.get("id", {}) if isinstance(person, dict) else {}
        bioguide = str(identity.get("bioguide") or "")
        name = person.get("name", {}) if isinstance(person, dict) else {}
        holder_name = str(
            name.get("official_full")
            or " ".join(part for part in (name.get("first"), name.get("last")) if part)
        )
        for term in person.get("terms", []) if isinstance(person, dict) else []:
            index += 1
            try:
                if not bioguide or not holder_name:
                    raise _RowError(
                        "unreadable_identity", "a member needs a bioguide id and a name"
                    )
                kind = term.get("type")
                if kind not in ("sen", "rep"):
                    raise _RowError(
                        "unknown_term_type", f"term type {kind!r} is not registered"
                    )
                start = _day(term.get("start"), "start")
                end = _day(term.get("end"), "end")
                if end < _FIRST_DAY:
                    continue
                if end < start:
                    raise _RowError(
                        "dates_reversed", f"term ends {end} before it starts {start}"
                    )
                state = str(term.get("state") or "")
                if not re.fullmatch(r"[A-Z]{2}", state):
                    raise _RowError(
                        "unreadable_state", f"state {state!r} is not a USPS code"
                    )
                district: int | None = None
                senate_class: int | None = None
                if kind == "sen":
                    senate_class = term.get("class")
                    if senate_class not in (1, 2, 3):
                        raise _RowError(
                            "unreadable_seat",
                            f"Senate class {senate_class!r} is not 1, 2 or 3",
                        )
                    office = "us_senate"
                else:
                    district = term.get("district")
                    if (
                        not isinstance(district, int)
                        or isinstance(district, bool)
                        or district < 0
                    ):
                        raise _RowError(
                            "unreadable_seat", f"district {district!r} is not a number"
                        )
                    office = "us_house"
                caucus = term.get("caucus")
                caucus_code = party_code(caucus) if caucus else None
                if caucus_code not in (None, "REP", "DEM"):
                    caucus_code = None
                spans = term.get("party_affiliations") or [
                    {
                        "start": term.get("start"),
                        "end": term.get("end"),
                        "party": term.get("party"),
                    }
                ]
                for span in spans:
                    span_start = _day(span.get("start"), "party_affiliations.start")
                    span_end = _day(span.get("end"), "party_affiliations.end")
                    if span_end < _FIRST_DAY:
                        continue
                    party_text = span.get("party")
                    terms.append(
                        SeatTerm(
                            record_index=index,
                            term_key=_sha(
                                office,
                                bioguide,
                                state,
                                senate_class,
                                district,
                                span_start,
                            ),
                            office=office,
                            state_code=state,
                            state_fips=STATE_FIPS.get(state),
                            district=district,
                            senate_class=senate_class,
                            holder_id=bioguide,
                            holder_name=holder_name,
                            party_text=party_text,
                            party_code=party_code(party_text),
                            caucus_code=caucus_code,
                            start_date=span_start,
                            end_date=span_end,
                            how=term.get("how"),
                            source_record_id=_sha(
                                "elections",
                                "legislators",
                                bioguide,
                                term.get("start"),
                                span_start,
                            ),
                        )
                    )
            except _RowError as error:
                quarantined.append(Quarantine(index, error.code, str(error)[:200]))
    return terms, quarantined


# ---------------------------------------------------------------------------
# Governors.
# ---------------------------------------------------------------------------


def _value(binding: Mapping[str, Any], key: str) -> str | None:
    item = binding.get(key)
    if isinstance(item, dict) and item.get("value") not in (None, ""):
        return str(item["value"])
    return None


def _qid(uri: str | None) -> str | None:
    match = _QID.search(uri or "")
    return match.group(1) if match else None


def _term_party(
    start: date,
    statements: list[tuple[str | None, str | None, date | None, date | None]],
) -> tuple[str | None, str]:
    """(party label, code) in force on ``start``."""
    covering = [
        s
        for s in statements
        if s[2] is not None and s[2] <= start and (s[3] is None or s[3] >= start)
    ]
    before = sorted(
        (s for s in statements if s[2] is not None and s[2] <= start),
        key=lambda s: s[2],
    )
    undated = [s for s in statements if s[2] is None and s[3] is None]
    for group in (covering, before[-1:] if before else [], undated):
        distinct = {(s[0], s[1]) for s in group}
        if len(distinct) == 1:
            label, qid = next(iter(distinct))
            return label, WIKIDATA_PARTIES.get(qid or "", party_code(label))
        if len(distinct) > 1:
            return "; ".join(sorted(str(label) for label, _ in distinct)), "OTH"
    return None, "OTH"


def parse_governors(raw_bytes: bytes) -> tuple[list[SeatTerm], list[Quarantine]]:
    """Governor terms from one Wikidata answer."""
    body = json.loads(raw_bytes.decode("utf-8"))
    bindings = body.get("results", {}).get("bindings", [])
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    quarantined: list[Quarantine] = []
    refused: set[tuple[str, str, str]] = set()
    for position, binding in enumerate(bindings, start=1):
        try:
            person = _qid(_value(binding, "person"))
            office_item = _qid(_value(binding, "position"))
            fips = _value(binding, "stateFips")
            start_text = _value(binding, "start")
            if not person or not office_item or not start_text:
                raise _RowError(
                    "unreadable_identity",
                    "a governor term needs a person, a position and a start",
                )
            fips = (fips or "").zfill(2)
            if fips not in FIPS_STATE:
                raise _RowError(
                    "unreadable_state", f"state FIPS {fips!r} is not a state"
                )
            start = _day(start_text, "start")
            end_text = _value(binding, "end")
            end = _known_day(end_text)
            if end_text and end is None:
                # The term ended on a date Wikidata does not know. It must not
                # read as serving, so the whole term is held back.
                refused.add((person, office_item, start.isoformat()))
                raise _RowError("unreadable_date", f"end {end_text!r} is not a date")
            if end is not None and end < start:
                raise _RowError(
                    "dates_reversed", f"term ends {end} before it starts {start}"
                )
        except _RowError as error:
            quarantined.append(Quarantine(position, error.code, str(error)[:200]))
            continue
        key = (person, office_item, start.isoformat())
        entry = grouped.setdefault(
            key,
            {
                "record_index": position,
                "fips": fips,
                "name": _value(binding, "personLabel") or person,
                "start": start,
                "end": end,
                "parties": set(),
            },
        )
        if end is not None and (entry["end"] is None or end > entry["end"]):
            entry["end"] = end
        party_uri = _value(binding, "party")
        if party_uri:
            party_start = _value(binding, "partyStart")
            party_end = _value(binding, "partyEnd")
            entry["parties"].add(
                (
                    _value(binding, "partyLabel"),
                    _qid(party_uri),
                    # A party statement with an unknown date is kept as
                    # undated on that side; it never aborts the read.
                    _known_day(party_start),
                    _known_day(party_end),
                )
            )
    terms: list[SeatTerm] = []
    for (person, office_item, _start), entry in sorted(
        grouped.items(), key=lambda kv: kv[1]["record_index"]
    ):
        if (person, office_item, _start) in refused:
            continue
        label, code = _term_party(entry["start"], sorted(entry["parties"], key=str))
        state = FIPS_STATE[entry["fips"]]
        terms.append(
            SeatTerm(
                record_index=entry["record_index"],
                term_key=_sha("governor", person, state, entry["start"]),
                office="governor",
                state_code=state,
                state_fips=entry["fips"],
                district=None,
                senate_class=None,
                holder_id=person,
                holder_name=entry["name"],
                party_text=label,
                party_code=code,
                caucus_code=None,
                start_date=entry["start"],
                end_date=entry["end"],
                how=None,
                source_record_id=_sha(
                    "elections", "governors", person, office_item, entry["start"]
                ),
            )
        )
    return terms, quarantined


# ---------------------------------------------------------------------------
# MEDSL results.
# ---------------------------------------------------------------------------

_MISSING = {"", "NA", "N/A", "NULL", "NAN"}


def _text(row: Mapping[str, str], *names: str) -> str | None:
    for name in names:
        value = row.get(name)
        if value is not None and value.strip().upper() not in _MISSING:
            return value.strip()
    return None


def _whole(raw: str | None, field: str) -> int:
    text = (raw or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    if not text.isdigit():
        raise _RowError("unreadable_votes", f"{field} {raw!r} is not a whole number")
    return int(text)


def _flag(raw: str | None) -> bool:
    return (raw or "").strip().upper() in ("TRUE", "T", "1", "YES")


def read_results_table(raw_bytes: bytes) -> list[dict[str, str]]:
    """Rows of a MEDSL file, comma- or tab-delimited, with lower-case headers."""
    text = raw_bytes.decode("utf-8-sig")
    first_line = text.split("\n", 1)[0]
    delimiter = "\t" if first_line.count("\t") > first_line.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    reader.fieldnames = [name.strip().lower() for name in reader.fieldnames or []]
    return list(reader)


def parse_results(
    stream: str, raw_bytes: bytes
) -> tuple[list[CandidateResult], list[Quarantine], int]:
    """(results, quarantine, records read) from one MEDSL file."""
    office = _RESULT_OFFICES[stream]
    rows = read_results_table(raw_bytes)
    results: list[CandidateResult] = []
    quarantined: list[Quarantine] = []
    seen: set[str] = set()
    for index, row in enumerate(rows, start=1):
        try:
            year = _whole(row.get("year"), "year")
            state = _text(row, "state_po") or ""
            if not re.fullmatch(r"[A-Z]{2}", state):
                raise _RowError(
                    "unreadable_state", f"state_po {state!r} is not a USPS code"
                )
            state_fips = STATE_FIPS.get(state)
            county_fips: str | None = None
            source_fips: str | None = None
            district: int | None = None
            if stream in COUNTY_STREAMS:
                geo_level = "county"
                raw_fips = _text(row, "county_fips")
                if raw_fips is None:
                    raise _RowError(
                        "no_county_code",
                        f"{row.get('county_name')!r} has no county code",
                    )
                county_fips = str(_whole(raw_fips, "county_fips")).zfill(5)
                if len(county_fips) != 5:
                    raise _RowError(
                        "no_county_code",
                        f"county_fips {raw_fips!r} is not a county code",
                    )
                if county_fips in COUNTY_SUCCESSOR:
                    # A retired code is read as its successor. Its own total
                    # is not the successor's, so it is dropped: the county's
                    # total becomes the sum of its candidates' votes.
                    source_fips = county_fips
                    county_fips = COUNTY_SUCCESSOR[county_fips]
            elif stream == HOUSE:
                geo_level = "district"
                district = _whole(_text(row, "district") or "", "district")
            else:
                geo_level = "state"
            party_text = _text(row, "party_detailed", "party", "party_simplified")
            candidate = _text(row, "candidate") or "(no candidate named)"
            mode = (_text(row, "mode") or "TOTAL").upper()
            stage = (_text(row, "stage") or "GEN").upper()
            special = _flag(row.get("special"))
            writein = _flag(row.get("writein"))
            votes = _whole(row.get("candidatevotes"), "candidatevotes")
            total_text = _text(row, "totalvotes")
            total = (
                _whole(total_text, "totalvotes")
                if total_text is not None
                and stream not in STATEWIDE_TOTAL_STREAMS
                and source_fips is None
                else None
            )
            key = _sha(
                stream,
                year,
                stage,
                special,
                state,
                source_fips or county_fips,
                district,
                candidate,
                party_text,
                mode,
                writein,
            )
            if key in seen:
                raise _RowError(
                    "duplicate_row",
                    f"{candidate!r} on {party_text!r} appears twice in one contest and mode",
                )
            seen.add(key)
            results.append(
                CandidateResult(
                    record_index=index,
                    result_key=key,
                    office=office,
                    election_year=year,
                    stage=stage,
                    special=special,
                    state_code=state,
                    state_fips=state_fips,
                    county_fips=county_fips,
                    district=district,
                    candidate=candidate,
                    party_text=party_text,
                    party_code=party_code(party_text),
                    writein=writein,
                    mode=mode,
                    candidate_votes=votes,
                    total_votes=total,
                    geo_level=geo_level,
                    source_record_id=_sha("elections", stream, key),
                )
            )
        except _RowError as error:
            quarantined.append(Quarantine(index, error.code, str(error)[:200]))
    return results, quarantined, len(rows)


# ---------------------------------------------------------------------------
# Algara & Amlani county returns.
# ---------------------------------------------------------------------------

#: The columns every Algara & Amlani release frame carries (read on
#: OFFICE_LAB 2026-11-07). The Senate frame adds ``election_id`` and
#: ``seat_class``.
_HISTORY_COLUMNS = (
    "election_year",
    "fips",
    "state",
    "office",
    "election_type",
    "democratic_raw_votes",
    "dem_nominee",
    "republican_raw_votes",
    "rep_nominee",
    "raw_county_vote_totals",
)
_HISTORY_OFFICES = {"PRES": "president", "SEN": "us_senate"}
OTHER_CANDIDATES = "(other candidates combined)"


class HistoryFileError(ValueError):
    """The R file does not hold the registered data frame in its known shape."""


def _history_votes(value: Any, field: str) -> int | None:
    if value is None:
        return None
    number = float(value)
    if number < 0 or not number.is_integer():
        raise _RowError("unreadable_votes", f"{field} {value!r} is not a whole number")
    return int(number)


def _history_frame(stream: str, raw_bytes: bytes) -> dict[str, list[Any]]:
    dataset = result_dataset(stream)
    try:
        frames = read_data_frames(raw_bytes)
    except RDataError as exc:
        raise HistoryFileError(f"{stream}: {exc}") from exc
    frame = next((f for f in frames if f.name == dataset.rdata_object), None)
    if frame is None:
        raise HistoryFileError(f"{stream}: no data frame {dataset.rdata_object}")
    missing = [c for c in _HISTORY_COLUMNS if c not in frame.columns]
    if missing:
        raise HistoryFileError(f"{stream}: columns missing: {', '.join(missing)}")
    return frame.columns


def parse_history(
    stream: str, raw_bytes: bytes, *, first_year: int
) -> tuple[list[CandidateResult], list[Quarantine], int]:
    """(results, quarantine, records read) from one Algara & Amlani file.

    Rows before ``first_year`` are not read and not counted. Two rows that
    map to one county and contest (a merged city and its county) are summed;
    two identical rows keep one and set the repeat aside as ``repeated_row``;
    two rows for one county and contest from the same code that disagree are
    both quarantined as ``conflicting_row``.
    """
    office = _RESULT_OFFICES[stream]
    columns = _history_frame(stream, raw_bytes)
    count = len(columns["fips"])
    seat_classes = columns.get("seat_class") or [None] * count
    quarantined: list[Quarantine] = []
    #: contest key -> source code -> (record index, row tuple)
    contests: dict[tuple, dict[str, list[tuple[int, tuple]]]] = {}
    read = 0
    for position in range(count):
        index = position + 1
        year_value = columns["election_year"][position]
        if year_value is None or float(year_value) < first_year:
            continue
        read += 1
        try:
            year = int(float(year_value))
            row_office = _HISTORY_OFFICES.get(str(columns["office"][position] or ""))
            if row_office != office:
                raise _RowError(
                    "unknown_office",
                    f"office {columns['office'][position]!r} is not {office}",
                )
            state = str(columns["state"][position] or "")
            if state not in STATE_FIPS:
                raise _RowError(
                    "unreadable_state", f"state {state!r} is not a USPS code"
                )
            source_fips = str(columns["fips"][position] or "").strip()
            if not re.fullmatch(r"[0-9]{5}", source_fips):
                raise _RowError(
                    "no_county_code", f"fips {source_fips!r} is not a county code"
                )
            if source_fips[:2] != STATE_FIPS[state]:
                raise _RowError(
                    "unreadable_state",
                    f"fips {source_fips} is not in {state}",
                )
            election_type = str(columns["election_type"][position] or "").upper()
            if election_type not in ("G", "S"):
                raise _RowError(
                    "unknown_election_type",
                    f"election_type {election_type!r} is not G or S",
                )
            democratic = _history_votes(
                columns["democratic_raw_votes"][position], "democratic_raw_votes"
            )
            republican = _history_votes(
                columns["republican_raw_votes"][position], "republican_raw_votes"
            )
            total = _history_votes(
                columns["raw_county_vote_totals"][position], "raw_county_vote_totals"
            )
            if democratic is None and republican is None:
                raise _RowError(
                    "no_major_party_votes", "neither major party's votes are given"
                )
            major = (democratic or 0) + (republican or 0)
            if total is not None and total < major:
                raise _RowError(
                    "total_below_parties",
                    f"total {total} is below the major parties' {major}",
                )
        except (_RowError, TypeError, ValueError) as error:
            code = error.code if isinstance(error, _RowError) else "unreadable_row"
            quarantined.append(Quarantine(index, code, str(error)[:200]))
            continue
        county = COUNTY_SUCCESSOR.get(source_fips, source_fips)
        seat_class = seat_classes[position]
        key = (year, election_type == "S", str(seat_class or ""), state, county)
        row = (
            democratic,
            columns["dem_nominee"][position],
            republican,
            columns["rep_nominee"][position],
            total,
        )
        contests.setdefault(key, {}).setdefault(source_fips, []).append((index, row))

    results: list[CandidateResult] = []
    for key, sources in contests.items():
        year, special, seat_class, state, county = key
        kept: list[tuple[int, tuple]] = []
        refused = False
        for source_fips, rows in sources.items():
            first_index, first_row = rows[0]
            for repeat_index, repeat_row in rows[1:]:
                if repeat_row == first_row:
                    quarantined.append(
                        Quarantine(
                            repeat_index,
                            "repeated_row",
                            f"{source_fips} {year} repeats row {first_index}",
                        )
                    )
                else:
                    refused = True
            if refused:
                quarantined.extend(
                    Quarantine(
                        i,
                        "conflicting_row",
                        f"{source_fips} {year} has rows that disagree",
                    )
                    for i, _row in rows
                )
                break
            kept.append((first_index, first_row))
        if refused:
            continue
        democratic = _sum_known(r[0] for _i, r in kept)
        republican = _sum_known(r[2] for _i, r in kept)
        totals = [r[4] for _i, r in kept]
        total = None if any(t is None for t in totals) else sum(totals)
        record_index = min(i for i, _r in kept)
        source_codes = "+".join(sorted(sources))
        lines: list[tuple[str, str | None, int]] = []
        if democratic is not None:
            lines.append((_nominee(kept, 1), "Democratic", democratic))
        if republican is not None:
            lines.append((_nominee(kept, 3), "Republican", republican))
        if total is not None:
            others = total - (democratic or 0) - (republican or 0)
            if others > 0:
                lines.append((OTHER_CANDIDATES, None, others))
        for candidate, party_text, votes in lines:
            result_key = _sha(
                stream, year, "GEN", special, seat_class, state, county, candidate,
                party_text,
            )  # fmt: skip
            results.append(
                CandidateResult(
                    record_index=record_index,
                    result_key=result_key,
                    office=office,
                    election_year=year,
                    stage="GEN",
                    special=special,
                    state_code=state,
                    state_fips=STATE_FIPS[state],
                    county_fips=county,
                    district=None,
                    candidate=candidate,
                    party_text=party_text,
                    party_code=party_code(party_text),
                    writein=False,
                    mode="TOTAL",
                    candidate_votes=votes,
                    total_votes=total,
                    geo_level="county",
                    source_record_id=_sha(
                        "elections", stream, source_codes, result_key
                    ),
                )
            )
    return results, quarantined, read


def _sum_known(values: Any) -> int | None:
    numbers = list(values)
    if all(n is None for n in numbers):
        return None
    return sum(n or 0 for n in numbers)


def _nominee(kept: list[tuple[int, tuple]], position: int) -> str:
    names = {str(r[position]).strip() for _i, r in kept if r[position]}
    if len(names) == 1:
        return names.pop()
    return "(nominee not named)" if not names else "; ".join(sorted(names))


def parse_district_counties(
    file_key: str, raw_bytes: bytes
) -> tuple[list[DistrictCountyPiece], list[Quarantine], int]:
    """One district's county pieces from a Census API answer; (pieces, quarantine, rows read).

    ``file_key`` is ``<plan>/<state FIPS>/<district>`` as the capture named it.
    """
    from ..client import census_table

    plan_key, state_fips, district_code = file_key.split("/")
    plan = district_plan(plan_key)
    rows = census_table(raw_bytes, file_key, "county (or part)")
    quarantine: list[Quarantine] = []
    merged: dict[str, tuple[int, int, str]] = {}
    for index, row in enumerate(rows, start=1):
        state = FIPS_STATE.get(row.get("state", ""))
        county = row.get("county (or part)", "")
        district = row.get("congressional district", "")
        raw_population = row.get(plan.population_variable, "")
        if state is None or row.get("state") != state_fips:
            quarantine.append(
                Quarantine(
                    index, "unreadable_state", "state code is not the one asked for"
                )
            )
            continue
        if district != district_code:
            quarantine.append(
                Quarantine(
                    index, "unexpected_district", "district is not the one asked for"
                )
            )
            continue
        if not (county.isdigit() and len(county) == 3):
            quarantine.append(
                Quarantine(index, "no_county_code", "county code is not three digits")
            )
            continue
        if not raw_population.isdigit():
            quarantine.append(
                Quarantine(
                    index, "unreadable_population", "population is not a whole number"
                )
            )
            continue
        source_fips = state_fips + county
        target = COUNTY_SUCCESSOR.get(source_fips, source_fips)
        population = int(raw_population)
        if target in merged:
            first_index, total, first_source = merged[target]
            merged[target] = (
                first_index,
                total + population,
                min(first_source, source_fips),
            )
        else:
            merged[target] = (index, population, source_fips)
    number = 0 if district_code == CENSUS_DELEGATE_DISTRICT else int(district_code)
    pieces = [
        DistrictCountyPiece(
            record_index=first_index,
            plan_key=plan.key,
            congress=plan.congress,
            census_year=plan.census_year,
            state_fips=state_fips,
            state_code=FIPS_STATE[state_fips],
            district=number,
            county_fips=county_fips,
            population=population,
            source_county_fips=source_fips,
            source_record_id=_sha(plan.key, state_fips, district_code, county_fips),
        )
        for county_fips, (first_index, population, source_fips) in sorted(
            merged.items()
        )
    ]
    return pieces, quarantine, len(rows)
