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
* A precinct row (MEDSL's 2024 Senate file) is one candidate, ballot line
  and mode in one precinct. Each line's precinct count is its TOTAL row
  where it has one, else the sum of its modes; the lines are then summed
  into the county, one row per candidate and line with no total of its own.
  A suppressed count (``*``) is quarantined, never zero.
"""

from __future__ import annotations

import bisect
import csv
import hashlib
import io
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

from data_ingestion_toolbox.utility.rdata import RDataError, read_data_frames

from ..registry import (
    CENSUS_DELEGATE_DISTRICT,
    COUNTY_PRESIDENT,
    COUNTY_PRESIDENT_HISTORY,
    COUNTY_SENATE,
    COUNTY_SENATE_HISTORY,
    COUNTY_SENATE_PRECINCT,
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
    COUNTY_SENATE_PRECINCT: "us_senate",
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
    #: Where the row was, when the row says: for auditing quarantine by place.
    state_code: str | None = None
    county_fips: str | None = None


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


#: Candidate names MEDSL uses for a line that is the area's total, not a
#: candidate (its 2024 county presidential file adds "TOTAL VOTES CAST" to
#: every county; its 2022 county Senate file a "TOTAL" line per mode).
TOTAL_LINE_CANDIDATES = frozenset({"TOTAL VOTES CAST", "TOTAL VOTES", "TOTAL"})


#: Ballot lines MEDSL publishes beside the candidates that are not votes for
#: anyone: over-, under-, blank and spoiled ballots. "SCATTERING" is
#: write-ins (votes) and stays; a line mixing the two ("BLANK
#: VOTE/SCATTERING") stays too, as Other.
NOT_A_VOTE_CANDIDATES = frozenset(
    {
        "OVERVOTES",
        "OVER VOTES",
        "OVER VOTE",
        "UNDERVOTES",
        "UNDER VOTES",
        "UNDER VOTE",
        "BLANK",
        "BLANKS",
        "BLANK VOTE",
        "BLANK VOTES",
        "SPOILED",
        "VOID",
    }
)
#: MEDSL's Alaska "counties" are its state House districts (02001-02040) and
#: an overseas line (02099) in every county presidential year (read on
#: OFFICE_LAB 2026-11-07); three of the codes are also real boroughs'.
#: States whose repeated MEDSL key is another round of the same race, not a
#: part of one count (Louisiana's open primary and runoff, 2002 Senate).
REPEAT_IS_ANOTHER_ROUND = frozenset({"LA"})
ALASKA_DISTRICT_CODES = frozenset(
    {f"020{number:02d}" for number in range(1, 41)} | {"02099"}
)


def _is_total_line(candidate: str, party_text: str | None) -> bool:
    return (
        candidate.strip().upper() in TOTAL_LINE_CANDIDATES
        and party_code(party_text) == "OTH"
    )


def _is_not_a_vote(candidate: str, party_text: str | None) -> bool:
    return (
        candidate.strip().upper() in NOT_A_VOTE_CANDIDATES
        and party_code(party_text) == "OTH"
    )


def parse_results(
    stream: str, raw_bytes: bytes
) -> tuple[list[CandidateResult], list[Quarantine], int]:
    """(results, quarantine, records read) from one MEDSL file."""
    office = _RESULT_OFFICES[stream]
    rows = read_results_table(raw_bytes)
    results: list[CandidateResult] = []
    quarantined: list[Quarantine] = []
    seen: dict[str, tuple[int, int | None]] = {}
    conflicting: set[str] = set()
    total_line_keys: dict[str, tuple[int, tuple[Any, ...]]] = {}
    # A total line's votes, by contest, area and mode: never a candidate, it
    # is the total of the candidate rows beside it where they carry none.
    total_lines: dict[tuple[Any, ...], int] = {}
    retired: set[int] = set()
    positions: dict[str, int] = {}
    for index, row in enumerate(rows, start=1):
        state: str | None = None
        county_fips: str | None = None
        try:
            year = _whole(row.get("year"), "year")
            state = _text(row, "state_po") or ""
            if not re.fullmatch(r"[A-Z]{2}", state):
                raise _RowError(
                    "unreadable_state", f"state_po {state!r} is not a USPS code"
                )
            state_fips = STATE_FIPS.get(state)
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
                if state == "AK" and county_fips in ALASKA_DISTRICT_CODES:
                    raise _RowError(
                        "not_a_county",
                        f"Alaska code {county_fips} is a state House district, not a borough",
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
            if _is_not_a_vote(candidate, party_text):
                raise _RowError(
                    "not_a_vote", f"{candidate!r} is a ballot count, not a candidate"
                )
            mode = (_text(row, "mode") or "TOTAL").upper()
            stage = (_text(row, "stage") or "GEN").upper()
            if _flag(row.get("runoff")) and "RUNOFF" not in stage:
                # MEDSL's House file marks a runoff in its own column.
                stage = f"{stage} RUNOFF"
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
            is_total_line = _is_total_line(candidate, party_text)
            if key in seen:
                if state in REPEAT_IS_ANOTHER_ROUND:
                    # Louisiana's November open primary and December runoff
                    # share one key in MEDSL's files: an identical repeat
                    # keeps one copy, copies that disagree are all set aside.
                    if seen[key] != (votes, total):
                        conflicting.add(key)
                        raise _RowError(
                            "conflicting_row",
                            f"{candidate!r} on {party_text!r} appears twice in one "
                            "contest and mode with different votes",
                        )
                    raise _RowError(
                        "duplicate_row",
                        f"{candidate!r} on {party_text!r} appears twice in one "
                        "contest and mode",
                    )
                # Elsewhere a repeated key is a part of the line's count:
                # precincts under a county (Indiana 2022), a mode MEDSL left
                # blank (North Carolina 2024), or several write-in and
                # "OTHER" lines folded into one name. The parts are added.
                if is_total_line:
                    if key in total_line_keys:
                        area = total_line_keys[key][1]
                        total_lines[area] = total_lines[area] + votes
                    continue
                position = positions.get(key)
                if position is not None:
                    first = results[position]
                    results[position] = replace(
                        first,
                        candidate_votes=first.candidate_votes + votes,
                        total_votes=max(
                            (t for t in (first.total_votes, total) if t is not None),
                            default=None,
                        ),
                    )
                continue
            seen[key] = (votes, total)
            if is_total_line:
                if source_fips is None:
                    area = (year, stage, special, state, county_fips, district, mode)
                    total_lines[area] = votes
                    total_line_keys[key] = (index, area)
                continue
            if source_fips is not None:
                retired.add(index)
            positions[key] = len(results)
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
            quarantined.append(
                Quarantine(
                    index,
                    error.code,
                    str(error)[:200],
                    state if state and re.fullmatch(r"[A-Z]{2}", state) else None,
                    county_fips
                    if county_fips and re.fullmatch(r"[0-9]{5}", county_fips)
                    else None,
                )
            )
    if conflicting:
        for result in results:
            if result.result_key in conflicting:
                quarantined.append(
                    Quarantine(
                        result.record_index,
                        "conflicting_row",
                        f"{result.candidate!r} on {result.party_text!r} appears twice "
                        "in one contest and mode with different votes",
                        result.state_code,
                        result.county_fips,
                    )
                )
        results = [r for r in results if r.result_key not in conflicting]
        for key, (index, area) in total_line_keys.items():
            if key in conflicting:
                total_lines.pop(area, None)
                quarantined.append(
                    Quarantine(index, "conflicting_row", "a total line appears twice")
                )
        quarantined.sort(key=lambda entry: (entry.record_index, entry.error_code))
    if total_lines:
        results = [
            replace(result, total_votes=total_lines.get(_area_mode(result)))
            if result.total_votes is None
            and result.record_index not in retired
            and _area_mode(result) in total_lines
            else result
            for result in results
        ]
    return results, quarantined, len(rows)


def _area_mode(result: CandidateResult) -> tuple[Any, ...]:
    return (
        result.election_year,
        result.stage,
        result.special,
        result.state_code,
        result.county_fips,
        result.district,
        result.mode,
    )


# ---------------------------------------------------------------------------
# MEDSL precinct returns, summed by county.
# ---------------------------------------------------------------------------

#: Lines of MEDSL's 2024 Senate precinct file that are a count of ballots or
#: of the contest, not of a candidate (read on OFFICE_LAB 2026-11-08: Maine's
#: "TOTAL BALLOTS CAST" counts undervotes too; New Jersey's "CONTEST TOTAL").
#: A county's total is the sum of its candidates' votes instead.
PRECINCT_TOTAL_LINES = TOTAL_LINE_CANDIDATES | {"TOTAL BALLOTS CAST", "CONTEST TOTAL"}
#: Ballot lines that are no one's votes, beyond ``NOT_A_VOTE_CANDIDATES``.
PRECINCT_NOT_A_VOTE = NOT_A_VOTE_CANDIDATES | {
    "UNDERVOTES-VOIDS",
    "INVALID",
    "INVALID VOTES",
}
#: The count MEDSL prints where a state suppressed a small precinct's count
#: (California, Nevada, New Mexico 2024).
SUPPRESSED_COUNT = "*"
#: Reporting units that count votes from parts of several counties without
#: saying how many came from each. Where the file carries one, the counties
#: it spans are not served: summed without it, Jackson County would read as if
#: Kansas City had not voted. Missouri's Kansas City election board spans
#: Cass, Clay, Jackson and Platte counties.
SPANNING_UNITS: dict[tuple[str, str], tuple[str, ...]] = {
    ("MO", "36000"): ("29037", "29047", "29095", "29165"),
}


def parse_precinct_results(
    stream: str, raw_bytes: bytes
) -> tuple[list[CandidateResult], list[Quarantine], int]:
    """(county results, quarantine, records read) from one MEDSL precinct file.

    One pass, holding one running sum per precinct line and per county line,
    so the 166 MB file is never held as a list of rows.
    """
    office = _RESULT_OFFICES[stream]
    text = raw_bytes.decode("utf-8-sig")
    first_line = text.split("\n", 1)[0]
    delimiter = "\t" if first_line.count("\t") > first_line.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    reader.fieldnames = [name.strip().lower() for name in reader.fieldnames or []]
    quarantined: list[Quarantine] = []
    # The contest each (state, special) names: one office per pair.
    contest_office: dict[tuple[str, bool], str] = {}
    # (county line key) -> [first record index, TOTAL votes, mode votes,
    # whether a TOTAL row was seen], per precinct.
    precinct_lines: dict[tuple[Any, ...], list[Any]] = {}
    # (line, place, mode) -> [rows, vote sum, largest row, its precinct, its
    # record index], to find a county total printed as a precinct.
    by_mode_line: dict[tuple[Any, ...], list[Any]] = {}
    # (year, state) -> counties a spanning reporting unit in the file covers.
    spanned: dict[tuple[int, str], set[str]] = {}
    read = 0
    for index, row in enumerate(reader, start=1):
        read = index
        state: str | None = None
        county_fips: str | None = None
        try:
            year = _whole(row.get("year"), "year")
            state = _text(row, "state_po") or ""
            if not re.fullmatch(r"[A-Z]{2}", state):
                raise _RowError(
                    "unreadable_state", f"state_po {state!r} is not a USPS code"
                )
            office_text = (_text(row, "office") or "").upper()
            if office_text.startswith("SHADOW"):
                raise _RowError(
                    "not_a_senate_seat",
                    f"{office_text!r} is the District of Columbia's shadow seat",
                )
            special = _flag(row.get("special"))
            named = contest_office.setdefault((state, special), office_text)
            if named != office_text:
                raise _RowError(
                    "second_contest",
                    f"{office_text!r} beside {named!r} in one state and special flag",
                )
            raw_fips = _text(row, "county_fips")
            if raw_fips is None:
                raise _RowError(
                    "no_county_code", f"{row.get('county_name')!r} has no county code"
                )
            county_fips = str(_whole(raw_fips, "county_fips")).zfill(5)
            if len(county_fips) != 5 or county_fips[:2] != STATE_FIPS.get(state):
                # Missouri's Kansas City board ("36000") counts parts of four
                # counties without saying which: not apportioned, and the
                # counties it spans are not served (SPANNING_UNITS).
                spans = SPANNING_UNITS.get((state, county_fips))
                if spans:
                    spanned.setdefault((year, state), set()).update(spans)
                raise _RowError(
                    "not_a_county",
                    f"county_fips {raw_fips!r} is not a county of {state}",
                )
            if state == "AK" and county_fips in ALASKA_DISTRICT_CODES:
                raise _RowError(
                    "not_a_county",
                    f"Alaska code {county_fips} is a state House district, not a borough",
                )
            county_fips = COUNTY_SUCCESSOR.get(county_fips, county_fips)
            party_text = _text(row, "party_detailed", "party_simplified")
            candidate = _text(row, "candidate") or "(no candidate named)"
            if candidate.upper() in PRECINCT_TOTAL_LINES:
                # New Jersey labels its "CONTEST TOTAL" line NONPARTISAN.
                continue
            if (
                candidate.upper() in PRECINCT_NOT_A_VOTE
                and party_code(party_text) != "REP"
                and party_code(party_text) != "DEM"
            ):
                raise _RowError(
                    "not_a_vote", f"{candidate!r} is a ballot count, not a candidate"
                )
            raw_votes = (row.get("votes") or "").strip()
            if raw_votes == SUPPRESSED_COUNT:
                raise _RowError(
                    "suppressed_count", "the state suppressed this precinct's count"
                )
            votes = _whole(raw_votes, "votes")
            stage = (_text(row, "stage") or "GEN").upper()
            writein = _flag(row.get("writein"))
            mode = (_text(row, "mode") or "TOTAL").upper()
            line = (
                year,
                stage,
                special,
                state,
                county_fips,
                candidate,
                party_text,
                writein,
            )
            precinct = (
                line,
                _text(row, "jurisdiction_fips") or "",
                _text(row, "precinct") or "",
            )
            held = precinct_lines.get(precinct)
            if held is None:
                held = precinct_lines[precinct] = [index, 0, 0, False]
            seen = by_mode_line.get((line, precinct[1], mode))
            if seen is None:
                by_mode_line[(line, precinct[1], mode)] = [
                    1,
                    votes,
                    votes,
                    precinct[2],
                    index,
                ]
            else:
                seen[0] += 1
                seen[1] += votes
                if votes > seen[2]:
                    seen[2:5] = [votes, precinct[2], index]
            if mode == "TOTAL":
                held[1] += votes
                held[3] = True
            else:
                held[2] += votes
        except _RowError as error:
            quarantined.append(
                Quarantine(
                    index,
                    error.code,
                    str(error)[:200],
                    state if state and re.fullmatch(r"[A-Z]{2}", state) else None,
                    county_fips
                    if county_fips and re.fullmatch(r"[0-9]{5}", county_fips)
                    else None,
                )
            )
    # Indiana's Hendricks County prints its county total as one more row of
    # a real precinct ("1229-Washington 29", in every mode). A row that is
    # the sum of every other row of its county, for every candidate line of
    # one mode and in the same precinct, is that total and is taken out.
    by_area_mode: dict[tuple[Any, ...], list[tuple[Any, ...]]] = {}
    for (line, place, mode), seen in by_mode_line.items():
        by_area_mode.setdefault((*line[:5], place, mode), []).append((line, seen))
    for (*area, place, mode), lines in by_area_mode.items():
        counted = [(line, seen) for line, seen in lines if seen[1] > 0]
        # Only a county of many rows, whose total is no small number: a town
        # of three precincts can split 2, 1, 1 by chance.
        if (
            not counted
            or any(seen[0] < 10 for _line, seen in counted)
            or max(seen[2] for _line, seen in counted) < 100
        ):
            continue
        precincts = {seen[3] for _line, seen in counted}
        if len(precincts) != 1 or not all(
            abs(2 * seen[2] - seen[1]) <= 1 for _line, seen in counted
        ):
            continue
        (name,) = precincts
        for line, seen in counted:
            held = precinct_lines[(line, place, name)]
            held[1 if mode == "TOTAL" else 2] -= seen[2]
        first = min(seen[4] for _line, seen in counted)
        quarantined.append(
            Quarantine(
                first,
                "county_total_row",
                f"a {mode.lower()} row of precinct {name!r} is the sum of every "
                "other precinct in its county",
                area[3],
                area[4],
            )
        )
    del by_mode_line, by_area_mode
    # Six New Jersey counties leave a candidate's party blank where the
    # rest of the state names it. A blank-party line takes the one party
    # its candidate carries elsewhere in the same state's contest, so Kim
    # is a Democrat in Mercer as he is in Bergen; a candidate with no party
    # or with several keeps the blank.
    parties: dict[tuple[Any, ...], set[str]] = {}
    for line, _place, _precinct in precinct_lines:
        if line[6] is not None:
            key = (*line[:4], line[5], line[7])
            parties.setdefault(key, set()).add(line[6])
    relabelled: dict[tuple[Any, ...], list[Any]] = {}
    for (line, place, precinct), held in precinct_lines.items():
        if line[6] is None:
            named = parties.get((*line[:4], line[5], line[7]), set())
            if len(named) == 1:
                line = (*line[:6], next(iter(named)), line[7])
        into = relabelled.get((line, place, precinct))
        if into is None:
            relabelled[(line, place, precinct)] = list(held)
        else:
            into[0] = min(into[0], held[0])
            into[1] += held[1]
            into[2] += held[2]
            into[3] = into[3] or held[3]
    precinct_lines = relabelled
    # A line's precinct count is its TOTAL row where it has one (New
    # Jersey, Connecticut, Delaware, Rhode Island and New York print both),
    # else its modes' sum.
    by_area: dict[tuple[Any, ...], dict[str, dict[tuple[Any, ...], int]]] = {}
    first_row: dict[tuple[Any, ...], int] = {}
    for (line, place, precinct), (
        first,
        total,
        by_mode,
        has_total,
    ) in precinct_lines.items():
        area = (*line[:5], place)
        by_area.setdefault(area, {}).setdefault(precinct, {})[line] = (
            total if has_total else by_mode
        )
        first_row[(area, precinct)] = min(first_row.get((area, precinct), first), first)
    # New Jersey's file also prints each town's total as a "precinct" named
    # for the town ("Alpine" beside "Alpine 1" and "Alpine Provisional"). A
    # precinct whose every line equals the sum of the precincts named after
    # it is such a subtotal and is set aside, so no vote is counted twice.
    # Another town whose name begins with this one ("Englewood Cliffs"
    # under "Englewood") is not part of it, and a town total one vote off
    # its precincts (Rockleigh, 63 beside 62) is still its total.
    subtotals: set[tuple[Any, ...]] = set()
    for area, precincts in by_area.items():
        names = sorted(precincts)
        for position, name in enumerate(names):
            prefix = name + " "
            descendants = []
            for later in names[bisect.bisect_left(names, prefix, lo=position + 1) :]:
                if not later.startswith(prefix):
                    break
                descendants.append(later)
            if not descendants:
                continue
            own = precincts[name]
            if not any(own.values()):
                continue
            # Two readings of "the precincts under this name": every precinct
            # directly under it, counting a nested name ("Linden Ward 1"
            # under "Linden") by its own count; or only the precincts that
            # name nothing beneath them, leaving out a nested name and
            # everything under it ("Englewood Cliffs" under "Englewood").
            nested = {
                other
                for other in descendants
                if any(later.startswith(other + " ") for later in descendants)
            }
            top = [
                child
                for child in descendants
                if not any(child.startswith(town + " ") for town in nested)
            ]
            leaves = [child for child in top if child not in nested]

            def adds_up(children: list[str], own: dict[Any, int] = own) -> bool:
                if not children:
                    return False
                lines = set(own).union(*(precincts[child] for child in children))
                return all(
                    abs(
                        own.get(line, 0)
                        - sum(precincts[child].get(line, 0) for child in children)
                    )
                    <= 1
                    for line in lines
                )

            if adds_up(top) or adds_up(leaves):
                subtotals.add((area, name))
    # Indiana's St. Joseph County prints every precinct twice, by its code
    # ("110004") and by its name ending in that code ("PENN TWP 4 110004").
    # A precinct whose name ends in another precinct's name, with the same
    # count on every line that has votes, is that precinct again and is set
    # aside (the longer copy lists two write-in lines at zero).
    repeats: set[tuple[Any, ...]] = set()
    for area, precincts in by_area.items():
        for name, own in precincts.items():
            if " " not in name:
                continue
            shorter = name.rsplit(" ", 1)[1]
            other = precincts.get(shorter)
            counted = {line: votes for line, votes in own.items() if votes}
            if (
                other is not None
                and counted
                and counted == {line: votes for line, votes in other.items() if votes}
            ):
                repeats.add((area, name))
    for area, name in sorted(repeats, key=lambda key: first_row[key]):
        quarantined.append(
            Quarantine(
                first_row[(area, name)],
                "repeated_precinct",
                f"precinct {name!r} repeats precinct {name.rsplit(' ', 1)[1]!r}",
                area[3],
                area[4],
            )
        )
    subtotals |= repeats
    for area, name in sorted(subtotals - repeats, key=lambda key: first_row[key]):
        quarantined.append(
            Quarantine(
                first_row[(area, name)],
                "subtotal_row",
                f"precinct {name!r} is the sum of the precincts named after it",
                area[3],
                area[4],
            )
        )
    # Then the precincts are added into the county, leaving out a county a
    # spanning reporting unit covers.
    counties: dict[tuple[Any, ...], list[int]] = {}
    withheld: dict[tuple[Any, ...], int] = {}
    for (line, place, precinct), (
        first,
        total,
        by_mode,
        has_total,
    ) in precinct_lines.items():
        if ((*line[:5], place), precinct) in subtotals:
            continue
        if line[4] in spanned.get((line[0], line[3]), ()):
            county_key = line[:5]
            withheld[county_key] = min(withheld.get(county_key, first), first)
            continue
        held_county = counties.get(line)
        count = total if has_total else by_mode
        if held_county is None:
            counties[line] = [first, count]
        else:
            held_county[0] = min(held_county[0], first)
            held_county[1] += count
    for county_key, first in sorted(withheld.items(), key=lambda item: item[1]):
        quarantined.append(
            Quarantine(
                first,
                "county_split_by_unit",
                f"county {county_key[4]} is partly counted in a reporting unit that "
                "spans several counties, so its own rows are not the county's vote",
                county_key[3],
                county_key[4],
            )
        )
    results: list[CandidateResult] = []
    for line, (first, votes) in sorted(counties.items(), key=lambda item: item[1][0]):
        year, stage, special, state, county_fips, candidate, party_text, writein = line
        key = _sha(stream, *line)
        results.append(
            CandidateResult(
                record_index=first,
                result_key=key,
                office=office,
                election_year=year,
                stage=stage,
                special=special,
                state_code=state,
                state_fips=STATE_FIPS.get(state),
                county_fips=county_fips,
                district=None,
                candidate=candidate,
                party_text=party_text,
                party_code=party_code(party_text),
                writein=writein,
                mode="TOTAL",
                candidate_votes=votes,
                total_votes=None,
                geo_level="county",
                source_record_id=_sha("elections", stream, key),
            )
        )
    quarantined.sort(key=lambda entry: (entry.record_index, entry.error_code))
    return results, quarantined, read


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
