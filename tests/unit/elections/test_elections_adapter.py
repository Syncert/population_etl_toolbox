"""Offline contract for the elections adapter.

Covers: ETL-111
"""

from __future__ import annotations

import importlib
import json
import sys
from datetime import date

import httpx
import pytest

from data_ingestion_toolbox.elections.client import (
    ElectionsFetchError,
    ElectionsPayloadError,
    check_body,
    data_request,
    fetch_file,
    governor_request,
    legislator_requests,
    listing_request,
    select_data_file,
)
from data_ingestion_toolbox.elections.client import (
    district_county_request,
    district_listing_request,
    listed_districts,
)
from data_ingestion_toolbox.elections.config import ElectionsConfig
from data_ingestion_toolbox.elections.registry import (
    DISTRICT_PLANS,
    HOUSE_PLAN_YEARS,
    congress_elected,
    district_plan,
    COUNTY_PRESIDENT,
    HOUSE,
    SENATE,
    STATE_FIPS,
    STREAMS,
    party_code,
    result_dataset,
)
from data_ingestion_toolbox.elections.silver_elections.parse import (
    parse_district_counties,
    parse_governors,
    parse_legislators,
    parse_results,
)
from tests.support.elections import fixture_bytes

pytestmark = pytest.mark.unit


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-111 — only the Census API takes a key, read from the environment, never defaulted."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.elections"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.elections.config")
    # The one secret is an optional Dataverse token; nothing defaults it.
    secrets = {
        field
        for field in module.ElectionsConfig.model_fields
        if "key" in field or "token" in field or "password" in field
    }
    assert secrets == {"dataverse_api_token", "census_api_key"}
    assert module.ElectionsConfig().dataverse_api_token == ""
    assert module.ElectionsConfig().census_api_key == ""
    assert "census_api_key" not in repr(module.ElectionsConfig(census_api_key="k"))
    assert len(STREAMS) == 10 and len(STATE_FIPS) == 51


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("DEMOCRAT", "DEM"),
        ("Democratic-Farmer-Labor", "DEM"),
        ("Democrat-Liberal", "DEM"),
        ("REPUBLICAN", "REP"),
        ("Republican-Conservative", "REP"),
        ("INDEPENDENT", "IND"),
        # An independent with a party's name is neither the party nor a
        # plain independent: no guess folds it into either.
        ("Ind. Democrat", "OTH"),
        ("Independent Republican", "OTH"),
        ("WORKING FAMILIES", "OTH"),
        ("", "OTH"),
        (None, "OTH"),
    ],
)
def test_party_text_maps_to_four_codes_without_guessing(
    text: str | None, expected: str
) -> None:
    """Covers: ETL-111 — decision L-2: REP, DEM, IND, OTH, the source's text kept beside."""
    assert party_code(text) == expected


def test_legislator_terms_split_where_the_party_changed() -> None:
    """Covers: ETL-111 — one span per party; caucus kept; terms before 1969 dropped."""
    terms, rejected = parse_legislators(fixture_bytes("legislators-current.json"))
    assert rejected == []
    van_drew = sorted(
        (t.start_date, t.end_date, t.party_code)
        for t in terms
        if t.holder_name == "Jefferson Van Drew"
    )[:2]
    assert van_drew == [
        (date(2019, 1, 3), date(2019, 12, 18), "DEM"),
        (date(2019, 12, 19), date(2021, 1, 3), "REP"),
    ]
    sanders = {
        (t.office, t.party_code, t.caucus_code)
        for t in terms
        if t.holder_name == "Bernard Sanders"
    }
    assert sanders == {("us_senate", "IND", "DEM"), ("us_house", "IND", "DEM")}
    historical, _ = parse_legislators(fixture_bytes("legislators-historical.json"))
    assert all(t.end_date > date(1969, 1, 1) for t in historical)
    assert len({t.term_key for t in terms + historical}) == len(terms + historical)


def test_a_governor_has_the_party_they_took_office_in() -> None:
    """Covers: ETL-111 — a dated party statement covering the start wins; no state, no row."""
    terms, rejected = parse_governors(fixture_bytes("wikidata-governors.json"))
    assert [q.error_code for q in rejected] == ["unreadable_state"]
    crist = next(t for t in terms if t.holder_name == "Charlie Crist")
    assert (crist.state_code, crist.party_code) == ("FL", "REP")
    evers = next(t for t in terms if t.holder_name == "Tony Evers")
    assert evers.end_date is None and evers.geo_id == "state:55"


def test_results_quarantine_what_cannot_be_counted() -> None:
    """Covers: ETL-111 — no county code, an NA count: quarantined, never zero."""
    results, rejected, read = parse_results(
        COUNTY_PRESIDENT, fixture_bytes("county_president.csv")
    )
    assert read == 17 and len(results) == 15
    assert [(q.record_index, q.error_code) for q in rejected] == [
        (16, "no_county_code"),
        (17, "unreadable_votes"),
    ]
    dane = [r for r in results if r.geo_id == "state:55|county:025"]
    assert {r.mode for r in dane if r.election_year == 2020} == {
        "ELECTION DAY",
        "ABSENTEE",
    }
    albany = {(r.candidate, r.party_code) for r in results if r.county_fips == "36001"}
    assert ("JOSEPH R BIDEN JR", "OTH") in albany and (
        "JOSEPH R BIDEN JR",
        "DEM",
    ) in albany


def test_tab_delimited_house_rows_are_districts() -> None:
    """Covers: ETL-111 — district 0 is at-large; a district is not a shared geography."""
    results, rejected, read = parse_results(HOUSE, fixture_bytes("house.tab"))
    assert rejected == [] and read == 3
    assert {(r.state_code, r.district, r.geo_level, r.geo_id) for r in results} == {
        ("WI", 2, "district", None),
        ("WY", 0, "district", None),
    }
    senate, _, _ = parse_results(SENATE, fixture_bytes("senate.csv"))
    assert {(r.state_code, r.special) for r in senate} == {
        ("WI", False),
        ("MN", False),
        ("MN", True),
    }


def test_the_registered_file_is_chosen_by_name_exactly_once() -> None:
    """Covers: ETL-111 — a second edition beside the first is refused, not guessed between."""
    dataset = result_dataset(COUNTY_PRESIDENT)
    listing = fixture_bytes("listing-county_president.json")
    assert select_data_file(dataset, listing) == (6104603, "countypres_2000-2024.csv")
    body = json.loads(listing)
    body["data"].append({"dataFile": {"id": 1, "filename": "countypres_2000-2028.tab"}})
    with pytest.raises(ElectionsPayloadError, match="ambiguous_file"):
        select_data_file(dataset, json.dumps(body).encode())
    body["data"] = []
    with pytest.raises(ElectionsPayloadError, match="no_registered_file"):
        select_data_file(dataset, json.dumps(body).encode())


def test_requests_name_their_provider_and_bodies_are_checked() -> None:
    """Covers: ETL-111 — GitHub raw files, the Wikidata query, Dataverse originals."""
    assert [r.url.rsplit("/", 1)[1] for r in legislator_requests()] == [
        "legislators-current.json",
        "legislators-historical.json",
    ]
    governors = governor_request()
    assert "P5087" in governors.parameters["query"]
    dataset = result_dataset(COUNTY_PRESIDENT)
    assert listing_request(dataset).parameters == {
        "persistentId": "doi:10.7910/DVN/VOQCHQ"
    }
    data = data_request(dataset, 6104603)
    assert data.url.endswith("/api/access/datafile/6104603")
    assert data.parameters == {"format": "original"}
    with pytest.raises(ElectionsPayloadError, match="not_a_results_file"):
        check_body(data, b"<html>maintenance</html>")
    with pytest.raises(ElectionsPayloadError, match="unexpected_body"):
        check_body(governors, b'{"head": {}}')
    with pytest.raises(ElectionsPayloadError, match="not_json"):
        check_body(legislator_requests()[0], b"<!doctype html>")


class _Scripted:
    def __init__(self, statuses: list[int], body: bytes) -> None:
        self.statuses = list(statuses)
        self.body = body
        self.calls = 0

    def get(self, url: str, **_kwargs: object) -> httpx.Response:
        self.calls += 1
        return httpx.Response(
            self.statuses.pop(0), content=self.body, request=httpx.Request("GET", url)
        )


def test_retries_are_for_the_provider_being_busy_only() -> None:
    """Covers: ETL-111 — a 503 is retried, a 404 is not, and retries end."""
    config = ElectionsConfig(min_spacing_seconds=0, max_attempts=3)
    request = legislator_requests()[0]
    body = fixture_bytes("legislators-current.json")
    busy = _Scripted([503, 200], body)
    fetched = fetch_file(request, config=config, client=busy, sleep=lambda _s: None)
    assert (fetched.http_status, busy.calls) == (200, 2)
    missing = _Scripted([404], b"")
    with pytest.raises(ElectionsFetchError, match="non_retryable_http"):
        fetch_file(request, config=config, client=missing, sleep=lambda _s: None)
    assert missing.calls == 1
    down = _Scripted([502, 502, 502], b"")
    with pytest.raises(ElectionsFetchError, match="retry_exhausted"):
        fetch_file(request, config=config, client=down, sleep=lambda _s: None)
    assert down.calls == 3


def _binding(person: str, start: str, **extra: dict[str, str]) -> dict:
    binding = {
        "position": {"type": "uri", "value": "http://www.wikidata.org/entity/Q9001"},
        "person": {"type": "uri", "value": f"http://www.wikidata.org/entity/{person}"},
        "personLabel": {"type": "literal", "value": person},
        "stateFips": {"type": "literal", "value": "01"},
        "start": {"type": "literal", "value": start},
    }
    binding.update(extra)
    return binding


def test_an_unknown_wikidata_date_is_held_back_not_fatal() -> None:
    """Covers: ETL-111 — Wikidata's "unknown value" link never aborts the read.

    OFFICE_LAB's first live read (2026-11-04) found eight such links on one
    Alabama governor's statements, and one raised out of the whole parse.
    """
    genid = {
        "type": "uri",
        "value": "http://www.wikidata.org/.well-known/genid/0123456789abcdef",
    }
    republican = {"type": "uri", "value": "http://www.wikidata.org/entity/Q29468"}
    body = {
        "results": {
            "bindings": [
                # An unknown party date: the term is kept, the party undated.
                _binding(
                    "Q1",
                    "1979-01-15T00:00:00Z",
                    end={"type": "literal", "value": "1983-01-17T00:00:00Z"},
                    party=republican,
                    partyLabel={"type": "literal", "value": "Republican Party"},
                    partyStart=genid,
                ),
                # An unknown end: the term must not read as serving.
                _binding("Q2", "1987-01-19T00:00:00Z", end=genid, party=republican),
                _binding("Q2", "1987-01-19T00:00:00Z", party=republican),
                # An unknown start cannot be placed at all.
                _binding("Q3", genid["value"]),
            ]
        }
    }
    terms, rejected = parse_governors(json.dumps(body).encode())
    assert [(t.holder_id, t.party_code) for t in terms] == [("Q1", "REP")]
    assert [(q.record_index, q.error_code) for q in rejected] == [
        (2, "unreadable_date"),
        (4, "unreadable_date"),
    ]


class _Guestbooked:
    """Dataverse 6.10's answer for a file behind a guestbook."""

    refusal = b'{"status":"ERROR","message":"required Guestbook response for guestbookID 458"}'

    def __init__(
        self, questions: list[dict] | None = None, *, inline: bool = False
    ) -> None:
        self.questions = questions or []
        self.inline = inline
        self.posted: list[dict] = []
        self.gets: list[str] = []

    def _response(self, url: str, status: int, body: bytes) -> httpx.Response:
        return httpx.Response(status, content=body, request=httpx.Request("GET", url))

    def get(self, url: str, **_kwargs: object) -> httpx.Response:
        self.gets.append(url)
        if url.endswith("/api/guestbooks/458"):
            body = {
                "status": "OK",
                "data": {"id": 458, "customQuestions": self.questions},
            }
            return self._response(url, 200, json.dumps(body).encode())
        if "token=" in url:
            return self._response(url, 200, fixture_bytes("county_president.csv"))
        return self._response(url, 400, self.refusal)

    def post(self, url: str, *, content: str, **_kwargs: object) -> httpx.Response:
        self.posted.append(json.loads(content))
        if self.inline:
            return self._response(url, 200, fixture_bytes("county_president.csv"))
        body = {"status": "OK", "data": {"signedUrl": f"{url}?token=signed"}}
        return self._response(url, 200, json.dumps(body).encode())


def test_a_guestbooked_file_is_answered_with_the_operators_identity() -> None:
    """Covers: ETL-111 — Dataverse's guestbook is answered from the environment, or refused by name.

    OFFICE_LAB's first live read (2026-11-04) got HTTP 400 "required Guestbook
    response for guestbookID 458" for MEDSL's county presidential file.
    """
    request = data_request(result_dataset(COUNTY_PRESIDENT), 6104603)
    quiet = {"sleep": lambda _s: None}
    anonymous = ElectionsConfig(min_spacing_seconds=0, max_attempts=1)
    with pytest.raises(ElectionsFetchError, match="guestbook_required"):
        fetch_file(request, config=anonymous, client=_Guestbooked(), **quiet)

    operator = anonymous.model_copy(
        update={
            "guestbook_name": "Operator",
            "guestbook_email": "operator@example.org",
            "guestbook_institution": "Example",
            "guestbook_position": "Analyst",
        }
    )
    assert "operator@example.org" not in repr(operator)
    client = _Guestbooked()
    fetched = fetch_file(request, config=operator, client=client, **quiet)
    assert fetched.raw_bytes == fixture_bytes("county_president.csv")
    assert client.posted == [
        {
            "guestbookResponse": {
                "name": "Operator",
                "email": "operator@example.org",
                "institution": "Example",
                "position": "Analyst",
                "answers": [],
            }
        }
    ]
    inline = fetch_file(
        request, config=operator, client=_Guestbooked(inline=True), **quiet
    )
    assert inline.raw_bytes == fixture_bytes("county_president.csv")
    # The House file is guestbooked too (OFFICE_LAB, 2026-11-04): any
    # results file that answers this way takes the same path.
    house = data_request(result_dataset(HOUSE), 11384761)
    assert fetch_file(house, config=operator, client=_Guestbooked(), **quiet).raw_bytes
    with pytest.raises(ElectionsFetchError, match="guestbook_required"):
        fetch_file(house, config=anonymous, client=_Guestbooked(), **quiet)
    asking = _Guestbooked([{"id": 9, "question": "Purpose?", "required": True}])
    with pytest.raises(ElectionsFetchError, match="guestbook_questions_unanswered"):
        fetch_file(request, config=operator, client=asking, **quiet)
    assert asking.posted == []


def test_the_guestbook_identity_is_read_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-111 — the identity is the operator's, never a default in the repository."""
    assert ElectionsConfig().guestbook_identity is None
    monkeypatch.setenv("ELECTIONS_GUESTBOOK_NAME", "Operator")
    monkeypatch.setenv("ELECTIONS_GUESTBOOK_EMAIL", "operator@example.org")
    monkeypatch.setenv("DATAVERSE_API_TOKEN", "not-a-real-token")
    config = ElectionsConfig.from_environment()
    assert config.guestbook_identity == {
        "name": "Operator",
        "email": "operator@example.org",
        "institution": "",
        "position": "",
    }
    assert "not-a-real-token" not in repr(config)


def _history(stream: str, name: str):  # noqa: ANN202
    from data_ingestion_toolbox.elections.registry import result_dataset
    from data_ingestion_toolbox.elections.silver_elections.parse import parse_history

    return parse_history(
        stream, fixture_bytes(name), first_year=result_dataset(stream).first_year
    )


def test_county_history_reads_from_1990_and_maps_retired_counties() -> None:
    """Covers: ETL-111 — Dade reads as Miami-Dade; South Boston city adds into Halifax."""
    results, quarantined, read = _history(
        "county_president_history", "county_president_history.Rdata"
    )
    # Fourteen rows; the 1988 row is not read.
    assert read == 13
    assert {r.election_year for r in results} == {1992, 1996, 2016}
    by_county = {
        (r.election_year, r.county_fips, r.party_code): r.candidate_votes
        for r in results
    }
    assert "12025" not in {r.county_fips for r in results}
    assert by_county[(1992, "12086", "REP")] == 230000
    # Halifax 6,000 + South Boston city 1,800; totals 12,000 + 3,600.
    assert by_county[(1992, "51083", "REP")] == 7800
    assert by_county[(1992, "51083", "OTH")] == 15600 - 6500 - 7800
    halifax = [r for r in results if r.county_fips == "51083"]
    assert {r.total_votes for r in halifax} == {15600}
    assert {(q.record_index, q.error_code) for q in quarantined} == {
        (7, "no_major_party_votes"),
        (10, "repeated_row"),
        (11, "conflicting_row"),
        (12, "conflicting_row"),
    }
    # Every county's rows add up to its total: nothing is left out.
    for year, county in {(r.election_year, r.county_fips) for r in results}:
        rows = [
            r for r in results if (r.election_year, r.county_fips) == (year, county)
        ]
        assert sum(r.candidate_votes for r in rows) == rows[0].total_votes


def test_county_senate_history_keeps_two_races_apart_and_no_missing_zero() -> None:
    """Covers: ETL-111 — a special is its own contest; an absent nominee is no row, not zero."""
    results, quarantined, read = _history(
        "county_senate_history", "county_senate_history.Rdata"
    )
    assert quarantined == [] and read == 6
    hennepin = {(r.special, r.candidate) for r in results if r.county_fips == "27053"}
    assert (False, "Amy Klobuchar") in hennepin and (True, "Tina Smith") in hennepin
    warner = [r for r in results if r.election_year == 2002]
    assert {r.party_code for r in warner} == {"REP", "OTH"}
    assert all(r.office == "us_senate" and r.geo_level == "county" for r in results)


def test_county_senate_2022_reads_county_votes_not_the_state_total() -> None:
    """Covers: ETL-111 — MEDSL repeats the statewide total on county rows; it is not kept."""
    from data_ingestion_toolbox.elections.registry import COUNTY_SENATE

    results, quarantined, _read = parse_results(
        COUNTY_SENATE, fixture_bytes("county_senate.csv")
    )
    assert quarantined == []
    assert {r.county_fips for r in results} == {"55025", "55133"}
    assert all(r.total_votes is None for r in results)
    assert all(r.office == "us_senate" and not r.special for r in results)
    assert sum(r.candidate_votes for r in results if r.county_fips == "55025") == (
        231818 + 68228 + 4 + 678
    )


def test_history_files_are_checked_as_r_files() -> None:
    """Covers: ETL-111 — an HTML error page in place of the .Rdata file is refused."""
    from data_ingestion_toolbox.elections.client import FileRequest

    request = FileRequest("county_senate_history", "data", "", {}, "results", "")
    check_body(request, fixture_bytes("county_senate_history.Rdata"))
    with pytest.raises(ElectionsPayloadError) as refused:
        check_body(request, b"<html>maintenance</html>")
    assert refused.value.code == "not_an_rdata_file"


def test_history_streams_select_their_own_file_from_one_dataset() -> None:
    """Covers: ETL-111 — two streams share one Dataverse dataset, one file each."""
    listing = fixture_bytes("listing-county_history.json")
    assert select_data_file(result_dataset("county_president_history"), listing) == (
        5028532,
        "dataverse_shareable_presidential_county_returns_1868_2020.Rdata",
    )
    assert select_data_file(result_dataset("county_senate_history"), listing)[0] == (
        5028534
    )
    request = data_request(result_dataset("county_senate_history"), 5028534)
    assert request.media_type == "application/octet-stream"


def test_a_medsl_county_row_with_a_retired_code_adds_into_its_successor() -> None:
    """Covers: ETL-111 — Bedford city 2008 counts in Bedford County, its total dropped."""
    header = b"year,state_po,county_name,county_fips,candidate,party,candidatevotes,totalvotes,mode\n"
    body = (
        b"2008,VA,BEDFORD,51019,JOHN MCCAIN,REPUBLICAN,23000,34000,TOTAL\n"
        b"2008,VA,BEDFORD CITY,51515,JOHN MCCAIN,REPUBLICAN,1500,3000,TOTAL\n"
    )
    results, quarantined, _read = parse_results(COUNTY_PRESIDENT, header + body)
    assert quarantined == []
    assert [(r.county_fips, r.candidate_votes, r.total_votes) for r in results] == [
        ("51019", 23000, 34000),
        ("51019", 1500, None),
    ]
    assert results[0].result_key != results[1].result_key


def _census(rows: list[list[str]]) -> bytes:
    return json.dumps(rows).encode()


def test_district_pieces_merge_retired_counties_and_read_the_delegate_as_zero() -> None:
    """Covers: ETL-111 — Census county pieces: successor codes summed, DC's 98 is district 0, bad rows held back."""
    header = ["NAME", "P001001", "state", "congressional district", "county (or part)"]
    pieces, quarantine, read = parse_district_counties(
        "cd110/51/05",
        _census(
            [
                header,
                ["Bedford County (part)", "60000", "51", "05", "019"],
                ["Bedford city", "6000", "51", "05", "515"],
                ["Halifax County", "37000", "51", "05", "083"],
                ["Nowhere", "x", "51", "05", "999"],
                ["Wrong district", "5", "51", "06", "001"],
            ]
        ),
    )
    assert read == 5
    assert [(p.county_fips, p.population, p.district, p.congress) for p in pieces] == [
        ("51019", 66000, 5, 110),
        ("51083", 37000, 5, 110),
    ]
    assert {q.error_code for q in quarantine} == {
        "unreadable_population",
        "unexpected_district",
    }
    (delegate,), _, _ = parse_district_counties(
        "cd118/11/98",
        _census(
            [
                [
                    "NAME",
                    "P1_001N",
                    "state",
                    "congressional district",
                    "county (or part)",
                ],
                ["District of Columbia", "689545", "11", "98", "001"],
            ]
        ),
    )
    assert (delegate.district, delegate.state_code, delegate.geo_id) == (
        0,
        "DC",
        "state:11|county:001",
    )


def test_the_census_key_is_sent_but_never_part_of_the_captured_request() -> None:
    """Covers: ETL-111 — the key rides on the wire only; a body without the geography is refused."""
    plan = district_plan("cd118")
    request = district_county_request(plan, "55", "02")
    assert "key" not in request.parameters
    assert request.parameters["in"] == "state:55 congressional district:02"
    seen: list[dict[str, str]] = []

    class _Census:
        def get(self, url, *, params, headers):
            seen.append(dict(params))
            body = _census(
                [
                    [
                        "NAME",
                        "P1_001N",
                        "state",
                        "congressional district",
                        "county (or part)",
                    ]
                ]
            )
            return httpx.Response(200, content=body, request=httpx.Request("GET", url))

    fetched = fetch_file(
        request,
        config=ElectionsConfig(census_api_key="secret", max_attempts=1),
        client=_Census(),
    )
    assert seen[0]["key"] == "secret" and "key" not in fetched.request.parameters
    with pytest.raises(ElectionsPayloadError, match="not_a_census_table"):
        check_body(request, b'[["NAME", "state"]]')
    listing = district_listing_request(plan, "56")
    assert listed_districts(
        _census(
            [
                ["NAME", "state", "congressional district"],
                ["x", "56", "00"],
                ["w", "56", "ZZ"],
            ]
        ),
        listing.endpoint,
    ) == ["00"]


def test_every_house_election_year_names_a_registered_plan_no_newer_than_its_congress() -> (
    None
):
    """Covers: ETL-111 — the registry and the gold plan views list the same plans and years."""
    import re as _re
    from pathlib import Path

    sql = (
        Path(__file__).resolve().parents[3]
        / "src/data_ingestion_toolbox/elections/gold_elections/DDL/gold_elections.sql"
    ).read_text()
    plans = set(_re.findall(r"\('(cd\d{3})', (\d{3}), (\d{4}), '([^']+)'\)", sql))
    assert plans == {
        (plan.key, str(plan.congress), str(plan.census_year), plan.path)
        for plan in DISTRICT_PLANS
    }
    years = {
        (int(year), key, tuple(_re.findall(r"'([A-Z]{2})'", states)))
        for year, key, states in _re.findall(
            r"\((\d{4}), '(cd\d{3})', ARRAY\[([^\]]*)\]", sql
        )
    }
    assert years == {
        (year, key, excluded) for year, (key, excluded) in HOUSE_PLAN_YEARS.items()
    }
    for year, (key, _excluded) in HOUSE_PLAN_YEARS.items():
        assert district_plan(key).congress <= congress_elected(year)
