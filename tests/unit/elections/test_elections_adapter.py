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
from data_ingestion_toolbox.elections.config import ElectionsConfig
from data_ingestion_toolbox.elections.registry import (
    COUNTY_PRESIDENT,
    HOUSE,
    SENATE,
    STATE_FIPS,
    STREAMS,
    party_code,
    result_dataset,
)
from data_ingestion_toolbox.elections.silver_elections.parse import (
    parse_governors,
    parse_legislators,
    parse_results,
)
from tests.support.elections import fixture_bytes

pytestmark = pytest.mark.unit


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-111 — none of the three providers needs a key, so there is none to leak."""
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
    assert secrets == {"dataverse_api_token"}
    assert module.ElectionsConfig().dataverse_api_token == ""
    assert len(STREAMS) == 6 and len(STATE_FIPS) == 51


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
