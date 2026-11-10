"""NDCP county childcare prices read from DOL's data portal API.

www.dol.gov refuses the warehouse's client, so the release is read from
``apiprod.dol.gov/v4/get/WB/ndcp/json`` with a registered key (Nick,
2026-11-09). Covers: ETL-123
"""

from __future__ import annotations

import json
from decimal import Decimal

import httpx
import pytest

from data_ingestion_toolbox.dol_childcare.capture import (
    API_PARSER_CONTRACT_VERSION,
    PARSER_CONTRACT_VERSION,
    parser_contract,
    release_endpoint,
)
from data_ingestion_toolbox.dol_childcare.client import (
    ChildcareFetchError,
    ChildcarePayloadError,
    api_fields,
    fetch_release,
)
from data_ingestion_toolbox.dol_childcare.config import ChildcareConfig
from data_ingestion_toolbox.dol_childcare.registry import (
    MEASURES,
    RELEASES,
    WORKBOOK_RELEASE,
)
from data_ingestion_toolbox.dol_childcare.silver_dol_childcare.parse import (
    parse_api_payload,
    parse_capture,
    parse_workbook,
)
from tests.support import dol_childcare as ndcp

pytestmark = pytest.mark.unit

RELEASE = RELEASES[0]
KEY = "fixture-dol-key"


def _config(**values: object) -> ChildcareConfig:
    return ChildcareConfig(
        **{"min_spacing_seconds": 0, "max_attempts": 1, "api_key": KEY, **values}
    )


def _read(client: object, **values: object):
    return fetch_release(
        RELEASE, config=_config(**values), client=client, sleep=lambda _s: None
    )


def test_the_api_is_asked_for_the_registered_fields_a_study_year_at_a_time() -> None:
    """Covers: ETL-123 — one keyed request per study year, in county code order, the key never in the address."""
    client = ndcp.FixtureClient()
    response = _read(client)
    assert client.urls == ["https://apiprod.dol.gov/v4/get/WB/ndcp/json"] * 15
    years = [json.loads(p["filter_object"]) for p in client.params]
    assert years == [
        {"field": "studyyear", "operator": "eq", "value": year}
        for year in range(2008, 2023)
    ]
    first = client.params[0]
    assert first["X-API-KEY"] == KEY
    assert (first["sort_by"], first["sort"], first["offset"]) == (
        "county_fips_code",
        "asc",
        0,
    )
    assert first["fields"] == ",".join(api_fields(RELEASE))
    assert {
        "county_fips_code",
        "studyyear",
        "county_name",
        "mcinfant",
        "imcbto5",
    } <= set(api_fields(RELEASE))
    assert KEY.encode() not in response.raw_bytes
    assert release_endpoint(RELEASE) == "https://apiprod.dol.gov/v4/get/WB/ndcp/json"
    assert parser_contract(RELEASE) == API_PARSER_CONTRACT_VERSION
    assert parser_contract(WORKBOOK_RELEASE) == PARSER_CONTRACT_VERSION


def test_the_api_records_read_as_the_workbook_does() -> None:
    """Covers: ETL-123 — the API's Dane, Rock and Los Angeles prices are the workbook's, with their codes."""
    api = parse_capture(_read(ndcp.FixtureClient()).raw_bytes, release=RELEASE)
    workbook = parse_workbook(ndcp.FIXTURE.read_bytes(), release=WORKBOOK_RELEASE)

    def key(p):
        return (p.fips_code, p.study_year, p.metric_key)

    def facts(parsed):
        return {
            key(p): (
                p.value,
                p.value_status,
                p.imputation_code,
                p.county_name,
                p.geo_id,
            )
            for p in parsed.prices
        }

    assert facts(api) == facts(workbook)
    assert len(api.prices) == 5 * len(MEASURES)
    assert api.row_count == 6
    assert [q.error_code for q in api.quarantined] == ["unreadable_row"]
    dane = {key(p): p for p in api.prices}[("55025", 2022, "center_infant")]
    assert dane.value == Decimal(293) and dane.value_source == "293.0"


def test_a_year_larger_than_a_page_is_read_page_by_page() -> None:
    """Covers: ETL-123 — offsets advance until a short page; nothing is read twice."""
    client = ndcp.FixtureClient()
    parsed = parse_capture(_read(client, api_page_size=1).raw_bytes, release=RELEASE)
    asked_2022 = [
        p["offset"]
        for p in client.params
        if json.loads(p["filter_object"])["value"] == 2022
    ]
    assert asked_2022 == [0, 1, 2, 3, 4]
    assert len(parsed.prices) == 5 * len(MEASURES)


class _Answer:
    """Answers every request with one body or status."""

    def __init__(self, content: bytes = b"", status: int = 200, error=None) -> None:
        self.content = content
        self.status = status
        self.error = error

    def get(self, url, *, headers, params=None):
        if self.error is not None:
            raise self.error
        return httpx.Response(
            self.status, content=self.content, request=httpx.Request("GET", url)
        )


@pytest.mark.parametrize(
    ("content", "code"),
    [
        (b"<html>busy</html>", "not_json"),
        (b'{"message": "no"}', "not_a_data_list"),
        (json.dumps({"data": [], "echo": KEY}).encode(), "key_echoed"),
        (b'{"data": []' + b" " * 4_500_000 + b"}", "page_may_be_cut"),
    ],
)
def test_an_answer_that_is_not_a_data_page_is_refused_before_it_is_kept(
    content: bytes, code: str
) -> None:
    """Covers: ETL-123 — an HTML page, an error body, an echoed key or a page at the 5 MB limit is never captured."""
    with pytest.raises(ChildcarePayloadError, match=code):
        _read(_Answer(content))


def test_a_refusal_or_transport_error_never_names_the_key() -> None:
    """Covers: ETL-123 — the error names the path and status only; no key, no query string."""
    with pytest.raises(ChildcareFetchError) as refused:
        _read(_Answer(status=403))
    assert refused.value.status == 403 and KEY not in str(refused.value)
    leaky = httpx.ConnectError(f"failed for ?X-API-KEY={KEY}")
    with pytest.raises(ChildcareFetchError) as broken:
        _read(_Answer(error=leaky))
    chain, error = [], broken.value
    while error is not None:
        chain.append(error)
        error = error.__cause__ or error.__context__
    assert all(KEY not in str(e) for e in chain)
    assert leaky not in chain
    assert "ConnectError" in str(broken.value.__cause__)
    with pytest.raises(ChildcareFetchError, match="missing_api_key"):
        _read(ndcp.FixtureClient(), api_key=" ")


def test_the_key_is_read_from_the_environment_and_never_shown(monkeypatch) -> None:
    """Covers: ETL-123 — DOL_API_KEY comes from the deployment's environment only."""
    monkeypatch.setenv("DOL_API_KEY", "from-environment")
    config = ChildcareConfig()
    assert config.api_key == "from-environment"
    assert "from-environment" not in repr(config)


def test_api_records_without_a_registered_field_are_refused_whole() -> None:
    """Covers: ETL-123 — a changed layout, no records or no pages is quarantined as row 0, never guessed."""
    record = ndcp.api_record()
    del record["mcsa"]
    payload = json.dumps({"pages": [{"body": {"data": [record]}}]}).encode()
    parsed = parse_api_payload(payload, release=RELEASE)
    assert parsed.rejected and parsed.prices == ()
    assert "MCSA" in parsed.quarantined[0].error_summary
    for body, code in [
        ({"pages": [{"body": {"data": []}}]}, "unexpected_payload"),
        ({"rows": []}, "unexpected_payload"),
        ({"pages": [{"body": {"data": ["text"]}}]}, "unexpected_payload"),
        ({"pages": [{"body": "text"}]}, "unexpected_payload"),
    ]:
        refused = parse_api_payload(json.dumps(body).encode(), release=RELEASE)
        assert [q.error_code for q in refused.quarantined] == [code]
    assert parse_api_payload(b"not json", release=RELEASE).rejected


def test_a_record_without_a_county_name_still_reads() -> None:
    """Covers: ETL-123 — the county name is kept when given, never required; a true/false year is unreadable."""
    named = ndcp.api_record() | {"county_name": None}
    odd = ndcp.api_record(COUNTY_FIPS_CODE=55105) | {"studyyear": True}
    payload = json.dumps({"pages": [{"body": {"data": [named, odd]}}]}).encode()
    parsed = parse_api_payload(payload, release=RELEASE)
    assert {p.county_name for p in parsed.prices} == {""}
    assert len(parsed.prices) == len(MEASURES)
    assert [q.error_code for q in parsed.quarantined] == ["unreadable_row"]


class _Sequence:
    """Answers each request with the next of the given statuses, then 200 pages."""

    def __init__(self, statuses: list[tuple[int, dict[str, str]]]) -> None:
        self.statuses = list(statuses)
        self.fixture = ndcp.FixtureClient()
        self.calls = 0

    def get(self, url, *, headers, params=None):
        self.calls += 1
        if self.statuses:
            status, answer_headers = self.statuses.pop(0)
            return httpx.Response(
                status, headers=answer_headers, request=httpx.Request("GET", url)
            )
        return self.fixture.get(url, headers=headers, params=params)


def test_a_429_waits_as_long_as_its_retry_after_asks_and_a_502_backs_off() -> None:
    """Covers: ETL-123 — DOL's 429 and 502 answers are waited out, never hammered."""
    waits: list[float] = []
    client = _Sequence([(429, {"Retry-After": "45"}), (502, {}), (502, {}), (429, {})])
    config = _config(
        max_attempts=6,
        retry_backoff_seconds=15,
        max_retry_after_seconds=600,
        min_spacing_seconds=10,
    )
    response = fetch_release(RELEASE, config=config, client=client, sleep=waits.append)
    # The first page: Retry-After 45, then the backoff of the attempt it is
    # (15 doubled once per attempt so far) for the 502s and for a 429 that
    # names no wait; then a pause of 10 before each new page.
    assert waits[:4] == [45, 30, 60, 120]
    assert set(waits[4:]) == {10}
    assert len(waits[4:]) == 14
    assert parse_capture(response.raw_bytes, release=RELEASE).prices


def test_the_wait_after_a_refusal_is_bounded() -> None:
    """Covers: ETL-123 — a Retry-After is honoured up to the cap; a date or nonsense falls back to backoff."""
    from data_ingestion_toolbox.dol_childcare.client import retry_wait

    config = _config(
        retry_backoff_seconds=15, max_retry_after_seconds=600, min_spacing_seconds=10
    )
    request = httpx.Request("GET", "https://apiprod.dol.gov/v4/get/WB/ndcp/json")

    def answer(status, retry_after=None):
        headers = {"Retry-After": retry_after} if retry_after is not None else {}
        return httpx.Response(status, headers=headers, request=request)

    assert retry_wait(answer(429, "120"), 1, config) == 120
    assert retry_wait(answer(429, "3600"), 1, config) == 600
    assert retry_wait(answer(429, "1"), 1, config) == 10
    assert retry_wait(answer(429, "Wed, 21 Oct 2026 07:28:00 GMT"), 2, config) == 30
    assert retry_wait(answer(429, "nan"), 1, config) == 15
    assert retry_wait(answer(502), 3, config) == 60
    assert retry_wait(None, 10, config) == 600


class _Pages:
    def __init__(self) -> None:
        self.kept: dict[str, bytes] = {}
        self.put_keys: list[tuple[int, int]] = []

    def get(self, request_key: str) -> bytes | None:
        return self.kept.get(request_key)

    def put(self, request_key: str, *, year: int, offset: int, body: bytes) -> None:
        self.kept[request_key] = body
        self.put_keys.append((year, offset))


def test_a_stopped_read_resumes_at_the_page_it_stopped_on() -> None:
    """Covers: ETL-123 — a read whose retries ran out keeps its answered pages; the next asks only for the rest."""
    pages = _Pages()

    class _StopsAt2015(ndcp.FixtureClient):
        def get(self, url, *, headers, params=None):
            if json.loads(params["filter_object"])["value"] == 2015:
                return httpx.Response(502, request=httpx.Request("GET", url))
            return super().get(url, headers=headers, params=params)

    first = _StopsAt2015()
    with pytest.raises(ChildcareFetchError, match="retry_exhausted"):
        fetch_release(
            RELEASE,
            config=_config(max_attempts=2),
            client=first,
            sleep=lambda _s: None,
            pages=pages,
        )
    assert [year for year, _offset in pages.put_keys] == list(range(2008, 2015))

    second = ndcp.FixtureClient()
    resumed = fetch_release(
        RELEASE, config=_config(), client=second, sleep=lambda _s: None, pages=pages
    )
    asked = [json.loads(p["filter_object"])["value"] for p in second.params]
    assert asked == list(range(2015, 2023))
    whole = _read(ndcp.FixtureClient())
    assert resumed.raw_bytes == whole.raw_bytes


def test_a_changed_page_size_or_field_list_asks_afresh() -> None:
    """Covers: ETL-123 — a kept page is reused only for the same request."""
    from data_ingestion_toolbox.dol_childcare.client import page_request_key

    key = page_request_key("a,b", 1000, 2022, 0)
    assert len(key) == 64
    assert key != page_request_key("a,b", 500, 2022, 0)
    assert key != page_request_key("a,b,c", 1000, 2022, 0)
    assert key != page_request_key("a,b", 1000, 2022, 1000)
    assert ChildcareConfig().api_page_size == 1000
    assert ChildcareConfig().min_spacing_seconds == 10


class _Cursor:
    def __init__(self, store: dict) -> None:
        self.store = store
        self.description = None
        self.rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        return None

    def execute(self, sql: str, parameters: tuple) -> None:
        self.store.setdefault("sql", []).append(" ".join(sql.split()))
        if sql.lstrip().startswith("SELECT"):
            self.description = ("body",)
            body = self.store.get(parameters[1])
            self.rows = [(memoryview(body),)] if body is not None else []
        elif sql.lstrip().startswith("INSERT"):
            self.store[parameters[1]] = parameters[4]

    def fetchall(self) -> list[tuple]:
        return self.rows


class _Connection:
    def __init__(self, store: dict, *, fail: bool = False) -> None:
        self.store = store
        self.fail = fail
        self.committed = self.rolled_back = self.closed = False

    def cursor(self) -> _Cursor:
        if self.fail:
            raise RuntimeError("database gone")
        return _Cursor(self.store)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def test_stored_pages_keep_each_page_as_it_is_answered() -> None:
    """Covers: ETL-123 — a page is committed when answered, read back as bytes, and a stale one is forgotten."""
    from data_ingestion_toolbox.dol_childcare.capture import PAGE_KEEP_DAYS, StoredPages

    store: dict = {}
    pages = StoredPages(lambda: _Connection(store), "ndcp2022")
    assert pages.get("a" * 64) is None
    pages.put("a" * 64, year=2008, offset=0, body=b'{"data":[]}')
    assert pages.get("a" * 64) == b'{"data":[]}'
    pages.forget_stale()
    statements = store["sql"]
    assert any(
        s.startswith("DELETE FROM control.dol_childcare_page") for s in statements
    )
    assert any("make_interval(days => %s)" in s for s in statements)
    assert PAGE_KEEP_DAYS == 7

    failing = _Connection({}, fail=True)
    with pytest.raises(RuntimeError, match="database gone"):
        StoredPages(lambda: failing, "ndcp2022").get("a" * 64)
    assert failing.rolled_back and failing.closed and not failing.committed
