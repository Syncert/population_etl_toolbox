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
