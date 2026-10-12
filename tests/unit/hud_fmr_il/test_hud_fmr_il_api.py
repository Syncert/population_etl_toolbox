"""Offline contracts for the HUD User Data API path.

Covers: ETL-065
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.hud_fmr_il.api import (
    HudApiClient,
    HudApiError,
    HudApiPayloadError,
    get_read,
    parse_fmr_state,
    parse_il_county,
    registered_reads,
    state_codes,
    whole_counties,
)
from data_ingestion_toolbox.hud_fmr_il.config import HudConfig

pytestmark = pytest.mark.unit

ANSWERS = Path(__file__).resolve().parents[2] / "fixtures" / "hud_fmr_il" / "api"
TOKEN = "fixture-token-not-a-real-secret"
FMR26 = get_read("api:fmr:fy2026")
FMR27 = get_read("api:fmr:fy2027")
IL26 = get_read("api:il:fy2026")


def _answer(name: str) -> bytes:
    return (ANSWERS / f"{name}.json").read_bytes()


def test_the_registered_reads_name_the_edition_they_serve() -> None:
    """Covers: ETL-065 — FY 2026 FMRs are the reissue; income limits are FY 2026."""
    assert [(read.key, read.edition) for read in registered_reads()] == [
        ("api:fmr:fy2026", "revised"),
        ("api:fmr:fy2027", "original"),
        ("api:il:fy2026", "original"),
    ]


def test_a_state_answer_parses_counties_towns_and_area_codes() -> None:
    """Covers: ETL-065 — metro counties keep HUD's area code; nonmetro has none; towns are subdivisions."""
    delaware = parse_fmr_state(_answer("fmr_statedata_DE_2026"), read=FMR26)
    assert (delaware.row_count, delaware.county_row_count, delaware.quarantined) == (
        3,
        3,
        (),
    )
    rows = {(obs.fips_code, obs.measure): obs for obs in delaware.observations}
    kent = rows[("1000199999", "fmr_2br")]
    assert (kent.value, kent.hud_area_code, kent.metro) == (
        Decimal("1470"),
        "METRO20100M20100",
        True,
    )
    sussex = rows[("1000599999", "fmr_2br")]
    assert (sussex.hud_area_code, sussex.hud_area_name, sussex.metro) == (
        None,
        "Sussex County, DE",
        False,
    )
    napa = parse_fmr_state(_answer("fmr_statedata_CA_2026"), read=FMR26)
    assert {obs.value for obs in napa.observations if obs.measure == "fmr_2br"} == {
        Decimal("3315")
    }
    town = parse_fmr_state(_answer("fmr_statedata_CT_2026"), read=FMR26)
    assert {obs.geo_type for obs in town.observations} == {"county_subdivision"}
    assert town.county_row_count == 0


def test_a_county_answer_parses_every_limit() -> None:
    """Covers: ETL-065 — median income and 24 limits; metro status read from the answer."""
    kent = parse_il_county(
        _answer("il_data_1000199999_2026"), read=IL26, fips="1000199999"
    )
    values = {obs.measure: obs.value for obs in kent.observations}
    assert len(values) == 25 and kent.quarantined == ()
    assert (values["median_family_income"], values["income_limit_50_4p"]) == (
        Decimal("112100"),
        Decimal("53900"),
    )
    sussex = parse_il_county(
        _answer("il_data_1000599999_2026"), read=IL26, fips="1000599999"
    )
    assert {obs.metro for obs in sussex.observations} == {False}


def test_wrong_years_bad_values_and_foreign_answers_are_refused() -> None:
    """Covers: ETL-065 — another year's answer, a negative value or a non-JSON body never loads."""
    assert [
        q.error_code
        for q in parse_fmr_state(
            _answer("fmr_statedata_DE_2027"), read=FMR26
        ).quarantined
    ] == ["wrong_year"]
    document = json.loads(_answer("fmr_statedata_DE_2026"))
    document["data"]["counties"][0]["Two-Bedroom"] = -5
    document["data"]["counties"][1]["fips_code"] = "10003"
    damaged = parse_fmr_state(json.dumps(document).encode(), read=FMR26)
    assert sorted(q.error_code for q in damaged.quarantined) == [
        "unreadable_row",
        "unreadable_row",
    ]
    assert damaged.county_row_count == 1
    missing = json.loads(_answer("il_data_1000199999_2026"))
    missing["data"]["very_low"]["il50_p4"] = None
    limits = parse_il_county(json.dumps(missing).encode(), read=IL26, fips="1000199999")
    four = next(
        obs for obs in limits.observations if obs.measure == "income_limit_50_4p"
    )
    assert (four.value, four.value_status, four.missing_reason) == (
        None,
        "missing",
        "provider_missing",
    )
    assert [
        q.error_code
        for q in parse_fmr_state(b"<html>busy</html>", read=FMR26).quarantined
    ] == ["not_json"]
    assert state_codes(_answer("fmr_listStates")) == ("CA", "CT", "DE")
    assert whole_counties(_answer("fmr_listCounties_DE"), "DE") == (
        "1000199999",
        "1000399999",
        "1000599999",
    )
    assert whole_counties(_answer("fmr_listCounties_CT"), "CT") == ()


class _Scripted:
    def __init__(self, *responses: httpx.Response) -> None:
        self.responses = list(responses)
        self.headers: list[dict[str, str]] = []

    def get(self, url: str, *, headers: dict[str, str]) -> httpx.Response:
        self.headers.append(headers)
        return self.responses.pop(0)


def _config(**overrides: object) -> HudConfig:
    return HudConfig(
        hud_user_api_token=TOKEN, api_min_spacing_seconds=0, max_attempts=2, **overrides
    )


def test_the_client_sends_the_token_as_a_header_and_never_names_it() -> None:
    """Covers: ETL-065 — bearer header only; a refused token, a 202 and a 404 are told apart."""
    request = httpx.Request("GET", "https://example.test")
    with pytest.raises(HudApiError, match="missing_api_token"):
        HudApiClient(HudConfig())
    scripted = _Scripted(
        httpx.Response(202, content=b"", request=request),
        httpx.Response(200, content=_answer("fmr_statedata_DE_2026"), request=request),
    )
    client = HudApiClient(_config(), client=scripted, sleep=lambda _seconds: None)
    assert client.get("fmr/statedata/DE?year=2026").http_status == 200
    assert {headers["Authorization"] for headers in scripted.headers} == {
        f"Bearer {TOKEN}"
    }
    for status, code in ((401, "token_refused"), (404, "non_retryable_http")):
        refused = HudApiClient(
            _config(),
            client=_Scripted(httpx.Response(status, content=b"{}", request=request)),
            sleep=lambda _seconds: None,
        )
        with pytest.raises(HudApiError) as raised:
            refused.get("fmr/statedata/DE?year=2026")
        assert (raised.value.code, raised.value.status) == (code, status)
        assert TOKEN not in str(raised.value)
    garbled = HudApiClient(
        _config(),
        client=_Scripted(httpx.Response(200, content=b"<html>", request=request)),
        sleep=lambda _seconds: None,
    )
    with pytest.raises(HudApiPayloadError, match="not_json"):
        garbled.get("fmr/listStates")


def test_calls_are_spaced_to_hud_users_limit() -> None:
    """Covers: ETL-065 — 60 calls a minute: consecutive calls wait out the spacing."""
    request = httpx.Request("GET", "https://example.test")
    slept: list[float] = []
    clock = iter([0.0, 0.2, 1.05])
    client = HudApiClient(
        HudConfig(hud_user_api_token=TOKEN, max_attempts=1),
        client=_Scripted(
            httpx.Response(200, content=_answer("fmr_listStates"), request=request),
            httpx.Response(200, content=_answer("fmr_listStates"), request=request),
        ),
        sleep=slept.append,
        clock=lambda: next(clock),
    )
    client.get("fmr/listStates")
    client.get("fmr/listStates")
    assert slept == [pytest.approx(0.85)]
