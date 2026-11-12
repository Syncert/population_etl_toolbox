"""Offline contracts for the USAspending place-of-performance adapter.

Covers: ETL-130
"""

from __future__ import annotations

import importlib
import json
import logging
import sys
from datetime import date
from decimal import Decimal
from typing import Any

import httpx
import pytest

from data_ingestion_toolbox.usaspending.capture import (
    RequestKey,
    agencies_of,
    agency_list_body,
    current_fiscal_year,
    fiscal_years,
    geography_body,
    planned_geography_keys,
)
from data_ingestion_toolbox.usaspending.client import (
    UsaspendingFetchError,
    UsaspendingPayloadError,
    check_agencies,
    check_geography,
    post_search,
)
from data_ingestion_toolbox.usaspending.config import UsaspendingConfig
from data_ingestion_toolbox.usaspending.registry import (
    AGENCY_PATH,
    AWARD_GROUPS,
    CUT_AGENCY,
    CUT_ALL,
    CUT_AWARD_TYPE,
    GEOGRAPHY_PATH,
    geo_id_of,
)
from data_ingestion_toolbox.usaspending.silver_usaspending.parse import (
    parse_agencies,
    parse_geography,
)
from tests.support.usaspending import (
    AGENCIES,
    COUNTY_GRANTS,
    DANE_FY2024_GRANTS,
    NATION_FY2024_STATE_ROWS,
    STATE_ALL,
    WI_COUNTIES,
    WI_FY2024_ALL_AWARDS,
    FixtureClient,
    empty_geography,
)

pytestmark = pytest.mark.unit


def _config() -> UsaspendingConfig:
    return UsaspendingConfig(min_spacing_seconds=0, max_attempts=2)


def _answer(rows: list[dict[str, Any]], layer: str = "state") -> bytes:
    return json.dumps(
        {"scope": "place_of_performance", "geo_layer": layer, "results": rows}
    ).encode()


def test_configuration_imports_with_no_io_and_no_credential() -> None:
    """Covers: ETL-130 — importing the configuration reads nothing and needs no key."""
    sys.modules.pop("data_ingestion_toolbox.usaspending.config", None)
    module = importlib.import_module("data_ingestion_toolbox.usaspending.config")
    config = module.UsaspendingConfig()
    assert not any("key" in name or "token" in name for name in config.model_dump())


def test_the_fiscal_year_turns_in_october() -> None:
    """Covers: ETL-130 — October 1 starts the next federal fiscal year."""
    assert current_fiscal_year(date(2026, 9, 30)) == 2026
    assert current_fiscal_year(date(2026, 10, 1)) == 2027


def test_an_ordinary_run_reads_two_years_and_history_adds_the_rest_oldest_first() -> (
    None
):
    """Covers: ETL-130 — refresh years first, then fiscal 2008 onward."""
    assert fiscal_years(date(2025, 3, 1), history=False) == [2025, 2024]
    history = fiscal_years(date(2025, 3, 1), history=True)
    assert history[:2] == [2025, 2024]
    assert history[2:] == list(range(2008, 2024))


def test_each_cut_filters_the_year_and_nothing_else() -> None:
    """Covers: ETL-130 — the body names the scope, layer, period and one cut."""
    period = {"start_date": "2023-10-01", "end_date": "2024-09-30"}
    whole = geography_body(RequestKey(2024, "state", CUT_ALL, ""))
    assert whole == {
        "scope": "place_of_performance",
        "geo_layer": "state",
        "filters": {"time_period": [period]},
    }
    grants = geography_body(RequestKey(2024, "county", CUT_AWARD_TYPE, "grants"))
    assert grants["filters"]["award_type_codes"] == ["02", "03", "04", "05"]
    agency = geography_body(
        RequestKey(
            2024, "county", CUT_AGENCY, "HHS", "Department of Health and Human Services"
        )
    )
    assert agency["filters"]["agencies"] == [
        {
            "type": "awarding",
            "tier": "toptier",
            "name": "Department of Health and Human Services",
        }
    ]
    assert agency_list_body(2024, 2) == {
        "filters": {"time_period": [period]},
        "limit": 100,
        "page": 2,
    }


def test_a_year_plans_every_cut_for_both_layers() -> None:
    """Covers: ETL-130 — all awards, each group and each agency, per layer."""
    keys = list(planned_geography_keys(2024, [("DOD", "Department of Defense")]))
    per_layer = 1 + len(AWARD_GROUPS) + 1
    assert len(keys) == 2 * per_layer
    assert {key.geo_layer for key in keys} == {"state", "county"}
    assert len({key.identity for key in keys}) == len(keys)


def test_award_groups_do_not_share_a_code() -> None:
    """Covers: ETL-130 — no award type is counted in two groups."""
    codes = [code for group in AWARD_GROUPS for code in group.codes]
    assert len(codes) == len(set(codes))


def test_shape_codes_map_to_shared_geographies_or_stay_unplaced() -> None:
    """Covers: ETL-130 — postal codes and county FIPS place; an empty code does not."""
    assert geo_id_of("state", "WI") == "state:55"
    assert geo_id_of("state", "PR") == "state:72"
    assert geo_id_of("state", "") is None
    assert geo_id_of("state", "ZZ") is None
    assert geo_id_of("county", "55025") == "state:55|county:025"
    assert geo_id_of("county", "") is None
    assert geo_id_of("county", "5502") is None


def test_the_reviewed_state_answer_parses_every_row_as_published() -> None:
    """Covers: ETL-130 — amounts keep USAspending's text; the empty code is kept."""
    parsed = parse_geography(STATE_ALL.read_bytes(), geo_layer="state")
    assert parsed.row_count == NATION_FY2024_STATE_ROWS
    assert len(parsed.rows) == NATION_FY2024_STATE_ROWS
    assert parsed.quarantined == []
    wisconsin = next(row for row in parsed.rows if row.shape_code == "WI")
    assert wisconsin.value_source == WI_FY2024_ALL_AWARDS
    assert wisconsin.amount == Decimal(WI_FY2024_ALL_AWARDS)
    assert wisconsin.population_source == "5893718"
    assert any(row.shape_code == "" for row in parsed.rows)


def test_the_reviewed_county_answer_parses_dane_county_grants() -> None:
    """Covers: ETL-130 — the county layer reads five-digit FIPS codes."""
    parsed = parse_geography(COUNTY_GRANTS.read_bytes(), geo_layer="county")
    assert len(parsed.rows) == WI_COUNTIES
    dane = next(row for row in parsed.rows if row.shape_code == "55025")
    assert dane.value_source == DANE_FY2024_GRANTS
    assert dane.display_name == "Dane County"


def test_a_null_amount_is_missing_never_zero() -> None:
    """Covers: ETL-130 — ``null`` is missing with no number."""
    parsed = parse_geography(
        _answer(
            [
                {
                    "shape_code": "WI",
                    "display_name": "Wisconsin",
                    "aggregated_amount": None,
                }
            ]
        ),
        geo_layer="state",
    )
    assert parsed.rows[0].amount is None
    assert parsed.rows[0].value_status == "missing"


def test_unreadable_and_repeated_rows_are_set_aside_with_their_reason() -> None:
    """Covers: ETL-130 — a bad row is quarantined; the rest still load."""
    parsed = parse_geography(
        _answer(
            [
                {"shape_code": "WI", "aggregated_amount": 1.5},
                {"shape_code": "WI", "aggregated_amount": 2.5},
                {"shape_code": "MN", "aggregated_amount": "lots"},
                {"display_name": "no code"},
            ]
        ),
        geo_layer="state",
    )
    assert [row.shape_code for row in parsed.rows] == ["WI"]
    assert [
        (item.source_row_index, item.error_code) for item in parsed.quarantined
    ] == [
        (2, "repeated_shape"),
        (3, "unreadable_value"),
        (4, "unregistered_fields"),
    ]
    assert parsed.row_count == 4


def test_an_answer_for_another_layer_is_refused_whole() -> None:
    """Covers: ETL-130 — a county answer read as a state answer is row 0."""
    parsed = parse_geography(COUNTY_GRANTS.read_bytes(), geo_layer="state")
    assert parsed.rows == []
    assert [item.source_row_index for item in parsed.quarantined] == [0]


def test_the_reviewed_agency_list_parses_all_78_agencies() -> None:
    """Covers: ETL-130 — code, name, id and amount as published."""
    parsed = parse_agencies(AGENCIES.read_bytes())
    assert len(parsed.rows) == 78
    first = parsed.rows[0]
    assert (first.agency_code, first.agency_id) == ("HHS", 806)
    assert first.value_source == "1854703443882.42"
    names = agencies_of(AGENCIES.read_bytes())
    assert len(names) == 78
    assert ("DOD", "Department of Defense") in names


def test_the_transport_checks_layer_scope_fields_and_paging() -> None:
    """Covers: ETL-130 — only the expected answer is kept."""
    assert check_geography(STATE_ALL.read_bytes(), geo_layer="state") == 57
    with pytest.raises(UsaspendingPayloadError, match="wrong_layer_or_scope"):
        check_geography(STATE_ALL.read_bytes(), geo_layer="county")
    with pytest.raises(UsaspendingPayloadError, match="not_json"):
        check_geography(b"<html>", geo_layer="state")
    with pytest.raises(UsaspendingPayloadError, match="unregistered_fields"):
        check_geography(_answer([{"shape_code": "WI"}]), geo_layer="state")
    assert check_agencies(AGENCIES.read_bytes()) == (78, False)
    with pytest.raises(UsaspendingPayloadError, match="no_page_metadata"):
        check_agencies(json.dumps({"results": []}).encode())


def test_the_scripted_client_answers_only_the_reviewed_reads() -> None:
    """Covers: ETL-130 — other reads answer USAspending's empty result."""
    client = FixtureClient()
    response = post_search(
        GEOGRAPHY_PATH,
        geography_body(RequestKey(2024, "state", CUT_ALL, "")),
        geo_layer="state",
        config=_config(),
        client=client,
    )
    assert response.raw_bytes == STATE_ALL.read_bytes()
    other = post_search(
        GEOGRAPHY_PATH,
        geography_body(RequestKey(2023, "state", CUT_ALL, "")),
        geo_layer="state",
        config=_config(),
        client=client,
    )
    assert other.raw_bytes == empty_geography("state") and other.row_count == 0
    agencies = post_search(
        AGENCY_PATH,
        agency_list_body(2024, 1),
        config=_config(),
        client=FixtureClient(agency_limit=3),
    )
    assert agencies.row_count == 3 and agencies.has_next is False


def _scripted(statuses: list[int], body: bytes) -> FixtureClient:
    remaining = list(statuses)

    def answer(path: str, request_body: dict[str, Any]) -> tuple[int, bytes] | None:
        status = remaining.pop(0) if remaining else 200
        return (status, b'{"detail": "slow down"}') if status != 200 else (200, body)

    return FixtureClient(answers=answer)


def test_a_rate_refusal_is_retried_and_logged_with_rate_headers_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: ETL-130 — 429 retries with backoff; a later 200 is kept."""
    client = _scripted([429], STATE_ALL.read_bytes())
    sleeps: list[float] = []
    retries: list[BaseException] = []
    with caplog.at_level(logging.WARNING):
        response = post_search(
            GEOGRAPHY_PATH,
            geography_body(RequestKey(2024, "state", CUT_ALL, "")),
            geo_layer="state",
            config=_config(),
            client=client,
            on_retry=retries.append,
            sleep=sleeps.append,
        )
    assert response.row_count == 57
    assert len(client.calls) == 2 and len(sleeps) == 1 and len(retries) == 1
    assert "HTTP 429" in caplog.text


def test_a_client_error_fails_at_once_and_names_path_and_status() -> None:
    """Covers: ETL-130 — 4xx other than 429 is not retried."""
    client = _scripted([400], b"")
    with pytest.raises(UsaspendingFetchError) as raised:
        post_search(
            GEOGRAPHY_PATH,
            geography_body(RequestKey(2024, "state", CUT_ALL, "")),
            geo_layer="state",
            config=_config(),
            client=client,
            sleep=lambda _seconds: None,
        )
    assert raised.value.status == 400 and raised.value.code == "non_retryable_http"
    assert GEOGRAPHY_PATH in str(raised.value)
    assert len(client.calls) == 1


def test_server_errors_exhaust_the_attempts() -> None:
    """Covers: ETL-130 — 5xx retries up to max_attempts, then fails."""
    client = _scripted([503, 503], b"")
    with pytest.raises(UsaspendingFetchError, match="retry_exhausted"):
        post_search(
            GEOGRAPHY_PATH,
            geography_body(RequestKey(2024, "state", CUT_ALL, "")),
            geo_layer="state",
            config=_config(),
            client=client,
            sleep=lambda _seconds: None,
        )
    assert len(client.calls) == 2


def test_a_transport_error_is_retried() -> None:
    """Covers: ETL-130 — a dropped connection counts as one attempt."""

    class Flaky(FixtureClient):
        def post(self, url: str, **kwargs: Any) -> httpx.Response:
            if not self.calls:
                self.calls.append(("dropped", {}))
                raise httpx.ConnectError("dropped")
            return super().post(url, **kwargs)

    response = post_search(
        GEOGRAPHY_PATH,
        geography_body(RequestKey(2024, "state", CUT_ALL, "")),
        geo_layer="state",
        config=_config(),
        client=Flaky(),
        sleep=lambda _seconds: None,
    )
    assert response.row_count == 57


def test_a_negative_obligation_is_kept_as_published() -> None:
    """Covers: ETL-130 — a deobligation (negative amount) keeps its sign and text."""
    parsed = parse_geography(
        b'{"scope": "place_of_performance", "geo_layer": "county", "results": '
        b'[{"shape_code": "55025", "display_name": "Dane County", '
        b'"aggregated_amount": -1234567.89}]}',
        geo_layer="county",
    )
    (row,) = parsed.rows
    assert (row.value_source, row.amount) == ("-1234567.89", Decimal("-1234567.89"))
