"""Offline contracts for the Monthly Treasury Statement (Table 9) adapter.

Covers: ETL-128
"""

from __future__ import annotations

import importlib
import json
import re
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.treasury_mts.capture import window_start
from data_ingestion_toolbox.treasury_mts.client import (
    TreasuryMtsFetchError,
    TreasuryMtsPayloadError,
    check_page,
    fetch_page,
    page_parameters,
)
from data_ingestion_toolbox.treasury_mts.config import TreasuryMtsConfig
from data_ingestion_toolbox.treasury_mts.registry import (
    FIELDS,
    LINES,
    outlay_function_lines,
)
from data_ingestion_toolbox.treasury_mts.silver_treasury_mts.parse import (
    amount,
    parse_page,
)
from tests.support.treasury_mts import FixtureClient, fixture_rows, page_body

pytestmark = pytest.mark.unit

GOLD_DDL = (
    Path(__file__).resolve().parents[3]
    / "src/data_ingestion_toolbox/treasury_mts/gold_treasury_mts/DDL/gold_treasury_mts.sql"
)


def _config() -> TreasuryMtsConfig:
    return TreasuryMtsConfig(min_spacing_seconds=0, max_attempts=2)


def test_configuration_imports_with_no_io_and_no_credential() -> None:
    """Covers: ETL-128 — importing the configuration reads nothing."""
    sys.modules.pop("data_ingestion_toolbox.treasury_mts.config", None)
    module = importlib.import_module("data_ingestion_toolbox.treasury_mts.config")
    config = module.TreasuryMtsConfig()
    assert not any("key" in name or "token" in name for name in config.model_dump())


def test_a_page_asks_for_every_registered_field_from_the_window_start() -> None:
    """Covers: ETL-128 — the request names the fields, filter, sort and page."""
    parameters = page_parameters(2, page_size=10_000, since="2026-05-01")
    assert parameters == {
        "fields": ",".join(FIELDS),
        "filter": "record_date:gte:2026-05-01",
        "sort": "record_date,src_line_nbr",
        "page[number]": "2",
        "page[size]": "10000",
    }


@pytest.mark.parametrize(
    ("newest", "history", "expected"),
    [
        (None, False, "2015-03-31"),
        (date(2026, 9, 30), False, "2026-06-01"),
        (date(2026, 2, 28), False, "2025-11-01"),
        (date(2026, 9, 30), True, "2015-03-31"),
        (date(2015, 4, 30), False, "2015-03-31"),
    ],
)
def test_the_read_window_restarts_three_months_back(newest, history, expected) -> None:
    """Covers: ETL-128 — a run re-reads the months Treasury can restate."""
    assert window_start(newest, history=history) == expected


def test_a_heading_null_is_missing_and_amounts_keep_their_text() -> None:
    """Covers: ETL-128 — `null` is no number, never zero; a number is itself."""
    parsed = parse_page(page_body(fixture_rows(), 1, 10_000))
    assert parsed.quarantined == []
    assert len(parsed.rows) == parsed.row_count == 60
    by_key = {(row.record_date.isoformat(), row.line_code): row for row in parsed.rows}
    heading = by_key[("2026-09-30", "10")]
    assert (
        heading.value_source,
        heading.current_fytd_amount,
        heading.value_status,
    ) == (
        "null",
        None,
        "missing",
    )
    total = by_key[("2026-09-30", "340")]
    assert total.current_fytd_amount == Decimal("7395799983313.23")
    assert total.value_source == "7395799983313.23"
    assert total.fiscal_year == 2026 and total.calendar_month == 9
    assert amount("-155018778526.26") == Decimal("-155018778526.26")
    assert amount("null") is None


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ({"record_date": "2026-13-40"}, "unreadable_key"),
        ({"record_calendar_month": "08"}, "inconsistent_month"),
        ({"current_fytd_rcpt_outly_amt": "$1,000"}, "unreadable_value"),
        ({"line_code_nbr": "x"}, "unreadable_key"),
    ],
)
def test_an_unreadable_row_is_quarantined_alone(change, code) -> None:
    """Covers: ETL-128 — one bad row is set aside with its reason."""
    rows = fixture_rows()
    september = next(
        index for index, row in enumerate(rows) if row["record_date"] == "2026-09-30"
    )
    rows[september] = {**rows[september], **change}
    parsed = parse_page(page_body(rows, 1, 10_000))
    assert [
        (item.source_row_index, item.error_code) for item in parsed.quarantined
    ] == [(september + 1, code)]
    assert len(parsed.rows) == 59


def test_a_body_that_is_not_a_page_is_refused_whole() -> None:
    """Covers: ETL-128 — row 0 names a refused page."""
    parsed = parse_page(b'{"error": "rate limited"}')
    assert [
        (item.source_row_index, item.error_code) for item in parsed.quarantined
    ] == [(0, "not_a_table9_page")]
    with pytest.raises(TreasuryMtsPayloadError):
        check_page(b'{"error": "rate limited"}', "/t9")
    rows = fixture_rows()
    del rows[0]["line_code_nbr"]
    with pytest.raises(TreasuryMtsPayloadError):
        check_page(page_body(rows, 1, 10_000), "/t9")


def test_pages_are_read_until_the_total_the_api_reports() -> None:
    """Covers: ETL-128 — the page count comes from `meta.total-pages`."""
    client = FixtureClient()
    page = fetch_page(
        1, since="2015-03-31", config=TreasuryMtsConfig(page_size=25), client=client
    )
    assert (page.row_count, page.total_pages, page.http_status) == (25, 3, 200)
    assert client.calls[0]["page[size]"] == "25"


class _Refusing:
    def __init__(self, status: int) -> None:
        self.status = status
        self.calls = 0

    def get(self, url: str, *, params: dict, headers: dict) -> httpx.Response:
        self.calls += 1
        return httpx.Response(
            self.status, content=b"{}", request=httpx.Request("GET", url)
        )


def test_a_refusal_is_retried_only_when_retryable() -> None:
    """Covers: ETL-128 — 429 and 5xx retry; another 4xx fails at once."""
    throttled = _Refusing(429)
    with pytest.raises(TreasuryMtsFetchError):
        fetch_page(
            1,
            since="2015-03-31",
            config=_config(),
            client=throttled,
            sleep=lambda _s: None,
        )
    assert throttled.calls == 2
    missing = _Refusing(404)
    with pytest.raises(TreasuryMtsFetchError):
        fetch_page(
            1,
            since="2015-03-31",
            config=_config(),
            client=missing,
            sleep=lambda _s: None,
        )
    assert missing.calls == 1


def test_the_gold_line_definition_is_the_registry() -> None:
    """Covers: ETL-128 — the served lines and their names are the registered ones."""
    sql = GOLD_DDL.read_text(encoding="utf-8")
    block = sql.split("FROM (VALUES", 1)[1].split(") AS line(", 1)[0]
    rows = re.findall(r"\('(\d+)', '(\w+)', '([^']+)', '(\w+)', (TRUE|FALSE)\)", block)
    assert rows == [
        (
            line.line_code,
            line.measure_id,
            line.label,
            line.kind,
            "TRUE" if line.is_total else "FALSE",
        )
        for line in LINES
    ]


def test_the_function_lines_add_to_total_outlays_in_the_fixture() -> None:
    """Covers: ETL-128 — lines 140 to 330 are the parts of line 340, as published."""
    codes = {line.line_code for line in outlay_function_lines()}
    for record_date in ("2026-08-31", "2026-09-30"):
        rows = {
            row["line_code_nbr"]: row
            for row in fixture_rows()
            if row["record_date"] == record_date
        }
        parts = sum(
            Decimal(rows[code]["current_fytd_rcpt_outly_amt"]) for code in codes
        )
        assert parts == Decimal(rows["340"]["current_fytd_rcpt_outly_amt"])
    assert len(codes) == 19
    assert json.loads(page_body(fixture_rows(), 1, 60))["meta"]["total-pages"] == 1
