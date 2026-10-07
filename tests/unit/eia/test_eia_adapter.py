"""EIA retail gasoline adapter: offline configuration, transport and parsing.

Covers: ETL-080
"""

from __future__ import annotations

import importlib
import json
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from data_ingestion_toolbox.eia.capture import window_parameters
from data_ingestion_toolbox.eia.client import EiaClient, EiaFetchError, EiaPayloadError
from data_ingestion_toolbox.eia.config import EiaConfig
from data_ingestion_toolbox.eia.registry import PRODUCTS, classify_area, metric_key
from data_ingestion_toolbox.eia.silver_eia.parse import parse_page

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "eia"
WINDOW = (FIXTURES / "weekly_2026-08-31_2026-09-07.json").read_bytes()
KEY = "unit-test-eia-key"


def _config(**overrides) -> EiaConfig:
    return EiaConfig(eia_api_key=KEY, min_spacing_seconds=0, max_attempts=3, **overrides)


class _Client:
    def __init__(self, outcomes: list) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict] = []

    def get(self, url: str, *, params, headers) -> httpx.Response:
        self.calls.append({"url": url, "params": list(params)})
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def close(self) -> None:
        return None


def _response(status: int, content: bytes = b"{}") -> httpx.Response:
    return httpx.Response(
        status, content=content, request=httpx.Request("GET", "https://api.eia.gov/v2")
    )


def test_configuration_imports_without_io_and_reads_the_key_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-080 — importing reads nothing; the key comes from EIA_API_KEY and is never in a repr."""
    sys.modules.pop("data_ingestion_toolbox.eia.config", None)
    module = importlib.import_module("data_ingestion_toolbox.eia.config")
    monkeypatch.setenv("EIA_API_KEY", KEY)
    config = module.EiaConfig.from_environment()
    assert config.eia_api_key == KEY
    assert KEY not in repr(config)
    monkeypatch.delenv("EIA_API_KEY")
    assert module.EiaConfig.from_environment().eia_api_key == ""


@pytest.mark.parametrize(
    ("code", "geo_type", "geo_id", "usps"),
    [
        ("NUS", "nation", "us:1", None),
        ("SCA", "state", None, "CA"),
        ("STX", "state", None, "TX"),
        ("R1X", "provider_area", "area:eia:R1X", None),
        ("R5XCA", "provider_area", "area:eia:R5XCA", None),
        ("Y35NY", "provider_area", "area:eia:Y35NY", None),
        ("YBOS", "provider_area", "area:eia:YBOS", None),
    ],
)
def test_an_area_is_read_from_its_code(code, geo_type, geo_id, usps) -> None:
    """Covers: ETL-080 — the nation, a state by USPS code, a PADD or city as EIA's own area."""
    area = classify_area(code)
    assert (area.geo_type, area.geo_id, area.usps) == (geo_type, geo_id, usps)


@pytest.mark.parametrize("code", ["", "US", "Z12", "NEW YORK", "S1"])
def test_an_unregistered_area_code_is_refused(code: str) -> None:
    """Covers: ETL-080 — a code that is not the nation, a state, a PADD or a city is not guessed."""
    assert classify_area(code) is None


def test_one_metric_per_grade() -> None:
    """Covers: ETL-080 — the catalog code is the product code: EIA:EPMR regular, and so on."""
    assert sorted(PRODUCTS) == ["EPM0", "EPMM", "EPMP", "EPMR"]
    assert metric_key("EPMR") == "EPMR"


def test_the_fixture_window_parses_every_grade_at_every_area_kind() -> None:
    """Covers: ETL-080 — 232 prices, two weeks, every registered grade, and nothing set aside."""
    page = parse_page(WINDOW)
    assert page.quarantined == ()
    assert page.total == page.row_count == len(page.prices) == 232
    assert {price.week_start for price in page.prices} == {
        date(2026, 8, 31),
        date(2026, 9, 7),
    }
    assert {price.product for price in page.prices} == set(PRODUCTS)
    assert {price.geo_type for price in page.prices} == {"nation", "state", "provider_area"}
    california = [
        p for p in page.prices if p.duoarea == "SCA" and p.product == "EPMR"
    ]
    assert {p.state_usps for p in california} == {"CA"}
    assert all(p.value_status == "valid" and p.value > 0 for p in page.prices)
    assert all(p.units == "$/GAL" for p in page.prices)


def _page(rows: list[dict]) -> bytes:
    return json.dumps({"response": {"total": len(rows), "data": rows}}).encode()


def _row(**overrides) -> dict:
    row = {
        "period": "2026-09-07",
        "duoarea": "NUS",
        "area-name": "U.S.",
        "product": "EPMR",
        "product-name": "Regular Gasoline",
        "process": "PTE",
        "series": "EMM_EPMR_PTE_NUS_DPG",
        "value": "3.512",
        "units": "$/GAL",
    }
    row.update(overrides)
    return row


def test_a_week_without_a_price_is_missing_and_never_zero() -> None:
    """Covers: ETL-080 — a null value keeps the row, as missing, with no number."""
    (price,) = parse_page(_page([_row(value=None)])).prices
    assert (price.value, price.value_status, price.value_source) == (None, "missing", None)


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"units": "$/LITER"}, "unexpected_unit"),
        ({"product": "EPD2D"}, "unregistered_product"),
        ({"duoarea": "Z99"}, "unregistered_area"),
        ({"period": "2026-W36"}, "unreadable_week"),
        ({"value": "n/a"}, "unreadable_value"),
        ({"value": "0"}, "implausible_value"),
        ({"series": ""}, "series_missing"),
    ],
)
def test_an_unreadable_row_is_set_aside_with_its_reason(overrides, code) -> None:
    """Covers: ETL-080 — diesel, an unknown area, a bad week, a zero price: set aside, not loaded."""
    page = parse_page(_page([_row(**overrides)]))
    assert page.prices == ()
    assert [q.error_code for q in page.quarantined] == [code]


def test_an_answer_that_is_not_the_registered_shape_is_refused_whole() -> None:
    """Covers: ETL-080 — a page without response.data is one payload-level rejection."""
    page = parse_page(b'{"error": "bad"}')
    assert page.prices == () and [q.row_index for q in page.quarantined] == [-1]


def test_the_key_travels_only_in_the_query_and_never_in_an_error() -> None:
    """Covers: ETL-080 — the key is sent, and no error, retry or refusal repeats it."""
    with pytest.raises(EiaFetchError) as missing:
        EiaClient(EiaConfig(eia_api_key=""))
    assert missing.value.code == "missing_api_key"

    client = _Client(
        [_response(503), httpx.ConnectError(f"https://api.eia.gov/v2?api_key={KEY}"), _response(200, WINDOW)]
    )
    api = EiaClient(_config(), client=client, sleep=lambda _seconds: None)
    retries: list[BaseException] = []
    answer = api.get("petroleum/pri/gnd/data/", [("frequency", "weekly")], on_retry=retries.append)
    assert answer.raw_bytes == WINDOW
    assert all(("api_key", KEY) in call["params"] for call in client.calls)
    assert all(KEY not in str(error) for error in retries)

    refused = EiaClient(_config(), client=_Client([_response(403)]), sleep=lambda _s: None)
    with pytest.raises(EiaFetchError) as denied:
        refused.get("petroleum/pri/gnd/data/", [])
    assert denied.value.code == "key_refused" and KEY not in str(denied.value)

    with pytest.raises(EiaFetchError) as smuggled:
        api.get("petroleum/pri/gnd/data/", [("api_key", "other")])
    assert smuggled.value.code == "key_in_parameters"

    echo = EiaClient(
        _config(), client=_Client([_response(200, KEY.encode())]), sleep=lambda _s: None
    )
    with pytest.raises(EiaPayloadError) as echoed:
        echo.get("petroleum/pri/gnd/data/", [])
    assert echoed.value.code == "answer_echoes_key"


def test_a_window_asks_for_every_grade_sorted_for_stable_paging() -> None:
    """Covers: ETL-080 — gasoline only, retail sales, sorted by week and series, and no key."""
    parameters = window_parameters(date(2026, 8, 31), None, offset=5000, length=5000)
    assert ("frequency", "weekly") in parameters
    assert [value for name, value in parameters if name == "facets[product][]"] == sorted(PRODUCTS)
    assert ("sort[0][column]", "period") in parameters
    assert ("sort[1][column]", "series") in parameters
    assert ("offset", "5000") in parameters
    assert not any(name == "api_key" for name, _ in parameters)
    assert not any(name == "end" for name, _ in parameters)


def test_a_price_is_a_decimal_as_published() -> None:
    """Covers: ETL-080 — the published text becomes an exact decimal."""
    (price,) = parse_page(_page([_row(value="3.512")])).prices
    assert price.value == Decimal("3.512") and price.value_source == "3.512"
