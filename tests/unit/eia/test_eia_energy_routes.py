"""EIA energy routes: the registry, the requests and the offline parser.

Covers: ETL-120
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from data_ingestion_toolbox.eia.capture import (
    _months_before,
    period_text,
    window_parameters,
)
from data_ingestion_toolbox.eia.registry import PROCESS, PRODUCTS
from data_ingestion_toolbox.eia.routes import (
    ENERGY_ROUTES,
    GASOLINE,
    ROUTES,
    recorded_parameters,
    request_parameters,
    route_for,
)
from data_ingestion_toolbox.eia.silver_eia.energy_parse import (
    classify_energy_area,
    parse_energy_page,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "eia" / "energy"


def _page(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def test_gasoline_request_and_record_are_the_ones_before_the_registry() -> None:
    """Covers: ETL-120 — The gasoline captures, fingerprints and replay must not move."""
    expected = [
        ("frequency", "weekly"),
        ("data[0]", "value"),
        *(("facets[product][]", code) for code in sorted(PRODUCTS)),
        ("facets[process][]", PROCESS),
        ("start", "2026-08-31"),
        ("end", "2026-09-07"),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
        ("sort[1][column]", "series"),
        ("sort[1][direction]", "asc"),
        ("offset", "0"),
        ("length", "5000"),
    ]
    assert (
        window_parameters(date(2026, 8, 31), date(2026, 9, 7), offset=0, length=5000)
        == expected
    )
    assert recorded_parameters(
        ROUTES[GASOLINE], "2026-08-31", None, offset=0, length=5000
    ) == {
        "start": "2026-08-31",
        "end": None,
        "offset": 0,
        "length": 5000,
        "products": sorted(PRODUCTS),
        "process": PROCESS,
    }
    assert (
        ROUTES[GASOLINE].parser_contract_version == "eia_v2_petroleum_pri_gnd_json:v1"
    )


def test_electricity_asks_for_residential_months_sorted_by_area_and_sector() -> None:
    """Covers: ETL-120 — electricity asks for residential months sorted by area and sector."""
    route = route_for("res_electricity")
    assert request_parameters(route, "2025-01", "2025-03", offset=0, length=5000) == [
        ("frequency", "monthly"),
        ("data[0]", "price"),
        ("facets[sectorid][]", "RES"),
        ("start", "2025-01"),
        ("end", "2025-03"),
        ("sort[0][column]", "period"),
        ("sort[0][direction]", "asc"),
        ("sort[1][column]", "stateid"),
        ("sort[1][direction]", "asc"),
        ("sort[2][column]", "sectorid"),
        ("sort[2][direction]", "asc"),
        ("offset", "0"),
        ("length", "5000"),
    ]
    record = recorded_parameters(route, "2025-01", None, offset=0, length=5000)
    assert record["route"] == "res_electricity"
    assert record["facets"] == {"sectorid": ["RES"]}


def test_every_energy_route_is_registered_and_unknown_routes_refused() -> None:
    """Covers: ETL-120 — every energy route is registered and unknown routes refused."""
    assert set(ENERGY_ROUTES) == {
        "diesel",
        "res_electricity",
        "res_natural_gas",
        "heating_fuels",
    }
    assert GASOLINE not in ENERGY_ROUTES
    with pytest.raises(ValueError, match="no EIA route"):
        route_for("coal")


def test_periods_are_read_in_the_routes_own_format() -> None:
    """Covers: ETL-120 — periods are read in the routes own format."""
    assert period_text(ROUTES["res_electricity"], date(2025, 3, 1)) == "2025-03"
    assert period_text(ROUTES["heating_fuels"], date(2025, 1, 6)) == "2025-01-06"
    assert period_text(ROUTES["diesel"], None) is None
    assert _months_before(date(2025, 3, 1), 14) == date(2024, 1, 1)
    assert _months_before(date(2025, 1, 1), 1) == date(2024, 12, 1)


def test_electricity_reads_the_nation_and_states_not_census_divisions() -> None:
    """Covers: ETL-120 — electricity reads the nation and states not census divisions."""
    route = ROUTES["res_electricity"]
    assert classify_energy_area(route, "US") == ("nation", "us:1", None)
    assert classify_energy_area(route, "WI") == ("state", None, "WI")
    assert classify_energy_area(route, "ENC") is None
    assert classify_energy_area(route, "PACC") is None

    page = parse_energy_page(route, _page("data_electricity.json"))
    assert page.row_count == 6 and len(page.prices) == 6 and not page.quarantined
    wisconsin = [p for p in page.prices if p.area_code == "WI"]
    assert {p.series_id for p in wisconsin} == {"res_electricity:WI:RES"}
    assert all(p.units == "cents per kilowatt-hour" for p in page.prices)
    assert all(p.metric_key == "ELEC_RES" for p in page.prices)
    first = min(wisconsin, key=lambda p: p.period_start)
    assert (first.period_start, first.period_end) == (
        date(2025, 1, 1),
        date(2025, 1, 31),
    )
    assert first.value is not None and first.value > Decimal("5")
    assert not any(p.outside_survey_season for p in page.prices)


def test_natural_gas_keeps_eias_series_and_dollars_per_mcf() -> None:
    """Covers: ETL-120 — natural gas keeps eias series and dollars per mcf."""
    page = parse_energy_page(ROUTES["res_natural_gas"], _page("data_natgas.json"))
    assert len(page.prices) == 6 and not page.quarantined
    assert {p.geo_type for p in page.prices} == {"nation", "state"}
    assert all(p.units == "$/MCF" and p.metric_key == "NG_RES" for p in page.prices)
    assert any(p.series_id == "N3010WI3" for p in page.prices)


def test_heating_fuels_set_wholesale_aside_and_a_summer_week_has_no_row() -> None:
    """Covers: ETL-120 — heating fuels set wholesale aside and a summer week has no row."""
    route = ROUTES["heating_fuels"]
    winter = parse_energy_page(route, _page("data_wfr_winter.json"))
    assert len(winter.prices) == 12
    assert {p.metric_key for p in winter.prices} == {"EPD2F", "EPLLPA"}
    assert all(p.process == "PRS" for p in winter.prices)
    assert len(winter.quarantined) == 12
    assert {q.error_code for q in winter.quarantined} == {"unregistered_product"}
    assert all("'PWR'" in q.error_summary for q in winter.quarantined)
    assert not any(p.outside_survey_season for p in winter.prices)

    summer = parse_energy_page(route, _page("data_wfr_summer.json"))
    assert summer.prices == () and summer.quarantined == ()


def test_a_value_outside_the_survey_season_is_kept_and_flagged() -> None:
    """Covers: ETL-120 — a value outside the survey season is kept and flagged."""
    route = ROUTES["heating_fuels"]
    july = (
        b'{"response":{"total":1,"data":[{"period":"2025-07-07","duoarea":"SWI",'
        b'"area-name":"WISCONSIN","product":"EPD2F","process":"PRS",'
        b'"series":"W_EPD2F_PRS_SWI_DPG","value":"3.5","units":"$/GAL"}]}}'
    )
    (price,) = parse_energy_page(route, july).prices
    assert price.outside_survey_season and price.value == Decimal("3.5")
    assert price.period_end == date(2025, 7, 13)


def test_diesel_reads_on_highway_retail_prices() -> None:
    """Covers: ETL-120 — diesel reads on highway retail prices."""
    page = parse_energy_page(ROUTES["diesel"], _page("data_diesel.json"))
    assert len(page.prices) == 3 and not page.quarantined
    assert {p.metric_key for p in page.prices} == {"EPD2D"}
    assert all(p.frequency == "weekly" for p in page.prices)


@pytest.mark.parametrize(
    ("row", "code"),
    [
        (
            '"period":"2025-01","stateid":"WI","sectorid":"COM","price":"9","price-units":"cents per kilowatt-hour"',
            "unregistered_product",
        ),
        (
            '"period":"2025-01","stateid":"WI","sectorid":"RES","price":"9","price-units":"dollars"',
            "unexpected_unit",
        ),
        (
            '"period":"2025-W01","stateid":"WI","sectorid":"RES","price":"9","price-units":"cents per kilowatt-hour"',
            "unreadable_period",
        ),
        (
            '"period":"2025-01","stateid":"ENC","sectorid":"RES","price":"9","price-units":"cents per kilowatt-hour"',
            "unregistered_area",
        ),
        (
            '"period":"2025-01","stateid":"WI","sectorid":"RES","price":"n/a","price-units":"cents per kilowatt-hour"',
            "unreadable_value",
        ),
        (
            '"period":"2025-01","stateid":"WI","sectorid":"RES","price":"0","price-units":"cents per kilowatt-hour"',
            "implausible_value",
        ),
    ],
)
def test_a_row_that_cannot_be_read_is_set_aside_with_its_reason(
    row: str, code: str
) -> None:
    """Covers: ETL-120 — a row that cannot be read is set aside with its reason."""
    payload = ('{"response":{"total":1,"data":[{' + row + "}]}}").encode()
    page = parse_energy_page(ROUTES["res_electricity"], payload)
    assert page.prices == ()
    assert [q.error_code for q in page.quarantined] == [code]


def test_a_month_with_no_price_is_missing_never_zero() -> None:
    """Covers: ETL-120 — a month with no price is missing never zero."""
    payload = (
        b'{"response":{"total":1,"data":[{"period":"2025-01","stateid":"WI",'
        b'"sectorid":"RES","price":null,"price-units":"cents per kilowatt-hour"}]}}'
    )
    (price,) = parse_energy_page(ROUTES["res_electricity"], payload).prices
    assert price.value is None and price.value_status == "missing"


def test_a_payload_that_is_not_an_answer_is_set_aside_whole() -> None:
    """Covers: ETL-120 — a payload that is not an answer is set aside whole."""
    page = parse_energy_page(ROUTES["res_electricity"], b"not json")
    assert page.prices == () and page.row_count == 0
    assert [q.row_index for q in page.quarantined] == [-1]


@pytest.mark.parametrize(
    ("route", "meta"),
    [
        ("res_electricity", "meta_electricity.json"),
        ("res_natural_gas", "meta_natgas.json"),
        ("heating_fuels", "meta_wfr.json"),
    ],
)
def test_a_full_read_starts_where_eia_says_the_route_starts(
    route: str, meta: str
) -> None:
    """Covers: ETL-120 — LOADER RULE: the first read asks for every period EIA publishes."""
    import json

    start = json.loads(_page(meta))["response"]["startPeriod"]
    assert period_text(ROUTES[route], ROUTES[route].history_start) == start
