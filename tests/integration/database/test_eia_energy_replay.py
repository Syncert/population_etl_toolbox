"""Real PostgreSQL EIA energy routes, capture to gold (home-heating-fuel-prices).

Covers: ETL-120
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.eia.capture import plan_window
from data_ingestion_toolbox.eia.routes import ROUTES
from data_ingestion_toolbox.quality.sources import (
    eia_outside_survey_season,
    eia_read_reconciliation,
)
from tests.support import eia

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def eia_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return eia.reviewed_warehouse(postgres_connection_factory, request)


def _rows(factory: Callable[[], connection], sql: str, parameters: tuple = ()) -> list:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_each_energy_route_reaches_gold_for_the_nation_and_a_state(
    eia_warehouse,
) -> None:
    """Covers: ETL-120 — every route replays offline; the summer week has no row."""
    factory = eia_warehouse
    counts = {
        window: eia.energy_run_to_gold(factory, window)[1:]
        for window in eia.ENERGY_WINDOWS
    }
    assert counts == {
        "res_electricity": (6, 1),
        "res_natural_gas": (6, 1),
        "heating_fuels": (12, 1),
        "heating_fuels_summer": (0, 1),
        "diesel": (3, 1),
    }
    served = _rows(
        factory,
        """
        SELECT metric_key, geo_level, unit, COUNT(*)
          FROM gold_eia.observation_latest
         WHERE route <> 'gasoline'
         GROUP BY 1, 2, 3 ORDER BY 1, 2
        """,
    )
    assert served == [
        ("ELEC_RES", "NATIONAL", "cents per kilowatt-hour", 3),
        ("ELEC_RES", "STATE", "cents per kilowatt-hour", 3),
        ("EPD2D", "NATIONAL", "U.S. dollars per gallon", 3),
        ("EPD2F", "NATIONAL", "U.S. dollars per gallon", 3),
        ("EPD2F", "STATE", "U.S. dollars per gallon", 3),
        ("EPLLPA", "NATIONAL", "U.S. dollars per gallon", 3),
        ("EPLLPA", "STATE", "U.S. dollars per gallon", 3),
        ("NG_RES", "NATIONAL", "U.S. dollars per thousand cubic feet", 3),
        ("NG_RES", "STATE", "U.S. dollars per thousand cubic feet", 3),
    ]
    # No week of July is served: EIA does not survey heating fuels then.
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM gold_eia.observation_revision
         WHERE route = 'heating_fuels' AND EXTRACT(MONTH FROM period_start) = 7
        """,
    ) == [(0,)]
    # Wholesale heating fuel rows are set aside with their reason, not served.
    assert _rows(
        factory,
        """
        SELECT error_code, COUNT(*) FROM silver_eia.observation_quarantine
         GROUP BY 1
        """,
    ) == [("unregistered_product", 12)]
    publisher = _rows(
        factory,
        """
        SELECT source_object_key, units, valid_time_grains
          FROM gold_eia.metric_publisher
         WHERE source_object_key IN ('ELEC_RES', 'NG_RES', 'EPD2F')
         ORDER BY 1
        """,
    )
    assert publisher == [
        ("ELEC_RES", "cents per kilowatt-hour", ["MONTHLY"]),
        ("EPD2F", "U.S. dollars per gallon", ["WEEKLY"]),
        ("NG_RES", "U.S. dollars per thousand cubic feet", ["MONTHLY"]),
    ]
    # The key reached no stored request.
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM raw_capture.response_capture
         WHERE source_code = 'EIA'
           AND (request_parameters::TEXT LIKE %s OR endpoint LIKE %s)
        """,
        (f"%{eia.FIXTURE_KEY}%", f"%{eia.FIXTURE_KEY}%"),
    ) == [(0,)]

    reader = factory()
    try:
        with reader.cursor() as cursor:
            (reconciled,) = eia_read_reconciliation(cursor, {})
            (season,) = eia_outside_survey_season(cursor, {})
    finally:
        reader.close()
    assert reconciled.result == "pass"
    assert season.result == "pass"


def test_a_rerun_over_unchanged_bytes_writes_no_row(eia_warehouse) -> None:
    """Covers: ETL-120 — CHURN RULE: a repeat is counted on the read, never stored."""
    factory = eia_warehouse
    eia.energy_run_to_gold(factory, "res_electricity")
    before = _rows(
        factory,
        "SELECT COUNT(*), MAX(created_at) FROM silver_eia.fact_energy_price",
    )
    rerun, facts, _ = eia.energy_run_to_gold(factory, "res_electricity")
    assert facts == 0
    assert (
        _rows(
            factory,
            "SELECT COUNT(*), MAX(created_at) FROM silver_eia.fact_energy_price",
        )
        == before
    )
    assert _rows(
        factory,
        "SELECT repeated_row_count, route FROM control.eia_read WHERE run_id = %s",
        (str(rerun),),
    ) == [(6, "res_electricity")]
    # A revised month is a second row beside the first; latest serves it.
    name, _, _ = eia.ENERGY_WINDOWS["res_electricity"]
    original = (eia.ENERGY_FIXTURE_DIR / name).read_bytes()
    revised = original.replace(b'"price":"', b'"price":"1', 1)
    assert revised != original
    _, revised_facts, _ = eia.energy_run_to_gold(
        factory, "res_electricity", body=revised
    )
    assert revised_facts == 1
    assert _rows(
        factory,
        """
        SELECT
          (SELECT COUNT(*) FROM gold_eia.observation_revision WHERE route = 'res_electricity'),
          (SELECT COUNT(*) FROM gold_eia.observation_latest WHERE route = 'res_electricity')
        """,
    ) == [(7, 6)]


def test_a_heating_fuel_price_in_summer_is_kept_and_warned_about(
    eia_warehouse,
) -> None:
    """Covers: ETL-120 — DQ-EIA-005 reports a value outside October to March."""
    factory = eia_warehouse
    july = (
        b'{"response":{"total":1,"data":[{"period":"2025-07-07","duoarea":"SWI",'
        b'"area-name":"WISCONSIN","product":"EPD2F","process":"PRS",'
        b'"series":"W_EPD2F_PRS_SWI_DPG","value":"3.5","units":"$/GAL"}]}}'
    )
    _, facts, _ = eia.energy_run_to_gold(factory, "heating_fuels_summer", body=july)
    assert facts == 1
    reader = factory()
    try:
        with reader.cursor() as cursor:
            (season,) = eia_outside_survey_season(cursor, {})
    finally:
        reader.close()
    assert season.result == "warn" and season.observed_count == 1


def test_the_next_read_asks_for_the_refresh_window_of_its_own_route(
    eia_warehouse,
) -> None:
    """Covers: ETL-120 — LOADER RULE: full history first, then the refresh window."""
    factory = eia_warehouse
    config = eia.fixture_config()
    electricity = ROUTES["res_electricity"]
    assert plan_window(factory, config, "res_electricity") == electricity.history_start
    eia.energy_run_to_gold(factory, "res_electricity")
    # Newest month 2025-03, fourteen months back, never before the history.
    assert plan_window(factory, config, "res_electricity").isoformat() == "2024-01-01"
    # Another route's read does not move this one.
    assert (
        plan_window(factory, config, "res_natural_gas")
        == ROUTES["res_natural_gas"].history_start
    )
