"""Deterministic EIA retail gasoline flow from captured answers to the API.

Every provider byte is EIA API v2's own answer, copied verbatim and played
through the adapter's capture path by a scripted client; nothing here
reaches the network. The node proves what a gasoline price must not lose on
the way to a consumer: its grade, its week, the kind of area it describes --
the nation, a state, or one of EIA's own PADDs or cities -- and its unit.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import eia
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]


@pytest.fixture
def eia_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return eia.reviewed_warehouse(postgres_connection_factory, request)


def test_weekly_gasoline_reaches_the_neutral_api_at_every_area_kind(
    eia_warehouse,
) -> None:
    """Covers: ETL-080 — a glossary-discovered grade answers through `/api/v1/observations`
    at the nation, a state and an EIA city, in dollars per gallon, week by week.
    """
    factory = eia_warehouse
    eia.run_to_gold(factory)
    assert harvest_publisher(factory, Publisher("gold_eia")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == "EIA"
        )["served_by_neutral_routes"]
        metric = client.get("/api/v1/catalog/metrics/EIA:EPMR").json()
        assert sorted(metric["valid_geo_grains"]) == [
            "NATIONAL",
            "PROVIDER_AREA",
            "STATE",
        ]

        for geo_id, geo_level in (
            ("us:1", "NATIONAL"),
            ("state:06", "STATE"),
            ("area:eia:Y35NY", "PROVIDER_AREA"),
        ):
            response = client.get(
                "/api/v1/observations",
                params={"metric_code": "EIA:EPMR", "geo_id": geo_id},
            )
            assert response.status_code == 200, response.text
            rows = response.json()["items"]
            assert [row["period_start"] for row in rows] == ["2026-08-31", "2026-09-07"]
            assert all(row["geo_level"] == geo_level for row in rows)
            assert all(row["unit"] == "U.S. dollars per gallon" for row in rows)
            assert all(Decimal(row["value"]) > 0 for row in rows)
            assert all(row["dimensions"]["grade"] == "Regular gasoline" for row in rows)

        by_padd = client.get(
            "/api/v1/observations",
            params={
                "metric_code": "EIA:EPMR",
                "geo_level": "PROVIDER_AREA",
                "limit": 500,
            },
        ).json()["items"]
        assert {row["dimensions"]["duoarea"] for row in by_padd} >= {
            "R10",
            "R1X",
            "YBOS",
        }


def test_home_energy_prices_reach_the_neutral_api_in_their_own_units(
    eia_warehouse,
) -> None:
    """Covers: ETL-120 — each energy metric answers at the nation and a state,
    month by month or week by week, in the unit EIA states.
    """
    factory = eia_warehouse
    for window in ("res_electricity", "res_natural_gas", "heating_fuels"):
        eia.energy_run_to_gold(factory, window)
    assert harvest_publisher(factory, Publisher("gold_eia")) > 0

    with real_api_client() as client:
        for code, unit, periods in (
            ("EIA:ELEC_RES", "cents per kilowatt-hour", 3),
            ("EIA:NG_RES", "U.S. dollars per thousand cubic feet", 3),
            ("EIA:EPD2F", "U.S. dollars per gallon", 3),
        ):
            metric = client.get(f"/api/v1/catalog/metrics/{code}").json()
            assert sorted(metric["valid_geo_grains"]) == ["NATIONAL", "STATE"]
            for geo_id, geo_level in (("us:1", "NATIONAL"), ("state:55", "STATE")):
                response = client.get(
                    "/api/v1/observations",
                    params={"metric_code": code, "geo_id": geo_id},
                )
                assert response.status_code == 200, response.text
                rows = response.json()["items"]
                assert len(rows) == periods
                assert all(row["geo_level"] == geo_level for row in rows)
                assert all(row["unit"] == unit for row in rows)
                assert all(Decimal(row["value"]) > 0 for row in rows)
