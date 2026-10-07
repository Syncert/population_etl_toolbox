"""Deterministic HUD FMR and income-limit flow from captured workbooks to the API.

Every provider byte is a HUD User workbook trimmed to five rows, played
through the adapter's own capture path by a scripted client; nothing here
reaches the network. The node proves what a HUD figure must not lose on the
way to a consumer: that it is the value of a HUD area, which area, which
edition, and when it took effect.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import hud_fmr_il as hud
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "HUD_FMR_IL"


@pytest.fixture
def hud_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return hud.reviewed_warehouse(postgres_connection_factory, request)


def test_rents_and_limits_reach_the_neutral_api_with_their_area(hud_warehouse) -> None:
    """Covers: E2E-014 — a glossary-discovered HUD metric answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: ETL-065 — the HUD area, the edition and its effective date travel
    with the value, and the reissued edition is the one served.
    """
    factory = hud_warehouse
    hud.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_hud_fmr_il")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]
        rent = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:fmr_2br",
                "geo_id": hud.NAPA,
                "year_from": 2026,
                "year_to": 2026,
            },
        )
        assert rent.status_code == 200, rent.text
        (row,) = rent.json()["items"]
        assert (row["value"], row["unit"], row["release"]) == (
            "3315",
            "dollars per month",
            "FY2026-revised",
        )
        assert (row["dimensions"]["edition"], row["dimensions"]["effective_date"]) == (
            "revised",
            "2026-05-21",
        )
        assert row["dimensions"]["hud_area_code"] == "METRO34900M34900"
        assert "not a county estimate" in row["dimensions"]["observation_basis"]

        limit = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:income_limit_50_4p",
                "geo_id": hud.KENT,
            },
        ).json()["items"]
        assert [(item["value"], item["unit"], item["geo_level"]) for item in limit] == [
            ("53900", "dollars per year", "COUNTY")
        ]
