"""Deterministic FCC broadband flow from captured summaries to the API.

Every provider byte is a row of the FCC's own summary files, played through
the adapter's own capture path by a scripted client; nothing here reaches the
network. The node proves what an availability share must not lose on the way
to a consumer: that it is provider-reported availability, its vintage, and
the FCC's revision.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import fcc_bdc as bdc
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "FCC_BDC"


@pytest.fixture
def bdc_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return bdc.reviewed_warehouse(postgres_connection_factory, request)


def test_county_and_place_availability_reach_the_neutral_api(bdc_warehouse) -> None:
    """Covers: E2E-014 — a glossary-discovered FCC metric answers through
    `/api/v1/observations` for a county and a place with the fixture's value.

    Covers: ETL-071 — the row carries its as-of date and revision and says it
    is reported availability, not subscription.
    """
    factory = bdc_warehouse
    bdc.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_fcc_bdc")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]
        for geo_id, level, value in (
            (bdc.KENT, "COUNTY", "0.636547056"),
            (bdc.DOVER, "PLACE", "0.930515721"),
        ):
            response = client.get(
                "/api/v1/observations",
                params={
                    "metric_code": f"{SOURCE_CODE}:share_any_1000_100",
                    "geo_id": geo_id,
                    "year_from": 2025,
                    "year_to": 2025,
                },
            )
            assert response.status_code == 200, response.text
            (row,) = response.json()["items"]
            assert (row["value"], row["geo_level"]) == (value, level)
            assert (row["period_start"], row["period_end"]) == (
                "2025-12-31",
                "2025-12-31",
            )
            assert row["dimensions"]["revision"] == "29sep2026"
            assert row["dimensions"]["as_of_date"] == "2025-12-31"
            assert (
                "not what households subscribe to"
                in row["dimensions"]["observation_basis"]
            )
