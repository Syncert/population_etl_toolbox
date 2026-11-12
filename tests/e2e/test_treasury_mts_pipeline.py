"""Deterministic Monthly Treasury Statement lines flow from captured pages to the API.

Every provider byte is a row of Treasury's own Table 9 read, played through
the adapter's own capture path by a scripted client; nothing here reaches the
network. The node proves what a fiscal-year total must keep on the way to a
consumer: the September statement's fiscal-year-to-date amount as published,
the October-to-September period, and the nation as its only geography.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import treasury_mts as mts
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "TREASURY_MTS"


@pytest.fixture
def mts_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return mts.reviewed_warehouse(postgres_connection_factory, request)


def test_fiscal_year_outlays_reach_the_neutral_api_as_published(mts_warehouse) -> None:
    """Covers: E2E-014 — a glossary-discovered Treasury metric answers through
    `/api/v1/observations` for the nation with the fixture's value.

    Covers: ETL-128 — the value is the September fiscal-year-to-date column,
    over the October-to-September fiscal year.
    """
    factory = mts_warehouse
    mts.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_treasury_mts")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"]
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:outlays_total",
                "geo_id": mts.NATION,
            },
        )
        assert response.status_code == 200, response.text
        (row,) = response.json()["items"]
        assert (row["value"], row["unit"], row["geo_level"]) == (
            mts.FY2026_TOTAL_OUTLAYS,
            "US dollars",
            "NATIONAL",
        )
        assert (row["period_start"], row["period_end"]) == ("2025-10-01", "2026-09-30")

        interest = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:outlays_net_interest",
                "geo_id": mts.NATION,
            },
        )
        assert interest.status_code == 200, interest.text
        assert [item["value"] for item in interest.json()["items"]] == [
            mts.FY2026_NET_INTEREST
        ]
