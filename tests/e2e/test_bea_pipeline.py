"""Deterministic BEA regional accounts flow from captured files to the API.

Every provider byte is a row copied verbatim from a published bulk file,
played through the adapter's own capture path by a scripted client; nothing
here reaches the network. The node proves what BEA figures must not lose on
the way to a consumer: the release they came from, whether a dollar figure
is current, chained or per capita, and a withheld cell kept as withheld
rather than zero.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import bea
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "BEA"


@pytest.fixture
def bea_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return bea.reviewed_warehouse(postgres_connection_factory, request)


def test_income_and_gdp_reach_the_neutral_api_with_their_dollar_basis(
    bea_warehouse,
) -> None:
    """Covers: E2E-014 — a glossary-discovered BEA metric answers through
    `/api/v1/observations` for a county with the fixture's values.

    Covers: E2E-006 — a withheld (D) cell is withheld with no value, not zero.
    Covers: ETL-058 — the release date and the dollar basis are on the row.
    """
    factory = bea_warehouse
    for code in ("CAINC1", "CAGDP1", "CAGDP2"):
        bea.run_to_gold(factory, code)
    emit_latest_publisher_ready(factory, publisher_schema="gold_bea")
    assert harvest_publisher(factory, Publisher("gold_bea")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]

        income = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:CAINC1:3",
                "geo_id": "state:10|county:001",
                "year_from": 2024,
            },
        )
        assert income.status_code == 200, income.text
        (row,) = income.json()["items"]
        assert Decimal(row["value"]) == Decimal("55474")
        assert row["unit"] == "Dollars"
        assert row["dimensions"]["dollar_basis"] == "per_capita_current_dollars"
        assert row["geo_level"] == "COUNTY"
        assert row["release"] == "2026-02-05"

        real = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:CAGDP1:1",
                "geo_id": "state:10|county:001",
                "year_from": 2024,
            },
        ).json()["items"]
        assert [item["dimensions"]["dollar_basis"] for item in real] == [
            "chained_dollars"
        ]

        withheld = []
        for line in bea.get_table("CAGDP2").lines:
            items = client.get(
                "/api/v1/observations",
                params={
                    "metric_code": f"{SOURCE_CODE}:CAGDP2:{line}",
                    "geo_level": "county",
                    "limit": 500,
                },
            ).json()["items"]
            withheld.extend(
                item for item in items if item["value_status"] == "withheld"
            )
        assert withheld
        assert all(
            item["value"] is None and item["dimensions"]["value_source"] == "(D)"
            for item in withheld
        )
