"""Deterministic IRS SOI migration flow from captured files to the API.

Every provider byte is a row copied verbatim from SOI's published county
files, played through the adapter's own capture path by a scripted client;
nothing here reaches the network. The node proves what a flow must not lose
on the way to a consumer: both ends of the flow, SOI's own categories kept
as categories, a deleted category withheld rather than zero, and no net
figure computed from the two directions.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import irs_migration as irs
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "IRS_MIGRATION"


@pytest.fixture
def irs_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return irs.reviewed_warehouse(postgres_connection_factory, request)


def test_top_origins_and_destinations_reach_the_api_with_withheld_categories(
    irs_warehouse,
) -> None:
    """Covers: E2E-014 — a glossary-discovered SOI file total answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: E2E-006 — a deleted SOI category is withheld with no value, not zero.
    Covers: API-166 — `/api/v1/migration-flows` ranks a county's origins and
    destinations with SOI's categories beside them.
    """
    factory = irs_warehouse
    irs.run_to_gold(factory, "inflow", "2021-2022")
    irs.run_to_gold(factory, "inflow", "2022-2023")
    irs.run_to_gold(factory, "outflow", "2022-2023")
    emit_latest_publisher_ready(factory, publisher_schema="gold_irs_migration")
    assert harvest_publisher(factory, Publisher("gold_irs_migration")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]

        total = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:inflow:total_us_and_foreign:returns",
                "geo_id": irs.KENT,
            },
        )
        assert total.status_code == 200, total.text
        by_years = {
            item["dimensions"]["year_pair"]: item for item in total.json()["items"]
        }
        assert by_years["2022-2023"]["value"] == "5357"
        assert by_years["2022-2023"]["geo_level"] == "COUNTY"

        origins = client.get(
            "/api/v1/migration-flows",
            params={"geo_id": irs.KENT, "direction": "inflow", "limit": 5},
        )
        assert origins.status_code == 200, origins.text
        body = origins.json()
        assert body["year_pair"] == "2022-2023"
        assert body["items"][0]["counterpart_geo_id"] == irs.PHILADELPHIA
        assert body["items"][0]["returns"] == 273
        assert body["items"][0]["destination_geo_id"] == irs.KENT
        assert {item["category"] for item in body["categories"]} >= {
            "other_flows_same_state"
        }
        assert (
            body["totals"][0]["category"] == "total_us_and_foreign"
            and body["totals"][0]["returns"] == 5357
        )

        older = client.get(
            "/api/v1/migration-flows",
            params={
                "geo_id": irs.KENT,
                "direction": "inflow",
                "year_pair": "2021-2022",
            },
        ).json()
        withheld = next(
            item
            for item in older["categories"]
            if item["category"] == "foreign_other_flows"
        )
        assert withheld["value_status"] == "withheld" and withheld["returns"] is None

        destinations = client.get(
            "/api/v1/migration-flows",
            params={"geo_id": irs.KENT, "direction": "outflow"},
        ).json()
        assert destinations["items"][0]["origin_geo_id"] == irs.KENT
        assert all("net" not in key for item in destinations["items"] for key in item)
