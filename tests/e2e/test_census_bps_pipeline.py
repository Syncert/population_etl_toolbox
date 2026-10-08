"""Deterministic Building Permits flow from captured files to the API.

Every provider byte is a row copied verbatim from a published file, played
through the adapter's own capture path by a scripted client; nothing here
reaches the network. The node proves what permits must not lose on the way
to a consumer: that a figure is an authorization, the figure jurisdictions
reported beside the Bureau's estimate, the structure type, and a place that
reported nothing kept as not reported rather than zero.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_bps.registry import ANNUAL, MONTHLY, BpsSlice
from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import census_bps as bps
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "CENSUS_BPS"


@pytest.fixture
def bps_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return bps.reviewed_warehouse(postgres_connection_factory, request)


def test_permits_reach_the_neutral_api_as_authorizations_for_a_county_and_a_place(
    bps_warehouse,
) -> None:
    """Covers: E2E-014 — a glossary-discovered permits metric answers through
    `/api/v1/observations` for a county and a place with the fixture's values.

    Covers: E2E-006 — a place that reported nothing is not_reported, not zero.
    Covers: ETL-057 — the reported figure and the authorization basis are on the row.
    """
    factory = bps_warehouse
    bps.run_to_gold(factory, MONTHLY, 2024, 3)
    bps.run_to_gold(
        factory,
        ANNUAL,
        2024,
        12,
        files=(
            BpsSlice("county", ANNUAL, 2024, 12),
            BpsSlice("place", ANNUAL, 2024, 12, "south"),
        ),
    )
    emit_latest_publisher_ready(factory, publisher_schema="gold_census_bps")
    assert harvest_publisher(factory, Publisher("gold_census_bps")) == 24

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]

        county = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:units:1_unit:annual",
                "geo_id": "state:10|county:001",
            },
        )
        assert county.status_code == 200, county.text
        (row,) = county.json()["items"]
        assert Decimal(row["value"]) == Decimal("1056")
        assert row["dimensions"]["reported_value"] == "1053"
        assert row["dimensions"]["observation_basis"].startswith(
            "authorized by building permits"
        )
        assert row["dimensions"]["structure_type"] == "1_unit"
        assert row["geo_level"] == "COUNTY"

        place = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:units:5_plus_units:annual",
                "geo_level": "place",
                "limit": 100,
            },
        ).json()["items"]
        by_place = {item["geo_id"]: item for item in place}
        assert Decimal(by_place["state:10|place:21200"]["value"]) == Decimal("108")
        newark = by_place["state:10|place:50670"]
        assert newark["value"] is None and newark["value_status"] == "not_reported"
        assert newark["dimensions"]["months_reported"] == "0"

        monthly = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:units:5_plus_units:monthly",
                "geo_id": "us:1",
            },
        ).json()["items"]
        assert [(item["period_start"], Decimal(item["value"])) for item in monthly] == [
            ("2024-03-01", Decimal("34835"))
        ]
