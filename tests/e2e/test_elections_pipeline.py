"""Deterministic elections flow from captured files to the API.

Every provider byte is served by a scripted client from the reviewed
fixtures; nothing here reaches the network. The node proves what an
election figure must not lose on the way to a consumer: that a two-party
margin says how it was counted, that it is dated on election day, and which
MEDSL file it was read from.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import elections as el
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "ELECTIONS"


@pytest.fixture
def elections_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return el.reviewed_warehouse(postgres_connection_factory, request)


def test_presidential_margins_reach_the_neutral_api(elections_warehouse) -> None:
    """Covers: E2E-014 — a glossary-discovered elections metric answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: ETL-111 — the margin is Republican minus Democratic share of the
    two-party vote, dated on election day, with the MEDSL file as the release;
    county years before 2000 come from Algara & Amlani and name that file.
    """
    factory = elections_warehouse
    el.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_elections")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]
        margin = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:president_two_party_margin",
                "geo_id": el.DANE,
            },
        )
        assert margin.status_code == 200, margin.text
        items = sorted(margin.json()["items"], key=lambda item: item["period_start"])
        assert [
            (item["period_start"], float(item["value"]), item["geo_level"])
            for item in items
        ] == [
            ("1992-11-03", -33.33, "COUNTY"),
            ("1996-11-05", -36.84, "COUNTY"),
            ("2016-11-08", -50.67, "COUNTY"),
            ("2020-11-03", -53.48, "COUNTY"),
        ]
        # The 1990s come from Algara & Amlani; 2000 on from MEDSL only.
        assert items[0]["release"].startswith("Algara & Amlani ")
        first = items[2]
        assert first["unit"] == "percentage points"
        assert first["release"] == "MEDSL countypres_2000-2024.csv"
        assert (
            "Republican share minus the Democratic share"
            in (first["dimensions"]["observation_basis"])
        )
        assert first["dimensions"]["leading_party"] == "DEM"
        state = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:president_two_party_republican_share",
                "geo_id": el.WISCONSIN,
            },
        ).json()["items"]
        assert {item["geo_level"] for item in state} == {"STATE"} and len(state) == 2
