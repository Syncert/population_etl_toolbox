"""Deterministic LEHD LODES flow from captured files to the API.

Every provider byte comes from Delaware's published LODES8 files, trimmed
to two tracts' blocks and played through the adapter's own capture path by
a scripted client; nothing here reaches the network. The node proves what a
LODES count must not lose on the way to a consumer: that it is this
warehouse's sum of protected block estimates, and the vintage it came from.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import census_lodes as lodes
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "CENSUS_LODES"


@pytest.fixture
def lodes_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return lodes.reviewed_warehouse(postgres_connection_factory, request)


def test_commuting_counts_reach_the_neutral_api_with_their_basis(
    lodes_warehouse,
) -> None:
    """Covers: E2E-014 — a glossary-discovered LODES metric answers through
    `/api/v1/observations` for a county with the fixture's sum.

    Covers: ETL-063 — the vintage is the release and the basis says the
    count is a warehouse sum of protected estimates.
    """
    factory = lodes_warehouse
    lodes.run_to_gold(factory)
    assert harvest_publisher(factory, Publisher("gold_census_lodes")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]
        values: dict[str, int] = {}
        for measure in ("jobs", "live_and_work", "inbound"):
            response = client.get(
                "/api/v1/observations",
                params={
                    "metric_code": f"{SOURCE_CODE}:{measure}",
                    "geo_id": lodes.KENT,
                },
            )
            assert response.status_code == 200, response.text
            (row,) = response.json()["items"]
            assert row["unit"] == "jobs" and row["geo_level"] == "COUNTY"
            assert row["release"] == "20251202_1657"
            assert (
                "summed from census blocks by this warehouse"
                in row["dimensions"]["observation_basis"]
            )
            values[measure] = int(row["value"])
        assert values["live_and_work"] + values["inbound"] == values["jobs"]
