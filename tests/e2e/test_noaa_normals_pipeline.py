"""Deterministic NOAA climate normals flow from a captured archive to the API.

Every provider byte is a station file from NCEI's own 1991-2020 archive,
played through the adapter's own capture path by a scripted client; nothing
here reaches the network. The node proves what a county climate figure must
not lose on the way to a consumer: that it is a 30-year normal derived from
stations, which stations, and the boundary vintage that placed them.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import noaa_normals as normals
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "NOAA_NORMALS"


@pytest.fixture
def normals_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return normals.reviewed_warehouse(postgres_connection_factory, request)


def test_county_climate_normals_reach_the_neutral_api_as_derived(
    normals_warehouse,
) -> None:
    """Covers: E2E-014 — a glossary-discovered NOAA metric answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: ETL-069 — the row names its stations and boundary vintage and
    says it is a 30-year normal, derived from stations.
    """
    factory = normals_warehouse
    normals.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_noaa_normals")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"]
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:annual_mean_temperature",
                "geo_id": normals.NEW_CASTLE,
            },
        )
        assert response.status_code == 200, response.text
        (row,) = response.json()["items"]
        assert (row["value"], row["unit"], row["geo_level"]) == (
            "54.87",
            "degrees Fahrenheit",
            "COUNTY",
        )
        assert (row["period_start"], row["period_end"]) == ("1991-01-01", "2020-12-31")
        assert row["dimensions"]["station_ids"] == "USC00076410,USC00079605,USW00013781"
        assert row["dimensions"]["boundary_vintage"] == str(normals.BOUNDARY_VINTAGE)
        assert "not the value of any one year" in row["dimensions"]["observation_basis"]
