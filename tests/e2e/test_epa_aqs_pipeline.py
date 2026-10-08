"""Deterministic EPA air quality flow from a captured AirData file to the API.

Every provider byte is a row of EPA's own 2024 annual monitor file, played
through the adapter's own capture path by a scripted client; nothing here
reaches the network. The node proves what a county air figure must not lose
on the way to a consumer: that it is derived from monitors, which monitor,
and how many complete monitors there were.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import epa_aqs as aqs
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "EPA_AQS"


@pytest.fixture
def aqs_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return aqs.reviewed_warehouse(postgres_connection_factory, request)


def test_county_air_quality_reaches_the_neutral_api_as_derived(aqs_warehouse) -> None:
    """Covers: E2E-014 — a glossary-discovered EPA metric answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: ETL-068 — the row names its highest monitor and says it is a
    derived summary, not an EPA design value.
    """
    factory = aqs_warehouse
    aqs.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_epa_aqs")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:pm25_annual_mean",
                "geo_id": aqs.SUSSEX,
            },
        )
        assert response.status_code == 200, response.text
        (row,) = response.json()["items"]
        assert (row["value"], row["unit"], row["geo_level"]) == (
            "6.188827",
            "micrograms per cubic meter",
            "COUNTY",
        )
        assert row["dimensions"]["highest_monitor"] == "10005-1002-88101-3"
        assert row["dimensions"]["complete_monitors"] == "1"
        assert "not an EPA design value" in row["dimensions"]["observation_basis"]
        kent = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:pm25_annual_mean",
                "geo_id": aqs.KENT,
            },
        ).json()["items"]
        assert kent == []
