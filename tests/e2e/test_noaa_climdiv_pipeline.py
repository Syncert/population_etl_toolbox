"""Deterministic NOAA nClimDiv county months flow from captured files to the API.

Every provider byte is a line of NCEI's own county files, played through the
adapter's own capture path by a scripted client; nothing here reaches the
network. The node proves what a county month must keep on the way to a
consumer: the value as NCEI wrote it, its calendar month, and that a month
NCEI has not published is absent rather than zero.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import noaa_climdiv as climdiv
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "NOAA_CLIMDIV"


@pytest.fixture
def climdiv_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return climdiv.reviewed_warehouse(postgres_connection_factory, request)


def test_county_months_reach_the_neutral_api_as_published(climdiv_warehouse) -> None:
    """Covers: E2E-014 — a glossary-discovered nClimDiv metric answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: ETL-126 — the row keeps NCEI's text and month; an unpublished
    month is not served.
    """
    factory = climdiv_warehouse
    climdiv.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_noaa_climdiv")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"]
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:monthly_precipitation_total",
                "geo_id": climdiv.KENT,
                "year_from": 1895,
                "year_to": 1895,
            },
        )
        assert response.status_code == 200, response.text
        items = response.json()["items"]
        assert len(items) == 12
        january = next(row for row in items if row["dimensions"]["month"] == "1")
        assert (january["value"], january["unit"], january["geo_level"]) == (
            "4.51",
            "inches",
            "COUNTY",
        )
        assert (january["period_start"], january["period_end"]) == (
            "1895-01-01",
            "1895-01-31",
        )
        assert january["dimensions"]["value_source"] == "4.51"

        latest = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:monthly_precipitation_total",
                "geo_id": climdiv.KENT,
                "year_from": 2026,
                "year_to": 2026,
            },
        )
        assert latest.status_code == 200, latest.text
        months = sorted(
            int(row["dimensions"]["month"]) for row in latest.json()["items"]
        )
        # NCEI's November and December 2026 are its missing markers.
        assert months == list(range(1, 11))
