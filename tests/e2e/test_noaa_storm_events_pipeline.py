"""Deterministic NOAA Storm Events counts flow from captured files to the API.

Every provider byte is a row of NCEI's own details files or a line of NWS's
zone-county files, played through the adapter's own capture path by a
scripted client; nothing here reaches the network. The node proves what a
count must keep on the way to a consumer: the events it is made of, how many
were reported for the county itself and how many for a forecast zone
covering it.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import noaa_storm_events as storm
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "NOAA_STORM_EVENTS"


@pytest.fixture
def storm_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return storm.reviewed_warehouse(postgres_connection_factory, request)


def test_event_counts_reach_the_neutral_api_as_counted(storm_warehouse) -> None:
    """Covers: E2E-014 — a glossary-discovered Storm Events metric answers through
    `/api/v1/observations` for a county with the fixture's count.

    Covers: ETL-127 — the count says how many events were county- and
    zone-reported.
    """
    factory = storm_warehouse
    storm.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_noaa_storm_events")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"]
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:storm_events_all_types",
                "geo_id": storm.DANE,
                "year_from": 2024,
                "year_to": 2024,
            },
        )
        assert response.status_code == 200, response.text
        items = response.json()["items"]
        assert len(items) == 1
        dane = items[0]
        assert (dane["value"], dane["unit"], dane["geo_level"]) == (
            "10",
            "events",
            "COUNTY",
        )
        assert (dane["period_start"], dane["period_end"]) == (
            "2024-01-01",
            "2024-12-31",
        )
        assert dane["dimensions"]["county_reported"] == "3"
        assert dane["dimensions"]["zone_reported"] == "7"

        tornadoes = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:storm_events_tornado",
                "geo_id": storm.NATION,
                "year_from": 2024,
                "year_to": 2024,
            },
        )
        assert tornadoes.status_code == 200, tornadoes.text
        nation = tornadoes.json()["items"]
        assert [(row["value"], row["geo_level"]) for row in nation] == [
            ("1", "NATIONAL")
        ]
