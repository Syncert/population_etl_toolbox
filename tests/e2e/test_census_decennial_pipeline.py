"""Deterministic 2020 Census flow from captured responses to the API.

Every provider byte is a checked-in Census Data API response, played through
the adapter's own capture path by a scripted client; nothing here reaches the
network. The node proves what the group-quarters counts must keep on the way
to a consumer: the published integer as written, the Census Day period, the
part-of relation to the group quarters total, and tract grain for Dane County.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_decennial.registry import PL
from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import census_decennial
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "CENSUS_DECENNIAL"
PUBLISHER_SCHEMA = "gold_census_decennial"


@pytest.fixture
def decennial_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return census_decennial.reviewed_warehouse(postgres_connection_factory, request)


def _dane_row() -> dict[str, str]:
    return next(
        row
        for row in census_decennial.fixture_rows("county", "*")
        if row["county"] == "025"
    )


def test_group_quarters_reach_the_neutral_api_for_dane_and_its_tracts(
    decennial_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-129 — a glossary-discovered 2020 Census count answers
    through `/api/v1/observations` for Dane County and its tracts with the
    fixture's integer, dated Census Day.

    Covers: E2E-004 — replaying the same captures adds no count and the API
        answers identical JSON.
    """
    factory = decennial_warehouse
    census_decennial.run_to_gold(factory)
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) == len(PL.variables)

    with real_api_client() as client:
        catalog = client.get(
            "/api/v1/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        ).json()
        codes = {item["metric_code"] for item in catalog["items"]}
        assert {f"{SOURCE_CODE}:pl:P5_008N", f"{SOURCE_CODE}:pl:P1_001N"} <= codes

        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"] is True
        assert "/api/v1/observations" in {
            route["path"] for route in entry["observation_routes"]
        }

        dane = _dane_row()
        answers = {}
        for variable_id in ("P5_001N", "P5_005N", "P5_008N", "P1_001N", "H1_001N"):
            response = client.get(
                "/api/v1/observations",
                params={
                    "metric_code": f"{SOURCE_CODE}:pl:{variable_id}",
                    "geo_id": census_decennial.DANE,
                },
            )
            assert response.status_code == 200, response.text
            payload = response.json()
            answers[variable_id] = payload
            assert payload["source_code"] == SOURCE_CODE
            (item,) = payload["items"]
            assert int(item["value"]) == int(dane[variable_id])
            assert item["value_status"] == "valid"
            assert item["geo_level"] == "COUNTY"
            assert item["period_start"] == "2020-04-01"
            assert item["dimensions"]["census_year"] == "2020"
        college = answers["P5_008N"]["items"][0]["dimensions"]
        assert college["parent_variable"] == "P5_007N"

        # Every Dane tract answers at tract grain, inside Wisconsin.
        tracts = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:pl:P5_008N",
                "geo_level": "tract",
                "state_fips": "55",
                "limit": 500,
            },
        ).json()
        assert tracts["total"] == len(census_decennial.dane_tract_codes())
        assert sum(int(item["value"]) for item in tracts["items"]) == int(
            dane["P5_008N"]
        )

        # Replay: the same bytes add no count, and the answer is unchanged.
        census_decennial.run_to_gold(factory)
        again = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:pl:P5_008N",
                "geo_id": census_decennial.DANE,
            },
        ).json()
        assert again["items"] == answers["P5_008N"]["items"]
