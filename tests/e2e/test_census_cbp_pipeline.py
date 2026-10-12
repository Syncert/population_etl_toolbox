"""Deterministic County Business Patterns flow from captured files to the API.

Every provider byte is a row copied verbatim from the Bureau's published
files, played through the adapter's own capture path by a scripted client;
nothing here reaches the network. The node proves what a CBP figure must
not lose on the way to a consumer: what it covers, the noise flag beside
it, and a withheld cell kept as withheld rather than the zero the file
writes.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import census_cbp as cbp
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "CENSUS_CBP"


@pytest.fixture
def cbp_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return cbp.reviewed_warehouse(postgres_connection_factory, request)


def test_business_patterns_reach_the_neutral_api_with_flags_and_coverage(
    cbp_warehouse,
) -> None:
    """Covers: E2E-014 — a glossary-discovered CBP metric answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: E2E-006 — a withheld (D) cell is withheld with no value, not zero.
    Covers: ETL-062 — the noise flag and the coverage statement are on the row.
    """
    factory = cbp_warehouse
    cbp.run_to_gold(factory, "county", 2016)
    cbp.run_to_gold(factory, "county", 2023)
    emit_latest_publisher_ready(factory, publisher_schema="gold_census_cbp")
    assert harvest_publisher(factory, Publisher("gold_census_cbp")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]

        jobs = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:emp:total",
                "geo_id": "state:10|county:001",
                "year_from": 2023,
            },
        )
        assert jobs.status_code == 200, jobs.text
        (row,) = jobs.json()["items"]
        assert row["value"] == "61078"
        assert row["unit"] == "employees"
        assert row["uncertainty"]["noise_flag"] == "G"
        assert "excludes the self-employed" in row["dimensions"]["observation_basis"]
        assert row["geo_level"] == "COUNTY"

        mining = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:emp:21",
                "geo_id": "state:10|county:005",
                "year_to": 2016,
            },
        ).json()["items"]
        assert [
            (
                item["value"],
                item["value_status"],
                item["dimensions"]["employment_range"],
            )
            for item in mining
        ] == [(None, "withheld", "B")]
