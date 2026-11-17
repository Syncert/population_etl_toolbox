"""Deterministic NCES IPEDS flow from captured files to the API.

Every provider byte is NCES's own: the Wisconsin rows of the 2023 directory,
housing and enrolment files and the University of Wisconsin-Madison rows of
the 2012 files, played through the adapter's own capture path by a scripted
client; nothing here reaches the network. The node proves what a consumer
must be able to read off a county's college figures: the sum of the
institutions' own reported figures, how many institutions it holds, what the
figure is and the IPEDS year it is for.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import nces_ipeds
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "NCES_IPEDS"
PUBLISHER_SCHEMA = "gold_nces_ipeds"


@pytest.fixture
def ipeds_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return nces_ipeds.reviewed_warehouse(postgres_connection_factory, request)


def test_dane_college_housing_and_enrolment_reach_the_neutral_api(
    ipeds_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-131 — a glossary-discovered IPEDS figure answers through
    `/api/v1/observations` for Dane County with the sum of its institutions'
    reported figures, their count and the IPEDS year.

    Covers: E2E-004 — reading the same files again adds no row and the API
        answers identical JSON.
    """
    factory = ipeds_warehouse
    nces_ipeds.load_files(factory)
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) == 4

    with real_api_client() as client:
        catalog = client.get(
            "/api/v1/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        ).json()
        codes = {item["metric_code"] for item in catalog["items"]}
        assert {
            f"{SOURCE_CODE}:ipeds:housing_capacity",
            f"{SOURCE_CODE}:ipeds:fall_enrolment_total",
        } <= codes

        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"] is True

        housing = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:ipeds:housing_capacity",
                "geo_id": nces_ipeds.DANE,
            },
        )
        assert housing.status_code == 200, housing.text
        by_year = {item["period_start"][:4]: item for item in housing.json()["items"]}
        latest = by_year["2023"]
        assert int(latest["value"]) == 9484
        assert latest["geo_level"] == "COUNTY"
        assert latest["dimensions"]["institution_count"] == "2"
        assert (
            latest["dimensions"]["derivation"] == "sum_of_published_institution_figures"
        )
        assert int(by_year["2012"]["value"]) == 8471

        enrolment = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:ipeds:fall_enrolment_total",
                "geo_id": nces_ipeds.DANE,
            },
        ).json()
        fall_2023 = next(
            item
            for item in enrolment["items"]
            if item["period_start"].startswith("2023")
        )
        assert int(fall_2023["value"]) == 69371
        assert fall_2023["dimensions"]["institution_count"] == "8"

        again = nces_ipeds.load_files(factory, conf={"history": True})
        assert {result[2] for result in again} <= {"unchanged", "not_published"}
        repeat = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:ipeds:housing_capacity",
                "geo_id": nces_ipeds.DANE,
            },
        ).json()
        assert repeat["items"] == housing.json()["items"]
