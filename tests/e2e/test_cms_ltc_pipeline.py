"""Deterministic CMS long-term care flow from captured files to the API.

Every provider byte is CMS's own: the catalog entries as data.json holds
them and the Wisconsin rows of three CSV releases, played through the
adapter's own capture path by a scripted client; nothing here reaches the
network. The node proves what a consumer must be able to read off a county's
nursing home figures: the sum of CMS's own facility figures, how many
facilities it holds, what each figure is (residents on a survey, beds on a
cost report) and the period it is for.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import cms_ltc
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "CMS_LTC"
PUBLISHER_SCHEMA = "gold_cms_ltc"


@pytest.fixture
def cms_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return cms_ltc.reviewed_warehouse(postgres_connection_factory, request)


def test_dane_nursing_home_residents_and_beds_reach_the_neutral_api(
    cms_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-130 — a glossary-discovered CMS figure answers through
    `/api/v1/observations` for Dane County with the sum of its facilities'
    published figures, their count and the release period.

    Covers: E2E-004 — reading the same files again adds no row and the API
        answers identical JSON.
    """
    factory = cms_warehouse
    cms_ltc.load_releases(factory)
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) == 7

    with real_api_client() as client:
        catalog = client.get(
            "/api/v1/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        ).json()
        codes = {item["metric_code"] for item in catalog["items"]}
        assert {
            f"{SOURCE_CODE}:characteristics:total_residents",
            f"{SOURCE_CODE}:cost_report:number_of_beds",
        } <= codes

        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"] is True

        residents = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:characteristics:total_residents",
                "geo_id": cms_ltc.DANE,
            },
        )
        assert residents.status_code == 200, residents.text
        by_quarter = {item["period_start"]: item for item in residents.json()["items"]}
        q3 = by_quarter["2026-07-01"]
        assert int(q3["value"]) == 883
        assert q3["period_end"] == "2026-09-30"
        assert q3["geo_level"] == "COUNTY"
        assert q3["dimensions"]["facility_count"] == "16"
        assert q3["dimensions"]["derivation"] == "sum_of_published_facility_figures"
        assert q3["dimensions"]["basis"].startswith("residents counted")

        beds = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:cost_report:number_of_beds",
                "geo_id": cms_ltc.DANE,
            },
        ).json()
        (item,) = beds["items"]
        assert int(item["value"]) == 1521
        assert item["period_start"] == "2024-01-01"
        assert item["dimensions"]["facility_count"] == "17"

        _found, again = cms_ltc.load_releases(factory)
        assert [result[1] for result in again] == ["unchanged"] * 3
        repeat = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:characteristics:total_residents",
                "geo_id": cms_ltc.DANE,
            },
        ).json()
        assert repeat["items"] == residents.json()["items"]
