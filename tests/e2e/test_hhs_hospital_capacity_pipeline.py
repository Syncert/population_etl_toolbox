"""Deterministic HHS hospital capacity flow from captured pages to the API.

Every provider byte is HHS's own: the 1,044 weekly rows healthdata.gov
``anag-cw7u`` holds for the five Dane County hospitals, cut to the
registered columns and split into three pages, played through the adapter's
own capture path by a scripted client; nothing here reaches the network.
The node proves what a consumer must be able to read off a county's weekly
hospital figure: the sum of its hospitals' own seven-day averages, how many
hospitals reported, how many HHS suppressed or left blank, and the week.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import hhs_hospital_capacity as hhs
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "HHS_HOSPITAL_CAPACITY"
PUBLISHER_SCHEMA = "gold_hhs_hospital_capacity"
INPATIENT_BEDS = f"{SOURCE_CODE}:hospital:inpatient_beds"


@pytest.fixture
def hhs_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return hhs.reviewed_warehouse(postgres_connection_factory, request)


def test_dane_weekly_hospital_beds_reach_the_neutral_api(
    hhs_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-133 — a glossary-discovered HHS figure answers through
    `/api/v1/observations` for Dane County with the sum of its hospitals'
    published seven-day averages, the reporting count and the week.

    Covers: E2E-004 — reading the frozen dataset again adds no row and the
        API answers identical JSON.
    """
    factory = hhs_warehouse
    hhs.load_snapshot(factory)
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) == 8

    with real_api_client() as client:
        catalog = client.get(
            "/api/v1/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        ).json()
        codes = {item["metric_code"] for item in catalog["items"]}
        assert {
            INPATIENT_BEDS,
            f"{SOURCE_CODE}:hospital:staffed_adult_icu_beds",
        } <= codes

        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"] is True

        beds = client.get(
            "/api/v1/observations",
            params={
                "metric_code": INPATIENT_BEDS,
                "geo_id": hhs.DANE,
                "year_from": 2020,
                "year_to": 2024,
                "limit": 500,
            },
        )
        assert beds.status_code == 200, beds.text
        by_week = {item["period_start"][:10]: item for item in beds.json()["items"]}
        week = by_week["2020-08-23"]
        assert Decimal(week["value"]) == Decimal("1302.1")
        assert week["period_end"][:10] == "2020-08-29"
        assert week["geo_level"] == "COUNTY"
        assert week["dimensions"]["reporting_facility_count"] == "5"
        assert week["dimensions"]["suppressed_count"] == "0"
        assert Decimal(by_week["2024-04-21"]["value"]) == Decimal("1335.7")

        assert hhs.load_snapshot(factory, conf={"history": True})[0] is None
        repeat = client.get(
            "/api/v1/observations",
            params={
                "metric_code": INPATIENT_BEDS,
                "geo_id": hhs.DANE,
                "year_from": 2020,
                "year_to": 2024,
                "limit": 500,
            },
        ).json()
        assert repeat["items"] == beds.json()["items"]
