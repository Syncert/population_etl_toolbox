"""Deterministic CDC COVID-19 case count flow from captured slices to the API.

Every provider byte is CDC's own: the grouped responses data.cdc.gov
``n8mc-b4w4`` gave for Wisconsin 2021-11 and California 2022-01, played
through the adapter's own capture path by a scripted client; nothing here
reaches the network. The node proves what a consumer must be able to read
off a county's monthly count: the number of CDC's published case records,
the case month, and that these are published records, not all cases.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary import emit_latest_publisher_ready
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import cdc_covid_cases as covid
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "CDC_COVID_CASES"
PUBLISHER_SCHEMA = "gold_cdc_covid_cases"
CASE_RECORDS = f"{SOURCE_CODE}:covid:case_records:all_ages"


@pytest.fixture
def covid_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return covid.reviewed_warehouse(postgres_connection_factory, request)


def test_dane_monthly_case_records_reach_the_neutral_api(
    covid_warehouse: Callable[[], connection],
) -> None:
    """Covers: ETL-135 — a glossary-discovered CDC count answers through
    `/api/v1/observations` for Dane County with CDC's published case records
    for the month, the age group and the coverage note.

    Covers: E2E-004 — reading the frozen dataset again adds no row and the
        API answers identical JSON.
    """
    factory = covid_warehouse
    covid.load_snapshot(factory)
    emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    assert harvest_publisher(factory, Publisher(PUBLISHER_SCHEMA)) == 30

    with real_api_client() as client:
        catalog = client.get(
            "/api/v1/catalog/metrics", params={"source_code": SOURCE_CODE, "limit": 100}
        ).json()
        codes = {item["metric_code"] for item in catalog["items"]}
        assert {
            CASE_RECORDS,
            f"{SOURCE_CODE}:covid:hospitalized_records:age_65_plus",
        } <= codes

        capabilities = client.get("/api/v1/catalog/capabilities").json()
        entry = next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )
        assert entry["served_by_neutral_routes"] is True

        params = {
            "metric_code": CASE_RECORDS,
            "geo_id": covid.DANE,
            "year_from": 2020,
            "year_to": 2024,
            "limit": 100,
        }
        cases = client.get("/api/v1/observations", params=params)
        assert cases.status_code == 200, cases.text
        items = cases.json()["items"]
        assert len(items) == 1
        month = items[0]
        assert int(float(month["value"])) == 3651
        assert month["period_start"][:10] == "2021-11-01"
        assert month["period_end"][:10] == "2021-11-30"
        assert month["geo_level"] == "COUNTY"
        assert month["dimensions"]["age_key"] == "all_ages"
        assert month["dimensions"]["unplaced_record_count"] == "0"
        assert "records CDC published" in month["dimensions"]["coverage_note"]

        assert covid.load_snapshot(factory, conf={"history": True})[0] is None
        repeat = client.get("/api/v1/observations", params=params).json()
        assert repeat["items"] == items
