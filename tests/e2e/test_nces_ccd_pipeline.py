"""Deterministic NCES school flow from captured CCD and EDGE files to the API.

Every provider byte is a row of NCES's own 2024-25 files, played through the
adapter's own capture path by a scripted client; nothing here reaches the
network. The node proves what a county school figure must not lose on the
way to a consumer: that it is summed from schools, how many schools had no
value, and that FRPL and direct certification stay apart.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import nces_ccd as ccd
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "NCES_CCD"


@pytest.fixture
def ccd_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return ccd.reviewed_warehouse(postgres_connection_factory, request)


def test_county_school_figures_reach_the_neutral_api_as_rollups(ccd_warehouse) -> None:
    """Covers: E2E-014 — a glossary-discovered NCES metric answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: ETL-070 — the row says how many schools had no value and that it
    is summed from schools; Delaware's missing FRPL is not a zero.
    """
    factory = ccd_warehouse
    ccd.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_nces_ccd")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]
        response = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:frpl_eligible",
                "geo_id": ccd.PROVIDENCE_RI,
            },
        )
        assert response.status_code == 200, response.text
        (row,) = response.json()["items"]
        assert (row["value"], row["unit"], row["geo_level"]) == (
            "56037",
            "students",
            "COUNTY",
        )
        assert (row["period_start"], row["period_end"]) == ("2024-07-01", "2025-06-30")
        assert row["dimensions"]["schools_with_value"] == "197"
        assert row["dimensions"]["schools_without_value"] == "3"
        assert row["dimensions"]["completeness"] == "partial"
        assert (
            "Community Eligibility Provision" in row["dimensions"]["observation_basis"]
        )
        enrolled = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:student_membership",
                "geo_id": ccd.PROVIDENCE_RI,
            },
        ).json()["items"]
        assert [
            (item["value"], item["dimensions"]["completeness"]) for item in enrolled
        ] == [("87970", "complete")]
        sussex = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:frpl_eligible",
                "geo_id": ccd.SUSSEX_DE,
            },
        ).json()["items"]
        assert [(item["value"], item["value_status"]) for item in sussex] == [
            (None, "missing")
        ]
