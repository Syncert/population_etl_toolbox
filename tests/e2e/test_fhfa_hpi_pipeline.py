"""Deterministic FHFA House Price Index flow from the captured workbook to the API.

Every provider byte is FHFA's county workbook trimmed to seven counties,
played through the adapter's own capture path by a scripted client;
nothing here reaches the network. The node proves what an HPI figure must
not lose on the way to a consumer: that it is an index of Enterprise-backed
loans with FHFA's notice, the workbook's own vintage, and a missing cell kept
as missing with its reason rather than a zero.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import fhfa_hpi as hpi
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "FHFA_HPI"


@pytest.fixture
def hpi_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return hpi.reviewed_warehouse(postgres_connection_factory, request)


def test_house_price_index_reaches_the_neutral_api_with_its_notice(
    hpi_warehouse,
) -> None:
    """Covers: E2E-014 — a glossary-discovered FHFA metric answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: ETL-064 — the vintage is the release, the basis carries FHFA's
    notice, and a missing index is `missing` with its reason, not zero.
    """
    factory = hpi_warehouse
    hpi.run_to_gold(factory)
    assert harvest_publisher(factory, Publisher("gold_fhfa_hpi")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]
        change = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:annual_change_pct",
                "geo_id": hpi.KENT,
                "year_from": 2025,
            },
        )
        assert change.status_code == 200, change.text
        (row,) = change.json()["items"]
        assert (row["value"], row["unit"], row["geo_level"], row["release"]) == (
            "2.53",
            "percent",
            "COUNTY",
            "2026-03-31",
        )
        assert (
            "neither endorsed nor certified by FHFA"
            in row["dimensions"]["observation_basis"]
        )

        index = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:hpi_base_2000",
                "geo_id": hpi.CHUGACH,
                "year_from": 2023,
                "year_to": 2023,
            },
        ).json()["items"]
        assert [
            (item["value"], item["value_status"], item["dimensions"]["missing_reason"])
            for item in index
        ] == [(None, "missing", "provider_missing")]
