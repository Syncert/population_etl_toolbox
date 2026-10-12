"""Deterministic USDA ERS flow from captured files to the API.

Every provider byte is one of ERS's own files trimmed to a few counties,
played through the adapter's own capture path by a scripted client; nothing
here reaches the network. The node proves what an ERS figure must not lose
on the way to a consumer: that a code is a code with ERS's label, that an
unset flag is not a zero, and that an Atlas sentinel keeps its reason.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import usda_ers as ers
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "USDA_ERS"


@pytest.fixture
def ers_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return ers.reviewed_warehouse(postgres_connection_factory, request)


def test_codes_flags_and_atlas_reach_the_neutral_api(ers_warehouse) -> None:
    """Covers: E2E-014 — a glossary-discovered ERS metric answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: ETL-066 — RUCC carries ERS's label, an unset Typology flag is
    `not_applicable` with its reason, and an Atlas sentinel is `missing`.
    """
    factory = ers_warehouse
    ers.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_usda_ers")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]
        code = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:rural_urban_continuum_code",
                "geo_id": ers.KENT,
            },
        )
        assert code.status_code == 200, code.text
        (row,) = code.json()["items"]
        assert (row["value"], row["unit"], row["geo_level"]) == (
            "3",
            "code (1-9)",
            "COUNTY",
        )
        assert (
            row["dimensions"]["code_label"]
            == "Metro - Counties in metro areas of fewer than 250,000 population"
        )

        flag = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:farming_dependent",
                "geo_id": ers.CAPITOL,
            },
        ).json()["items"]
        assert [
            (item["value"], item["value_status"], item["dimensions"]["missing_reason"])
            for item in flag
        ] == [(None, "not_applicable", "not_computed_for_geography")]
        stores = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:snap_households_low_store_access",
                "geo_id": ers.CHUGACH,
            },
        ).json()["items"]
        assert sorted(
            (
                item["period_start"][:4],
                item["value_status"],
                item["dimensions"]["missing_reason"],
            )
            for item in stores
        ) == [
            ("2015", "missing", "county_did_not_exist"),
            ("2019", "missing", "not_available"),
        ]
