"""Deterministic FEMA flow from captured pages to the API.

Every provider byte is one of FEMA's own answers, saved verbatim and served
by a scripted client; nothing here reaches the network. The node proves what
a FEMA figure must not lose on the way to a consumer: that expected annual
loss is a modelled estimate with its version, that a hazard FEMA rates Not
Applicable carries no number, and that a declaration count says what it
counted.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from tests.support import fema_nri as fema
from tests.support.api import real_api_client

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

SOURCE_CODE = "FEMA_NRI"


@pytest.fixture
def fema_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return fema.reviewed_warehouse(postgres_connection_factory, request)


def test_losses_and_declarations_reach_the_neutral_api(fema_warehouse) -> None:
    """Covers: E2E-014 — a glossary-discovered FEMA metric answers through
    `/api/v1/observations` for a county with the fixture's value.

    Covers: ETL-067 — the NRI version is the release, a Not Applicable hazard
    has no number, and a declaration count lists the declarations it counted.
    """
    factory = fema_warehouse
    fema.run_all(factory)
    assert harvest_publisher(factory, Publisher("gold_fema_nri")) > 0

    with real_api_client() as client:
        capabilities = client.get("/api/v1/catalog/capabilities").json()
        assert next(
            item for item in capabilities["items"] if item["source_code"] == SOURCE_CODE
        )["served_by_neutral_routes"]
        loss = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:expected_annual_loss",
                "geo_id": fema.KENT,
            },
        )
        assert loss.status_code == 200, loss.text
        (row,) = loss.json()["items"]
        assert (row["unit"], row["release"], row["geo_level"]) == (
            "dollars per year",
            "December 2025",
            "COUNTY",
        )
        assert float(row["value"]) == pytest.approx(
            fema.fixture_records("nri")[0]["EAL_VALT"]
        )
        assert "not a measurement" in row["dimensions"]["observation_basis"]

        tsunami = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:expected_annual_loss_tsunami",
                "geo_id": fema.ADJUNTAS,
            },
        ).json()["items"]
        assert [
            (item["value"], item["value_status"], item["dimensions"]["rating"])
            for item in tsunami
        ] == [(None, "not_applicable", "Not Applicable")]
        counts = client.get(
            "/api/v1/observations",
            params={
                "metric_code": f"{SOURCE_CODE}:major_disaster_declarations",
                "geo_id": fema.KENT,
            },
        ).json()["items"]
        assert counts and all(
            item["dimensions"]["declarations"].startswith("DR-") for item in counts
        )
