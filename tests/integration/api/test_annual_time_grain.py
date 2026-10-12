"""Calendar grains over the real warehouse: provider first, derived labelled, gaps named.

Covers: API-168
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection

from tests.integration.api.test_catalog_serving_agreement import (  # noqa: F401
    api_client,
    published_bls_metric,
)
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.capture_seed import delete_geography, seed_capture, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.api, pytest.mark.database]

ANNUAL_VALUE = "571.5"


def test_the_annual_grain_answers_the_providers_annual_average(
    api_client: TestClient,  # noqa: F811
    published_bls_metric: str,  # noqa: F811
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: API-168 — BLS's M13 figure is served as the year, with its own value and status."""
    series_id = published_bls_metric.split(":", 1)[1]
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20970101, "2097-01-01")
            geo_sk = seed_geography(
                cursor,
                geo_type="state",
                state_fips="93",
                vintage=2097,
                name="Test State",
            )
            capture_id = seed_capture(cursor, "BLS")
            cursor.execute(
                """
                INSERT INTO silver_bls.fact_labor_statistics (
                    time_sk, geo_sk, duration_start, duration_end, period_date,
                    series_id, program, geo_level, geo_id, state_fips, value,
                    source_value, value_status, year, period, period_name,
                    capture_id, load_batch_id
                ) VALUES (
                    20970101, %s, '2097-01-01', '2097-12-31', '2097-12-31',
                    %s, 'sw', 'STATE', 'state:93', '93', %s, %s, 'valid',
                    2097, 'M13', 'Annual', %s, gen_random_uuid()
                )
                """,
                (geo_sk, series_id, ANNUAL_VALUE, ANNUAL_VALUE, str(capture_id)),
            )
            # A derived year for the same window loses to BLS's own figure; a
            # derived quarter is served, and an incomplete one says why.
            for grain, start, end, expected, present, value, reason in (
                ("year", "2097-01-01", "2097-12-31", 12, 12, 570, None),
                ("quarter", "2097-01-01", "2097-03-31", 3, 3, 560, None),
                (
                    "quarter",
                    "2097-04-01",
                    "2097-06-30",
                    3,
                    2,
                    None,
                    "incomplete_window: 2 of 3 periods reported",
                ),
            ):
                cursor.execute(
                    """
                    INSERT INTO gold_bls.derived_calendar_rollup (
                        metric_code, geo_id, geo_level, unit, grain,
                        window_start, window_end, method, method_version,
                        expected_periods, present_periods, value,
                        refusal_reason, component_releases
                    ) VALUES (%s, 'state:93', 'STATE', 'persons', %s, %s, %s,
                              'mean', 1, %s, %s, %s, %s, ARRAY['2097-12-31'])
                    """,
                    (
                        published_bls_metric,
                        grain,
                        start,
                        end,
                        expected,
                        present,
                        value,
                        reason,
                    ),
                )
        writer.commit()
    finally:
        writer.close()

    try:
        annual = api_client.get(
            "/api/v1/observations",
            params={"metric_code": published_bls_metric, "time_grain": "annual"},
        )
        quarterly = api_client.get(
            "/api/v1/observations",
            params={"metric_code": published_bls_metric, "time_grain": "quarterly"},
        )
        native = api_client.get(
            "/api/v1/observations", params={"metric_code": published_bls_metric}
        )
        capabilities = api_client.get("/api/v1/catalog/metrics/" + published_bls_metric)
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_bls.derived_calendar_rollup WHERE metric_code = %s",
                    (published_bls_metric,),
                )
                cursor.execute(
                    "DELETE FROM silver_bls.fact_labor_statistics WHERE series_id = %s",
                    (series_id,),
                )
                delete_geography(cursor, "state:93")
            cleanup.commit()
        finally:
            cleanup.close()

    assert annual.status_code == 200, annual.text
    body = annual.json()
    assert body["time_grain"] == "annual"
    assert body["total"] == 1
    (row,) = body["items"]
    assert row["metric_code"] == published_bls_metric
    assert row["geo_id"] == "state:93"
    assert (row["period_start"], row["period_end"]) == ("2097-01-01", "2097-12-31")
    assert float(row["value"]) == float(ANNUAL_VALUE)
    assert row["value_status"] == "valid"
    assert row["derivation"]["kind"] == "provider_published"

    assert quarterly.status_code == 200, quarterly.text
    first, second = quarterly.json()["items"]
    assert (first["period_start"], float(first["value"])) == ("2097-01-01", 560.0)
    assert first["derivation"]["kind"] == "derived"
    assert first["derivation"]["method"] == "mean"
    assert second["value"] is None and second["value_status"] == "incomplete_window"
    assert second["derivation"]["refusal_reason"] == (
        "incomplete_window: 2 of 3 periods reported"
    )

    # The native read is the monthly publication, unchanged by the annual fact.
    assert native.status_code == 200
    assert native.json()["time_grain"] == "native"
    assert {item["geo_id"] for item in native.json()["items"]} == {
        "us:1",
        "state:93",
        "state:93|county:001",
        # The BLS fixture also publishes a region, a division and a CPI area
        # (grocery-and-gasoline-prices).
        "region:2",
        "division:3",
        "area:bls_cpi:S35A",
    }
    assert all(item["value"] != ANNUAL_VALUE for item in native.json()["items"])

    assert capabilities.status_code == 200, capabilities.text
    assert capabilities.json()["time_grains"] == ["native", "quarterly", "annual"]
