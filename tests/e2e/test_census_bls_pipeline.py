"""Complete deterministic Census ACS and BLS fixture pipelines."""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls.gold_bls import transform as bls_gold
from data_ingestion_toolbox.bls.silver_bls import transform as bls_silver
from data_ingestion_toolbox.census_acs.gold_census import transform as census_gold
from data_ingestion_toolbox.census_acs.silver_census import transform as census_silver
from tests.e2e.test_fred_pipeline import _real_client
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]


def _call_refresh(
    factory: Callable[[], connection], procedure: str, start: str, end: str
) -> None:
    database_connection = factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(f"CALL {procedure}(%s, %s, TRUE)", (start, end))
        database_connection.commit()
    finally:
        database_connection.close()


def test_bls_fixture_flows_raw_to_gold_and_replays_identically(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: E2E-002 — BLS raw-to-API rows are exact.

    Covers: E2E-004 — the complete BLS fixture replays identically.
    """
    series_id = "LAUST970000000000003"
    metric_code = f"BLS:{series_id}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20950101, "2095-01-01")
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_geo (
                    geo_level, geo_id, state_fips, name, state_name,
                    is_active, source, source_year, ingested_at
                ) VALUES ('state', 'state:97', '97', 'E2E State', 'E2E State',
                          TRUE, 'test', 2095, NOW())
                ON CONFLICT (geo_level, geo_id) DO NOTHING
                """
            )
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_series (
                    program, series_id, title, seasonal, measure, area_code
                ) VALUES ('la', %s, 'E2E unemployment rate', 'U', '03', 'ST9700000000000')
                """,
                (series_id,),
            )
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_long (
                    program, series_id, year, period, period_name, value, load_batch_id
                ) VALUES ('la', %s, 2095, 'M01', 'January', 4.5, %s)
                """,
                (series_id, str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    monkeypatch.setattr(bls_silver, "_get_hook", lambda: hook)
    try:
        responses = []
        for _ in range(2):
            assert bls_silver.transform_bls_to_silver("la") == 1
            bls_gold.refresh_bls_elements(hook)
            _call_refresh(
                postgres_connection_factory,
                "gold_bls.refresh_dashboard_serving_layer_bls",
                "2095-01-01",
                "2095-01-31",
            )
            with _real_client() as client:
                source = client.get(
                    "/api/bls/observations/timeseries",
                    params={"metric_code": metric_code, "geo_id": "state:97"},
                )
                common = client.get(
                    "/api/observations/latest", params={"metric_code": metric_code}
                )
            assert source.status_code == common.status_code == 200
            assert source.json()["total"] == common.json()["total"] == 1
            assert source.json()["items"][0]["value"] == "4.5"
            responses.append(source.json())
        assert responses[0] == responses[1]
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_bls.mv_bls_latest WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM gold_bls.rpt_bls_observations WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    """
                    DELETE FROM gold_glossary.bridge_metric_bls_series b
                    USING gold_bls.dim_bls_series s
                    WHERE b.bls_series_sk = s.bls_series_sk AND s.series_id = %s
                    """,
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code = %s",
                    (metric_code,),
                )
                cursor.execute(
                    "DELETE FROM gold_bls.dim_bls_series WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM silver_bls.fact_labor_statistics WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM raw_bls.bls_long WHERE series_id = %s", (series_id,)
                )
                cursor.execute(
                    "DELETE FROM raw_bls.bls_series WHERE series_id = %s", (series_id,)
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo WHERE geo_id = 'state:97'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20950101"
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_census_fixture_flows_raw_to_gold_and_replays_identically(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: E2E-001 — ACS raw-to-API rows are exact.

    Covers: E2E-004 — the complete ACS fixture replays identically.
    """
    variable = "B99998_001"
    metric_code = f"ACS:acs5:{variable}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20900101, "2090-01-01")
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_geo (
                    geo_level, geo_id, state_fips, name, state_name,
                    is_active, source, source_year, ingested_at
                ) VALUES ('state', 'state:96', '96', 'ACS E2E State', 'ACS E2E State',
                          TRUE, 'test', 2094, NOW())
                ON CONFLICT (geo_level, geo_id) DO NOTHING
                """
            )
            cursor.execute(
                """
                INSERT INTO raw_census.acs_variables (
                    dataset, year, variable_name, table_id, label,
                    concept, predicate_type, group_name
                ) VALUES
                    ('acs5', 2094, %s, 'B99998', 'Estimate', 'E2E population', 'int', 'B99998'),
                    ('acs5', 2094, %s, 'B99998', 'MOE', 'E2E population', 'int', 'B99998')
                """,
                (f"{variable}E", f"{variable}M"),
            )
            cursor.execute(
                """
                INSERT INTO raw_census.acs_long (
                    dataset, year, geo_level, geo_id, state_fips, table_id,
                    variable_name, measure_type, value, load_batch_id
                ) VALUES
                    ('acs5', 2094, 'state', 'raw', '96', 'B99998', %s, 'E', 500, %s),
                    ('acs5', 2094, 'state', 'raw', '96', 'B99998', %s, 'M', 5, %s)
                """,
                (f"{variable}E", str(uuid4()), f"{variable}M", str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    monkeypatch.setattr(census_silver, "_get_hook", lambda: hook)
    monkeypatch.setattr(census_silver, "_get_approx_row_count", lambda _: 2)
    try:
        responses = []
        for expected_inserted in (1, 0):
            assert census_silver.transform_census_to_silver() == expected_inserted
            census_gold.refresh_acs_elements(hook)
            _call_refresh(
                postgres_connection_factory,
                "gold_census.refresh_dashboard_serving_layer_acs",
                "2090-01-01",
                "2094-12-31",
            )
            with _real_client() as client:
                source = client.get(
                    "/api/census/observations/timeseries",
                    params={"metric_code": metric_code, "geo_id": "state:96"},
                )
                common = client.get(
                    "/api/observations/latest", params={"metric_code": metric_code}
                )
            assert source.status_code == common.status_code == 200
            assert source.json()["total"] == common.json()["total"] == 1
            assert source.json()["items"][0]["value"] == "500.0"
            assert source.json()["items"][0]["margin_of_error"] == "5.0"
            responses.append(source.json())
        assert responses[0] == responses[1]
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_census.mv_acs_latest WHERE variable_code = %s",
                    (variable,),
                )
                cursor.execute(
                    "DELETE FROM gold_census.rpt_acs_observations WHERE variable_code = %s",
                    (variable,),
                )
                cursor.execute(
                    """
                    DELETE FROM gold_glossary.bridge_metric_acs_variable b
                    USING gold_census.dim_acs_variable v
                    WHERE b.acs_variable_sk = v.acs_variable_sk AND v.variable_code = %s
                    """,
                    (variable,),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code = %s",
                    (metric_code,),
                )
                cursor.execute(
                    "DELETE FROM gold_census.dim_acs_variable WHERE variable_code = %s",
                    (variable,),
                )
                cursor.execute(
                    "DELETE FROM gold_census.dim_acs_table WHERE table_id = 'B99998'"
                )
                cursor.execute(
                    "DELETE FROM silver_census.fact_demographics WHERE variable_code = %s",
                    (variable,),
                )
                cursor.execute(
                    "DELETE FROM raw_census.acs_long WHERE table_id = 'B99998'"
                )
                cursor.execute(
                    "DELETE FROM raw_census.acs_variables WHERE table_id = 'B99998'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo WHERE geo_id = 'state:96'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20900101"
                )
            cleanup.commit()
        finally:
            cleanup.close()
