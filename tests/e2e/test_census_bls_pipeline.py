"""Complete deterministic Census ACS and BLS fixture pipelines."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls.gold_bls import transform as bls_gold
from data_ingestion_toolbox.bls import ingest as bls_ingest
from data_ingestion_toolbox.bls.silver_bls import transform as bls_silver
from data_ingestion_toolbox.census_acs import ingest as census_ingest
from data_ingestion_toolbox.census_acs.gold_census import transform as census_gold
from data_ingestion_toolbox.census_acs.silver_census import transform as census_silver
from tests.e2e.test_fred_pipeline import _real_client
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.postgres import PostgresHookStub
from tests.support.capture_seed import delete_geography, seed_geography

pytestmark = [pytest.mark.e2e, pytest.mark.database, pytest.mark.slow]

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures"


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
    """Covers: E2E-002 — BLS capture-to-API rows are exact.

    Covers: E2E-004 — the complete BLS fixture replays identically.
    """
    series_id = "LAUST970000000000003"
    metric_code = f"BLS:{series_id}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20950101, "2095-01-01")
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="97",
                vintage=2095,
                name="E2E State",
            )
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_series (
                    program, series_id, title, seasonal, measure, area_code
                ) VALUES ('la', %s, 'E2E unemployment rate', 'U', '03', 'ST9700000000000')
                """,
                (series_id,),
            )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    payload = json.loads(
        (FIXTURE_ROOT / "bls/e2e_pipeline.json").read_text(encoding="utf-8")
    )
    monkeypatch.setattr(bls_ingest, "_get_pg_connection", postgres_connection_factory)
    monkeypatch.setattr(bls_silver, "_get_hook", lambda: hook)
    try:
        active_payload = payload
        monkeypatch.setattr(
            bls_ingest, "get_curated_series_for_program", lambda _program: ["03"]
        )
        monkeypatch.setattr(
            bls_ingest, "expand_laus_series_ids", lambda **_kwargs: [series_id]
        )
        monkeypatch.setattr(
            bls_ingest, "fetch_bls_api", lambda **_kwargs: active_payload
        )
        assert bls_ingest.ingest_slice("la", 2095, 2095, "state", "97") == 1
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
            assert source.json()["items"][0]["value"] == "4.50"
            responses.append(source.json())
        assert responses[0] == responses[1]

        revised_payload = copy.deepcopy(payload)
        revised_payload["Results"]["series"][0]["data"][0]["value"] = "5.25"
        active_payload = revised_payload
        assert bls_ingest.ingest_slice("la", 2095, 2095, "state", "97") == 1
        assert bls_silver.transform_bls_to_silver("la") == 1
        bls_gold.refresh_bls_elements(hook)
        _call_refresh(
            postgres_connection_factory,
            "gold_bls.refresh_dashboard_serving_layer_bls",
            "2095-01-01",
            "2095-01-31",
        )
        with _real_client() as client:
            revised = client.get(
                "/api/bls/observations/latest", params={"metric_code": metric_code}
            )
        assert revised.status_code == 200
        assert revised.json()["items"][0]["value"] == "5.25"
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
                    "DELETE FROM raw_bls.bls_series WHERE series_id = %s", (series_id,)
                )
                delete_geography(cursor, "state:97")
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
    """Covers: E2E-001 — ACS capture-to-API rows are exact.

    Covers: E2E-004 — the complete ACS fixture replays identically.
    """
    variable = "B99998_001"
    metric_code = f"ACS:acs5:{variable}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20900101, "2090-01-01")
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="96",
                vintage=2094,
                name="ACS E2E State",
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
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    payload = json.loads(
        (FIXTURE_ROOT / "census/e2e_pipeline.json").read_text(encoding="utf-8")
    )
    monkeypatch.setattr(
        census_ingest, "_get_pg_connection", postgres_connection_factory
    )
    monkeypatch.setattr(census_silver, "_get_hook", lambda: hook)
    monkeypatch.setattr(census_silver, "_get_approx_row_count", lambda _: 2)
    try:
        active_payload = payload
        monkeypatch.setattr(
            census_ingest,
            "get_curated_variables",
            lambda _year, _dataset: [f"{variable}E", f"{variable}M"],
        )
        monkeypatch.setattr(
            census_ingest, "fetch_acs_api", lambda **_kwargs: active_payload
        )
        assert census_ingest.ingest_slice(2094, "acs5", "state") == 2
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

        revised_payload = copy.deepcopy(payload)
        revised_payload[1][0] = "525"
        revised_payload[1][1] = "6"
        active_payload = revised_payload
        assert census_ingest.ingest_slice(2094, "acs5", "state") == 2
        assert census_silver.transform_census_to_silver() == 1
        census_gold.refresh_acs_elements(hook)
        _call_refresh(
            postgres_connection_factory,
            "gold_census.refresh_dashboard_serving_layer_acs",
            "2090-01-01",
            "2094-12-31",
        )
        with _real_client() as client:
            revised = client.get(
                "/api/census/observations/latest", params={"metric_code": metric_code}
            )
        assert revised.status_code == 200
        assert revised.json()["items"][0]["value"] == "525.0"
        assert revised.json()["items"][0]["margin_of_error"] == "6.0"
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
                    "DELETE FROM raw_census.acs_variables WHERE table_id = 'B99998'"
                )
                delete_geography(cursor, "state:96")
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20900101"
                )
            cleanup.commit()
        finally:
            cleanup.close()
