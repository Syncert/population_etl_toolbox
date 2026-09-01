"""Complete deterministic FRED fixture pipeline."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import connection
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from apps.api.dependencies import get_db_session_dep
from apps.api.main import app
from data_ingestion_toolbox.fred import ingest as fred_ingest
from data_ingestion_toolbox.fred.gold_fred import transform as gold_transform
from data_ingestion_toolbox.fred.silver_fred import transform as silver_transform
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.postgres import PostgresHookStub, PostgresTestConfig
from tests.support.warehouse_scope import warehouse_scope

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.database,
    pytest.mark.slow,
]

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures/fred"


@contextmanager
def _real_client() -> Iterator[TestClient]:
    settings = PostgresTestConfig.from_environment()
    assert settings is not None
    engine = create_engine(
        "postgresql+psycopg2://",
        connect_args={
            "host": settings.host,
            "port": settings.port,
            "user": settings.user,
            "password": settings.password,
            "dbname": settings.database,
        },
        pool_pre_ping=True,
    )

    def override_db() -> Iterator[Session]:
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_db_session_dep] = override_db
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


def _refresh_gold(factory: Callable[[], connection], start: str, end: str) -> None:
    hook = PostgresHookStub(factory)
    gold_transform.refresh_fred_elements(hook)
    database_connection = factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                "CALL gold_fred.refresh_dashboard_serving_layer_fred(%s, %s, TRUE)",
                (start, end),
            )
        database_connection.commit()
    finally:
        database_connection.close()


def test_fred_fixture_replay_revision_and_missing_data_reconcile_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    caplog: pytest.LogCaptureFixture,
    request: pytest.FixtureRequest,
) -> None:
    """Covers: E2E-003 — FRED fixture flows capture-first through the API exactly.

    Covers: E2E-004 — replay preserves fact counts and API JSON.
        Covers: E2E-005, ETL-023 — the latest source revision wins deterministically.
    Covers: E2E-006 — dimension misses reconcile without corrupt serving data.
    """
    token = uuid4().hex[:12].upper()
    series_id = f"TEST_E2E_FRED_{token}"
    missing_series = f"{series_id}_MISS"
    domain = f"test_e2e_{token.lower()}"
    metric_code = f"FRED:{series_id}"
    # The production ingest starts its own runs, so the scope adopts every run
    # this node adds for the source and removes the capture graph the targeted
    # deletes below cannot reach.
    warehouse_scope(
        postgres_connection_factory,
        request,
        source_code="FRED",
        silver_statements=("DELETE FROM silver_fred.observation_revision",),
    )
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20960101, "2096-01-01")
            _seed_time(cursor, 20960201, "2096-02-01")
            cursor.execute(
                """
                INSERT INTO raw_fred.fred_series (
                    series_id, title, units, frequency, seasonal_adjustment
                ) VALUES
                    (%s, 'E2E series', 'Index', 'Monthly', 'Not Adjusted'),
                    (%s, 'Missing series', 'Index', 'Monthly', 'Not Adjusted')
                """,
                (series_id, missing_series),
            )
        writer.commit()
    finally:
        writer.close()

    monkeypatch.setattr(fred_ingest, "_get_pg_connection", postgres_connection_factory)
    monkeypatch.setattr(
        silver_transform,
        "_get_hook",
        lambda: PostgresHookStub(postgres_connection_factory),
    )
    try:
        invalid_payload = json.loads(
            (FIXTURE_ROOT / "e2e_invalid.json").read_text(encoding="utf-8")
        )
        with pytest.raises(fred_ingest.FredPayloadError):
            fred_ingest.parse_fred_response(invalid_payload, series_id, domain, uuid4())

        payload = json.loads(
            (FIXTURE_ROOT / "e2e_pipeline.json").read_text(encoding="utf-8")
        )
        missing_payload = json.loads(
            (FIXTURE_ROOT / "e2e_dimension_miss.json").read_text(encoding="utf-8")
        )
        payloads = {series_id: payload, missing_series: missing_payload}
        monkeypatch.setattr(
            fred_ingest,
            "fetch_fred_observations",
            lambda series_id, **_kwargs: payloads[series_id],
        )
        assert (
            fred_ingest.ingest_slice(
                domain, [series_id, missing_series], "2096-01-01", "2199-12-31"
            )
            == 3
        )

        raw_reader = postgres_connection_factory()
        try:
            with raw_reader.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM silver_fred.observation_revision WHERE series_id IN (%s, %s)",
                    (series_id, missing_series),
                )
                assert cursor.fetchone() == (3,)
        finally:
            raw_reader.close()

        with caplog.at_level(logging.WARNING):
            assert silver_transform.transform_fred_to_silver(domain) == 2
        assert "Dropped 1 FRED rows with missing time_sk" in caplog.text
        _refresh_gold(postgres_connection_factory, "2096-01-01", "2096-02-28")

        with _real_client() as client:
            first = client.get(
                "/api/v1/fred/observations/timeseries",
                params={"metric_code": metric_code, "geo_id": "us:1"},
            )
            common = client.get(
                "/api/v1/observations/latest", params={"metric_code": metric_code}
            )
        assert first.status_code == common.status_code == 200
        assert [item["value"] for item in first.json()["items"]] == ["10", "20"]
        assert common.json()["items"][0]["value"] == "20"

        assert silver_transform.transform_fred_to_silver(domain) == 2
        _refresh_gold(postgres_connection_factory, "2096-01-01", "2096-02-28")
        with _real_client() as client:
            replay = client.get(
                "/api/v1/fred/observations/timeseries",
                params={"metric_code": metric_code, "geo_id": "us:1"},
            )
        assert replay.json() == first.json()

        revised_payload = json.loads(json.dumps(payload))
        revised_payload["observations"][1]["value"] = "25"
        revised_payload["observations"][1]["realtime_start"] = "2096-04-01"
        revised_payload["observations"][1]["realtime_end"] = "2096-04-01"
        payloads[series_id] = revised_payload
        assert (
            fred_ingest.ingest_slice(domain, [series_id], "2096-01-01", "2096-02-28")
            == 2
        )
        assert silver_transform.transform_fred_to_silver(domain) == 2
        _refresh_gold(postgres_connection_factory, "2096-01-01", "2096-02-28")
        with _real_client() as client:
            revised = client.get(
                "/api/v1/fred/observations/timeseries",
                params={"metric_code": metric_code, "geo_id": "us:1"},
            )
        assert [item["value"] for item in revised.json()["items"]] == ["10", "25"]

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM silver_fred.fact_economic_indicators WHERE series_id = %s",
                    (missing_series,),
                )
                assert cursor.fetchone() == (0,)
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_fred.mv_fred_latest WHERE series_id IN (%s, %s)",
                    (series_id, missing_series),
                )
                cursor.execute(
                    "DELETE FROM gold_fred.rpt_fred_observations WHERE series_id IN (%s, %s)",
                    (series_id, missing_series),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code LIKE %s",
                    (f"FRED:TEST_E2E_FRED_{token}%",),
                )
                cursor.execute(
                    "DELETE FROM gold_fred.dim_fred_series WHERE series_id IN (%s, %s)",
                    (series_id, missing_series),
                )
                cursor.execute(
                    "DELETE FROM silver_fred.fact_economic_indicators WHERE series_id IN (%s, %s)",
                    (series_id, missing_series),
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_series WHERE series_id IN (%s, %s)",
                    (series_id, missing_series),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk IN (20960101, 20960201)"
                )
            cleanup.commit()
        finally:
            cleanup.close()
