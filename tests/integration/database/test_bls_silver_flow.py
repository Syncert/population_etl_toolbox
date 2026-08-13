"""BLS raw-to-silver database integration contract."""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls.silver_bls import transform
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_bls_raw_rows_transform_to_exact_silver_keys(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-009 — BLS raw rows produce exact periods and dimension keys."""
    series_id = "LAUST990000000000003"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20980101, "2098-01-01")
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_geo (
                    geo_level, geo_id, state_fips, name, is_active,
                    source, source_year, ingested_at
                ) VALUES ('state', 'state:99', '99', 'Test State', TRUE,
                          'test', 2098, NOW())
                ON CONFLICT (geo_level, geo_id) DO NOTHING
                """
            )
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_series (
                    program, series_id, title, seasonal, measure, area_code
                ) VALUES ('la', %s, 'Test unemployment', 'U', '03', 'ST9900000000000')
                """,
                (series_id,),
            )
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_long (
                    program, series_id, year, period, period_name,
                    value, load_batch_id
                ) VALUES ('la', %s, 2098, 'M01', 'January', 4.25, %s)
                """,
                (series_id, str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()

    monkeypatch.setattr(
        transform, "_get_hook", lambda: PostgresHookStub(postgres_connection_factory)
    )
    try:
        assert transform.transform_bls_to_silver("la") == 1

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT f.series_id, f.period_date::TEXT,
                           f.duration_start::TEXT, f.duration_end::TEXT,
                           f.time_sk, f.geo_sk, g.geo_id, f.value,
                           f.measure_code, f.period
                    FROM silver_bls.fact_labor_statistics f
                    JOIN silver_ref.dim_geo g ON g.geo_sk = f.geo_sk
                    WHERE f.series_id = %s
                    """,
                    (series_id,),
                )
                row = cursor.fetchone()
                assert row[:5] == (
                    series_id,
                    "2098-01-31",
                    "2098-01-01",
                    "2098-01-31",
                    20980101,
                )
                assert row[6:] == ("state:99", 4.25, "03", "M01")
                assert row[5] is not None
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
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
                    "DELETE FROM silver_ref.dim_geo WHERE geo_id = 'state:99'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20980101"
                )
            cleanup.commit()
        finally:
            cleanup.close()
