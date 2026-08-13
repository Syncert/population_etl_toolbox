"""Census ACS raw-to-silver database integration contract."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_acs.silver_census import transform
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_census_raw_rows_transform_to_exact_silver_keys(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-008 — ACS estimate/MOE rows produce one exact silver fact."""
    variable = "B99999_001"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20940101, "2094-01-01")
            cursor.execute(
                """
                INSERT INTO silver_ref.dim_geo (
                    geo_level, geo_id, state_fips, name, is_active,
                    source, source_year, ingested_at
                ) VALUES ('state', 'state:98', '98', 'Fixture State', TRUE,
                          'test', 2098, NOW())
                ON CONFLICT (geo_level, geo_id) DO NOTHING
                """
            )
            cursor.execute(
                """
                INSERT INTO raw_census.acs_variables (
                    dataset, year, variable_name, table_id, label,
                    concept, predicate_type, group_name
                ) VALUES
                    ('acs5', 2098, %s, 'B99999', 'Estimate label', 'Fixture concept', 'int', 'B99999'),
                    ('acs5', 2098, %s, 'B99999', 'MOE label', 'Fixture concept', 'int', 'B99999')
                """,
                (f"{variable}E", f"{variable}M"),
            )
            cursor.execute(
                """
                INSERT INTO raw_census.acs_long (
                    dataset, year, geo_level, geo_id, state_fips,
                    table_id, variable_name, measure_type, value, load_batch_id
                ) VALUES
                    ('acs5', 2098, 'state', 'ignored-raw-id', '98',
                     'B99999', %s, 'E', 1234, %s),
                    ('acs5', 2098, 'state', 'ignored-raw-id', '98',
                     'B99999', %s, 'M', 12, %s)
                """,
                (f"{variable}E", str(uuid4()), f"{variable}M", str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()

    monkeypatch.setattr(
        transform, "_get_hook", lambda: PostgresHookStub(postgres_connection_factory)
    )
    monkeypatch.setattr(transform, "_get_approx_row_count", lambda _: 2)
    try:
        assert transform.transform_census_to_silver() == 1
        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT f.dataset, f.table_id, f.variable_code, f.geo_id,
                           f.estimate_year, f.time_sk, f.duration_start::TEXT,
                           f.duration_end::TEXT, f.estimate_value,
                           f.margin_of_error, f.margin_of_error_pct, g.geo_id
                    FROM silver_census.fact_demographics f
                    JOIN silver_ref.dim_geo g ON g.geo_sk = f.geo_sk
                    WHERE f.variable_code = %s AND f.estimate_year = 2098
                    """,
                    (variable,),
                )
                assert cursor.fetchone() == (
                    "acs5",
                    "B99999",
                    variable,
                    "state:98",
                    2098,
                    20940101,
                    "2094-01-01",
                    "2098-12-31",
                    Decimal("1234.0"),
                    Decimal("12.0"),
                    Decimal("0.9724473257698543"),
                    "state:98",
                )
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_census.fact_demographics WHERE variable_code = %s",
                    (variable,),
                )
                cursor.execute(
                    "DELETE FROM raw_census.acs_long WHERE table_id = 'B99999'"
                )
                cursor.execute(
                    "DELETE FROM raw_census.acs_variables WHERE table_id = 'B99999'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_geo WHERE geo_id = 'state:98'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20940101"
                )
            cleanup.commit()
        finally:
            cleanup.close()
