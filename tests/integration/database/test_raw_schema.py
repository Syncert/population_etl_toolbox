"""Raw PostgreSQL bootstrap, constraint, and isolation contracts."""

from __future__ import annotations

from collections.abc import Callable

import psycopg2
import pytest
from psycopg2.extensions import connection

from tests.support.postgres import RAW_DDL_FILES, apply_sql_files

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_raw_ddl_creates_every_source_table(postgres_connection: connection) -> None:
    expected_tables = {
        "raw_census": {
            "acs_datasets",
            "acs_ingestion_slices",
            "acs_long",
            "acs_tables",
            "acs_variables",
            "geo_dim",
        },
        "raw_bls": {
            "bls_datasets",
            "bls_ingestion_slices",
            "bls_long",
            "bls_series",
        },
        "raw_fred": {
            "fred_datasets",
            "fred_ingestion_slices",
            "fred_long",
            "fred_series",
        },
    }

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = ANY(%s)
              AND table_type = 'BASE TABLE'
            """,
            (list(expected_tables),),
        )
        actual = {(schema, table) for schema, table in cursor.fetchall()}

    expected = {
        (schema, table)
        for schema, tables in expected_tables.items()
        for table in tables
    }
    assert actual == expected


def test_raw_ddl_can_be_applied_twice(postgres_connection: connection) -> None:
    apply_sql_files(postgres_connection, RAW_DDL_FILES)
    apply_sql_files(postgres_connection, RAW_DDL_FILES)


@pytest.mark.parametrize(
    "insert_sql,parameters",
    [
        (
            """
            INSERT INTO raw_census.acs_long (
                dataset, year, geo_level, geo_id, table_id, variable_name,
                measure_type, value, load_batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                "acs5",
                2024,
                "state",
                "state:55",
                "B01001",
                "B01001_001E",
                "E",
                100,
                "00000000-0000-0000-0000-000000000001",
            ),
        ),
        (
            """
            INSERT INTO raw_bls.bls_long (
                program, series_id, year, period, value, load_batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                "la",
                "LASST550000000000003",
                2024,
                "M01",
                3.2,
                "00000000-0000-0000-0000-000000000002",
            ),
        ),
        (
            """
            INSERT INTO raw_fred.fred_long (
                domain, series_id, obs_date, value, realtime_start,
                realtime_end, load_batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                "labor_cycle",
                "UNRATE",
                "2024-01-01",
                3.7,
                "2024-02-01",
                "2024-02-01",
                "00000000-0000-0000-0000-000000000003",
            ),
        ),
    ],
    ids=("census", "bls", "fred"),
)
def test_raw_natural_keys_reject_duplicate_observations(
    postgres_connection: connection,
    insert_sql: str,
    parameters: tuple[object, ...],
) -> None:
    with postgres_connection.cursor() as cursor:
        cursor.execute(insert_sql, parameters)
        with pytest.raises(psycopg2.errors.UniqueViolation):
            cursor.execute(insert_sql, parameters)


@pytest.mark.parametrize(
    "insert_sql,parameters",
    [
        (
            """
            INSERT INTO raw_census.acs_ingestion_slices
                (dataset, year, geo_level, status, rows_loaded)
            VALUES (%s, %s, %s, %s, %s)
            """,
            ("acs5", 2024, "state", "invalid", 0),
        ),
        (
            """
            INSERT INTO raw_bls.bls_ingestion_slices
                (program, year_start, year_end, status, rows_loaded)
            VALUES (%s, %s, %s, %s, %s)
            """,
            ("la", 2024, 2024, "invalid", 0),
        ),
        (
            """
            INSERT INTO raw_fred.fred_ingestion_slices
                (domain, date_start, date_end, status, rows_loaded)
            VALUES (%s, %s, %s, %s, %s)
            """,
            ("labor_cycle", "2024-01-01", "2024-12-31", "invalid", 0),
        ),
    ],
    ids=("census", "bls", "fred"),
)
def test_slice_ledgers_reject_unknown_statuses(
    postgres_connection: connection,
    insert_sql: str,
    parameters: tuple[object, ...],
) -> None:
    with postgres_connection.cursor() as cursor:
        with pytest.raises(psycopg2.errors.CheckViolation):
            cursor.execute(insert_sql, parameters)


def test_uncommitted_test_data_is_rolled_back(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    series_id = "TEST_ROLLBACK_SENTINEL"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "INSERT INTO raw_fred.fred_series (series_id) VALUES (%s)",
                (series_id,),
            )
    finally:
        writer.rollback()
        writer.close()

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM raw_fred.fred_series WHERE series_id = %s",
                (series_id,),
            )
            assert cursor.fetchone() == (0,)
    finally:
        reader.rollback()
        reader.close()
