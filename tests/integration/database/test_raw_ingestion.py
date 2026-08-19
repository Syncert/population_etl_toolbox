"""Capture-first ingestion persistence and atomicity contracts."""

from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2 import IntegrityError
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls import ingest as bls_ingest
from data_ingestion_toolbox.census_acs import ingest as census_ingest
from data_ingestion_toolbox.fred import ingest as fred_ingest
from tests.support.capture_seed import seed_capture

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_legacy_parsed_raw_contract_is_removed(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-006, DB-007 — capture/revision replaces parsed-raw loaders."""
    conn = postgres_connection_factory()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """SELECT to_regclass('raw_census.acs_long'),
                          to_regclass('raw_bls.bls_long'),
                          to_regclass('raw_fred.fred_long')"""
            )
            assert cursor.fetchone() == (None, None, None)
        assert not hasattr(census_ingest, "load_df_to_acs_long")
        assert not hasattr(bls_ingest, "load_df_to_bls_long")
        assert not hasattr(fred_ingest, "load_df_to_fred_long")
    finally:
        conn.close()


@pytest.mark.parametrize(
    ("source_code", "table_name", "columns", "values"),
    [
        (
            "FRED",
            "silver_fred.observation_revision",
            "observation_index, domain, series_id, observation_date_source, observation_date, value_source, value, value_status",
            "0, 'test', 'TEST_DUP', '2099-01-01', DATE '2099-01-01', '1', 1, 'valid'",
        ),
        (
            "BLS",
            "silver_bls.observation_revision",
            "observation_index, program, series_id, year_source, period_source, year, period, value_source, value, value_status",
            "0, 'la', 'TEST_DUP', '2099', 'M01', 2099, 'M01', '1', 1, 'valid'",
        ),
        (
            "CENSUS_ACS",
            "silver_census.observation_revision",
            "source_row_index, source_column_index, source_header, dataset, year, geo_level, variable_name, table_id, measure_type, value_source, value, value_status",
            "0, 0, 'B00001_001E', 'acs5', 2099, 'state', 'B00001_001E', 'B00001', 'E', '1', 1, 'valid'",
        ),
    ],
)
def test_revision_primary_keys_reject_duplicate_capture_positions(
    postgres_connection_factory: Callable[[], connection],
    source_code: str,
    table_name: str,
    columns: str,
    values: str,
) -> None:
    """Covers: DB-016 — replay positions are unique within an immutable capture."""
    conn = postgres_connection_factory()
    try:
        with conn.cursor() as cursor:
            capture_id = seed_capture(cursor, source_code)
            statement = f"INSERT INTO {table_name} (capture_id, {columns}) VALUES (%s, {values})"
            cursor.execute(statement, (capture_id,))
            with pytest.raises(IntegrityError):
                cursor.execute(statement, (capture_id,))
        conn.rollback()
    finally:
        conn.close()


def test_failed_capture_transaction_rolls_back_all_rows(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-014, DB-017, DB-018 — failed capture batches commit all or none."""
    marker = uuid4()
    conn = postgres_connection_factory()
    try:
        with pytest.raises(IntegrityError), conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO control.ingestion_run (run_id, source_code, status) VALUES (%s, 'FRED', 'running')",
                (marker,),
            )
            cursor.execute(
                "INSERT INTO control.ingestion_run (run_id, source_code, status) VALUES (%s, 'FRED', 'not-a-status')",
                (uuid4(),),
            )
        conn.rollback()
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM control.ingestion_run WHERE run_id = %s",
                (marker,),
            )
            assert cursor.fetchone() == (0,)
    finally:
        conn.close()
