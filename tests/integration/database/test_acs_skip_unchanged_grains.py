"""A year's grain is re-served only when silver changed it (skip-unchanged-grains).

Covers: DB-065
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.census_acs.silver_census.transform import (
    _record_slice_fingerprint,
)
from data_ingestion_toolbox.utility.gold_schema import _step_has_changes
from data_ingestion_toolbox.utility.serving_reserve import ACS_CHUNK_CONFIG
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

YEAR = 2091
STEPS = {step.name: step for step in ACS_CHUNK_CONFIG.grain_steps}


@pytest.fixture
def ledger(postgres_connection_factory: Callable[[], connection]):
    """Write slice-ledger rows for YEAR; remove them afterwards."""
    database = postgres_connection_factory()
    database.autocommit = True

    def record(slice_key: str, rows_changed: int) -> None:
        # The transform's own writer, so the test exercises its rules.
        _record_slice_fingerprint(
            PostgresHookStub(postgres_connection_factory),
            YEAR,
            slice_key,
            "f",
            rows_changed,
        )

    try:
        yield record
    finally:
        with database.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_census.transform_slice_state WHERE year = %s",
                (YEAR,),
            )
            cursor.execute(
                "DELETE FROM control.serving_refresh_chunk_state "
                "WHERE source_code = 'CENSUS_ACS' AND chunk_start = %s",
                (date(YEAR, 1, 1),),
            )
        database.close()


def _database_now(factory) -> datetime:
    """The serve time, on the database's clock the ledger is written with."""
    database = factory()
    try:
        with database.cursor() as cursor:
            cursor.execute("SELECT clock_timestamp()")
            return cursor.fetchone()[0]
    finally:
        database.close()


def _changed(factory, step: str, since: datetime | None) -> bool:
    chunk = {"start": date(YEAR, 1, 1), "completed_at": since}
    return _step_has_changes(
        PostgresHookStub(factory), ACS_CHUNK_CONFIG, STEPS[step], chunk
    )


def test_a_grain_no_slice_changed_since_its_last_serve_is_skipped(
    postgres_connection_factory: Callable[[], connection], ledger
) -> None:
    """Covers: DB-065 -- the places changed; the counties and tracts did not."""
    ledger("acs5/county/10", 5)
    ledger("acs5/tract/10", 7)
    served = _database_now(postgres_connection_factory)
    # A later run replays every slice; only places change.
    ledger("acs5/county/10", 0)
    ledger("acs5/tract/10", 0)
    ledger("acs5/place/10", 12)

    assert _changed(postgres_connection_factory, "area", served) is False
    assert _changed(postgres_connection_factory, "tract", served) is False
    assert _changed(postgres_connection_factory, "place", served) is True


def test_a_change_not_yet_served_survives_a_later_unchanged_replay(
    postgres_connection_factory: Callable[[], connection], ledger
) -> None:
    """Covers: DB-065 -- a revision transformed, unserved, then replayed with 0 changes still publishes."""
    served = datetime.now(timezone.utc) - timedelta(days=1)
    ledger("acs5/county/10", 3)  # a backdated revision, not yet served
    ledger("acs5/county/10", 0)  # a retry finds nothing further to change
    assert _changed(postgres_connection_factory, "area", served) is True


def test_unknown_or_never_served_grains_publish(
    postgres_connection_factory: Callable[[], connection], ledger
) -> None:
    """Covers: DB-065 -- no ledger row, or a chunk never served, means publish."""
    served = datetime.now(timezone.utc)
    assert _changed(postgres_connection_factory, "tract", served) is True
    ledger("acs5/tract/10", 0)
    assert _changed(postgres_connection_factory, "tract", None) is True
    # The nation's slices are named `us`, the step's grain `NATIONAL`.
    ledger("acs5/us", 4)
    assert _changed(postgres_connection_factory, "area", served - timedelta(days=1))


def test_a_ledger_row_from_before_the_column_is_bounded_by_its_completion(
    postgres_connection_factory: Callable[[], connection], ledger
) -> None:
    """Covers: DB-065 -- re-applying the DDL dates old rows by completion, not by deploy time."""
    from pathlib import Path

    ledger("acs5/county/10", 0)
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            cursor.execute(
                "UPDATE silver_census.transform_slice_state "
                "SET completed_at = NOW() - INTERVAL '30 days', "
                "    last_changed_at = NOW() WHERE year = %s",
                (YEAR,),
            )
            ddl = (
                Path(__file__).resolve().parents[3]
                / "src/data_ingestion_toolbox/census_acs/DDL/silver_census.sql"
            )
            cursor.execute(ddl.read_text(encoding="utf-8"))
    finally:
        database.close()
    served = datetime.now(timezone.utc) - timedelta(days=1)
    assert _changed(postgres_connection_factory, "area", served) is False


def _serve_chunk(factory, *, complete: bool) -> datetime:
    """Record YEAR's chunk as served now, fully or not; return the serve time."""
    database = factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.serving_refresh_chunk_state (
                    source_code, chunk_start, chunk_end, target_silver_ingested_at,
                    completed_silver_ingested_at, status, last_refresh_completed_at,
                    updated_at
                )
                VALUES ('CENSUS_ACS', %s, %s, NOW(),
                        CASE WHEN %s THEN NOW() END, %s, clock_timestamp(), NOW())
                RETURNING last_refresh_completed_at
                """,
                (
                    date(YEAR, 1, 1),
                    date(YEAR, 12, 31),
                    complete,
                    "COMPLETE" if complete else "RUNNING",
                ),
            )
            return cursor.fetchone()[0]
    finally:
        database.close()


def test_a_slice_first_recorded_unchanged_after_a_full_serve_is_skipped(
    postgres_connection_factory: Callable[[], connection], ledger
) -> None:
    """Covers: DB-065 -- rows older than the ledger were published by that serve."""
    served = _serve_chunk(postgres_connection_factory, complete=True)
    ledger("acs5/county/10", 0)
    ledger("acs5/place/10", 9)
    assert _changed(postgres_connection_factory, "area", served) is False
    assert _changed(postgres_connection_factory, "place", served) is True


def test_a_slice_first_recorded_unchanged_after_a_partial_serve_publishes(
    postgres_connection_factory: Callable[[], connection], ledger
) -> None:
    """Covers: DB-065 -- a year whose last serve did not finish proves nothing."""
    served = _serve_chunk(postgres_connection_factory, complete=False)
    ledger("acs5/county/10", 0)
    assert _changed(postgres_connection_factory, "area", served) is True
