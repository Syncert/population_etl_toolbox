"""A value the provider withheld reaches the fact as withheld.

Covers: DB-059 -- the ACS and BLS revision relations have always recorded why a
        value is absent. The fact aggregation kept only the number, so a cell
        Census suppressed and a cell Census never published were the same row:
        one with no estimate. Applying `027_acs_bls_fact_lineage.sql` to the
        internal stack put a number on it -- 31,481,530 of 99,783,997 ACS fact
        rows carry no estimate, and every one of them was indistinguishable
        from a geography that does not exist.

This is the silver half. Serving still filters those rows out; publishing them
is the next deliverable of the plan this belongs to.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls.silver_bls import transform as bls_transform
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.capture_seed import seed_capture, seed_geography
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_a_bls_observation_with_no_value_reaches_the_fact_as_missing(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-059 — the status and the capture survive the transform.

    The provider's own token is kept beside the status, so a reading that
    turns out to be wrong can be re-derived from the fact rather than only
    from the capture.
    """
    series_id = "LAUST970000000000003"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20970101, "2097-01-01")
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="97",
                vintage=2097,
                name="Withheld State",
            )
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_series (
                    program, series_id, title, seasonal, measure, area_code
                ) VALUES ('la', %s, 'Withheld fixture', 'U', '03',
                          'ST9700000000000')
                ON CONFLICT DO NOTHING
                """,
                (series_id,),
            )
            capture_id = seed_capture(cursor, "BLS")
            cursor.execute(
                """
                INSERT INTO silver_bls.observation_revision (
                    capture_id, observation_index, program, series_id,
                    year_source, period_source, period_name_source,
                    value_source, year, period, period_name, value,
                    value_status, is_latest
                ) VALUES (%s, 0, 'la', %s, '2097', 'M01', 'January', '-',
                          2097, 'M01', 'January', NULL, 'missing', TRUE)
                """,
                (capture_id, series_id),
            )
        writer.commit()
    finally:
        writer.close()

    monkeypatch.setattr(
        bls_transform,
        "_get_hook",
        lambda: PostgresHookStub(postgres_connection_factory),
    )

    try:
        assert bls_transform.transform_bls_to_silver("la") >= 1

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT value, value_status, source_value, capture_id
                    FROM silver_bls.fact_labor_statistics
                    WHERE series_id = %s
                    """,
                    (series_id,),
                )
                row = cursor.fetchone()
        finally:
            reader.close()

        assert row is not None, (
            "the observation did not reach the fact at all, which is the old "
            "behaviour this test exists to refuse"
        )
        value, status, source_value, fact_capture = row
        assert value is None
        assert status == "missing", (
            f"the fact says {status!r}; a consumer cannot tell a value BLS "
            f"withheld from one it never published"
        )
        assert source_value == "-", (
            "the provider's own token was dropped, so the status cannot be "
            "re-derived from the fact"
        )
        assert str(fact_capture) == str(capture_id), (
            "the fact names no capture, so DQ-SHARED-001 cannot verify the "
            "bytes this value was read from"
        )
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_bls.fact_labor_statistics WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM silver_bls.observation_revision WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM raw_bls.bls_series WHERE series_id = %s",
                    (series_id,),
                )
                # The transform writes a resolution ledger row per geography
                # it reads, and `silver_ref` is shared: leaving one behind
                # makes the next suite's view of "which geographies has BLS
                # seen" this suite's, which is what
                # `test_no_suite_left_shared_provider_state_behind` exists to
                # catch.
                # `source_code` here is the resolved geography's id, not the
                # BLS area code -- checked against the row the transform
                # actually wrote rather than guessed from the series id.
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = 'BLS' AND source_code = 'state:97'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20970101"
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_the_fact_refuses_a_published_status_with_no_value(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-059 — the constraint is real, not only declared.

    The status only means something because the warehouse refuses a row that
    claims a published value and carries none. Asserted against the database
    rather than against the DDL text, because a `CHECK` that failed to apply
    reads exactly like one that did.
    """
    import psycopg2

    insert = """
        INSERT INTO silver_bls.fact_labor_statistics (
            time_sk, geo_sk, duration_start, duration_end, period_date,
            series_id, program, value, year, period, value_status,
            load_batch_id
        )
        VALUES (%s, %s, DATE '2096-01-01', DATE '2096-01-31',
                DATE '2096-01-01', 'REFUSED_FIXTURE', 'la', %s, 2096, 'M01',
                %s, gen_random_uuid())
    """

    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            _seed_time(cursor, 20960101, "2096-01-01")
            geo_sk = seed_geography(
                cursor,
                geo_type="state",
                state_fips="96",
                vintage=2096,
                name="Refusal State",
            )

            # The same row with a value is accepted, so the refusal below is
            # about the status and the null together rather than about
            # anything else in the row.
            cursor.execute(insert, (20960101, geo_sk, 1.0, "valid"))
            cursor.execute(
                "DELETE FROM silver_bls.fact_labor_statistics "
                "WHERE series_id = 'REFUSED_FIXTURE'"
            )

            with pytest.raises(psycopg2.errors.CheckViolation):
                cursor.execute(insert, (20960101, geo_sk, None, "valid"))
    finally:
        database.rollback()
        database.close()
