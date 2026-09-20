"""BLS revision-to-silver database integration contract."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls.silver_bls import transform
from data_ingestion_toolbox.quality.sources import (
    bls_geography_accountability,
)
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.postgres import PostgresHookStub
from tests.support.capture_seed import delete_geography, seed_capture, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_bls_raw_rows_transform_to_exact_silver_keys(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-009 — BLS revisions produce exact periods and dimension keys."""
    series_id = "LAUST990000000000003"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20980101, "2098-01-01")
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="99",
                vintage=2098,
                name="Test State",
            )
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_series (
                    program, series_id, title, seasonal, measure, area_code
                ) VALUES ('la', %s, 'Test unemployment', 'U', '03', 'ST9900000000000')
                """,
                (series_id,),
            )
            capture_id = seed_capture(cursor, "BLS")
            cursor.execute(
                """INSERT INTO silver_bls.observation_revision (
                    capture_id, observation_index, program, series_id,
                    year_source, period_source, period_name_source, value_source,
                    year, period, period_name, value, value_status, is_latest
                ) VALUES (%s, 0, 'la', %s, '2098', 'M01', 'January', '4.25',
                          2098, 'M01', 'January', 4.25, 'valid', TRUE)""",
                (capture_id, series_id),
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
                cursor.execute(
                    """SELECT status, resolution_method, reason_code
                       FROM silver_ref.geography_resolution
                       WHERE provider_source = 'BLS'
                         AND provider_dataset = 'la'
                         AND source_code = 'state:99'
                         AND source_vintage = 2098"""
                )
                assert cursor.fetchone() == ("resolved", "exact_code", None)
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
                    "DELETE FROM raw_bls.bls_series WHERE series_id = %s", (series_id,)
                )
                cursor.execute(
                    # Pending work the next run's transform reads; see
                    # tests/integration/database/test_tier_repeatability.py.
                    "DELETE FROM silver_bls.observation_revision WHERE series_id = %s",
                    (series_id,),
                )
                delete_geography(cursor, "state:99")
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20980101"
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_an_unchanged_bls_row_keeps_the_ingestion_a_release_is_read_from(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-039 — the watermark a BLS release identity rests on holds.

    The served `as_of_date` is the silver row's `ingested_at` (DB-039), which
    is only an honest release identity because ETL-037's upsert advances that
    column when the row's content changed and leaves it alone when it did not.
    ETL-037 asserts the predicate as a string in the transform's source; this
    asserts the behaviour against a real database, and FRED's flow already
    has the equivalent node.
    """
    series_id = "LAUST980000000000003"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20980201, "2098-02-01")
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="98",
                vintage=2098,
                name="Watermark State",
            )
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_series (
                    program, series_id, title, seasonal, measure, area_code
                ) VALUES ('la', %s, 'Watermark series', 'U', '03',
                          'ST9800000000000')
                """,
                (series_id,),
            )
            capture_id = seed_capture(cursor, "BLS")
            cursor.execute(
                """INSERT INTO silver_bls.observation_revision (
                    capture_id, observation_index, program, series_id,
                    year_source, period_source, period_name_source, value_source,
                    year, period, period_name, value, value_status, is_latest
                ) VALUES (%s, 0, 'la', %s, '2098', 'M02', 'February', '5.5',
                          2098, 'M02', 'February', 5.5, 'valid', TRUE)""",
                (capture_id, series_id),
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
                    "SELECT ingested_at FROM silver_bls.fact_labor_statistics "
                    "WHERE series_id = %s",
                    (series_id,),
                )
                first = cursor.fetchone()[0]
        finally:
            reader.close()

        # The same revision, transformed again: rows processed, nothing
        # materially changed, so the watermark must not move.
        assert transform.transform_bls_to_silver("la") == 1
        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    "SELECT ingested_at FROM silver_bls.fact_labor_statistics "
                    "WHERE series_id = %s",
                    (series_id,),
                )
                assert cursor.fetchone()[0] == first
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
                    "DELETE FROM raw_bls.bls_series WHERE series_id = %s", (series_id,)
                )
                cursor.execute(
                    "DELETE FROM silver_bls.observation_revision WHERE series_id = %s",
                    (series_id,),
                )
                delete_geography(cursor, "state:98")
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20980201"
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_a_bls_geography_the_reference_cannot_resolve_reaches_the_ledger(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-009, DQ-BLS-004 — a dropped row leaves a queryable trace.

    `silver_bls.fact_labor_statistics.geo_sk` is `NOT NULL`, so a series whose
    area the shared reference does not carry cannot be stored at all. Before
    the ledger, the only record that the provider had published it was one
    warning line: `BETA_RESET_REINGESTION.md` §5 tells an operator to check
    `geography_resolution GROUP BY provider_source` rather than silently
    accepting misses, and BLS could not appear in that query by construction.

    This seeds a series for a state the reference has no entity for, and
    asserts the three things that make the miss recoverable: the fact table
    holds nothing, the ledger holds an `unmapped` row naming the geography,
    and replaying the same capture does not duplicate it.
    """
    series_id = "LAUST980000000000003"
    unresolved_geo = "state:98"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20990101, "2099-01-01")
            # Deliberately no `seed_geography` for state 98: this is the
            # unsynced-reference case, not a malformed-code case.
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_series (
                    program, series_id, title, seasonal, measure, area_code
                ) VALUES ('la', %s, 'Unresolvable area', 'U', '03', 'ST9800000000000')
                """,
                (series_id,),
            )
            capture_id = seed_capture(cursor, "BLS")
            cursor.execute(
                """INSERT INTO silver_bls.observation_revision (
                    capture_id, observation_index, program, series_id,
                    year_source, period_source, period_name_source, value_source,
                    year, period, period_name, value, value_status, is_latest
                ) VALUES (%s, 0, 'la', %s, '2099', 'M01', 'January', '5.50',
                          2099, 'M01', 'January', 5.50, 'valid', TRUE)""",
                (capture_id, series_id),
            )
        writer.commit()
    finally:
        writer.close()

    monkeypatch.setattr(
        transform, "_get_hook", lambda: PostgresHookStub(postgres_connection_factory)
    )
    try:
        transform.transform_bls_to_silver(program="la")
        # Replay: the same capture, transformed again. The ledger's unique key
        # is what makes this a no-op rather than a second row, and a resumed
        # or retried run is the ordinary case rather than the exceptional one.
        transform.transform_bls_to_silver(program="la")

        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM silver_bls.fact_labor_statistics "
                    "WHERE series_id = %s",
                    (series_id,),
                )
                assert cursor.fetchone()[0] == 0, (
                    "a row whose geography did not resolve reached the fact "
                    "table, which its NOT NULL geo_sk should have prevented"
                )

                cursor.execute(
                    """SELECT status, geo_sk, resolution_method, reason_code,
                              count(*) OVER (), evidence_capture_id
                         FROM silver_ref.geography_resolution
                        WHERE provider_source = 'BLS'
                          AND provider_dataset = 'la'
                          AND source_code = %s
                          AND source_vintage = 2099""",
                    (unresolved_geo,),
                )
                rows = cursor.fetchall()
                assert rows, (
                    "the geography the transform could not resolve is in no "
                    "ledger, so nothing records that the provider published it"
                )
                status, geo_sk, method, reason, total, evidence = rows[0]
                assert status == "unmapped", status
                assert geo_sk is None
                assert method is None, (
                    "an unresolved geography claims a resolution method"
                )
                assert reason, "the ledger row says nothing about why it failed"
                assert total == 1, (
                    f"replaying one capture wrote {total} ledger rows; the "
                    "unique key is not carrying the idempotency"
                )
                assert evidence == capture_id, (
                    "the ledger row cannot name the capture that published the "
                    "geography, so the miss is recorded without its evidence"
                )

                # The operator query in BETA_RESET_REINGESTION.md §5 is the
                # reason this matters, so it is the shape asserted.
                cursor.execute(
                    """SELECT provider_source, count(*)
                         FROM silver_ref.geography_resolution
                        WHERE status <> 'resolved'
                        GROUP BY provider_source
                       HAVING provider_source = 'BLS'"""
                )
                assert cursor.fetchone() is not None, (
                    "§5's GROUP BY provider_source query still cannot see BLS"
                )
        finally:
            reader.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_bls.observation_revision WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM raw_bls.bls_series WHERE series_id = %s", (series_id,)
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = 'BLS' AND source_code = %s",
                    (unresolved_geo,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20990101"
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_a_geography_in_neither_the_facts_nor_the_ledger_fails_the_rule(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DQ-BLS-004, DB-009 — the rule reports a geography that left no trace.

    The rule passes on a healthy warehouse, which is what a rule first run
    long after it was declared is least entitled to be trusted for. This
    removes the ledger row the transform wrote and asserts the rule notices:
    the geography is then in neither the fact table -- its `geo_sk` could not
    resolve -- nor the ledger, which is exactly the state the rule exists to
    find.
    """
    series_id = "LAUST970000000000003"
    unresolved_geo = "state:97"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20970101, "2097-01-01")
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_series (
                    program, series_id, title, seasonal, measure, area_code
                ) VALUES ('la', %s, 'Untraceable area', 'U', '03', 'ST9700000000000')
                """,
                (series_id,),
            )
            capture_id = seed_capture(cursor, "BLS")
            cursor.execute(
                """INSERT INTO silver_bls.observation_revision (
                    capture_id, observation_index, program, series_id,
                    year_source, period_source, period_name_source, value_source,
                    year, period, period_name, value, value_status, is_latest
                ) VALUES (%s, 0, 'la', %s, '2097', 'M01', 'January', '6.00',
                          2097, 'M01', 'January', 6.00, 'valid', TRUE)""",
                (capture_id, series_id),
            )
        writer.commit()
    finally:
        writer.close()

    monkeypatch.setattr(
        transform, "_get_hook", lambda: PostgresHookStub(postgres_connection_factory)
    )
    try:
        transform.transform_bls_to_silver(program="la")

        database = postgres_connection_factory()
        try:
            with database.cursor() as cursor:
                outcomes = bls_geography_accountability(cursor, {})
                assert outcomes[0].result == "pass", (
                    "the transform recorded the miss and the rule still "
                    f"reports it: {outcomes[0].evidence}"
                )

                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = 'BLS' AND source_code = %s",
                    (unresolved_geo,),
                )
                outcomes = bls_geography_accountability(cursor, {})
            database.rollback()

            assert outcomes[0].result == "fail", (
                "a BLS geography in neither the facts nor the ledger passed "
                "the rule, so the rule cannot see the state it exists for"
            )
            assert any(
                "ST9700000000000" in str(item) for item in outcomes[0].evidence
            ), f"the failure names no offending area: {outcomes[0].evidence}"
        finally:
            database.close()
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_bls.observation_revision WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM raw_bls.bls_series WHERE series_id = %s", (series_id,)
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE provider_source = 'BLS' AND source_code = %s",
                    (unresolved_geo,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20970101"
                )
            cleanup.commit()
        finally:
            cleanup.close()
