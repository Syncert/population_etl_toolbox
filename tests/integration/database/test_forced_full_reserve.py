"""A forced full re-serve, against real PostgreSQL.

Covers: ETL-049 — the changed-year plan selects years by silver watermark, so a
change to what a served row *means* (a metric identity, a geography vocabulary,
units) skips exactly the years still carrying the old meaning. These tests hold
the watermark still and prove the forced plan visits the year anyway, resumes
where it stopped, and stays idempotent.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred.gold_fred import transform as gold_transform
from data_ingestion_toolbox.utility.gold_schema import (
    refresh_serving_layer_in_year_chunks,
)
from data_ingestion_toolbox.utility.serving_reserve import FRED_CHUNK_CONFIG
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

YEARS = (2091, 2092)


@pytest.fixture
def reserve_token(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    token = uuid4().hex[:10].upper()
    try:
        yield token
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                for statement in (
                    "DELETE FROM gold_fred.mv_fred_latest WHERE series_id LIKE %s",
                    "DELETE FROM gold_fred.rpt_fred_observations WHERE series_id LIKE %s",
                    "DELETE FROM gold_fred.dim_fred_series WHERE series_id LIKE %s",
                    "DELETE FROM silver_fred.fact_economic_indicators WHERE series_id LIKE %s",
                    "DELETE FROM raw_fred.fred_series WHERE series_id LIKE %s",
                ):
                    cursor.execute(statement, (f"TEST_RESERVE_{token}%",))
                cursor.execute(
                    "DELETE FROM control.serving_refresh_chunk_state "
                    "WHERE source_code = 'FRED'"
                )
                cursor.execute(
                    "DELETE FROM control.serving_refresh_state WHERE source_code = 'FRED'"
                )
            cleanup.commit()
        finally:
            cleanup.close()


def _seed(factory: Callable[[], connection], token: str) -> str:
    """One FRED series with a value in each of two years, then gold metadata."""
    series_id = f"TEST_RESERVE_{token}"
    writer = factory()
    try:
        with writer.cursor() as cursor:
            for year in YEARS:
                time_sk = int(f"{year}0101")
                cursor.execute(
                    """
                    INSERT INTO silver_ref.dim_time (
                        time_sk, date_key, year, quarter, month, day, day_of_week,
                        day_name, month_name, week_of_year, is_weekend,
                        is_month_start, is_month_end, is_quarter_start,
                        is_quarter_end, is_year_start, is_year_end, ingested_at
                    ) VALUES (
                        %s, %s, %s, 1, 1, 1, 4, 'Thursday', 'January', 1, FALSE,
                        TRUE, FALSE, TRUE, FALSE, TRUE, FALSE, NOW()
                    ) ON CONFLICT (time_sk) DO NOTHING
                    """,
                    (time_sk, f"{year}-01-01", year),
                )
                cursor.execute(
                    """
                    INSERT INTO silver_fred.fact_economic_indicators (
                        time_sk, duration_start, duration_end, observation_date,
                        series_id, domain, value, is_missing, series_title,
                        unit_of_measure, frequency, seasonal_adjustment,
                        source_system, load_batch_id, ingested_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, 'fixture', %s, FALSE, 'Reserve fixture',
                        'Index', 'Monthly', 'Not Adjusted', 'FRED', %s, NOW()
                    )
                    """,
                    (
                        time_sk,
                        f"{year}-01-01",
                        f"{year}-01-31",
                        f"{year}-01-01",
                        series_id,
                        float(year),
                        str(uuid4()),
                    ),
                )
        writer.commit()
    finally:
        writer.close()

    gold_transform.refresh_fred_elements(PostgresHookStub(factory))
    return series_id


def _served_years(factory: Callable[[], connection], series_id: str) -> dict[int, str]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXTRACT(YEAR FROM observation_date)::INT, COUNT(*)::TEXT
                FROM gold_fred.rpt_fred_observations
                WHERE series_id = %s
                GROUP BY 1 ORDER BY 1
                """,
                (series_id,),
            )
            return {row[0]: row[1] for row in cursor.fetchall()}
    finally:
        reader.close()


def _chunk_rows(factory: Callable[[], connection]) -> dict[int, tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXTRACT(YEAR FROM chunk_start)::INT, status, attempt_count
                FROM control.serving_refresh_chunk_state
                WHERE source_code = 'FRED'
                ORDER BY chunk_start
                """
            )
            return {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
    finally:
        reader.close()


def _full_reserve_marker(factory: Callable[[], connection]):
    """When the current forced re-serve began, as the warehouse records it."""
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT last_full_reserve_started_at FROM control.serving_refresh_state "
                "WHERE source_code = 'FRED'"
            )
            row = cursor.fetchone()
            return row[0] if row else None
    finally:
        reader.close()


def test_the_forced_plan_visits_a_year_the_changed_plan_skips(
    postgres_connection_factory: Callable[[], connection],
    reserve_token: str,
) -> None:
    """Covers: ETL-049 — the defect this contract exists for.

    After an incremental refresh the watermark covers both years, so a second
    incremental run plans nothing at all. That is precisely the state a metric
    identity change leaves behind, and the forced plan must still rewrite both.
    """
    series_id = _seed(postgres_connection_factory, reserve_token)
    hook = PostgresHookStub(postgres_connection_factory)

    first = refresh_serving_layer_in_year_chunks(hook=hook, config=FRED_CHUNK_CONFIG)
    assert first["completed"] == len(YEARS)
    assert set(_served_years(postgres_connection_factory, series_id)) == set(YEARS)

    # Nothing moved in silver, so the scheduled plan has no work.
    second = refresh_serving_layer_in_year_chunks(hook=hook, config=FRED_CHUNK_CONFIG)
    assert second["planned"] == 0

    # The forced plan visits every year regardless.
    forced = refresh_serving_layer_in_year_chunks(
        hook=hook, config=FRED_CHUNK_CONFIG, force_full=True
    )
    assert forced["planned"] >= len(YEARS)
    assert forced["completed"] == forced["planned"]
    assert forced["skipped"] == 0
    assert set(_served_years(postgres_connection_factory, series_id)) == set(YEARS)


def test_a_forced_reserve_is_idempotent(
    postgres_connection_factory: Callable[[], connection],
    reserve_token: str,
) -> None:
    """Covers: ETL-049 — running it twice changes no row counts."""
    series_id = _seed(postgres_connection_factory, reserve_token)
    hook = PostgresHookStub(postgres_connection_factory)

    refresh_serving_layer_in_year_chunks(hook=hook, config=FRED_CHUNK_CONFIG)
    refresh_serving_layer_in_year_chunks(
        hook=hook, config=FRED_CHUNK_CONFIG, force_full=True
    )
    once = _served_years(postgres_connection_factory, series_id)

    refresh_serving_layer_in_year_chunks(
        hook=hook, config=FRED_CHUNK_CONFIG, force_full=True
    )
    assert _served_years(postgres_connection_factory, series_id) == once


def test_a_forced_reserve_resumes_at_the_year_it_stopped_on(
    postgres_connection_factory: Callable[[], connection],
    reserve_token: str,
) -> None:
    """Covers: ETL-049 — an interruption is not a restart.

    The watermark cannot decide this: a forced re-serve deliberately leaves
    watermarks alone, so every year looks equally "already done". Progress
    rides on when the chunk last completed relative to the run marker, and the
    marker is durable because an Airflow retry is a new process -- re-running
    ACS's first nineteen years because the twentieth failed is the behaviour
    this exists to avoid.
    """
    _seed(postgres_connection_factory, reserve_token)
    hook = PostgresHookStub(postgres_connection_factory)
    refresh_serving_layer_in_year_chunks(hook=hook, config=FRED_CHUNK_CONFIG)
    refresh_serving_layer_in_year_chunks(
        hook=hook, config=FRED_CHUNK_CONFIG, force_full=True
    )
    marker = _full_reserve_marker(postgres_connection_factory)

    # Stand one year back down to "outstanding for this run", which is the
    # state an interrupted re-serve leaves: the run marker stays, and the
    # chunks it had not reached have no completion at or after it.
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.serving_refresh_chunk_state
                SET last_refresh_completed_at = %s - INTERVAL '1 hour',
                    status = 'FAILED'
                WHERE source_code = 'FRED'
                  AND chunk_start = %s
                """,
                (marker, f"{YEARS[-1]}-01-01"),
            )
        writer.commit()
    finally:
        writer.close()

    resumed = refresh_serving_layer_in_year_chunks(
        hook=hook, config=FRED_CHUNK_CONFIG, force_full=True
    )

    # The same run continued: the marker did not move, the year already done
    # was skipped, and only the outstanding one was rewritten.
    assert _full_reserve_marker(postgres_connection_factory) == marker
    assert resumed["completed"] == 1
    assert resumed["skipped"] == resumed["planned"] - 1
    assert all(
        status == "COMPLETE"
        for status, _ in _chunk_rows(postgres_connection_factory).values()
    )

    # With nothing outstanding, the next forced run is a new one.
    fresh = refresh_serving_layer_in_year_chunks(
        hook=hook, config=FRED_CHUNK_CONFIG, force_full=True
    )
    assert _full_reserve_marker(postgres_connection_factory) > marker
    assert fresh["skipped"] == 0


def test_a_forced_reserve_does_not_push_the_source_watermark_forward(
    postgres_connection_factory: Callable[[], connection],
    reserve_token: str,
) -> None:
    """Covers: ETL-049 — a re-serve must not make later ingests skip rows.

    If a forced run advanced the watermark to wall-clock time, silver rows
    ingested before it but after the genuine watermark would never be served
    again, and nothing would report that.
    """
    _seed(postgres_connection_factory, reserve_token)
    hook = PostgresHookStub(postgres_connection_factory)
    refresh_serving_layer_in_year_chunks(hook=hook, config=FRED_CHUNK_CONFIG)

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT last_silver_ingested_at FROM control.serving_refresh_state "
                "WHERE source_code = 'FRED'"
            )
            before = cursor.fetchone()[0]
            cursor.execute(
                "SELECT MAX(ingested_at) FROM silver_fred.fact_economic_indicators"
            )
            silver_max = cursor.fetchone()[0]
    finally:
        reader.close()

    refresh_serving_layer_in_year_chunks(
        hook=hook, config=FRED_CHUNK_CONFIG, force_full=True
    )

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT last_silver_ingested_at FROM control.serving_refresh_state "
                "WHERE source_code = 'FRED'"
            )
            after = cursor.fetchone()[0]
    finally:
        reader.close()

    assert after >= before
    assert after <= silver_max


# --- DB-048: the serving relations are vacuumed and analysed on purpose ------


def test_every_serving_table_carries_its_autovacuum_settings(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-048 — the bootstrap applies them, not an operator.

    At PostgreSQL's 20% default scale factor these tables reach their
    autovacuum threshold only after millions of dead tuples, which is how
    `BETA_RESET_REINGESTION.md` §7 came to record 54.7 million dead rows
    against 8.9 million live. The DDL sets the thresholds and `ensure_*`
    re-applies it, so a warehouse picks them up without a migration.

    Read on whatever actually stores rows. `gold_census.rpt_acs_observations`
    is partitioned, and a partitioned parent has no storage -- `reloptions`
    set on it is read by nothing. Checking the parent alone would have passed
    on a relation whose thirty-seven partitions carried no settings at all.
    """
    expected = {
        "autovacuum_vacuum_scale_factor=0.02",
        "autovacuum_analyze_scale_factor=0.01",
        "autovacuum_vacuum_cost_limit=2000",
    }
    relations = [
        "gold_census.rpt_acs_observations",
        "gold_census.mv_acs_latest",
        "gold_bls.rpt_bls_observations",
        "gold_bls.mv_bls_latest",
        "gold_fred.rpt_fred_observations",
        "gold_fred.mv_fred_latest",
    ]

    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            for relation in relations:
                # A partitioned parent holds no storage, so `reloptions` on it
                # sets nothing the autovacuum daemon reads. The settings have
                # to be on the relations that have rows, and on every one of
                # them -- including the default partition, which a year chunk
                # never truncates (DB-056).
                cursor.execute(
                    """
                    SELECT c.oid::regclass::TEXT, c.reloptions
                    FROM pg_class c
                    WHERE c.oid = %s::regclass AND c.relkind <> 'p'
                    UNION ALL
                    SELECT c.oid::regclass::TEXT, c.reloptions
                    FROM pg_inherits i
                    JOIN pg_class c ON c.oid = i.inhrelid
                    WHERE i.inhparent = %s::regclass
                    ORDER BY 1
                    """,
                    (relation, relation),
                )
                carriers = cursor.fetchall()
                assert carriers, f"{relation} does not exist"
                for name, reloptions in carriers:
                    options = set(reloptions or [])
                    assert expected <= options, f"{name} carries {sorted(options)}"
    finally:
        database.close()


def test_a_chunk_leaves_current_statistics_behind(
    postgres_connection_factory: Callable[[], connection],
    reserve_token: str,
) -> None:
    """Covers: DB-048 — the next chunk plans against the rows that are there.

    The refresh is delete-then-reinsert per year. Without this, by the second
    chunk of a twenty-year re-serve every plan was built from statistics
    describing rows that had been deleted.
    """
    _seed(postgres_connection_factory, reserve_token)
    hook = PostgresHookStub(postgres_connection_factory)

    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT last_analyze, last_autoanalyze "
                "FROM pg_stat_user_tables "
                "WHERE schemaname = 'gold_fred' AND relname = 'rpt_fred_observations'"
            )
            before = cursor.fetchone()
    finally:
        database.close()

    refresh_serving_layer_in_year_chunks(
        hook=hook, config=FRED_CHUNK_CONFIG, force_full=True
    )

    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                "SELECT last_analyze, last_autoanalyze "
                "FROM pg_stat_user_tables "
                "WHERE schemaname = 'gold_fred' AND relname = 'rpt_fred_observations'"
            )
            after = cursor.fetchone()
    finally:
        database.close()

    assert after is not None
    latest_after = max(stamp for stamp in after if stamp is not None)
    latest_before = (
        max((stamp for stamp in (before or ()) if stamp is not None), default=None)
        if before
        else None
    )
    assert latest_before is None or latest_after > latest_before, (
        "the chunk committed and left the planner's statistics where they were"
    )
