"""FRED raw-to-silver integration and dimension-miss contracts."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred.silver_fred import transform
from data_ingestion_toolbox.fred.gold_fred import transform as gold_transform
from data_ingestion_toolbox.utility.gold_schema import (
    ServingRefreshChunkConfig,
    refresh_serving_layer_in_year_chunks,
)
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def fred_silver_token(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[str]:
    token = uuid4().hex[:12].upper()
    try:
        yield token
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_fred.mv_fred_latest WHERE series_id LIKE %s",
                    (f"TEST_FRED_SILVER_{token}%",),
                )
                cursor.execute(
                    "DELETE FROM gold_fred.rpt_fred_observations WHERE series_id LIKE %s",
                    (f"TEST_FRED_SILVER_{token}%",),
                )
                cursor.execute(
                    """
                    DELETE FROM gold_glossary.bridge_metric_fred_series b
                    USING gold_fred.dim_fred_series s
                    WHERE b.fred_series_sk = s.fred_series_sk
                      AND s.series_id LIKE %s
                    """,
                    (f"TEST_FRED_SILVER_{token}%",),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.dim_metric_catalog WHERE metric_code LIKE %s",
                    (f"FRED:TEST_FRED_SILVER_{token}%",),
                )
                cursor.execute(
                    "DELETE FROM gold_fred.dim_fred_series WHERE series_id LIKE %s",
                    (f"TEST_FRED_SILVER_{token}%",),
                )
                cursor.execute(
                    "DELETE FROM silver_fred.fact_economic_indicators WHERE series_id LIKE %s",
                    (f"TEST_FRED_SILVER_{token}%",),
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_long WHERE series_id LIKE %s",
                    (f"TEST_FRED_SILVER_{token}%",),
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_series WHERE series_id LIKE %s",
                    (f"TEST_FRED_SILVER_{token}%",),
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.serving_refresh_chunk_state "
                    "WHERE source_code = 'FRED'"
                )
                cursor.execute(
                    "DELETE FROM gold_glossary.serving_refresh_state "
                    "WHERE source_code = 'FRED'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time "
                    "WHERE time_sk IN (20980101, 20990101, 20990201)"
                )
            cleanup.commit()
        finally:
            cleanup.close()


def _seed_time(cursor, time_sk: int, value: str) -> None:
    cursor.execute(
        """
        INSERT INTO silver_ref.dim_time (
            time_sk, date_key, year, quarter, month, day, day_of_week,
            day_name, month_name, week_of_year, is_weekend,
            is_month_start, is_month_end, is_quarter_start,
            is_quarter_end, is_year_start, is_year_end, ingested_at
        ) VALUES (
            %s, %s, EXTRACT(YEAR FROM %s::DATE), EXTRACT(QUARTER FROM %s::DATE),
            EXTRACT(MONTH FROM %s::DATE), EXTRACT(DAY FROM %s::DATE), 4,
            'Thursday', TO_CHAR(%s::DATE, 'Month'), 1, FALSE,
            TRUE, FALSE, TRUE, FALSE, TRUE, FALSE, NOW()
        ) ON CONFLICT (time_sk) DO NOTHING
        """,
        (time_sk, value, value, value, value, value, value),
    )


def test_fred_raw_rows_transform_to_exact_silver_durations(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
) -> None:
    """Covers: DB-010 — FRED raw rows produce exact silver keys and durations."""
    series_id = f"TEST_FRED_SILVER_{fred_silver_token}"
    domain = f"test_{fred_silver_token.lower()}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20990101, "2099-01-01")
            _seed_time(cursor, 20990201, "2099-02-01")
            cursor.execute(
                """
                INSERT INTO raw_fred.fred_series (
                    series_id, title, units, frequency, seasonal_adjustment
                ) VALUES (%s, 'Test series', 'Index', 'Monthly', 'Not Adjusted')
                """,
                (series_id,),
            )
            cursor.execute(
                """
                INSERT INTO raw_fred.fred_long (
                    domain, series_id, obs_date, value, is_missing,
                    realtime_start, realtime_end, load_batch_id
                ) VALUES
                    (%s, %s, '2099-01-01', 10.5, FALSE, '2099-03-01', '2099-03-01', %s),
                    (%s, %s, '2099-02-01', 11.5, FALSE, '2099-03-01', '2099-03-01', %s)
                """,
                (domain, series_id, str(uuid4()), domain, series_id, str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()

    monkeypatch.setattr(
        transform, "_get_hook", lambda: PostgresHookStub(postgres_connection_factory)
    )
    assert transform.transform_fred_to_silver(domain) == 2

    reader = postgres_connection_factory()
    first_watermarks: list = []
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT time_sk, observation_date::TEXT, duration_start::TEXT,
                       duration_end::TEXT, value, frequency
                FROM silver_fred.fact_economic_indicators
                WHERE series_id = %s
                ORDER BY observation_date
                """,
                (series_id,),
            )
            assert cursor.fetchall() == [
                (20990101, "2099-01-01", "2099-01-01", "2099-01-31", 10.5, "Monthly"),
                (20990201, "2099-02-01", "2099-02-01", "2099-02-28", 11.5, "Monthly"),
            ]
            cursor.execute(
                """
                SELECT ingested_at FROM silver_fred.fact_economic_indicators
                WHERE series_id = %s ORDER BY observation_date
                """,
                (series_id,),
            )
            first_watermarks = [row[0] for row in cursor.fetchall()]
    finally:
        reader.close()

    # The transform's public return value is rows processed, while the SQL
    # conflict predicate decides whether an existing row is materially changed.
    assert transform.transform_fred_to_silver(domain) == 2
    replay_reader = postgres_connection_factory()
    try:
        with replay_reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT ingested_at FROM silver_fred.fact_economic_indicators
                WHERE series_id = %s ORDER BY observation_date
                """,
                (series_id,),
            )
            assert [row[0] for row in cursor.fetchall()] == first_watermarks
    finally:
        replay_reader.close()


def test_fred_missing_time_dimension_is_counted_and_not_inserted(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Covers: DB-011 — a time-dimension miss is counted and not inserted."""
    series_id = f"TEST_FRED_SILVER_{fred_silver_token}_MISS"
    domain = f"test_{fred_silver_token.lower()}_miss"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_fred.fred_series (
                    series_id, title, units, frequency, seasonal_adjustment
                ) VALUES (%s, 'Missing dimension', 'Index', 'Daily', 'Not Adjusted')
                """,
                (series_id,),
            )
            cursor.execute(
                """
                INSERT INTO raw_fred.fred_long (
                    domain, series_id, obs_date, value, is_missing,
                    realtime_start, realtime_end, load_batch_id
                ) VALUES (%s, %s, '2199-01-01', 1, FALSE, '2199-02-01', '2199-02-01', %s)
                """,
                (domain, series_id, str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()

    monkeypatch.setattr(
        transform, "_get_hook", lambda: PostgresHookStub(postgres_connection_factory)
    )
    with caplog.at_level(logging.WARNING):
        assert transform.transform_fred_to_silver(domain) == 0
    assert "Dropped 1 FRED rows with missing time_sk" in caplog.text

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM silver_fred.fact_economic_indicators WHERE series_id = %s",
                (series_id,),
            )
            assert cursor.fetchone() == (0,)
    finally:
        reader.close()


def test_fred_silver_to_gold_refresh_populates_catalog_bridge_and_serving(
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
) -> None:
    """Covers: DB-012 — silver-to-gold refresh creates exact serving relationships."""
    series_id = f"TEST_FRED_SILVER_{fred_silver_token}_GOLD"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20990101, "2099-01-01")
            cursor.execute(
                """
                INSERT INTO silver_fred.fact_economic_indicators (
                    time_sk, duration_start, duration_end, observation_date,
                    series_id, domain, value, is_missing, series_title,
                    unit_of_measure, frequency, seasonal_adjustment,
                    source_system, load_batch_id, ingested_at
                ) VALUES (
                    20990101, '2099-01-01', '2099-01-31', '2099-01-01',
                    %s, 'fixture', 42.5, FALSE, 'Gold fixture',
                    'Index', 'Monthly', 'Not Adjusted', 'FRED', %s, NOW()
                )
                """,
                (series_id, str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    assert gold_transform.refresh_fred_elements(hook) >= 1
    refresher = postgres_connection_factory()
    try:
        with refresher.cursor() as cursor:
            cursor.execute(
                "CALL gold_fred.refresh_dashboard_serving_layer_fred(%s, %s, TRUE)",
                ("2099-01-01", "2099-01-31"),
            )
        refresher.commit()
    finally:
        refresher.close()

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT s.series_id, c.metric_code, r.geo_id, r.value, m.value
                FROM gold_fred.dim_fred_series s
                JOIN gold_glossary.bridge_metric_fred_series b
                  ON b.fred_series_sk = s.fred_series_sk
                JOIN gold_glossary.dim_metric_catalog c
                  ON c.metric_catalog_sk = b.metric_catalog_sk
                JOIN gold_fred.rpt_fred_observations r
                  ON r.series_id = s.series_id AND r.metric_code = c.metric_code
                JOIN gold_fred.mv_fred_latest m
                  ON m.series_id = s.series_id AND m.metric_code = c.metric_code
                WHERE s.series_id = %s
                """,
                (series_id,),
            )
            assert cursor.fetchone() == (
                series_id,
                f"FRED:{series_id}",
                "us:1",
                42.5,
                42.5,
            )
    finally:
        reader.close()


def test_fred_revision_refreshes_latest_without_losing_prior_date(
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
) -> None:
    """Covers: DB-013 — revised latest value refreshes while history remains."""
    series_id = f"TEST_FRED_SILVER_{fred_silver_token}_REV"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20990101, "2099-01-01")
            _seed_time(cursor, 20990201, "2099-02-01")
            cursor.execute(
                """
                INSERT INTO silver_fred.fact_economic_indicators (
                    time_sk, duration_start, duration_end, observation_date,
                    series_id, domain, value, is_missing, series_title,
                    unit_of_measure, frequency, seasonal_adjustment,
                    source_system, load_batch_id, ingested_at
                ) VALUES
                    (20990101, '2099-01-01', '2099-01-31', '2099-01-01',
                     %s, 'fixture', 10, FALSE, 'Revision fixture', 'Index',
                     'Monthly', 'Not Adjusted', 'FRED', %s, NOW()),
                    (20990201, '2099-02-01', '2099-02-28', '2099-02-01',
                     %s, 'fixture', 20, FALSE, 'Revision fixture', 'Index',
                     'Monthly', 'Not Adjusted', 'FRED', %s, NOW())
                """,
                (series_id, str(uuid4()), series_id, str(uuid4())),
            )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    gold_transform.refresh_fred_elements(hook)
    for revised_value in (20, 25):
        refresher = postgres_connection_factory()
        try:
            with refresher.cursor() as cursor:
                if revised_value == 25:
                    cursor.execute(
                        """
                        UPDATE silver_fred.fact_economic_indicators
                        SET value = 25, ingested_at = clock_timestamp()
                        WHERE series_id = %s AND observation_date = '2099-02-01'
                        """,
                        (series_id,),
                    )
                cursor.execute(
                    "CALL gold_fred.refresh_dashboard_serving_layer_fred(%s, %s, TRUE)",
                    ("2099-01-01", "2099-02-28"),
                )
            refresher.commit()
        finally:
            refresher.close()

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT observation_date::TEXT, value
                FROM gold_fred.rpt_fred_observations
                WHERE series_id = %s ORDER BY observation_date
                """,
                (series_id,),
            )
            assert cursor.fetchall() == [("2099-01-01", 10), ("2099-02-01", 25)]
            cursor.execute(
                "SELECT value FROM gold_fred.mv_fred_latest WHERE series_id = %s",
                (series_id,),
            )
            assert cursor.fetchone() == (25,)
    finally:
        reader.close()


def _fred_chunk_config(*, latest_procedure: str) -> ServingRefreshChunkConfig:
    return ServingRefreshChunkConfig(
        source_code="FRED",
        log_label="FRED",
        report_table="gold_fred.rpt_fred_observations",
        report_date_column="observation_date",
        changed_chunks_sql="""
            SELECT
                MAKE_DATE(EXTRACT(YEAR FROM s.observation_date)::INTEGER, 1, 1),
                MAKE_DATE(EXTRACT(YEAR FROM s.observation_date)::INTEGER, 12, 31),
                MAX(s.ingested_at)
            FROM silver_fred.fact_economic_indicators s
            WHERE s.is_missing = FALSE AND s.ingested_at > %s
            GROUP BY EXTRACT(YEAR FROM s.observation_date)
            ORDER BY EXTRACT(YEAR FROM s.observation_date)
        """,
        report_procedure="gold_fred.refresh_rpt_fred_observations",
        latest_procedure=latest_procedure,
        statement_timeout="30min",
    )


@pytest.mark.slow
def test_incremental_gold_refresh_recovers_failed_annual_checkpoint(
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
) -> None:
    """Covers: ETL-037 — real watermarks, chunks, failure, and replay reconcile."""
    series_id = f"TEST_FRED_SILVER_{fred_silver_token}_CHUNKS"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20980101, "2098-01-01")
            _seed_time(cursor, 20990101, "2099-01-01")
            cursor.execute(
                """
                INSERT INTO silver_fred.fact_economic_indicators (
                    time_sk, duration_start, duration_end, observation_date,
                    series_id, domain, value, is_missing, series_title,
                    unit_of_measure, frequency, seasonal_adjustment,
                    source_system, load_batch_id, ingested_at
                ) VALUES
                    (20980101, '2098-01-01', '2098-01-31', '2098-01-01',
                     %s, 'fixture', 31, FALSE, 'Chunk fixture', 'Index',
                     'Monthly', 'Not Adjusted', 'FRED', %s, NOW()),
                    (20990101, '2099-01-01', '2099-01-31', '2099-01-01',
                     %s, 'fixture', 32, FALSE, 'Chunk fixture', 'Index',
                     'Monthly', 'Not Adjusted', 'FRED', %s, NOW())
                """,
                (series_id, str(uuid4()), series_id, str(uuid4())),
            )
            cursor.execute(
                "DELETE FROM gold_glossary.serving_refresh_chunk_state "
                "WHERE source_code = 'FRED'"
            )
            cursor.execute(
                "DELETE FROM gold_glossary.serving_refresh_state "
                "WHERE source_code = 'FRED'"
            )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    gold_transform.refresh_fred_elements(hook)
    with pytest.raises(Exception, match="procedure|does not exist"):
        refresh_serving_layer_in_year_chunks(
            hook=hook,
            config=_fred_chunk_config(
                latest_procedure="gold_fred.missing_latest_procedure"
            ),
        )

    failed_reader = postgres_connection_factory()
    try:
        with failed_reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, attempt_count FROM
                    gold_glossary.serving_refresh_chunk_state
                WHERE source_code = 'FRED' ORDER BY chunk_start
                """
            )
            assert cursor.fetchall() == [("FAILED", 1), ("PENDING", 0)]
            cursor.execute(
                "SELECT COUNT(*) FROM gold_fred.rpt_fred_observations "
                "WHERE series_id = %s",
                (series_id,),
            )
            assert cursor.fetchone() == (0,)
    finally:
        failed_reader.close()

    recovered = refresh_serving_layer_in_year_chunks(
        hook=hook,
        config=_fred_chunk_config(latest_procedure="gold_fred.refresh_mv_fred_latest"),
    )
    assert recovered == {"planned": 2, "completed": 2, "skipped": 0}
    assert refresh_serving_layer_in_year_chunks(
        hook=hook,
        config=_fred_chunk_config(latest_procedure="gold_fred.refresh_mv_fred_latest"),
    ) == {"planned": 0, "completed": 0, "skipped": 0}

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, attempt_count,
                       completed_silver_ingested_at >= target_silver_ingested_at
                FROM gold_glossary.serving_refresh_chunk_state
                WHERE source_code = 'FRED' ORDER BY chunk_start
                """
            )
            assert cursor.fetchall() == [
                ("COMPLETE", 2, True),
                ("COMPLETE", 1, True),
            ]
            cursor.execute(
                """
                SELECT last_window_start::TEXT, last_window_end::TEXT
                FROM gold_glossary.serving_refresh_state
                WHERE source_code = 'FRED'
                """
            )
            assert cursor.fetchone() == ("2098-01-01", "2099-12-31")
            cursor.execute(
                """
                SELECT observation_date::TEXT, value
                FROM gold_fred.rpt_fred_observations
                WHERE series_id = %s ORDER BY observation_date
                """,
                (series_id,),
            )
            assert cursor.fetchall() == [("2098-01-01", 31), ("2099-01-01", 32)]
    finally:
        reader.close()
