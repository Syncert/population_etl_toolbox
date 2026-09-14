"""FRED revision-to-silver integration and dimension-miss contracts."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.fred.silver_fred import transform
from data_ingestion_toolbox.capture import (
    ResponseCapture,
    persist_response_capture,
    request_fingerprint,
)
from data_ingestion_toolbox.fred.silver_fred.replay import replay_fred_capture
from data_ingestion_toolbox.glossary.harvest import Publisher, harvest_publisher
from data_ingestion_toolbox.fred.gold_fred import transform as gold_transform
from data_ingestion_toolbox.utility.gold_schema import (
    ServingRefreshChunkConfig,
    refresh_serving_layer_in_year_chunks,
)
from tests.support.postgres import PostgresHookStub
from tests.support.capture_seed import seed_capture, seed_geography

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
                    "DELETE FROM raw_fred.fred_series WHERE series_id LIKE %s",
                    (f"TEST_FRED_SILVER_{token}%",),
                )
                # Revision rows are pending work the next run's transform
                # reads, so leaving them outlives the reference rows above.
                cursor.execute(
                    "DELETE FROM silver_fred.observation_revision "
                    "WHERE series_id LIKE %s",
                    (f"TEST_FRED_SILVER_{token}%",),
                )
                cursor.execute(
                    "DELETE FROM control.serving_refresh_chunk_state "
                    "WHERE source_code = 'FRED'"
                )
                cursor.execute(
                    "DELETE FROM control.serving_refresh_state "
                    "WHERE source_code = 'FRED'"
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


def test_fred_capture_replay_retains_revisions_and_selects_latest_silver(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
) -> None:
    """Covers: DB-010, DB-020, DB-022 — FRED replays offline by revision."""
    run_id = uuid4()
    request_id = uuid4()
    series_id = f"TEST_FRED_SILVER_{fred_silver_token}_CAPTURE"
    domain = f"test_{fred_silver_token.lower()}_capture"
    endpoint = "https://api.stlouisfed.org/fred/series/observations"
    parameters = {
        "series_id": series_id,
        "domain": domain,
        "observation_start": "2099-01-01",
        "observation_end": "2099-01-31",
        "file_type": "json",
    }
    fingerprint = request_fingerprint("FRED", endpoint, parameters)
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20990101, "2099-01-01")
            cursor.execute(
                """
                INSERT INTO raw_fred.fred_series (
                    series_id, title, units, frequency, seasonal_adjustment
                ) VALUES (%s, 'Captured series', 'Index', 'Monthly', 'Not Adjusted')
                """,
                (series_id,),
            )
            cursor.execute(
                """
                INSERT INTO control.ingestion_run (
                    run_id, source_code, status, started_at
                ) VALUES (%s, 'FRED', 'running', NOW())
                """,
                (run_id,),
            )
            cursor.execute(
                """
                INSERT INTO control.ingestion_request (
                    request_id, run_id, source_code, endpoint,
                    request_parameters, request_fingerprint, status,
                    attempt_count, max_attempts, started_at
                ) VALUES (%s, %s, 'FRED', %s, %s::JSONB, %s,
                          'running', 1, 3, NOW())
                """,
                (request_id, run_id, endpoint, "{}", fingerprint),
            )
        writer.commit()
    finally:
        writer.close()

    captures = []
    for day, value in ((1, "3.1"), (2, "3.2")):
        capture_id = uuid4()
        payload = (
            '{"observations":[{"realtime_start":"2099-02-0%d",'
            '"realtime_end":"2099-02-0%d","date":"2099-01-01",'
            '"value":"%s"},{"date":"2099-01-02","value":"."}]}' % (day, day, value)
        ).encode()
        persist_response_capture(
            postgres_connection_factory,
            ResponseCapture(
                capture_id=capture_id,
                request_id=request_id,
                run_id=run_id,
                source_code="FRED",
                endpoint=endpoint,
                request_parameters=parameters,
                retrieved_at=datetime(2099, 2, day, tzinfo=UTC),
                http_status=200,
                response_headers={"content-type": "application/json"},
                media_type="application/json",
                payload=payload,
                source_revision=f"2099-02-0{day}",
            ),
        )
        assert (
            replay_fred_capture(
                postgres_connection_factory,
                capture_id=capture_id,
                series_id=series_id,
                domain=domain,
            )
            == 2
        )
        captures.append(capture_id)

    monkeypatch.setattr(
        transform, "_get_hook", lambda: PostgresHookStub(postgres_connection_factory)
    )
    assert transform.transform_fred_to_silver(domain) == 1

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT capture_id, value_source, value_status
                FROM silver_fred.observation_revision
                WHERE series_id = %s
                ORDER BY capture_id, observation_index
                """,
                (series_id,),
            )
            revisions = cursor.fetchall()
            assert len(revisions) == 4
            assert sum(row[2] == "missing" and row[1] == "." for row in revisions) == 2
            cursor.execute(
                """
                SELECT value, source_value, realtime_start, capture_id
                FROM silver_fred.fact_economic_indicators
                WHERE series_id = %s
                """,
                (series_id,),
            )
            assert cursor.fetchone() == (
                Decimal("3.2"),
                "3.2",
                datetime(2099, 2, 2).date(),
                captures[1],
            )
    finally:
        reader.close()


def test_fred_raw_rows_transform_to_exact_silver_durations(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
) -> None:
    """Covers: DB-010 — FRED revisions produce exact silver keys and durations."""
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
            capture_id = seed_capture(cursor, "FRED")
            cursor.execute(
                """INSERT INTO silver_fred.observation_revision (
                    capture_id, observation_index, domain, series_id,
                    observation_date_source, value_source, realtime_start_source,
                    realtime_end_source, observation_date, value, value_status,
                    realtime_start, realtime_end
                ) VALUES
                    (%s, 0, %s, %s, '2099-01-01', '10.5', '2099-03-01',
                     '2099-03-01', '2099-01-01', 10.5, 'valid', '2099-03-01', '2099-03-01'),
                    (%s, 1, %s, %s, '2099-02-01', '11.5', '2099-03-01',
                     '2099-03-01', '2099-02-01', 11.5, 'valid', '2099-03-01', '2099-03-01')""",
                (capture_id, domain, series_id, capture_id, domain, series_id),
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
            capture_id = seed_capture(cursor, "FRED")
            cursor.execute(
                """INSERT INTO silver_fred.observation_revision (
                    capture_id, observation_index, domain, series_id,
                    observation_date_source, value_source, realtime_start_source,
                    realtime_end_source, observation_date, value, value_status,
                    realtime_start, realtime_end
                ) VALUES (%s, 0, %s, %s, '2199-01-01', '1', '2199-02-01',
                          '2199-02-01', '2199-01-01', 1, 'valid',
                          '2199-02-01', '2199-02-01')""",
                (capture_id, domain, series_id),
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


def test_fred_silver_to_gold_refresh_populates_harvested_catalog_and_serving(
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
) -> None:
    """Covers: DB-012 — independent harvest links source facts to serving rows.

    Also covers: ETL-047 — ``silver_ref.dim_geo`` carries ``us:1`` at level
    ``us``, so the refresh has the dimension row that used to override the
    normalised vocabulary. Without this seed the ``NATIONAL`` assertion below
    passes vacuously.
    """
    series_id = f"TEST_FRED_SILVER_{fred_silver_token}_GOLD"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20990101, "2099-01-01")
            seed_geography(
                cursor,
                geo_type="nation",
                vintage=2099,
                name="United States",
            )
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
    assert harvest_publisher(postgres_connection_factory, Publisher("gold_fred")) >= 1
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
                SELECT s.series_id, c.metric_code, r.geo_id, r.geo_level,
                       m.geo_level, r.value, m.value
                FROM gold_fred.dim_fred_series s
                JOIN gold_glossary.dim_metric_catalog c
                  ON c.source_code = 'FRED'
                 AND c.source_object_type = 'series'
                 AND c.source_object_key = s.series_id
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
                "NATIONAL",
                "NATIONAL",
                42.5,
                42.5,
            )
    finally:
        reader.close()


def test_freds_revision_window_reaches_the_served_relation(
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
) -> None:
    """Covers: DB-040 — the realtime window is carried into gold, not dropped.

    Migration 004 added `realtime_start`/`realtime_end` to the silver
    revision, and `uq_rpt_fred_observations_nk`, the latest-selection index
    and `uq_mv_fred_latest` are all built around them --
    `gold_fred.fact_fred_observation` published `NULL::DATE` for both, so
    every served row collapsed onto the `'0001-01-01'` sentinel those keys
    COALESCE to and the window a row was published under was reachable only
    in silver. The window seeded here is FRED's own vintage identity, and the
    served rows must carry it exactly.
    """
    series_id = f"TEST_FRED_WINDOW_{fred_silver_token}"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20990101, "2099-01-01")
            seed_geography(
                cursor, geo_type="nation", vintage=2099, name="United States"
            )
            cursor.execute(
                """
                INSERT INTO silver_fred.fact_economic_indicators (
                    time_sk, duration_start, duration_end, observation_date,
                    series_id, domain, value, is_missing, series_title,
                    unit_of_measure, frequency, seasonal_adjustment,
                    source_system, load_batch_id, ingested_at,
                    realtime_start, realtime_end
                ) VALUES (
                    20990101, '2099-01-01', '2099-01-31', '2099-01-01',
                    %s, 'fixture', 42.5, FALSE, 'Window fixture',
                    'Index', 'Monthly', 'Not Adjusted', 'FRED', %s,
                    '2024-06-15 12:00:00+00'::TIMESTAMPTZ,
                    '2024-06-15', '9999-12-31'
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
            # The fact view keys geography and the series surrogate rather
            # than the series id, so it is read through its dimension.
            cursor.execute(
                """
                SELECT DISTINCT fact.realtime_start::TEXT, fact.realtime_end::TEXT
                  FROM gold_fred.fact_fred_observation AS fact
                  JOIN gold_fred.dim_fred_series AS series
                    ON series.fred_series_sk = fact.fred_series_sk
                 WHERE series.series_id = %s
                """,
                (series_id,),
            )
            assert cursor.fetchall() == [("2024-06-15", "9999-12-31")]

            for relation in (
                "gold_fred.rpt_fred_observations",
                "gold_fred.mv_fred_latest",
            ):
                cursor.execute(
                    f"""
                    SELECT DISTINCT realtime_start::TEXT, realtime_end::TEXT
                      FROM {relation} WHERE series_id = %s
                    """,
                    (series_id,),
                )
                assert cursor.fetchall() == [("2024-06-15", "9999-12-31")], relation
    finally:
        refresher.close()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                for relation in (
                    "gold_fred.mv_fred_latest",
                    "gold_fred.rpt_fred_observations",
                    "gold_fred.dim_fred_series",
                    "silver_fred.fact_economic_indicators",
                ):
                    cursor.execute(
                        f"DELETE FROM {relation} WHERE series_id = %s", (series_id,)
                    )
                cursor.execute(
                    "DELETE FROM control.serving_refresh_chunk_state "
                    "WHERE source_code = 'FRED'"
                )
                cursor.execute(
                    "DELETE FROM control.serving_refresh_state "
                    "WHERE source_code = 'FRED'"
                )
            cleanup.commit()
        finally:
            cleanup.close()


def test_a_fred_release_is_the_ingestion_not_the_refresh(
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
) -> None:
    """Covers: DB-039 — re-serving a FRED chunk publishes no new release.

    `gold_fred.fact_fred_observation` published `CURRENT_DATE AS as_of_date`
    and the serving refresh materialised the literal, so a release of a FRED
    series was the day a chunk of it was last written. The row is seeded with
    an ingestion date in the past, which is what makes this failing-first: a
    test that ingested and served in one session saw today's date either way.
    """
    series_id = f"TEST_FRED_RELEASE_{fred_silver_token}"
    ingested_on = "2024-06-15"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20990101, "2099-01-01")
            seed_geography(
                cursor, geo_type="nation", vintage=2099, name="United States"
            )
            cursor.execute(
                """
                INSERT INTO silver_fred.fact_economic_indicators (
                    time_sk, duration_start, duration_end, observation_date,
                    series_id, domain, value, is_missing, series_title,
                    unit_of_measure, frequency, seasonal_adjustment,
                    source_system, load_batch_id, ingested_at
                ) VALUES (
                    20990101, '2099-01-01', '2099-01-31', '2099-01-01',
                    %s, 'fixture', 42.5, FALSE, 'Release fixture',
                    'Index', 'Monthly', 'Not Adjusted', 'FRED', %s,
                    %s::TIMESTAMPTZ
                )
                """,
                (series_id, str(uuid4()), f"{ingested_on} 12:00:00+00"),
            )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    assert gold_transform.refresh_fred_elements(hook) >= 1

    refresher = postgres_connection_factory()
    try:
        with refresher.cursor() as cursor:
            for _ in range(2):
                cursor.execute(
                    "CALL gold_fred.refresh_dashboard_serving_layer_fred(%s, %s, TRUE)",
                    ("2099-01-01", "2099-01-31"),
                )
            refresher.commit()
            cursor.execute(
                """
                SELECT DISTINCT as_of_date::TEXT, updated_at::DATE::TEXT
                  FROM gold_fred.rpt_fred_observations WHERE series_id = %s
                """,
                (series_id,),
            )
            assert cursor.fetchall() == [(ingested_on, ingested_on)]
            cursor.execute(
                """
                SELECT COUNT(DISTINCT as_of_date)
                  FROM gold_fred.rpt_fred_observations WHERE series_id = %s
                """,
                (series_id,),
            )
            assert cursor.fetchone() == (1,)
    finally:
        refresher.close()
        # This node serves rows and forces a refresh, so it owns three kinds
        # of state: the served rows, the silver row behind them, and the
        # source's serving watermark. Leaving the watermark behind is what
        # makes the next chunked-refresh test plan nothing at all.
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                for relation in (
                    "gold_fred.mv_fred_latest",
                    "gold_fred.rpt_fred_observations",
                    "gold_fred.dim_fred_series",
                    "silver_fred.fact_economic_indicators",
                ):
                    cursor.execute(
                        f"DELETE FROM {relation} WHERE series_id = %s", (series_id,)
                    )
                cursor.execute(
                    "DELETE FROM control.serving_refresh_chunk_state "
                    "WHERE source_code = 'FRED'"
                )
                cursor.execute(
                    "DELETE FROM control.serving_refresh_state "
                    "WHERE source_code = 'FRED'"
                )
            cleanup.commit()
        finally:
            cleanup.close()


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
                "DELETE FROM control.serving_refresh_chunk_state "
                "WHERE source_code = 'FRED'"
            )
            cursor.execute(
                "DELETE FROM control.serving_refresh_state WHERE source_code = 'FRED'"
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
                    control.serving_refresh_chunk_state
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
                FROM control.serving_refresh_chunk_state
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
                FROM control.serving_refresh_state
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


def test_fred_publishes_only_the_grains_its_served_rows_carry(
    postgres_connection_factory: Callable[[], connection],
    fred_silver_token: str,
) -> None:
    """Covers: ARC-006 — the FRED catalog grain is a fact about served rows.

    ``gold_fred.metric_publisher`` used to declare ``ARRAY['NATIONAL']`` for
    every series. It was true of every series served, and it would have stayed
    true in the catalog for the first regional series configured, which is one
    entry away. Two series are seeded here and only one is served: the served
    one publishes the grain its rows carry, and the unserved one publishes no
    grain rather than the grain the view used to invent for it.

    The order below is the contract, not an accident of the fixture. A derived
    grain is read at harvest time, so the harvest has to run after the serving
    refresh or it publishes the empty array for everything. The ingest DAG
    already sequences ``publisher_ready`` downstream of the serving refresh.
    """
    served_series = f"TEST_FRED_SILVER_{fred_silver_token}_SERVED"
    unserved_series = f"TEST_FRED_SILVER_{fred_silver_token}_UNSERVED"

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            _seed_time(cursor, 20990101, "2099-01-01")
            _seed_time(cursor, 20990601, "2099-06-01")
            seed_geography(
                cursor,
                geo_type="nation",
                vintage=2099,
                name="United States",
            )
            for series_id, time_sk, start, end in (
                (served_series, 20990101, "2099-01-01", "2099-01-31"),
                (unserved_series, 20990601, "2099-06-01", "2099-06-30"),
            ):
                cursor.execute(
                    """
                    INSERT INTO silver_fred.fact_economic_indicators (
                        time_sk, duration_start, duration_end, observation_date,
                        series_id, domain, value, is_missing, series_title,
                        unit_of_measure, frequency, seasonal_adjustment,
                        source_system, load_batch_id, ingested_at
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, 'fixture', 42.5, FALSE, 'Grain fixture',
                        'Index', 'Monthly', 'Not Adjusted', 'FRED', %s, NOW()
                    )
                    """,
                    (time_sk, start, end, start, series_id, str(uuid4())),
                )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    assert gold_transform.refresh_fred_elements(hook) >= 2

    # January only: the June series reaches gold as a dimension and a fact, and
    # is never served, which is the case the declaration could not express.
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

    assert harvest_publisher(postgres_connection_factory, Publisher("gold_fred")) >= 1

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_object_key, valid_geo_grains
                FROM gold_glossary.dim_metric_catalog
                WHERE source_code = 'FRED' AND source_object_key IN (%s, %s)
                ORDER BY source_object_key
                """,
                (served_series, unserved_series),
            )
            published = dict(cursor.fetchall())

            # The grain published for the served series is the word its own
            # served rows carry, read back from the relation the API reads.
            cursor.execute(
                """
                SELECT DISTINCT UPPER(geo_level)
                FROM gold_fred.mv_fred_latest
                WHERE metric_code = %s
                """,
                (f"FRED:{served_series}",),
            )
            served_grains = sorted(row[0] for row in cursor.fetchall())
    finally:
        reader.close()

    assert published.get(served_series) == served_grains == ["NATIONAL"], (
        f"the served series published {published.get(served_series)!r} "
        f"while its rows carry {served_grains!r}"
    )
    assert published.get(unserved_series) == [], (
        "a series nothing serves published "
        f"{published.get(unserved_series)!r}; an unserved series must publish "
        "no grain, so the agreement guards report the empty code rather than "
        "a grain the publisher invented"
    )
