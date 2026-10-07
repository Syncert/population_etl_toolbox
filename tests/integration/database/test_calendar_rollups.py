"""Calendar rollups over real served rows: complete windows only, gaps named.

Covers: ETL-074
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.quality.sources import bls_derived_annual_reconciliation
from data_ingestion_toolbox.semantics.rollups import refresh_calendar_rollups
from data_ingestion_toolbox.semantics.time_aggregation import TimeMethod
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support import fbi_release
from tests.support.capture_seed import delete_geography, seed_capture, seed_geography

pytestmark = [pytest.mark.integration, pytest.mark.database]

METRIC = "BLS:ROLLUPTEST0001"


def _served_month(cursor, year: int, month: int, value: str | None) -> None:
    cursor.execute(
        """
        INSERT INTO gold_bls.rpt_bls_observations (
            source_code, observation_date, duration_start, duration_end,
            time_sk, as_of_date, updated_at, geo_id, geo_level, series_id,
            program_code, series_title, value, value_status, units,
            seasonal_adjustment_status, metric_code, metric_display_name
        ) VALUES (
            'BLS', (MAKE_DATE(%(y)s, %(m)s, 1) + INTERVAL '1 month - 1 day')::DATE,
            MAKE_DATE(%(y)s, %(m)s, 1),
            (MAKE_DATE(%(y)s, %(m)s, 1) + INTERVAL '1 month - 1 day')::DATE,
            %(y)s * 10000 + %(m)s * 100 + 1, '2097-02-01', NOW(), 'state:94',
            'STATE', 'ROLLUPTEST0001', 'RT', 'Rollup test', %(v)s,
            CASE WHEN %(v)s IS NULL THEN 'missing' ELSE 'valid' END,
            'index', 'Not Seasonally Adjusted', %(code)s, 'Rollup test'
        )
        """,
        {"y": year, "m": month, "v": value, "code": METRIC},
    )


def _rollups(factory: Callable[[], connection], relation: str, code: str) -> list:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT grain, window_start::TEXT, window_end::TEXT, method,
                       expected_periods, present_periods, value, refusal_reason,
                       component_releases, derived
                FROM {relation} WHERE metric_code = %s
                ORDER BY geo_id, subject_code, grain, window_start
                """,
                (code,),
            )
            return cursor.fetchall()
    finally:
        reader.close()


def test_bls_windows_are_derived_only_when_every_month_is_reported(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-074 — complete windows get the mean; a missing month refuses its quarter and year."""
    registry = {METRIC: TimeMethod(METRIC, "mean", "approved", "Nick", 1)}
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            for month in range(1, 13):
                _served_month(cursor, 2095, month, str(month))
                # December 2096 is withheld: never zero, never averaged.
                _served_month(cursor, 2096, month, None if month == 12 else "10")
            # A quarterly row is not a month and composes no calendar window.
            cursor.execute(
                """
                INSERT INTO gold_bls.rpt_bls_observations (
                    source_code, observation_date, duration_start, duration_end,
                    as_of_date, updated_at, geo_id, geo_level, series_id,
                    program_code, value, value_status, metric_code
                ) VALUES ('BLS', '2095-01-01', '2095-01-01', '2095-03-31',
                          '2097-02-01', NOW(), 'state:94', 'STATE',
                          'ROLLUPTEST0001', 'RT', 999, 'valid', %s)
                """,
                (METRIC,),
            )
        writer.commit()

        connection_ = postgres_connection_factory()
        try:
            first = refresh_calendar_rollups(connection_, "BLS", registry)
            rows = _rollups(postgres_connection_factory, "gold_bls.derived_calendar_rollup", METRIC)
            # Replaying over the same served rows writes the same rows.
            assert refresh_calendar_rollups(connection_, "BLS", registry) == first
            assert _rollups(
                postgres_connection_factory, "gold_bls.derived_calendar_rollup", METRIC
            ) == rows
        finally:
            connection_.close()

        by_window = {(grain, start): row for grain, start, *row in rows}
        assert len(rows) == 10  # 8 quarters and 2 years
        year_2095 = by_window[("year", "2095-01-01")]
        assert year_2095[0] == "2095-12-31"
        assert year_2095[1] == "mean"
        assert (year_2095[2], year_2095[3], year_2095[4], year_2095[5]) == (
            12,
            12,
            Decimal("6.5"),
            None,
        )
        assert year_2095[6] == ["2097-02-01"] and year_2095[7] is True
        assert by_window[("quarter", "2095-01-01")][4] == Decimal("2")
        assert by_window[("quarter", "2096-07-01")][4] == Decimal("10")
        refused_year = by_window[("year", "2096-01-01")]
        assert (refused_year[3], refused_year[4], refused_year[5]) == (
            11,
            None,
            "incomplete_window: 11 of 12 periods reported",
        )
        refused_quarter = by_window[("quarter", "2096-10-01")]
        assert (refused_quarter[4], refused_quarter[5]) == (
            None,
            "incomplete_window: 2 of 3 periods reported",
        )

        # DQ-BLS-008: BLS's own annual average for 2095 agrees with the
        # derived year to its published rounding, then is altered and does not.
        writer = postgres_connection_factory()
        with writer.cursor() as cursor:
            _seed_time(cursor, 20950101, "2095-01-01")
            geo_sk = seed_geography(
                cursor, geo_type="state", state_fips="94", vintage=2095, name="Test State"
            )
            capture_id = seed_capture(cursor, "BLS")
            cursor.execute(
                """
                INSERT INTO silver_bls.fact_labor_statistics (
                    time_sk, geo_sk, duration_start, duration_end, period_date,
                    series_id, program, geo_level, geo_id, state_fips, value,
                    source_value, value_status, year, period, period_name,
                    capture_id, load_batch_id
                ) VALUES (
                    20950101, %s, '2095-01-01', '2095-12-31', '2095-12-31',
                    'ROLLUPTEST0001', 'rt', 'STATE', 'state:94', '94', 6.5,
                    '6.500', 'valid', 2095, 'M13', 'Annual', %s, gen_random_uuid()
                )
                """,
                (geo_sk, str(capture_id)),
            )
        writer.commit()
        with writer.cursor() as cursor:
            (agreeing,) = bls_derived_annual_reconciliation(cursor, {})
            cursor.execute(
                "UPDATE silver_bls.fact_labor_statistics SET value = 6.6 "
                "WHERE series_id = 'ROLLUPTEST0001'"
            )
            (disagreeing,) = bls_derived_annual_reconciliation(cursor, {})
        writer.rollback()
        writer.close()
        assert agreeing.result == "pass"
        assert disagreeing.result == "fail" and disagreeing.observed_count == 1
        assert disagreeing.evidence[0].startswith(f"{METRIC}|state:94|2095-01-01|6.5")

        # Withdrawing the approval leaves no derived value behind.
        connection_ = postgres_connection_factory()
        try:
            assert refresh_calendar_rollups(connection_, "BLS", {}) == 0
        finally:
            connection_.close()
        assert _rollups(
            postgres_connection_factory, "gold_bls.derived_calendar_rollup", METRIC
        ) == []
    finally:
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM gold_bls.derived_calendar_rollup WHERE metric_code = %s",
                    (METRIC,),
                )
                cursor.execute(
                    "DELETE FROM gold_bls.rpt_bls_observations WHERE metric_code = %s",
                    (METRIC,),
                )
                cursor.execute(
                    "DELETE FROM silver_bls.fact_labor_statistics "
                    "WHERE series_id = 'ROLLUPTEST0001'"
                )
                delete_geography(cursor, "state:94")
            cleanup.commit()
        finally:
            cleanup.close()


def test_fbi_counts_sum_over_the_published_release(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-074 — an FBI count's window is the sum of its reported months, or refused with the reason."""
    for factory in fbi_release.reviewed_warehouse(postgres_connection_factory):
        captured = fbi_release.persist_fixture_release(factory)
        fbi_release.run_pipeline(factory, captured)
        reader = factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 'FBI_UCR:' || product_id || ':' || measure_id
                    FROM gold_fbi.latest_release_observation
                    WHERE measure_form = 'absolute_total'
                    ORDER BY 1 LIMIT 1
                    """
                )
                code = cursor.fetchone()[0]
        finally:
            reader.close()
        registry = {code: TimeMethod(code, "sum", "approved", "Nick", 1)}
        connection_ = factory()
        try:
            written = refresh_calendar_rollups(connection_, "FBI_UCR", registry)
            reader = connection_.cursor()
            # Each complete window equals the sum of its months, computed here
            # independently of the builder.
            reader.execute(
                """
                SELECT rollup.grain, rollup.window_start, rollup.value,
                       rollup.present_periods, rollup.expected_periods,
                       rollup.refusal_reason, (
                           SELECT SUM(observation.value)
                           FROM gold_fbi.latest_release_observation AS observation
                           WHERE 'FBI_UCR:' || observation.product_id || ':'
                                 || observation.measure_id = rollup.metric_code
                             AND observation.subject_code IS NOT DISTINCT FROM rollup.subject_code
                             AND observation.period_start BETWEEN rollup.window_start
                                                              AND rollup.window_end
                             AND observation.value_status = 'reported'
                       ) AS months
                FROM gold_fbi.derived_calendar_rollup AS rollup
                WHERE rollup.metric_code = %s
                """,
                (code,),
            )
            rows = reader.fetchall()
            connection_.rollback()
        finally:
            connection_.close()
        try:
            assert written == len(rows) > 0
            for grain, _start, value, present, expected, reason, months in rows:
                assert expected == (3 if grain == "quarter" else 12)
                if present == expected:
                    assert value == months and reason is None
                else:
                    assert value is None
                    assert reason == (
                        f"incomplete_window: {present} of {expected} periods reported"
                    )
        finally:
            cleanup = factory()
            try:
                with cleanup.cursor() as cursor:
                    cursor.execute("DELETE FROM gold_fbi.derived_calendar_rollup")
                cleanup.commit()
            finally:
                cleanup.close()
