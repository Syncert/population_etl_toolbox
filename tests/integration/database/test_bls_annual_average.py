"""BLS's annual averages are kept as provider facts beside December.

Covers: ETL-072
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls.silver_bls import transform
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.capture_seed import delete_geography, seed_capture, seed_geography
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_m13_and_m12_are_two_facts_and_only_m12_is_a_month(
    monkeypatch: pytest.MonkeyPatch,
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-072 — the annual average sits beside December, is not served as a month, and is kept."""
    series_id = "LAUST990000000000003"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            # The annual average is keyed to the year's first month.
            _seed_time(cursor, 20980101, "2098-01-01")
            _seed_time(cursor, 20981201, "2098-12-01")
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="99",
                vintage=2098,
                name="Test State",
            )
            cursor.execute(
                """
                INSERT INTO raw_bls.bls_series (program, series_id, title, seasonal, measure, area_code)
                VALUES ('la', %s, 'Test unemployment', 'U', '03', 'ST9900000000000')
                """,
                (series_id,),
            )
            capture_id = seed_capture(cursor, "BLS")
            for index, (period, name, value) in enumerate(
                (("M12", "December", "4.10"), ("M13", "Annual", "4.35"))
            ):
                cursor.execute(
                    """INSERT INTO silver_bls.observation_revision (
                        capture_id, observation_index, program, series_id,
                        year_source, period_source, period_name_source, value_source,
                        year, period, period_name, value, value_status, is_latest
                    ) VALUES (%s, %s, 'la', %s, '2098', %s, %s, %s, 2098, %s, %s, %s, 'valid', FALSE)""",
                    (
                        capture_id,
                        index,
                        series_id,
                        period,
                        name,
                        value,
                        period,
                        name,
                        value,
                    ),
                )
        writer.commit()
    finally:
        writer.close()

    monkeypatch.setattr(
        transform, "_get_hook", lambda: PostgresHookStub(postgres_connection_factory)
    )
    try:
        assert transform.transform_bls_to_silver("la") == 2
        reader = postgres_connection_factory()
        try:
            with reader.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT period, period_date::TEXT, duration_start::TEXT, value
                    FROM silver_bls.fact_labor_statistics WHERE series_id = %s ORDER BY period
                    """,
                    (series_id,),
                )
                assert [(p, d, s, float(v)) for p, d, s, v in cursor.fetchall()] == [
                    ("M12", "2098-12-31", "2098-12-01", 4.10),
                    ("M13", "2098-12-31", "2098-01-01", 4.35),
                ]
                cursor.execute(
                    """
                    -- Only for this read; rolled back below.
                    WITH survey AS (
                        INSERT INTO gold_bls.dim_bls_survey (program_code, survey_name, observation_basis)
                        VALUES ('LA', 'Local Area Unemployment Statistics', 'PEOPLE')
                        ON CONFLICT (program_code) DO UPDATE SET survey_name = EXCLUDED.survey_name
                        RETURNING bls_survey_sk
                    )
                    INSERT INTO gold_bls.dim_bls_series (
                        bls_survey_sk, program_code, series_id, measure_category, value_type
                    )
                    SELECT bls_survey_sk, 'LA', %s, 'UNEMPLOYMENT', 'RATE' FROM survey
                    RETURNING bls_series_sk
                    """,
                    (series_id,),
                )
                series_sk = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT period_code FROM gold_bls.fact_bls_observation WHERE bls_series_sk = %s",
                    (series_sk,),
                )
                assert cursor.fetchall() == [("M12",)]
                reader.rollback()
                cursor.execute(
                    """
                    SELECT year, period_start::TEXT, period_end::TEXT, value
                    FROM gold_bls.provider_annual_average WHERE series_id = %s
                    """,
                    (series_id,),
                )
                assert [(y, s, e, float(v)) for y, s, e, v in cursor.fetchall()] == [
                    (2098, "2098-01-01", "2098-12-31", 4.35)
                ]
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
                delete_geography(cursor, "state:99")
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20981201"
                )
            cleanup.commit()
        finally:
            cleanup.close()


MIGRATION = "sql/migrations/033_bls_annual_average_identity.sql"


def test_migration_033_swaps_the_old_key_and_is_safe_to_rerun(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-072 — a warehouse keyed by (series_id, period_date) is moved to (series_id, year, period)."""
    from pathlib import Path

    sql = (Path(__file__).resolve().parents[3] / MIGRATION).read_text(encoding="utf-8")
    connection_ = postgres_connection_factory()
    try:
        with connection_.cursor() as cursor:
            cursor.execute(
                "ALTER TABLE silver_bls.fact_labor_statistics DROP CONSTRAINT fact_labor_stats_uk"
            )
            cursor.execute(
                "ALTER TABLE silver_bls.fact_labor_statistics "
                "ADD CONSTRAINT fact_labor_stats_uk UNIQUE (series_id, period_date)"
            )
            cursor.execute(sql)
            cursor.execute(sql)
            cursor.execute(
                """
                SELECT pg_get_constraintdef(oid) FROM pg_constraint
                WHERE conname = 'fact_labor_stats_uk'
                  AND conrelid = 'silver_bls.fact_labor_statistics'::REGCLASS
                """
            )
            assert cursor.fetchall() == [("UNIQUE (series_id, year, period)",)]
    finally:
        connection_.rollback()
        connection_.close()


def test_dq_bls_007_does_not_count_an_annual_average_as_an_unserved_month(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ETL-072 — DQ-BLS-007 compares served months with published months only.

    The serving view excludes `M13` by design, so a published side that counts
    it reads every series-year BLS averages as unbacked: the weekly check on
    the local warehouse failed 3,366 groups such as `BLS:CUUR0000SA0|1990`
    this way. A served value the warehouse does not hold still fails.
    """
    from data_ingestion_toolbox.quality.sources import bls_contract_conformance

    series_id = "CUURDQ0007TEST"
    metric = "BLS:" + series_id
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            geo_sk = seed_geography(
                cursor, geo_type="state", state_fips="93", vintage=2093, name="Test"
            )
        writer.commit()
        with writer.cursor() as cursor:
            _seed_time(cursor, 20930101, "2093-01-01")
            capture_id = seed_capture(cursor, "BLS")
            for period, start, end, value in (
                ("M01", "2093-01-01", "2093-01-31", "130.1"),
                ("M02", "2093-02-01", "2093-02-28", "130.3"),
                ("M13", "2093-01-01", "2093-12-31", "130.2"),
            ):
                cursor.execute(
                    """
                    INSERT INTO silver_bls.fact_labor_statistics (
                        time_sk, geo_sk, duration_start, duration_end, period_date,
                        series_id, program, geo_level, geo_id, state_fips, value,
                        source_value, value_status, year, period, period_name,
                        capture_id, load_batch_id
                    ) VALUES (
                        20930101, %s, %s, %s, %s, %s, 'cu', 'STATE', 'state:93',
                        '93', %s, %s, 'valid', 2093, %s, %s, %s, gen_random_uuid()
                    )
                    """,
                    (
                        geo_sk,
                        start,
                        end,
                        end,
                        series_id,
                        value,
                        value,
                        period,
                        period,
                        str(capture_id),
                    ),
                )
                if period == "M13":
                    continue
                cursor.execute(
                    """
                    INSERT INTO gold_bls.rpt_bls_observations (
                        source_code, observation_date, duration_start, duration_end,
                        time_sk, as_of_date, updated_at, geo_id, geo_level,
                        series_id, program_code, series_title, value,
                        value_status, units, seasonal_adjustment_status,
                        metric_code, metric_display_name
                    ) VALUES (
                        'BLS', %s, %s, %s, 20930101, '2093-03-01', NOW(),
                        'state:93', 'STATE', %s, 'CU', 'DQ-BLS-007 test', %s,
                        'valid', 'index', 'Not Seasonally Adjusted', %s,
                        'DQ-BLS-007 test'
                    )
                    """,
                    (end, start, end, series_id, value, metric),
                )

            fact, _, _ = bls_contract_conformance(cursor, {})
            assert not [e for e in fact.evidence if e.startswith(metric + "|")], (
                fact.evidence
            )

            cursor.execute(
                "UPDATE gold_bls.rpt_bls_observations SET value = 131.0 "
                "WHERE metric_code = %s AND observation_date = '2093-02-28'",
                (metric,),
            )
            fact, _, _ = bls_contract_conformance(cursor, {})
            assert fact.result == "fail"
            assert f"{metric}|2093" in fact.evidence
        writer.rollback()
    finally:
        writer.close()
        cleanup = postgres_connection_factory()
        try:
            with cleanup.cursor() as cursor:
                delete_geography(cursor, "state:93")
            cleanup.commit()
        finally:
            cleanup.close()
