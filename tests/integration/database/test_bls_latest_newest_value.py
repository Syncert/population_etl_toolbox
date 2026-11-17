"""A BLS key's latest is its newest published value, and its catalog says so.

Covers: DB-071, ARC-008
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls.gold_bls import transform as bls_transform
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.integration.database.test_serving_refresh_writes_only_changes import (
    WINDOW,
    YEAR,
    _rows_of,
    _run,
    bls_series,  # noqa: F401 -- the fixture, used by name
)
from tests.support.capture_seed import seed_geography
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "sql/migrations/041_bls_serves_its_newest_published_value.sql"
)
REFRESH = (
    ("CALL gold_bls.refresh_rpt_bls_observations(%s, %s)", WINDOW),
    ("CALL gold_bls.refresh_mv_bls_latest(%s, %s)", WINDOW),
)


def _seed(
    factory: Callable[[], connection], series_id: str, values: dict[int, int | None]
) -> None:
    writer = factory()
    try:
        with writer.cursor() as cursor:
            geo_sk = seed_geography(
                cursor,
                geo_type="state",
                state_fips="88",
                vintage=YEAR,
                name="Churn State",
            )
            for month, value in values.items():
                time_sk = YEAR * 10000 + month * 100 + 1
                _seed_time(cursor, time_sk, f"{YEAR}-{month:02d}-01")
                cursor.execute(
                    """
                    INSERT INTO silver_bls.fact_labor_statistics (
                        time_sk, geo_sk, duration_start, duration_end,
                        period_date, series_id, program, geo_level, geo_id,
                        state_fips, value, source_value, value_status, year,
                        period, measure_code, load_batch_id, ingested_at
                    ) VALUES (
                        %(t)s, %(g)s, MAKE_DATE(%(y)s, %(m)s, 1),
                        (MAKE_DATE(%(y)s, %(m)s, 1) + INTERVAL '1 month - 1 day')::DATE,
                        MAKE_DATE(%(y)s, %(m)s, 1), %(s)s, 'la', 'state',
                        'state:88', '88', %(v)s, %(sv)s,
                        CASE WHEN %(v)s IS NULL THEN 'missing' ELSE 'valid' END,
                        %(y)s, 'M' || LPAD(%(m)s::TEXT, 2, '0'), '03',
                        gen_random_uuid(), NOW()
                    )
                    """,
                    {
                        "t": time_sk,
                        "g": geo_sk,
                        "y": YEAR,
                        "m": month,
                        "s": series_id,
                        "v": value,
                        "sv": "-" if value is None else str(value),
                    },
                )
        writer.commit()
    finally:
        writer.close()
    bls_transform.refresh_bls_elements(PostgresHookStub(factory))
    _run(factory, *REFRESH)


def _latest(factory: Callable[[], connection], series_id: str) -> list:
    return [
        (date, None if value is None else int(value))
        for date, value in _rows_of(
            factory,
            "SELECT observation_date::TEXT, value FROM gold_bls.mv_bls_latest "
            "WHERE series_id = %s",
            (series_id,),
        )
    ]


def test_a_withheld_newest_month_does_not_become_the_latest(
    postgres_connection_factory: Callable[[], connection],
    bls_series: str,  # noqa: F811
) -> None:
    """Covers: DB-071 — the October 2025 shutdown month is not the latest."""
    _seed(postgres_connection_factory, bls_series, {1: 1, 2: 2, 3: None})

    # The withheld month is still served, with its status.
    served = _rows_of(
        postgres_connection_factory,
        "SELECT observation_date::TEXT, value_status, source_value "
        "FROM gold_bls.rpt_bls_observations WHERE series_id = %s "
        "ORDER BY observation_date",
        (bls_series,),
    )
    assert served[-1] == (f"{YEAR}-03-01", "missing", "-")
    assert _latest(postgres_connection_factory, bls_series) == [(f"{YEAR}-02-01", 2)]


def test_the_migration_corrects_a_withheld_latest_row_already_served(
    postgres_connection_factory: Callable[[], connection],
    bls_series: str,  # noqa: F811
) -> None:
    """Covers: DB-071 — rows the old ordering wrote are replaced once."""
    _seed(postgres_connection_factory, bls_series, {1: 1, 2: 2, 3: None})
    # What the old procedure left: the withheld month as the latest row.
    _run(
        postgres_connection_factory,
        ("DELETE FROM gold_bls.mv_bls_latest WHERE series_id = %s", (bls_series,)),
        (
            "INSERT INTO gold_bls.mv_bls_latest SELECT * FROM gold_bls.rpt_bls_observations "
            "WHERE series_id = %s AND value IS NULL",
            (bls_series,),
        ),
    )
    assert _latest(postgres_connection_factory, bls_series) == [(f"{YEAR}-03-01", None)]

    sql = MIGRATION.read_text(encoding="utf-8")
    _run(postgres_connection_factory, (sql, ()))
    assert _latest(postgres_connection_factory, bls_series) == [(f"{YEAR}-02-01", 2)]

    # Rerunnable: a second application writes nothing.
    before = _rows_of(
        postgres_connection_factory,
        "SELECT xmin::TEXT, ctid::TEXT FROM gold_bls.mv_bls_latest WHERE series_id = %s",
        (bls_series,),
    )
    _run(postgres_connection_factory, (sql, ()))
    assert before == _rows_of(
        postgres_connection_factory,
        "SELECT xmin::TEXT, ctid::TEXT FROM gold_bls.mv_bls_latest WHERE series_id = %s",
        (bls_series,),
    )


def test_a_key_whose_every_row_is_withheld_still_serves_its_newest_row(
    postgres_connection_factory: Callable[[], connection],
    bls_series: str,  # noqa: F811
) -> None:
    """Covers: DB-071 — nothing is invented for a key with no value at all."""
    _seed(postgres_connection_factory, bls_series, {1: None, 2: None})
    assert _latest(postgres_connection_factory, bls_series) == [(f"{YEAR}-02-01", None)]


def test_the_catalog_check_admits_ended(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: ARC-008 — the bootstrapped table accepts the harvest's new state."""
    rows = _rows_of(
        postgres_connection_factory,
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conrelid = 'gold_glossary.dim_metric_catalog'::regclass "
        "AND conname = 'dim_metric_catalog_freshness_state_check'",
        (),
    )
    assert len(rows) == 1 and "'ended'" in rows[0][0]
