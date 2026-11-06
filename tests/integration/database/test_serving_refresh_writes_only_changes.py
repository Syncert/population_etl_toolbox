"""A serving refresh writes only the rows that changed.

The BLS and FRED report and latest procedures used to delete every served row
of a year and insert it again whenever one silver row of that year moved; the
BLS and FRED dimension upserts rewrote every series; the calendar rollups
deleted and re-inserted the whole table. Each is now staged and compared, so
a row whose content did not change keeps its physical version (`xmin`,
`ctid`), and an idle refresh writes nothing (refresh-churn audit, 2026-11-06).

Covers: DB-069
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls.gold_bls import transform as bls_transform
from data_ingestion_toolbox.fred.gold_fred import transform as fred_transform
from data_ingestion_toolbox.semantics.rollups import refresh_calendar_rollups
from data_ingestion_toolbox.semantics.time_aggregation import TimeMethod
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.capture_seed import seed_geography
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

YEAR = 2093
WINDOW = (f"{YEAR}-01-01", f"{YEAR}-12-31")


def _versions(factory: Callable[[], connection], sql: str, params: tuple) -> dict:
    """Each row's physical version, keyed by the row's own identity."""
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, params)
            return {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
    finally:
        reader.close()


def _run(factory: Callable[[], connection], *statements: tuple[str, tuple]) -> None:
    """Run statements in one transaction, the way the chunk driver does."""
    writer = factory()
    try:
        with writer.cursor() as cursor:
            for sql, params in statements:
                cursor.execute(sql, params)
        writer.commit()
    finally:
        writer.close()


@pytest.fixture
def bls_series(postgres_connection_factory: Callable[[], connection]) -> Iterator[str]:
    series_id = f"LAUCHURN{uuid4().hex[:8].upper()}03"
    yield series_id
    cleanup = postgres_connection_factory()
    try:
        with cleanup.cursor() as cursor:
            for table in (
                "gold_bls.mv_bls_latest",
                "gold_bls.rpt_bls_observations",
                "gold_bls.dim_bls_series",
                "silver_bls.fact_labor_statistics",
            ):
                cursor.execute(
                    f"DELETE FROM {table} WHERE series_id = %s", (series_id,)
                )
            cursor.execute(
                "DELETE FROM silver_ref.dim_time WHERE time_sk BETWEEN %s AND %s",
                (YEAR * 10000, YEAR * 10000 + 9999),
            )
        cleanup.commit()
    finally:
        cleanup.close()


def test_a_bls_refresh_rewrites_only_the_month_that_changed(
    postgres_connection_factory: Callable[[], connection],
    bls_series: str,
) -> None:
    """Covers: DB-069 — unchanged BLS report, latest and series rows keep their version."""
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            geo_sk = seed_geography(
                cursor,
                geo_type="state",
                state_fips="88",
                vintage=YEAR,
                name="Churn State",
            )
            for month in (1, 2, 3):
                time_sk = YEAR * 10000 + month * 100 + 1
                _seed_time(cursor, time_sk, f"{YEAR}-{month:02d}-01")
                cursor.execute(
                    """
                    INSERT INTO silver_bls.fact_labor_statistics (
                        time_sk, geo_sk, duration_start, duration_end,
                        period_date, series_id, program, geo_level, geo_id,
                        state_fips, value, value_status, year, period,
                        measure_code, load_batch_id, ingested_at
                    ) VALUES (
                        %(t)s, %(g)s, MAKE_DATE(%(y)s, %(m)s, 1),
                        (MAKE_DATE(%(y)s, %(m)s, 1) + INTERVAL '1 month - 1 day')::DATE,
                        MAKE_DATE(%(y)s, %(m)s, 1), %(s)s, 'la', 'state',
                        'state:88', '88', %(v)s, 'valid', %(y)s,
                        'M' || LPAD(%(m)s::TEXT, 2, '0'), '03',
                        gen_random_uuid(), NOW()
                    )
                    """,
                    {
                        "t": time_sk,
                        "g": geo_sk,
                        "y": YEAR,
                        "m": month,
                        "s": bls_series,
                        "v": month,
                    },
                )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    bls_transform.refresh_bls_elements(hook)
    refresh = (
        ("CALL gold_bls.refresh_rpt_bls_observations(%s, %s)", WINDOW),
        ("CALL gold_bls.refresh_mv_bls_latest(%s, %s)", WINDOW),
    )
    _run(postgres_connection_factory, *refresh)

    report_sql = (
        "SELECT observation_date::TEXT, xmin::TEXT, ctid::TEXT "
        "FROM gold_bls.rpt_bls_observations WHERE series_id = %s"
    )
    latest_sql = (
        "SELECT series_id, xmin::TEXT, ctid::TEXT "
        "FROM gold_bls.mv_bls_latest WHERE series_id = %s"
    )
    series_sql = (
        "SELECT series_id, xmin::TEXT, ctid::TEXT "
        "FROM gold_bls.dim_bls_series WHERE series_id = %s"
    )
    report = _versions(postgres_connection_factory, report_sql, (bls_series,))
    latest = _versions(postgres_connection_factory, latest_sql, (bls_series,))
    series = _versions(postgres_connection_factory, series_sql, (bls_series,))
    assert sorted(report) == [f"{YEAR}-01-01", f"{YEAR}-02-01", f"{YEAR}-03-01"]
    assert len(latest) == 1 and len(series) == 1

    # Nothing moved: the same refresh writes nothing.
    bls_transform.refresh_bls_elements(hook)
    _run(postgres_connection_factory, *refresh)
    assert _versions(postgres_connection_factory, report_sql, (bls_series,)) == report
    assert _versions(postgres_connection_factory, latest_sql, (bls_series,)) == latest
    assert _versions(postgres_connection_factory, series_sql, (bls_series,)) == series

    # February is revised: only February is rewritten, and the latest row
    # (March) is left alone because it did not change.
    _run(
        postgres_connection_factory,
        (
            "UPDATE silver_bls.fact_labor_statistics SET value = 22, "
            "ingested_at = clock_timestamp() WHERE series_id = %s AND period = 'M02'",
            (bls_series,),
        ),
        *refresh,
    )
    after = _versions(postgres_connection_factory, report_sql, (bls_series,))
    assert after[f"{YEAR}-01-01"] == report[f"{YEAR}-01-01"]
    assert after[f"{YEAR}-03-01"] == report[f"{YEAR}-03-01"]
    assert after[f"{YEAR}-02-01"] != report[f"{YEAR}-02-01"]
    assert _versions(postgres_connection_factory, latest_sql, (bls_series,)) == latest

    # A revision to the newest month moves the latest row, served once.
    _run(
        postgres_connection_factory,
        (
            "UPDATE silver_bls.fact_labor_statistics SET value = 33, "
            "ingested_at = clock_timestamp() WHERE series_id = %s AND period = 'M03'",
            (bls_series,),
        ),
        *refresh,
    )
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT observation_date::TEXT, value FROM gold_bls.rpt_bls_observations "
                "WHERE series_id = %s ORDER BY observation_date",
                (bls_series,),
            )
            assert [(d, int(v)) for d, v in cursor.fetchall()] == [
                (f"{YEAR}-01-01", 1),
                (f"{YEAR}-02-01", 22),
                (f"{YEAR}-03-01", 33),
            ]
            cursor.execute(
                "SELECT observation_date::TEXT, value FROM gold_bls.mv_bls_latest "
                "WHERE series_id = %s",
                (bls_series,),
            )
            assert [(d, int(v)) for d, v in cursor.fetchall()] == [
                (f"{YEAR}-03-01", 33)
            ]
    finally:
        reader.close()

    # A month silver no longer holds leaves the served table.
    _run(
        postgres_connection_factory,
        (
            "DELETE FROM silver_bls.fact_labor_statistics "
            "WHERE series_id = %s AND period = 'M01'",
            (bls_series,),
        ),
        *refresh,
    )
    assert sorted(
        _versions(postgres_connection_factory, report_sql, (bls_series,))
    ) == [
        f"{YEAR}-02-01",
        f"{YEAR}-03-01",
    ]


@pytest.fixture
def fred_series(postgres_connection_factory: Callable[[], connection]) -> Iterator[str]:
    series_id = f"TEST_FRED_CHURN_{uuid4().hex[:8].upper()}"
    yield series_id
    cleanup = postgres_connection_factory()
    try:
        with cleanup.cursor() as cursor:
            for table in (
                "gold_fred.mv_fred_latest",
                "gold_fred.rpt_fred_observations",
                "gold_fred.dim_fred_series",
                "silver_fred.fact_economic_indicators",
            ):
                cursor.execute(
                    f"DELETE FROM {table} WHERE series_id = %s", (series_id,)
                )
            cursor.execute(
                "DELETE FROM silver_ref.dim_time WHERE time_sk BETWEEN %s AND %s",
                (YEAR * 10000, YEAR * 10000 + 9999),
            )
        cleanup.commit()
    finally:
        cleanup.close()


def test_a_fred_refresh_rewrites_only_the_month_that_changed(
    postgres_connection_factory: Callable[[], connection],
    fred_series: str,
) -> None:
    """Covers: DB-069 — unchanged FRED report, latest and series rows keep their version."""
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            for month in (1, 2):
                time_sk = YEAR * 10000 + month * 100 + 1
                _seed_time(cursor, time_sk, f"{YEAR}-{month:02d}-01")
                cursor.execute(
                    """
                    INSERT INTO silver_fred.fact_economic_indicators (
                        time_sk, duration_start, duration_end, observation_date,
                        series_id, domain, value, is_missing, series_title,
                        unit_of_measure, frequency, seasonal_adjustment,
                        source_system, load_batch_id, ingested_at
                    ) VALUES (
                        %s, MAKE_DATE(%s, %s, 1),
                        (MAKE_DATE(%s, %s, 1) + INTERVAL '1 month - 1 day')::DATE,
                        MAKE_DATE(%s, %s, 1), %s, 'fixture', %s, FALSE,
                        'Churn fixture', 'Index', 'Monthly', 'Not Adjusted',
                        'FRED', %s, NOW()
                    )
                    """,
                    (
                        time_sk,
                        YEAR,
                        month,
                        YEAR,
                        month,
                        YEAR,
                        month,
                        fred_series,
                        month * 10,
                        str(uuid4()),
                    ),
                )
        writer.commit()
    finally:
        writer.close()

    hook = PostgresHookStub(postgres_connection_factory)
    fred_transform.refresh_fred_elements(hook)
    refresh = (
        ("CALL gold_fred.refresh_rpt_fred_observations(%s, %s)", WINDOW),
        ("CALL gold_fred.refresh_mv_fred_latest(%s, %s)", WINDOW),
    )
    _run(postgres_connection_factory, *refresh)

    report_sql = (
        "SELECT observation_date::TEXT, xmin::TEXT, ctid::TEXT "
        "FROM gold_fred.rpt_fred_observations WHERE series_id = %s"
    )
    latest_sql = (
        "SELECT series_id, xmin::TEXT, ctid::TEXT "
        "FROM gold_fred.mv_fred_latest WHERE series_id = %s"
    )
    series_sql = (
        "SELECT series_id, xmin::TEXT, ctid::TEXT "
        "FROM gold_fred.dim_fred_series WHERE series_id = %s"
    )
    report = _versions(postgres_connection_factory, report_sql, (fred_series,))
    latest = _versions(postgres_connection_factory, latest_sql, (fred_series,))
    series = _versions(postgres_connection_factory, series_sql, (fred_series,))
    assert len(report) == 2 and len(latest) == 1 and len(series) == 1

    fred_transform.refresh_fred_elements(hook)
    _run(postgres_connection_factory, *refresh)
    assert _versions(postgres_connection_factory, report_sql, (fred_series,)) == report
    assert _versions(postgres_connection_factory, latest_sql, (fred_series,)) == latest
    assert _versions(postgres_connection_factory, series_sql, (fred_series,)) == series

    _run(
        postgres_connection_factory,
        (
            "UPDATE silver_fred.fact_economic_indicators SET value = 25, "
            "ingested_at = clock_timestamp() "
            "WHERE series_id = %s AND observation_date = MAKE_DATE(%s, 2, 1)",
            (fred_series, YEAR),
        ),
        *refresh,
    )
    after = _versions(postgres_connection_factory, report_sql, (fred_series,))
    assert after[f"{YEAR}-01-01"] == report[f"{YEAR}-01-01"]
    assert after[f"{YEAR}-02-01"] != report[f"{YEAR}-02-01"]
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT value FROM gold_fred.mv_fred_latest WHERE series_id = %s",
                (fred_series,),
            )
            assert cursor.fetchall() == [(25,)]
    finally:
        reader.close()


ROLLUP_METRIC = "BLS:CHURNROLLUP0001"


@pytest.fixture
def rollup_rows(
    postgres_connection_factory: Callable[[], connection],
) -> Iterator[None]:
    yield
    cleanup = postgres_connection_factory()
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(
                "DELETE FROM gold_bls.derived_calendar_rollup WHERE metric_code = %s",
                (ROLLUP_METRIC,),
            )
            cursor.execute(
                "DELETE FROM gold_bls.rpt_bls_observations WHERE metric_code = %s",
                (ROLLUP_METRIC,),
            )
        cleanup.commit()
    finally:
        cleanup.close()


def test_an_idle_calendar_rollup_refresh_writes_nothing(
    postgres_connection_factory: Callable[[], connection],
    rollup_rows: None,
) -> None:
    """Covers: DB-069 — an unchanged window keeps its row; a changed month rewrites only its windows."""
    from tests.integration.database.test_calendar_rollups import _served_month

    registry = {ROLLUP_METRIC: TimeMethod(ROLLUP_METRIC, "mean", "approved", "Nick", 1)}
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            for month in range(1, 13):
                _served_month(cursor, YEAR, month, str(month))
            cursor.execute(
                "UPDATE gold_bls.rpt_bls_observations SET metric_code = %s, "
                "series_id = 'CHURNROLLUP0001' "
                "WHERE metric_code = 'BLS:ROLLUPTEST0001' "
                "AND observation_date BETWEEN %s AND %s",
                (ROLLUP_METRIC, *WINDOW),
            )
        writer.commit()
    finally:
        writer.close()

    version_sql = (
        "SELECT grain || ':' || window_start::TEXT, xmin::TEXT, ctid::TEXT "
        "FROM gold_bls.derived_calendar_rollup WHERE metric_code = %s"
    )

    def refresh() -> int:
        connection_ = postgres_connection_factory()
        try:
            return refresh_calendar_rollups(connection_, "BLS", registry)
        finally:
            connection_.close()

    held = refresh()
    first = _versions(postgres_connection_factory, version_sql, (ROLLUP_METRIC,))
    assert held == 5 and len(first) == 5  # four quarters and the year

    assert refresh() == held
    assert (
        _versions(postgres_connection_factory, version_sql, (ROLLUP_METRIC,)) == first
    )

    # A revised December changes the fourth quarter and the year only.
    _run(
        postgres_connection_factory,
        (
            "UPDATE gold_bls.rpt_bls_observations SET value = 120 "
            "WHERE metric_code = %s AND duration_start = MAKE_DATE(%s, 12, 1)",
            (ROLLUP_METRIC, YEAR),
        ),
    )
    assert refresh() == held
    after = _versions(postgres_connection_factory, version_sql, (ROLLUP_METRIC,))
    changed = {key for key in first if after[key] != first[key]}
    assert changed == {f"quarter:{YEAR}-10-01", f"year:{YEAR}-01-01"}


def test_a_lean_group_rebuild_rewrites_only_rows_whose_figures_changed(
    postgres_connection: connection,
) -> None:
    """Covers: DB-069 — an unchanged lean row keeps its place and refresh id; a changed input rewrites only its rows."""
    from tests.integration.database.test_elections_lean_groups import (
        ALPHA,
        BETA,
        _seed_inputs,
    )

    place_sql = (
        "SELECT measure || ':' || geo_id, ctid::TEXT, refresh_id "
        "FROM gold_elections.lean_place_change"
    )
    group_sql = (
        "SELECT measure || ':' || peer_type || ':' || peer_group || ':' || lean_band, "
        "ctid::TEXT, refresh_id FROM gold_elections.lean_group_change"
    )

    def rows(db, sql: str) -> dict:
        db.execute(sql)
        return {key: (ctid, refresh_id) for key, ctid, refresh_id in db.fetchall()}

    with postgres_connection.cursor() as db:
        _seed_inputs(db)
        db.execute("SELECT gold_elections.build_lean_groups(1)")
        places, groups = rows(db, place_sql), rows(db, group_sql)
        assert places and groups

        # Same inputs, a later rebuild: nothing is rewritten, and every row
        # still names the rebuild that wrote it.
        db.execute("SELECT gold_elections.build_lean_groups(2)")
        assert db.fetchone()[0] == len(places)
        assert rows(db, place_sql) == places
        assert rows(db, group_sql) == groups

        # Beta's 2020 poverty moves: Beta's poverty row and the poverty groups
        # it belongs to are rewritten under the new rebuild, nothing else.
        db.execute(
            "UPDATE lean_input SET value = 13.0 "
            "WHERE measure = 'poverty_rate' AND geo_id = %s AND year = 2020",
            (BETA,),
        )
        db.execute("SELECT gold_elections.build_lean_groups(3)")
        after = rows(db, place_sql)
        changed = {key for key in places if after.get(key) != places[key]}
        assert changed == {f"poverty_rate:{BETA}"}
        assert after[f"poverty_rate:{BETA}"][1] == 3
        assert after[f"poverty_rate:{ALPHA}"] == places[f"poverty_rate:{ALPHA}"]
        group_after = rows(db, group_sql)
        changed_groups = {key for key in groups if group_after.get(key) != groups[key]}
        assert changed_groups and all(
            key.startswith("poverty_rate:") for key in changed_groups
        )


def test_an_idle_place_search_refresh_writes_nothing(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-069 — the nightly place-search refresh leaves unchanged rows in place."""
    version_sql = "SELECT geo_id, xmin::TEXT, ctid::TEXT FROM gold_glossary.geo_search"
    _run(
        postgres_connection_factory,
        ("CALL gold_glossary.refresh_dim_geo_latest()", ()),
        ("CALL gold_glossary.refresh_geo_search()", ()),
    )
    first = _versions(postgres_connection_factory, version_sql, ())
    if not first:
        pytest.skip("the test warehouse holds no searchable geography")
    _run(postgres_connection_factory, ("CALL gold_glossary.refresh_geo_search()", ()))
    assert _versions(postgres_connection_factory, version_sql, ()) == first
