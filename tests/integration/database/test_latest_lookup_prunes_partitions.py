"""The latest-value lookup stops at a partition instead of asking all of them.

Covers: DB-060 -- partitioning `gold_census.rpt_acs_observations` by vintage
        year made `refresh_mv_acs_latest` three times slower, because it asked
        for the newest row per key *across all history* and that is the one
        question time-partitioning is worst at: the answer can be in any
        partition, so nothing prunes.

        Measured on the internal stack before the fix: 126 buffer hits to find
        one key's latest row, against a single index scan when the relation was
        one heap; 1,294 seconds for a year's 4.4 million keys. After: 410
        seconds, and the chunk as a whole is faster than it was before the
        table was ever partitioned.

The duration is not what this asserts -- a timing test on shared hardware
measures the hardware. What it asserts is the plan shape that caused it.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

pytestmark = [pytest.mark.integration, pytest.mark.database]

RELATION = "gold_census.rpt_acs_observations"

#: Three partitions, so "ask every partition and rank the answers" is visibly
#: different from "stop at the newest one that has the key".
SEEDED_YEARS = (2003, 2004, 2006)


@pytest.fixture
def a_key_across_three_vintages(
    postgres_connection_factory: Callable[[], connection],
):
    """One key present in three year partitions, newest last to be written."""
    token = uuid4().hex[:8]
    geo_id = f"test:prune:{token}"
    variable_code = f"B99{token[:3].upper()}_001"
    metric_code = f"CENSUS_ACS:acs5:{variable_code}"

    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            for year in SEEDED_YEARS:
                cursor.execute(
                    f"""
                    INSERT INTO {RELATION} (
                        observation_date, as_of_date, updated_at, geo_id,
                        geo_level, value, dataset_code, vintage_year, table_id,
                        variable_code, estimate_value, metric_code
                    )
                    VALUES (MAKE_DATE(%s, 1, 1), MAKE_DATE(%s, 1, 1),
                            MAKE_DATE(%s, 1, 1)::TIMESTAMPTZ, %s, 'COUNTY',
                            %s, 'acs5', %s, 'B99999', %s, %s, %s)
                    """,
                    (
                        year,
                        year,
                        year,
                        geo_id,
                        year,
                        year,
                        variable_code,
                        year,
                        metric_code,
                    ),
                )
    finally:
        database.close()

    yield geo_id, variable_code, metric_code

    cleanup = postgres_connection_factory()
    cleanup.autocommit = True
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(f"DELETE FROM {RELATION} WHERE geo_id = %s", (geo_id,))
            cursor.execute(
                "DELETE FROM gold_census.mv_acs_latest WHERE geo_id = %s", (geo_id,)
            )
    finally:
        cleanup.close()


def _plan(cursor, sql: str, parameters: tuple) -> str:
    cursor.execute("EXPLAIN (COSTS OFF) " + sql, parameters)
    return "\n".join(row[0] for row in cursor.fetchall())


def _partitions_scanned(plan: str) -> set[str]:
    """Which partitions the plan actually reads.

    Counting the substring is not enough: a plan names each partition twice,
    once as the scanned relation and once inside the index name it scans it
    with, so a plan reading exactly one partition counts two.
    """
    return set(re.findall(r"\son (rpt_acs_observations_\w+)", plan))


def test_the_superseded_shape_is_the_one_that_asks_every_partition(
    postgres_connection_factory: Callable[[], connection],
    a_key_across_three_vintages: tuple[str, str, str],
) -> None:
    """Covers: DB-060 — the defect is demonstrated, not just described.

    This plans the query the procedure used to run. If PostgreSQL ever learns
    to prune it, this test fails and the fix below stops being necessary --
    which is a thing worth being told rather than carrying forever.
    """
    geo_id, variable_code, metric_code = a_key_across_three_vintages
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            plan = _plan(
                cursor,
                f"""
                SELECT d.* FROM {RELATION} d
                WHERE d.geo_id = %s AND d.variable_code = %s
                  AND d.metric_code = %s
                ORDER BY d.observation_date DESC, d.updated_at DESC,
                         d.vintage_year DESC
                LIMIT 1
                """,
                (geo_id, variable_code, metric_code),
            )
    finally:
        database.close()

    scanned = _partitions_scanned(plan)
    assert len(scanned) > len(SEEDED_YEARS), (
        f"the superseded shape planned only {len(scanned)} partition scans, "
        f"so it is no longer the defect this test documents:\n{plan}"
    )


def test_the_refresh_resolves_a_key_from_one_partition(
    postgres_connection_factory: Callable[[], connection],
    a_key_across_three_vintages: tuple[str, str, str],
) -> None:
    """Covers: DB-060 — the shape the procedure runs now reads one partition.

    The procedure walks partitions newest-first and stops once every key is
    resolved, so what it plans per step is a join against a *single* relation.
    Planning that step is what this checks: no `Merge Append`, and one
    partition named.
    """
    geo_id, variable_code, metric_code = a_key_across_three_vintages
    newest = max(SEEDED_YEARS)
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            plan = _plan(
                cursor,
                f"""
                SELECT DISTINCT ON (d.geo_id, d.variable_code, d.metric_code) d.*
                FROM {RELATION}_{newest} d
                WHERE d.geo_id = %s AND d.variable_code = %s
                  AND d.metric_code = %s
                ORDER BY d.geo_id, d.variable_code, d.metric_code,
                         d.observation_date DESC, d.updated_at DESC,
                         d.vintage_year DESC
                """,
                (geo_id, variable_code, metric_code),
            )
    finally:
        database.close()

    assert "Merge Append" not in plan, (
        f"the per-partition step still merges across partitions:\n{plan}"
    )
    scanned = _partitions_scanned(plan)
    assert scanned == {f"rpt_acs_observations_{newest}"}, (
        f"the per-partition step reads more than the one partition it was "
        f"given ({sorted(scanned)}):\n{plan}"
    )


def test_the_refresh_finds_the_newest_vintage_not_the_newest_partition(
    postgres_connection_factory: Callable[[], connection],
    a_key_across_three_vintages: tuple[str, str, str],
) -> None:
    """Covers: DB-060, ETL-037 — stopping early still finds the right row.

    The key exists in 2003, 2004 and 2006 but not in the 30 partitions newer
    than 2006, so a loop that stopped at the first partition it *looked* at
    rather than the first one that *held the key* would return nothing. This
    is the behaviour the speed-up could have broken.
    """
    geo_id, variable_code, metric_code = a_key_across_three_vintages
    database = postgres_connection_factory()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            cursor.execute(
                "CALL gold_census.refresh_mv_acs_latest(%s, %s)",
                (f"{min(SEEDED_YEARS)}-01-01", f"{min(SEEDED_YEARS)}-12-31"),
            )
            cursor.execute(
                """
                SELECT vintage_year, observation_date::TEXT
                FROM gold_census.mv_acs_latest
                WHERE geo_id = %s AND variable_code = %s AND metric_code = %s
                """,
                (geo_id, variable_code, metric_code),
            )
            rows = cursor.fetchall()
    finally:
        database.close()

    assert len(rows) == 1, (
        f"the key resolved to {len(rows)} latest rows rather than one: {rows}"
    )
    assert rows[0][0] == max(SEEDED_YEARS), (
        f"refreshing {min(SEEDED_YEARS)} left {rows[0][0]} as the latest "
        f"vintage; it should be {max(SEEDED_YEARS)}, which is the rule an "
        f"old-year refresh must not break"
    )
