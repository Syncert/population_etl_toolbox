"""The ACS serving table is partitioned, and a year refresh truncates a partition.

Covers: DB-056 -- `gold_census.rpt_acs_observations` was a plain heap refreshed
        one year at a time by `DELETE ... WHERE observation_date BETWEEN`
        followed by a re-insert. `BETA_RESET_REINGESTION.md` section 7 recorded
        what that cost on the real relation: tens of millions of dead rows per
        chunk, a heap that grew as it was re-served, and an operator rule
        reading "vacuum manually; do not wait for autovacuum".

        Every ACS row's `observation_date` is `MAKE_DATE(estimate_year, 1, 1)`
        and the serving driver's chunk is exactly one calendar year, so the
        year chunk and the partition are the same thing.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

pytestmark = [pytest.mark.integration, pytest.mark.database]

RELATION = "gold_census.rpt_acs_observations"

#: A declared partition no ACS vintage occupies, so this module owns it outright
#: and a concurrent fixture's rows cannot be mistaken for -- or destroyed with
#: -- its own. ACS 1-year estimates begin in 2005.
SCRATCH_YEAR = 2001

ROWS = 400


def _insert_rows(cursor, year: int, token: str, count: int) -> None:
    """`count` rows in `year`, each its own natural key."""
    cursor.execute(
        f"""
        INSERT INTO {RELATION} (
            observation_date, as_of_date, updated_at, geo_id, geo_level,
            value, dataset_code, vintage_year, table_id, variable_code,
            estimate_value, metric_code
        )
        SELECT MAKE_DATE(%s, 1, 1), MAKE_DATE(%s, 1, 1),
               MAKE_DATE(%s, 1, 1)::TIMESTAMPTZ,
               'test:acs-part:' || %s || ':' || g,
               'COUNTY', g, 'acs5', %s, 'B99999',
               'B99999_' || %s || 'E', g,
               'CENSUS_ACS:acs5:B99999_' || %s || 'E'
        FROM generate_series(1, %s) AS g
        """,
        (year, year, year, token, year, token, token, count),
    )


def _dead_tuples(cursor, partition: str) -> int:
    """`n_dead_tup` for one partition, allowing the statistic time to arrive.

    Statistics are reported at transaction end rather than synchronously, so a
    read immediately after a commit can see the previous value. This polls
    rather than sleeping a fixed time, and returns what it last saw.
    """
    observed = -1
    for _ in range(40):
        cursor.execute(
            """
            SELECT COALESCE(n_dead_tup, 0)
            FROM pg_stat_user_tables
            WHERE schemaname = 'gold_census' AND relname = %s
            """,
            (partition,),
        )
        row = cursor.fetchone()
        observed = int(row[0]) if row else 0
        if observed > 0:
            return observed
        time.sleep(0.05)
    return observed


@pytest.fixture
def scratch_partition(
    postgres_connection_factory: Callable[[], connection],
) -> Callable[[], connection]:
    """Leave the scratch year as it was found, whatever the test did to it."""
    yield postgres_connection_factory
    cleanup = postgres_connection_factory()
    cleanup.autocommit = True
    try:
        with cleanup.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {RELATION} WHERE observation_date = MAKE_DATE(%s, 1, 1)",
                (SCRATCH_YEAR,),
            )
            cursor.execute(f"VACUUM (ANALYZE) {RELATION}_{SCRATCH_YEAR}")
    finally:
        cleanup.close()


def test_the_serving_table_is_range_partitioned_on_the_observation_date(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-056 — the partition key is the column the chunk selects on.

    Partitioned on anything else and the year chunk could not be a partition,
    which is the whole mechanism: the refresh truncates what the driver asked
    to rewrite.
    """
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.relkind, pg_get_partkeydef(c.oid)
                FROM pg_class c
                WHERE c.oid = %s::regclass
                """,
                (RELATION,),
            )
            relkind, partition_key = cursor.fetchone()
            assert relkind == "p", (
                f"{RELATION} is relkind {relkind!r}, not a partitioned table. "
                f"On an existing warehouse `CREATE TABLE IF NOT EXISTS` does "
                f"not convert a heap; the rebuild is BETA_RESET_REINGESTION.md "
                f"section 7"
            )
            assert partition_key == "RANGE (observation_date)"
    finally:
        database.close()


def test_every_year_the_table_holds_has_its_own_partition(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-056 — no served vintage falls into the default partition.

    The default partition exists so a row outside the declared range is
    countable rather than rejected -- the repository's fixtures use 2099 as a
    synthetic marker in a dozen files. A *real* vintage landing there would be
    silent and would break the refresh, because the year chunk truncates
    `..._YYYY` and never the default.
    """
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT DISTINCT EXTRACT(YEAR FROM observation_date)::INT
                FROM {RELATION}
                ORDER BY 1
                """
            )
            years = [row[0] for row in cursor.fetchall()]

            cursor.execute(
                """
                SELECT c.relname
                FROM pg_class c
                JOIN pg_inherits i ON i.inhrelid = c.oid
                WHERE i.inhparent = %s::regclass
                """,
                (RELATION,),
            )
            partitions = {row[0] for row in cursor.fetchall()}

            assert "rpt_acs_observations_unranged" in partitions, (
                "the default partition is gone, so a row outside the declared "
                "range now aborts the insert that carries it"
            )

            missing = [
                year
                for year in years
                if f"rpt_acs_observations_{year}" not in partitions
            ]
            # 2099 is the repository's synthetic-data marker and belongs in the
            # default; a real vintage does not.
            unexpected = [year for year in missing if year < 2090]
            assert not unexpected, (
                f"these served vintages have no partition of their own, so "
                f"they sit in the default and a year refresh cannot clear "
                f"them: {unexpected}"
            )
    finally:
        database.close()


def test_two_forced_chunks_leave_the_partition_with_no_dead_tuples(
    scratch_partition: Callable[[], connection],
) -> None:
    """Covers: DB-056 — the refresh truncates, and truncating reclaims at once.

    Two chunks rather than one, because the first could clear an empty
    partition and prove nothing: the second runs against rows the first
    inserted path left behind.

    The procedure's own report is read as well as the statistic. A partition
    with no dead tuples is also what an untouched partition looks like, so
    without `cleared_partitions=1` this would pass if the refresh had silently
    done nothing at all.
    """
    partition = f"rpt_acs_observations_{SCRATCH_YEAR}"
    token = uuid4().hex[:8]
    database = scratch_partition()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            for _ in range(2):
                _insert_rows(cursor, SCRATCH_YEAR, token, ROWS)
                del database.notices[:]
                cursor.execute(
                    "CALL gold_census.refresh_rpt_acs_observations(%s, %s)",
                    (f"{SCRATCH_YEAR}-01-01", f"{SCRATCH_YEAR}-12-31"),
                )

            reported = "".join(database.notices)
            assert "cleared_partitions=1" in reported, (
                "the refresh did not report clearing a partition, so it took "
                "the delete path and this measurement is of the wrong thing: "
                f"{reported}"
            )

            assert _dead_tuples(cursor, partition) == 0, (
                f"{partition} carries dead tuples after two chunks, so the "
                f"refresh left vacuum debt behind -- which is what "
                f"partitioning this relation exists to remove"
            )
    finally:
        database.close()


def test_the_dead_tuple_statistic_moves_when_rows_are_deleted(
    scratch_partition: Callable[[], connection],
) -> None:
    """Covers: DB-056 — the measurement above is not vacuously zero.

    `n_dead_tup` reads 0 on a partition nothing has touched, on a partition the
    autovacuum daemon has just cleaned, and in a database whose statistics
    collector is not reporting at all. Deleting from the same partition, the
    way the refresh used to, has to move it -- otherwise the test above
    measures nothing and would keep passing if the truncate were removed.
    """
    partition = f"rpt_acs_observations_{SCRATCH_YEAR}"
    token = uuid4().hex[:8]
    database = scratch_partition()
    database.autocommit = True
    try:
        with database.cursor() as cursor:
            _insert_rows(cursor, SCRATCH_YEAR, token, ROWS)
            cursor.execute(
                f"DELETE FROM {RELATION} WHERE observation_date = MAKE_DATE(%s, 1, 1)",
                (SCRATCH_YEAR,),
            )
            assert _dead_tuples(cursor, partition) > 0, (
                "deleting rows left no dead tuples this database will report, "
                "so `n_dead_tup` proves nothing here and the truncate test "
                "above is measuring an unreported statistic"
            )
    finally:
        database.close()
