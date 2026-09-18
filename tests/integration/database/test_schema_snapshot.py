"""The schema a bootstrap produces is reviewed, not discovered.

Covers: DB-051 -- a DDL change reaches the warehouse as a reviewable diff.
"""

from __future__ import annotations

import difflib
from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from tests.support.schema_snapshot import EXCLUDED_SCHEMAS, SNAPSHOT_PATH, render

pytestmark = [pytest.mark.integration, pytest.mark.database]


def test_the_bootstrapped_schema_matches_the_checked_in_snapshot(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-051 — an unmirrored DDL edit is a red job, not a surprise.

    The manifest is the upgrade path: re-running it against an existing
    database *is* the migration, so a DDL edit under `src/` and a migration
    that later overwrites it are the same kind of event and neither announced
    itself. `gold_glossary.dim_metric` was defined in three files and
    `dim_geography` in two, and nothing failed.

    Rendered from `pg_catalog` rather than from the DDL text, because the DDL
    is the input and the question is what the database ended up holding.
    """
    database = postgres_connection_factory()
    try:
        rendered = render(database)
    finally:
        database.close()

    assert SNAPSHOT_PATH.exists(), (
        "no schema snapshot is checked in; generate one with "
        "`python -m tests.support.schema_snapshot --write`"
    )
    expected = SNAPSHOT_PATH.read_text(encoding="utf-8")
    if rendered == expected:
        return

    diff = "\n".join(
        difflib.unified_diff(
            expected.splitlines(),
            rendered.splitlines(),
            fromfile="tests/sql/warehouse_schema_snapshot.txt",
            tofile="the bootstrapped warehouse",
            lineterm="",
            n=2,
        )
    )
    # Bounded, because a snapshot of five thousand lines can diverge in a
    # thousand of them and the first few are what says why.
    shown = diff.splitlines()[:60]
    raise AssertionError(
        "the bootstrapped schema and the checked-in snapshot differ. If the "
        "change is deliberate, regenerate with `python -m "
        "tests.support.schema_snapshot --write` and review the diff as part "
        "of the change:\n" + "\n".join(shown)
    )


def test_every_contract_view_has_exactly_one_body() -> None:
    """Covers: DB-051 — a view defined twice is a view that drifts.

    Two files could each define `gold_glossary.dim_metric`, and whichever ran
    last in manifest order won. That is not a hypothetical: it is why the
    snapshot above exists, and removing the duplicates is only safe *because*
    the snapshot proves the surviving definition is unchanged.

    `CREATE OR REPLACE` in a migration that drops columns first is not a
    duplicate body -- it is a migration re-creating what it just altered, and
    it must stay.
    """
    from tests.support.schema_snapshot import REPOSITORY_ROOT

    contract = REPOSITORY_ROOT / "sql/gold_contract"
    sources = sorted(contract.glob("*.sql"))
    assert sources, "no gold contract files were read; this guard proved nothing"

    for view in ("dim_metric", "dim_geography"):
        definers = [
            path.name
            for path in sources
            if f"CREATE OR REPLACE VIEW gold_glossary.{view}"
            in path.read_text(encoding="utf-8")
            or f"CREATE VIEW gold_glossary.{view}" in path.read_text(encoding="utf-8")
        ]
        assert len(definers) <= 1, (
            f"gold_glossary.{view} is defined in {definers}; whichever runs "
            "last in manifest order wins, and the other body is a copy that "
            "drifts"
        )


def test_every_partition_carries_its_parents_columns(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-051, DB-056 — the snapshot's omission rests on a checked fact.

    `schema_snapshot` renders a partitioned parent and skips its partitions,
    because a partition's columns and indexes are the parent's by construction
    and rendering 37 ACS year partitions in full added 1,933 lines that said
    the same thing 37 times. That is only safe while the construction holds, so
    it is checked here rather than assumed: every partition's column names,
    types, nullability and order must equal its parent's, and it must carry an
    index for each of the parent's.

    PostgreSQL enforces both today. A future `ALTER TABLE ... DETACH` leaving a
    diverged table attached, or an index created on one partition alone, would
    be invisible in the snapshot -- which is exactly the kind of silent schema
    change the snapshot exists to surface.
    """
    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            cursor.execute(
                """
                SELECT i.inhparent::regclass::TEXT, i.inhrelid::regclass::TEXT
                FROM pg_inherits AS i
                JOIN pg_class AS c ON c.oid = i.inhparent
                JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE c.relkind = 'p'
                  AND n.nspname <> ALL(%s)
                ORDER BY 1, 2
                """,
                (list(EXCLUDED_SCHEMAS),),
            )
            pairs = cursor.fetchall()
            assert pairs, (
                "the warehouse holds no partitioned relation, so this guard "
                "proved nothing -- and the snapshot's partition summary "
                "describes nothing either"
            )

            def columns(relation: str) -> list[tuple]:
                cursor.execute(
                    """
                    SELECT a.attnum, a.attname,
                           pg_catalog.format_type(a.atttypid, a.atttypmod),
                           a.attnotnull
                    FROM pg_attribute AS a
                    WHERE a.attrelid = %s::regclass
                      AND a.attnum > 0 AND NOT a.attisdropped
                    ORDER BY a.attnum
                    """,
                    (relation,),
                )
                return cursor.fetchall()

            def index_count(relation: str) -> int:
                cursor.execute(
                    "SELECT count(*) FROM pg_index WHERE indrelid = %s::regclass",
                    (relation,),
                )
                return int(cursor.fetchone()[0])

            for parent, partition in pairs:
                assert columns(partition) == columns(parent), (
                    f"{partition} does not carry {parent}'s columns, so the "
                    f"snapshot -- which renders only the parent -- describes a "
                    f"shape this partition does not have"
                )
                assert index_count(partition) == index_count(parent), (
                    f"{partition} carries {index_count(partition)} indexes "
                    f"where {parent} declares {index_count(parent)}, so an "
                    f"index exists that the snapshot cannot show"
                )
    finally:
        database.close()
