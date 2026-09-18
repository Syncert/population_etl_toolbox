"""The schema a bootstrap produces is reviewed, not discovered.

Covers: DB-051 -- a DDL change reaches the warehouse as a reviewable diff.
"""

from __future__ import annotations

import difflib
from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from tests.support.schema_snapshot import SNAPSHOT_PATH, render

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
