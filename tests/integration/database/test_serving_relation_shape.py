"""The serving relations are what the API's reasons about them say they are.

Three places in the API justify a decision by naming what commits underneath
a read: the warehouse engine runs `REPEATABLE READ` (API-100) and the
distribution query evaluates its CTE once (API-084), both because a serving
refresh can commit between two statements. All three named
`REFRESH MATERIALIZED VIEW CONCURRENTLY`, and this warehouse has no
materialized view at all: `mv_*_latest` and `rpt_*_observations` are ordinary
tables, and `refresh_serving_layer_in_year_chunks` rebuilds them a calendar
year at a time, committing "each report/latest pair independently" so an
interrupted run can resume.

The decisions were right and the reason was wrong in the direction that
matters: a per-year commit leaves a wider window than an atomic swap, because
a reader without a snapshot can see some years rebuilt and others not. A
reason that names a mechanism the source does not contain is also a reason
nobody can check -- grep for `REFRESH MATERIALIZED VIEW`, find nothing, and
the isolation level looks like an optimisation to reclaim.

So the shape is asserted here instead: the relations the registry serves are
tables, each `mv_*_latest` has the refresh procedure the reserve declares,
and no materialized view is introduced without this file and those reasons
being revisited together.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from psycopg2.extensions import connection

from apps.api.registry import OBSERVATION_DISPATCH
from data_ingestion_toolbox.utility.serving_reserve import FULL_RESERVE_CONFIGS

pytestmark = [pytest.mark.integration, pytest.mark.database]

ROOT = Path(__file__).resolve().parents[3]

#: Relation kinds a served relation may be. `r` is an ordinary table -- what
#: every rebuilt serving relation is -- and `v` a view, which is what a source
#: whose latest read is a live projection serves (`gold_pep.mv_pep_latest`,
#: `gold_cdc.latest_release_observation`).
_SERVED_KINDS = {"r", "v"}


def _kind_of(cursor, relation: str) -> str | None:
    schema, name = relation.split(".", 1)
    cursor.execute(
        """
        SELECT c.relkind::text
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, name),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def test_no_serving_relation_is_a_materialized_view(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-041 — the reason the API states is the mechanism it has.

    Read across every relation the observation registry dispatches to, both
    scopes, so a source added later is covered without being named here.
    """
    relations = sorted(
        {
            relation
            for dispatch in OBSERVATION_DISPATCH.values()
            for relation in (dispatch.latest_relation, dispatch.released_relation)
        }
    )
    assert relations, "the registry dispatches to nothing; the rule read nothing"

    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            kinds = {relation: _kind_of(cursor, relation) for relation in relations}
            cursor.execute(
                """
                SELECT n.nspname || '.' || c.relname
                FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'm' AND n.nspname NOT LIKE 'pg%'
                ORDER BY 1
                """
            )
            materialized = [row[0] for row in cursor.fetchall()]
    finally:
        database_connection.close()

    missing = sorted(name for name, kind in kinds.items() if kind is None)
    assert not missing, (
        f"the registry serves relations the warehouse has not: {missing}"
    )
    unexpected = sorted(
        f"{name}={kind}" for name, kind in kinds.items() if kind not in _SERVED_KINDS
    )
    assert not unexpected, (
        "these served relations are neither a table nor a view, so the API's "
        f"stated reason for one snapshot per request describes something else: "
        f"{unexpected}"
    )
    assert materialized == [], (
        "this warehouse gained a materialized view, and three reasons in the "
        "API describe what commits underneath a read as a chunked per-year "
        "rebuild of a table (apps/api/database.py, "
        "apps/api/services/distribution_service.py, "
        f"tests/integration/api/test_request_snapshot.py). Revisit them: {materialized}"
    )


def test_every_reserved_latest_relation_has_the_procedure_that_rebuilds_it(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DB-041 — an `mv_`-named table is rebuilt by a declared procedure.

    The prefix is historical and the object is a table, so what makes the
    name honest is that something rebuilds it. The serving reserve declares
    both procedures per source, and its report table is the one the chunk
    driver rewrites a year at a time; this asserts the warehouse has each
    procedure and that the rewritten relation is a table, which is what makes
    a per-chunk commit the real mechanism rather than a guess about one.
    """
    assert FULL_RESERVE_CONFIGS, "no source declares a serving reserve"
    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            for reserve in FULL_RESERVE_CONFIGS.values():
                for procedure in (
                    reserve.report_procedure,
                    reserve.latest_procedure,
                ):
                    schema, name = procedure.split(".", 1)
                    cursor.execute(
                        """
                        SELECT count(*)
                        FROM pg_proc p
                        JOIN pg_namespace n ON n.oid = p.pronamespace
                        WHERE n.nspname = %s AND p.proname = %s
                          AND p.prokind = 'p'
                        """,
                        (schema, name),
                    )
                    assert cursor.fetchone()[0] >= 1, (
                        f"{reserve.log_label} declares {procedure} and the "
                        f"warehouse has no such procedure, so nothing rebuilds "
                        f"the relation the API serves"
                    )
                kind = _kind_of(cursor, reserve.report_table)
                assert kind == "r", (
                    f"{reserve.report_table} is a {kind}, and the reserve "
                    f"rebuilds it in year chunks with a commit per year, which "
                    f"only an ordinary table supports"
                )
    finally:
        database_connection.close()
