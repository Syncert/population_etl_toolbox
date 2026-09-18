"""Re-running the manifest against a warehouse holding data changes nothing.

Covers: DB-052 -- the documented upgrade path, exercised against rows.

`BETA_RESET_REINGESTION.md` says re-running a file against an already-deployed
database *is* the migration, and several steps exist only for that path: `024`
and `026` each run six `UPDATE`s over the serving tables, `025` rewrites
resolution columns, and `019` and `015` drop and re-add constraints, which
validates every existing row.

None of that was exercised against data. The fixtures apply the manifest to an
empty database and `test_warehouse_manifest_is_idempotent` re-runs it on an
empty one. "Idempotent when there is nothing to be idempotent about" is not
the claim the upgrade path needs.

This seeds rows through the real transforms, digests what is served, reapplies
the whole manifest through the applier a deployment runs, and asserts the
served rows are identical afterwards.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.utility.warehouse_manifest import apply_manifest
from tests.support.capture_seed import delete_geography, seed_capture, seed_geography
from tests.integration.database.test_fred_silver_flow import _seed_time
from tests.support.postgres import PostgresHookStub

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: The relations a reader is served from. A reapply may rebuild anything it
#: likes underneath; what it may not do is change one of these.
SERVED_RELATIONS = (
    "gold_fred.rpt_fred_observations",
    "gold_fred.mv_fred_latest",
    "gold_bls.rpt_bls_observations",
    "gold_census.rpt_acs_observations",
    "silver_fred.fact_economic_indicators",
    "silver_bls.fact_labor_statistics",
    "silver_ref.dim_geo_entity",
    "silver_ref.geography_resolution",
    "control.data_quality_result",
)


def _digest(cursor, relation: str) -> tuple[int, str]:
    """Row count and a content digest that does not depend on row order."""
    cursor.execute(f"SELECT to_regclass('{relation}')")
    if cursor.fetchone()[0] is None:
        return (-1, "absent")
    # `t::text` renders the whole row; summing per-row hashes is
    # order-independent, so a digest change means content changed rather than
    # a plan changed.
    cursor.execute(
        f"SELECT count(*), COALESCE(md5(string_agg(row_hash, '' ORDER BY row_hash)), '') "
        f"FROM (SELECT md5(t::text) AS row_hash FROM {relation} AS t) AS hashed"
    )
    count, digest = cursor.fetchone()
    return (int(count), str(digest))


def _snapshot(cursor) -> dict[str, tuple[int, str]]:
    return {relation: _digest(cursor, relation) for relation in SERVED_RELATIONS}


@pytest.fixture
def a_populated_warehouse(
    postgres_connection_factory: Callable[[], connection],
) -> Callable[[], None]:
    """Rows in the layers a reapply touches, through the real transforms."""
    series_id = "REAPPLY_PROBE"
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            seed_geography(
                cursor, geo_type="state", state_fips="91", vintage=2091, name="Reapply"
            )
            _seed_time(cursor, 20910101, "2091-01-01")
            capture_id = seed_capture(cursor, "FRED")
            cursor.execute(
                """
                INSERT INTO raw_fred.fred_series (
                    series_id, title, units, frequency, seasonal_adjustment
                ) VALUES (%s, 'Reapply probe', 'Index', 'Monthly', 'Not Adjusted')
                ON CONFLICT (series_id) DO NOTHING
                """,
                (series_id,),
            )
            cursor.execute(
                """INSERT INTO silver_fred.observation_revision (
                    capture_id, observation_index, domain, series_id,
                    observation_date_source, value_source, realtime_start_source,
                    realtime_end_source, observation_date, value, value_status,
                    realtime_start, realtime_end
                ) VALUES (%s, 0, 'reapply_probe', %s, '2091-01-01', '1.5',
                          '2091-01-01', '2091-01-01', '2091-01-01', 1.5,
                          'valid', '2091-01-01', '2091-01-01')""",
                (capture_id, series_id),
            )
        writer.commit()
    finally:
        writer.close()

    def cleanup() -> None:
        connection_ = postgres_connection_factory()
        try:
            with connection_.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM silver_fred.observation_revision WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM silver_fred.fact_economic_indicators "
                    "WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM raw_fred.fred_series WHERE series_id = %s",
                    (series_id,),
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution "
                    "WHERE source_code = 'state:91'"
                )
                delete_geography(cursor, "state:91")
                cursor.execute(
                    "DELETE FROM silver_ref.dim_time WHERE time_sk = 20910101"
                )
            connection_.commit()
        finally:
            connection_.close()

    try:
        yield cleanup
    finally:
        cleanup()


def test_reapplying_the_manifest_leaves_every_served_row_unchanged(
    postgres_connection_factory: Callable[[], connection],
    a_populated_warehouse: Callable[[], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: DB-052 — the upgrade path is safe on a warehouse with rows in it.

    Reapplied through `apply_manifest`, which is what
    `scripts/apply_warehouse_manifest.py` and the reset procedure run, rather
    than through a bare loop over the files: the thing under test is the
    documented upgrade path, and one asset per transaction is part of it.
    """
    from data_ingestion_toolbox.fred.silver_fred import transform as fred_transform

    monkeypatch.setattr(
        fred_transform,
        "_get_hook",
        lambda: PostgresHookStub(postgres_connection_factory),
    )
    fred_transform.transform_fred_to_silver(domain="reapply_probe")

    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            before = _snapshot(cursor)
        assert any(count > 0 for count, _ in before.values()), (
            "no served relation holds a row, so a reapply that changed "
            "everything would still pass"
        )

        applied = apply_manifest(database)
        assert len(applied) > 30, applied

        with database.cursor() as cursor:
            after = _snapshot(cursor)
    finally:
        database.close()

    changed = {
        relation: (before[relation], after[relation])
        for relation in SERVED_RELATIONS
        if before[relation] != after[relation]
    }
    assert not changed, (
        "reapplying the manifest changed what these relations hold, so the "
        "documented upgrade path rewrites served data: "
        + "; ".join(
            f"{relation}: {was} -> {now}" for relation, (was, now) in changed.items()
        )
    )


def test_a_reapply_that_rewrites_a_served_value_is_caught(
    postgres_connection_factory: Callable[[], connection],
    a_populated_warehouse: Callable[[], None],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: DB-052 — the digest notices a changed value, not just a count.

    The test above is only worth running if it can fail, and the failure it
    must catch is the quiet one: a migration that rewrites a value in place
    leaves the row count identical. So this rewrites one deliberately and
    asserts the digest moves, which is the property the assertion rests on.
    """
    from data_ingestion_toolbox.fred.silver_fred import transform as fred_transform

    monkeypatch.setattr(
        fred_transform,
        "_get_hook",
        lambda: PostgresHookStub(postgres_connection_factory),
    )
    fred_transform.transform_fred_to_silver(domain="reapply_probe")

    database = postgres_connection_factory()
    try:
        with database.cursor() as cursor:
            before = _digest(cursor, "silver_fred.fact_economic_indicators")
            assert before[0] > 0, "nothing was transformed; the probe proves nothing"
            cursor.execute(
                "UPDATE silver_fred.fact_economic_indicators "
                "SET value = value + 1 WHERE series_id = 'REAPPLY_PROBE'"
            )
            after = _digest(cursor, "silver_fred.fact_economic_indicators")
        database.rollback()
    finally:
        database.close()

    assert after[0] == before[0], "the probe changed the row count, not a value"
    assert after[1] != before[1], (
        "a rewritten value left the digest unchanged, so the reapply test "
        "above would not notice a migration that rewrote served data"
    )
