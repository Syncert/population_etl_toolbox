"""Real PostgreSQL IRS SOI county migration capture-to-gold contract.

Covers: ETL-059
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import psycopg2
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.irs_migration.schema import (
    REQUIRED_RELATIONS,
    ensure_irs_migration_schema,
)
from data_ingestion_toolbox.quality.sources import (
    irs_migration_file_reconciliation,
    irs_migration_total_reconciliation,
)
from tests.support import irs_migration as irs

pytestmark = [pytest.mark.integration, pytest.mark.database]


@pytest.fixture
def irs_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return irs.reviewed_warehouse(postgres_connection_factory, request)


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple | None = None
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_flows_reach_gold_with_both_ends_resolved_and_categories_labelled(
    irs_warehouse,
) -> None:
    """Covers: ETL-059 — a flow names two resolved counties; a category names none; totals are their own rows."""
    factory = irs_warehouse
    _run, flows, published = irs.run_to_gold(factory, "inflow", "2022-2023")
    assert flows > 0 and published == 1
    philadelphia = _rows(
        factory,
        """
        SELECT origin_geo_id, destination_geo_id, returns, individuals, agi::TEXT, year_pair,
               period_start::TEXT, period_end::TEXT
        FROM gold_irs_migration.flow_latest
        WHERE direction = 'inflow' AND subject_geo_id = %s AND counterpart_code = '42:101'
        """,
        (irs.KENT,),
    )
    assert philadelphia == [
        (
            irs.PHILADELPHIA,
            irs.KENT,
            273,
            588,
            "17321",
            "2022-2023",
            "2022-01-01",
            "2023-12-31",
        )
    ]
    categories = dict(
        _rows(
            factory,
            """
            SELECT category, origin_geo_id FROM gold_irs_migration.flow_latest
            WHERE direction = 'inflow' AND subject_geo_id = %s AND category LIKE 'other_flows%%'
            """,
            (irs.KENT,),
        )
    )
    assert set(categories) >= {"other_flows_same_state", "other_flows_different_state"}
    assert set(categories.values()) == {None}
    # Every county the reference does not hold was refused by side, not loaded.
    refused = _rows(
        factory,
        "SELECT DISTINCT error_code FROM silver_irs_migration.flow_quarantine",
    )
    assert refused == [("origin_unresolved",)]
    assert _rows(
        factory,
        """
        SELECT COUNT(*) FROM gold_irs_migration.flow_latest
        WHERE category = 'county' AND (origin_geo_sk IS NULL OR destination_geo_sk IS NULL)
        """,
    ) == [(0,)]
    totals = _rows(
        factory,
        """
        SELECT metric_key, value::TEXT, unit, geo_type FROM gold_irs_migration.total_observation_latest
        WHERE geo_id = %s AND metric_key LIKE 'inflow:total_us_and_foreign:%%' ORDER BY metric_key
        """,
        (irs.KENT,),
    )
    assert totals == [
        ("inflow:total_us_and_foreign:agi", "303199", "thousands of dollars", "county"),
        ("inflow:total_us_and_foreign:individuals", "9709", "individuals", "county"),
        ("inflow:total_us_and_foreign:returns", "5357", "returns", "county"),
    ]


def test_a_withheld_category_has_no_value_and_the_publisher_harvests_totals(
    irs_warehouse,
) -> None:
    """Covers: ETL-059 — SOI's -1 is withheld; the publisher names the file totals only."""
    factory = irs_warehouse
    irs.run_to_gold(factory, "inflow", "2021-2022")
    withheld = _rows(
        factory,
        """
        SELECT subject_geo_id, returns, individuals, agi, value_source FROM gold_irs_migration.flow_latest
        WHERE value_status = 'withheld' ORDER BY subject_geo_id
        """,
    )
    assert [row[0] for row in withheld] == [irs.KENT, irs.NEW_CASTLE, irs.SUSSEX]
    assert all(
        row[1:4] == (None, None, None) and row[4] == "-1,-1,-1" for row in withheld
    )
    published = dict(
        _rows(
            factory,
            "SELECT source_object_key, valid_geo_grains FROM gold_irs_migration.metric_publisher",
        )
    )
    assert "inflow:total_us:returns" in published and published[
        "inflow:total_us:returns"
    ] == ["COUNTY"]
    assert not any(":county:" in key or "other_flows" in key for key in published)
    assert harvest_publisher(factory, Publisher("gold_irs_migration")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'IRS_MIGRATION'",
    ) == [(len(published),)]


def test_a_rerun_adds_nothing_and_a_revised_file_is_kept_beside_the_old(
    irs_warehouse,
) -> None:
    """Covers: ETL-059 — the same bytes change nothing; a changed file keeps both checksums."""
    factory = irs_warehouse
    first, _f, _p = irs.run_to_gold(factory, "outflow", "2022-2023")
    second, _f, _p = irs.run_to_gold(factory, "outflow", "2022-2023")
    count = _rows(factory, "SELECT COUNT(*) FROM gold_irs_migration.flow_latest")
    revised = irs.fixture_bytes("countyoutflow2223.csv").replace(
        b"10,001,42,101,PA,Philadelphia County,91,145,4788",
        b"10,001,42,101,PA,Philadelphia County,92,146,4790",
    )
    third, _f, _p = irs.run_to_gold(
        factory,
        "outflow",
        "2022-2023",
        client=irs.FixtureClient(
            {"countyoutflow2223.csv": httpx.Response(200, content=revised)}
        ),
    )
    history = _rows(
        factory,
        """
        SELECT revision.returns, capture.payload_checksum, revision.run_id::TEXT
        FROM gold_irs_migration.flow_revision AS revision
        JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
        WHERE revision.subject_geo_id = %s AND revision.counterpart_code = '42:101'
        ORDER BY revision.retrieved_at
        """,
        (irs.KENT,),
    )
    assert [row[2] for row in history] == [str(first), str(second), str(third)]
    assert [row[0] for row in history] == [91, 91, 92]
    assert history[0][1] == history[1][1] != history[2][1]
    assert _rows(
        factory,
        "SELECT returns FROM gold_irs_migration.flow_latest WHERE subject_geo_id = %s AND counterpart_code = '42:101'",
        (irs.KENT,),
    ) == [(92,)]
    assert (
        _rows(factory, "SELECT COUNT(*) FROM gold_irs_migration.flow_latest") == count
    )


def test_quality_rules_pass_the_fixtures_and_catch_a_loss(irs_warehouse) -> None:
    """Covers: ETL-059 — DQ-IRS-002 and DQ-IRS-004 pass, then fail on a lost row; a short row is quarantined."""
    factory = irs_warehouse

    def outcome(executor):
        reader = factory()
        try:
            with reader.cursor() as cursor:
                return executor(cursor, {})[0]
        finally:
            reader.close()

    assert outcome(irs_migration_file_reconciliation).result == "not_applicable"
    malformed = irs.fixture_bytes("countyinflow2223.csv").replace(
        b"10,001,42,101,PA,Philadelphia County,273,588,17321",
        b"10,001,42,101,PA,Philadelphia County,273",
    )
    run_id, _flows, published = irs.run_to_gold(
        factory,
        "inflow",
        "2022-2023",
        client=irs.FixtureClient(
            {"countyinflow2223.csv": httpx.Response(200, content=malformed)}
        ),
    )
    assert published == 1
    assert _rows(
        factory,
        "SELECT source_row_index, error_code FROM silver_irs_migration.flow_quarantine WHERE run_id = %s AND error_code = 'ragged_row'",
        (str(run_id),),
    ) == [(7, "ragged_row")]
    irs.run_to_gold(factory, "inflow", "2022-2023")
    irs.run_to_gold(factory, "outflow", "2022-2023")
    passed = outcome(irs_migration_file_reconciliation)
    assert (passed.result, passed.observed_count) == ("pass", 0)
    totals = outcome(irs_migration_total_reconciliation)
    assert (totals.result, totals.observed_count) == ("pass", 0)
    writer = factory()
    try:
        with writer.cursor() as cursor:
            for table in (
                "silver_irs_migration.fact_flow",
                "silver_irs_migration.flow_revision",
            ):
                cursor.execute(
                    f"""
                    DELETE FROM {table}
                    WHERE direction = 'outflow' AND subject_geo_id = %s AND counterpart_code = '42:101'
                    """,
                    (irs.KENT,),
                )
        writer.commit()
    finally:
        writer.close()
    failed = outcome(irs_migration_file_reconciliation)
    assert (failed.result, failed.observed_count) == ("fail", 1)
    short = outcome(irs_migration_total_reconciliation)
    assert (short.result, short.observed_count) == ("warn", 1)


def test_the_fact_refuses_a_flow_with_a_missing_end_and_the_schema_reapplies(
    irs_warehouse,
) -> None:
    """Covers: ETL-059 — ADR-0008's CHECK refuses a county flow without both keys."""
    factory = irs_warehouse
    run_id, _f, _p = irs.run_to_gold(factory, "inflow", "2022-2023")
    writer = factory()
    try:
        with writer.cursor() as cursor, pytest.raises(psycopg2.errors.CheckViolation):
            cursor.execute(
                """
                UPDATE silver_irs_migration.fact_flow SET origin_geo_sk = NULL
                WHERE category = 'county' AND run_id = %s
                """,
                (str(run_id),),
            )
        writer.rollback()
    finally:
        writer.close()

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_irs_migration_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]
