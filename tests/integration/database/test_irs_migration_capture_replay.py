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
    DDL_FILES,
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
    """Covers: ETL-059, ETL-116 — the same bytes store nothing; a changed file keeps both checksums."""
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
    assert [row[2] for row in history] == [str(first), str(third)]
    assert [row[0] for row in history] == [91, 92]
    assert history[0][1] != history[1][1]
    # The second read repeats the first byte for byte: recorded, not stored again.
    assert _rows(
        factory,
        "SELECT DISTINCT status FROM control.irs_migration_file WHERE run_id = %s",
        (str(second),),
    ) == [("unchanged",)]
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_irs_migration.fact_flow WHERE run_id = %s",
        (str(second),),
    ) == [(0,)]
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


def test_a_2011_layout_file_counts_state_rows_and_serves_exemptions(
    irs_warehouse,
) -> None:
    """Covers: ETL-059 — a pre-2018 file's state summary rows are counted, not loaded; its individuals are exemptions."""
    factory = irs_warehouse
    # The 2011-2012 to 2017-2018 files open each state with five rows for
    # subject county 000 (tests/fixtures/irs_migration/csv_2011_2018). Put
    # such a block in front of the reviewed Delaware rows.
    header, rows = irs.fixture_bytes("countyinflow2223.csv").split(b"\n", 1)
    state_block = b"\n".join(
        b"10,000," + code + b",DE,State total,100,200,300"
        for code in (b"96,000", b"97,000", b"97,001", b"97,003", b"98,000")
    )
    payload = header + b"\n" + state_block + b"\n" + rows
    run_id, flows, published = irs.run_to_gold(
        factory,
        "inflow",
        "2011-2012",
        client=irs.FixtureClient(
            {"countyinflow1112.csv": httpx.Response(200, content=payload)}
        ),
    )
    assert flows > 0 and published == 1
    ((captured, parsed, refused),) = _rows(
        factory,
        "SELECT captured_row_count, parsed_row_count, refused_row_count FROM control.irs_migration_file WHERE run_id = %s",
        (str(run_id),),
    )
    assert captured == parsed + 5 and flows + refused == parsed
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_irs_migration.flow_quarantine WHERE run_id = %s AND error_code NOT LIKE '%%_unresolved'",
        (str(run_id),),
    ) == [(0,)]
    assert _rows(
        factory,
        "SELECT DISTINCT people_basis FROM gold_irs_migration.flow_latest WHERE year_pair = '2011-2012'",
    ) == [("exemptions",)]
    assert _rows(
        factory,
        "SELECT DISTINCT people_basis FROM gold_irs_migration.total_observation_latest WHERE year_pair = '2011-2012'",
    ) == [("exemptions",)]
    irs.run_to_gold(factory, "inflow", "2022-2023")
    assert _rows(
        factory,
        "SELECT DISTINCT people_basis FROM gold_irs_migration.flow_latest WHERE year_pair = '2022-2023'",
    ) == [("individuals",)]
    reader = factory()
    try:
        with reader.cursor() as cursor:
            outcome = irs_migration_file_reconciliation(cursor, {})[0]
    finally:
        reader.close()
    assert (outcome.result, outcome.observed_count) == ("pass", 0)


def test_zip_layouts_publish_with_no_agi_before_1995_and_reconcile(
    irs_warehouse,
) -> None:
    """Covers: ETL-059 — 1990 text and 1993 and 2003 workbooks replay, publish and reconcile; no AGI before 1995."""
    factory = irs_warehouse
    published = {}
    for direction in ("inflow", "outflow"):
        for pair in ("1990-1991", "1993-1994", "2003-2004"):
            run_id, flows, count = irs.run_to_gold(factory, direction, pair)
            assert flows > 0 and count == 1
            published[(direction, pair)] = run_id
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM silver_irs_migration.flow_quarantine WHERE error_code NOT LIKE '%%_unresolved'",
    ) == [(0,)]
    assert _rows(
        factory,
        """
        SELECT year_pair, COUNT(*) FILTER (WHERE agi IS NOT NULL), COUNT(*) FILTER (WHERE agi IS NULL)
        FROM gold_irs_migration.flow_latest
        WHERE subject_geo_id = %s AND direction = 'inflow' AND value_status = 'valid'
        GROUP BY year_pair ORDER BY year_pair
        """,
        (irs.KENT,),
    )[0][1:] == (
        0,
        _rows(
            factory,
            "SELECT COUNT(*) FROM gold_irs_migration.flow_latest WHERE subject_geo_id = %s AND direction = 'inflow' AND year_pair = '1990-1991' AND value_status = 'valid'",
            (irs.KENT,),
        )[0][0],
    )
    measures = _rows(
        factory,
        """
        SELECT year_pair, ARRAY_AGG(DISTINCT measure ORDER BY measure)
        FROM gold_irs_migration.total_observation_latest
        WHERE geo_id = %s AND direction = 'inflow' GROUP BY year_pair ORDER BY year_pair
        """,
        (irs.KENT,),
    )
    assert measures == [
        ("1990-1991", ["individuals", "returns"]),
        ("1993-1994", ["individuals", "returns"]),
        ("2003-2004", ["agi", "individuals", "returns"]),
    ]
    reader = factory()
    try:
        with reader.cursor() as cursor:
            ledger = irs_migration_file_reconciliation(cursor, {})[0]
            totals = irs_migration_total_reconciliation(cursor, {})[0]
    finally:
        reader.close()
    assert (ledger.result, ledger.observed_count) == ("pass", 0)
    assert (totals.result, totals.observed_count) == ("pass", 0)


def test_the_older_agi_checks_are_replaced_once(irs_warehouse) -> None:
    """Covers: ETL-059 — a warehouse with the pre-1990 checks gets the new ones; a valid 1995 row still needs AGI."""
    factory = irs_warehouse
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                ALTER TABLE silver_irs_migration.fact_flow
                    DROP CONSTRAINT irs_flow_valid_individuals_present,
                    DROP CONSTRAINT irs_flow_valid_agi_from_1995,
                    ADD CONSTRAINT irs_flow_valid_measures_present CHECK (
                        value_status <> 'valid' OR (individuals IS NOT NULL AND agi IS NOT NULL)
                    )
                """
            )
        writer.commit()
    finally:
        writer.close()
    # Re-apply the silver DDL as an upgrade would (the file hash changed).
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(DDL_FILES[0].read_text(encoding="utf-8"))
        writer.commit()
    finally:
        writer.close()
    names = {
        row[0]
        for row in _rows(
            factory,
            "SELECT conname FROM pg_constraint WHERE conrelid = 'silver_irs_migration.fact_flow'::REGCLASS",
        )
    }
    assert "irs_flow_valid_measures_present" not in names
    assert {
        "irs_flow_valid_individuals_present",
        "irs_flow_valid_agi_from_1995",
    } <= names
    irs.run_to_gold(factory, "inflow", "1990-1991")
    writer = factory()
    try:
        with writer.cursor() as cursor, pytest.raises(psycopg2.errors.CheckViolation):
            cursor.execute(
                "UPDATE silver_irs_migration.fact_flow SET year1 = 1995, year2 = 1996 WHERE year_pair = '1990-1991' AND value_status = 'valid'"
            )
        writer.rollback()
    finally:
        writer.close()


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


def test_a_revised_parser_replays_the_same_bytes_once_and_serves_the_new_rows(
    irs_warehouse, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-059 — a file published under an older contract is planned again, replayed from the same stored bytes, and gold reads the newer capture."""
    from data_ingestion_toolbox.irs_migration import registry
    from data_ingestion_toolbox.irs_migration.plan import answered_year_pairs

    factory = irs_warehouse
    monkeypatch.setattr(registry, "PARSE_REVISIONS", {})
    for direction in ("inflow", "outflow"):
        irs.run_to_gold(factory, direction, "1990-1991")

    def answered() -> set[str]:
        reader = factory()
        try:
            return answered_year_pairs(reader)
        finally:
            reader.close()

    assert "1990-1991" in answered()
    blobs = _rows(factory, "SELECT COUNT(*) FROM raw_capture.payload_blob")
    monkeypatch.setattr(registry, "PARSE_REVISIONS", {"1990-1991": 2})
    assert "1990-1991" not in answered()
    rerun, flows, published = irs.run_to_gold(factory, "inflow", "1990-1991")
    assert flows > 0 and published == 1
    irs.run_to_gold(factory, "outflow", "1990-1991")
    assert "1990-1991" in answered()
    # The bytes are stored once; only the contract differs.
    assert _rows(factory, "SELECT COUNT(*) FROM raw_capture.payload_blob") == blobs
    assert _rows(
        factory,
        """
        SELECT DISTINCT run_id::TEXT FROM gold_irs_migration.flow_latest
        WHERE direction = 'inflow' AND year_pair = '1990-1991'
        """,
    ) == [(str(rerun),)]


def test_a_capture_left_unreplayed_is_resumed_or_settled(
    irs_warehouse, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covers: ETL-059 — a retry resumes its failed attempt's capture; an orphan whose bytes a later run published is marked unchanged."""
    from data_ingestion_toolbox.irs_migration import registry
    from data_ingestion_toolbox.irs_migration.capture import (
        capture_file,
        pending_run,
        settle_superseded_captures,
    )
    from data_ingestion_toolbox.irs_migration.config import IrsMigrationConfig
    from data_ingestion_toolbox.irs_migration.silver_irs_migration.load import (
        publish_run,
        replay_run,
    )

    factory = irs_warehouse
    item = registry.get_file("outflow", "2022-2023")

    def capture_only():  # noqa: ANN202
        run_id, _capture = capture_file(
            factory,
            item,
            config=IrsMigrationConfig(min_spacing_seconds=0, max_attempts=1),
            client=irs.FixtureClient(),
        )
        return run_id

    # An attempt that failed after its capture: the retry finds and replays it.
    stopped = capture_only()
    assert pending_run(factory, item) == stopped
    replay_run(factory, run_id=stopped)
    assert publish_run(factory, run_id=stopped) == 1
    assert pending_run(factory, item) is None

    # The 11-05 case: a capture never replayed, then a later run that
    # published the same bytes. Settling marks the orphan unchanged.
    orphan = capture_only()  # same bytes and contract: recorded unchanged
    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "UPDATE control.irs_migration_file SET status = 'captured' WHERE run_id = %s",
                (str(orphan),),
            )
        writer.commit()
    finally:
        writer.close()
    assert settle_superseded_captures(factory) == []  # nothing later yet
    monkeypatch.setattr(registry, "PARSE_REVISIONS", {"2022-2023": 2})
    later, flows, published = irs.run_to_gold(factory, "outflow", "2022-2023")
    assert flows > 0 and published == 1
    assert settle_superseded_captures(factory) == [
        ("outflow", "2022-2023", str(orphan))
    ]
    assert _rows(
        factory,
        "SELECT status FROM control.irs_migration_file WHERE run_id = %s",
        (str(orphan),),
    ) == [("unchanged",)]
    assert settle_superseded_captures(factory) == []
