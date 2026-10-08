"""Real PostgreSQL BLS QCEW capture-to-gold contract.

Covers: ETL-056
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.bls_qcew.registry import TOTAL, get_industry
from data_ingestion_toolbox.bls_qcew.schema import (
    REQUIRED_RELATIONS,
    ensure_bls_qcew_schema,
)
from data_ingestion_toolbox.glossary.harvest import (
    Publisher,
    harvest_publisher,
    process_pending_events,
)
from data_ingestion_toolbox.quality.sources import qcew_slice_reconciliation
from tests.support import bls_qcew as qcew

pytestmark = [pytest.mark.integration, pytest.mark.database]

HEALTH = get_industry("62")


@pytest.fixture
def qcew_warehouse(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> Callable[[], connection]:
    return qcew.reviewed_warehouse(postgres_connection_factory, request)


def _rows(
    factory: Callable[[], connection], sql: str, parameters: tuple = ()
) -> list[tuple]:
    reader = factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        reader.close()


def test_a_quarter_reaches_gold_with_withheld_cells_preserved(qcew_warehouse) -> None:
    """Covers: ETL-056 — every registered value reaches gold; a withheld cell is no zero."""
    factory = qcew_warehouse
    _run, facts, published = qcew.run_to_gold(factory, 2024, "1", (TOTAL, HEALTH))
    assert facts == 12 * 6 + 6 * 6
    assert published == 2

    kent = _rows(
        factory,
        """
        SELECT metric_key, period_start::TEXT, value::TEXT, value_status, observation_basis, geo_level
        FROM gold_bls_qcew.observation_latest
        WHERE geo_id = 'state:10|county:001' AND own_code = '0'
          AND measure_id IN ('establishments', 'employment')
        ORDER BY metric_key, period_start
        """,
    )
    assert kent[0][:4] == ("employment:10:0", "2024-01-01", "69258", "valid")
    assert [row[1] for row in kent if row[0] == "employment:10:0"] == [
        "2024-01-01",
        "2024-02-01",
        "2024-03-01",
    ]
    assert ("establishments:10:0", "2024-01-01", "5775", "valid") == kent[-1][:4]
    assert kent[0][4].startswith("establishment-based")
    assert {row[5] for row in kent} == {"COUNTY"}

    withheld = _rows(
        factory,
        """
        SELECT COUNT(*), COUNT(*) FILTER (WHERE value IS NULL), COUNT(*) FILTER (WHERE value_source IS NOT NULL)
        FROM gold_bls_qcew.observation_latest WHERE value_status = 'withheld'
        """,
    )
    assert withheld[0][0] > 0 and withheld[0][0] == withheld[0][1] == withheld[0][2]

    # The unseeded Alabama county is served unmapped, never dropped, and ledgered.
    assert _rows(
        factory,
        """
        SELECT DISTINCT geography_status FROM gold_bls_qcew.observation_latest
        WHERE geo_id = 'state:01|county:001'
        """,
    ) == [("unmapped",)]
    assert _rows(
        factory,
        """
        SELECT status FROM silver_ref.geography_resolution
        WHERE provider_source = 'BLS_QCEW' AND source_code = '01001' AND source_vintage = 2024
        """,
    ) == [("unmapped",)]
    ledger = _rows(
        factory,
        "SELECT industry_code, captured_row_count, in_scope_row_count, status FROM control.bls_qcew_slice ORDER BY industry_code",
    )
    assert ledger == [("10", 47, 12, "published"), ("62", 25, 6, "published")]


def test_the_publisher_states_the_establishment_basis_and_harvests(
    qcew_warehouse,
) -> None:
    """Covers: ETL-056 — one catalog metric per measure, industry and ownership."""
    factory = qcew_warehouse
    qcew.run_to_gold(factory, 2024, "1", (TOTAL, HEALTH))
    published = _rows(
        factory,
        """
        SELECT source_object_key, metric_display_name, valid_geo_grains, valid_time_grains, source_type
        FROM gold_bls_qcew.metric_publisher ORDER BY source_object_key
        """,
    )
    keys = [row[0] for row in published]
    assert "employment:10:0" in keys and "avg_weekly_wage:62:5" in keys
    assert len(keys) == 4 * 3
    for _key, name, grains, _time, source_type in published:
        assert "jobs located here" in name
        assert grains == ["COUNTY", "NATIONAL", "STATE"]
        assert source_type == "government-establishment-statistics"
    assert dict((row[0], row[3]) for row in published)["employment:10:0"] == ["MONTHLY"]
    assert harvest_publisher(factory, Publisher("gold_bls_qcew")) > 0
    assert process_pending_events(factory) >= 1
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM gold_glossary.dim_metric WHERE source_code = 'BLS_QCEW'",
    ) == [(12,)]


def test_a_rerun_is_idempotent_and_a_changed_file_keeps_both_checksums(
    qcew_warehouse,
) -> None:
    """Covers: ETL-056 — the same bytes add nothing new; a revised file is a second row."""
    factory = qcew_warehouse
    first, _f, _p = qcew.run_to_gold(factory, 2024, "1", (TOTAL,))
    second, _f, _p = qcew.run_to_gold(factory, 2024, "1", (TOTAL,))
    assert _rows(factory, "SELECT COUNT(*) FROM gold_bls_qcew.observation_latest") == [
        (72,)
    ]

    revised = qcew.fixture_bytes(2024, "1", "10").replace(
        b'"10001","0","10","70","0","2024","1","",5775',
        b'"10001","0","10","70","0","2024","1","",5780',
    )
    assert revised != qcew.fixture_bytes(2024, "1", "10")
    third, _f, _p = qcew.run_to_gold(
        factory,
        2024,
        "1",
        (TOTAL,),
        client=qcew.FixtureClient(
            {(2024, "1", "10"): httpx.Response(200, content=revised)}
        ),
    )
    history = _rows(
        factory,
        """
        SELECT revision.value::TEXT, capture.payload_checksum, revision.run_id::TEXT
        FROM gold_bls_qcew.observation_revision AS revision
        JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
        WHERE revision.metric_key = 'establishments:10:0' AND revision.geo_id = 'state:10|county:001'
        ORDER BY revision.retrieved_at
        """,
    )
    assert [row[2] for row in history] == [str(first), str(second), str(third)]
    assert history[0][1] == history[1][1] != history[2][1]
    assert _rows(
        factory,
        """
        SELECT value::TEXT FROM gold_bls_qcew.observation_latest
        WHERE metric_key = 'establishments:10:0' AND geo_id = 'state:10|county:001'
        """,
    ) == [("5780",)]
    assert _rows(factory, "SELECT COUNT(*) FROM gold_bls_qcew.observation_latest") == [
        (72,)
    ]


def test_a_malformed_file_is_captured_and_quarantined(qcew_warehouse) -> None:
    """Covers: ETL-056 — the bytes are kept; the slice is held back with a sanitized reason."""
    factory = qcew_warehouse
    run_id, facts, published = qcew.run_to_gold(
        factory,
        2024,
        "1",
        (TOTAL, HEALTH),
        client=qcew.FixtureClient(
            {
                (2024, "1", "62"): httpx.Response(
                    200,
                    content=qcew.fixture_bytes(2024, "1", "62").replace(
                        b'"10001","5","62","74","0","2024","1"',
                        b'"10001","5","62","74","0","2023","1"',
                    ),
                )
            }
        ),
    )
    assert published == 2
    assert facts == 12 * 6 + 5 * 6
    assert _rows(
        factory,
        "SELECT error_code FROM silver_bls_qcew.observation_quarantine WHERE run_id = %s",
        (str(run_id),),
    ) == [("unexpected_period",)]


def test_an_unpublished_period_is_recorded_empty_and_the_schema_reapplies(
    qcew_warehouse,
) -> None:
    """Covers: ETL-056 — a 404 slice is empty, not a failure; the DDL is rerunnable."""
    factory = qcew_warehouse
    run_id, facts, published = qcew.run_to_gold(factory, 2026, "4", (TOTAL,))
    assert (facts, published) == (0, 0)
    assert _rows(
        factory,
        "SELECT status FROM control.bls_qcew_slice WHERE run_id = %s",
        (str(run_id),),
    ) == [("empty",)]

    class _Hook:
        def get_conn(self) -> connection:
            return factory()

    ensure_bls_qcew_schema(_Hook())
    assert _rows(
        factory,
        "SELECT COUNT(*) FROM unnest(%s::TEXT[]) AS name WHERE to_regclass(name) IS NOT NULL",
        (list(REQUIRED_RELATIONS),),
    ) == [(len(REQUIRED_RELATIONS),)]


def test_the_slice_reconciliation_rule_passes_the_fixtures_and_catches_a_loss(
    qcew_warehouse,
) -> None:
    """Covers: ETL-056 — DQ-QCEW-002 runs against the fixtures and fails on a lost row."""
    factory = qcew_warehouse

    def outcome():
        reader = factory()
        try:
            with reader.cursor() as cursor:
                return qcew_slice_reconciliation(cursor, {})[0]
        finally:
            reader.close()

    assert outcome().result == "not_applicable"
    qcew.run_to_gold(factory, 2024, "1", (TOTAL, HEALTH))
    passed = outcome()
    assert (passed.result, passed.observed_count) == ("pass", 0)

    writer = factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM silver_bls_qcew.fact_observation
                WHERE industry_code = '62' AND geo_id = 'state:10|county:001'
                """
            )
            cursor.execute(
                """
                DELETE FROM silver_bls_qcew.observation_revision
                WHERE industry_code = '62' AND geo_id = 'state:10|county:001'
                """
            )
        writer.commit()
    finally:
        writer.close()
    failed = outcome()
    assert (failed.result, failed.observed_count) == ("fail", 1)
