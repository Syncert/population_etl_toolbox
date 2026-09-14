"""Real PostgreSQL source-specific coverage and validity checks (DQ-004)."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from psycopg2.extensions import connection

from data_ingestion_toolbox.capture import (
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.quality.reconciliation import (
    EVIDENCE_LIMIT,
    SHARED_RECONCILIATION_EXECUTORS,
)
from data_ingestion_toolbox.quality.sources import (
    SOURCE_EXECUTORS,
    acs_slice_reconciliation,
    bls_chunk_reconciliation,
    cdc_watermark_monotonicity,
    fred_slice_reconciliation,
    nass_slice_ledger,
    pep_sentinel_conformance,
    publisher_registry_reconciliation,
    reference_resolution_accounting,
)

pytestmark = [pytest.mark.integration, pytest.mark.database]


#: Relations the source quality executors read to decide "valid emptiness".
#: Asserted rather than assumed, so a suite that leaks committed rows is named
#: here instead of surfacing as an unrelated rule failure several tests later.
REQUIRED_EMPTY_RELATIONS = (
    "silver_ref.geography_resolution",
    "control.acs_ingestion_slices",
    "control.bls_ingestion_slices",
    "control.fred_ingestion_slices",
    "gold_glossary.publisher_registry",
    "raw_fred.fred_datasets",
    "raw_fred.fred_series",
)


def _assert_warehouse_is_empty(cursor) -> None:
    """Fail naming the relation that is not empty, not the rule that noticed."""
    populated = []
    for relation in REQUIRED_EMPTY_RELATIONS:
        cursor.execute(f"SELECT COUNT(*) FROM {relation}")
        count = cursor.fetchone()[0]
        if count:
            populated.append(f"{relation}={count}")
    assert not populated, (
        "this test asserts what the quality rules do on an empty warehouse, so "
        "it must start from one; these relations still hold committed rows "
        f"from earlier in the session: {populated}"
    )


def test_an_empty_warehouse_is_valid_emptiness_not_failure(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — no source data means not_applicable, never a false alarm."""
    with postgres_connection.cursor() as cursor:
        for statement in (
            "DELETE FROM silver_ref.geography_resolution",
            "DELETE FROM control.acs_ingestion_slices",
            "DELETE FROM control.bls_ingestion_slices",
            "DELETE FROM control.fred_ingestion_slices",
            "DELETE FROM gold_glossary.publisher_registry",
            # Configuration and captured metadata are two different relations,
            # and a rule that reconciles them is right to fail when one is
            # populated and the other is not. This test's premise is that
            # neither is, so it must empty both rather than inherit whatever
            # an earlier suite in the session committed.
            "DELETE FROM raw_fred.fred_datasets",
            "DELETE FROM raw_fred.fred_series",
        ):
            cursor.execute(statement)
        _assert_warehouse_is_empty(cursor)
        for rule_id, executor in sorted(SOURCE_EXECUTORS.items()):
            for outcome in executor(cursor, {}):
                assert outcome.result in {"not_applicable", "pass"}, (
                    f"{rule_id} produced {outcome.result} on an empty warehouse"
                )
    postgres_connection.rollback()


def test_slice_ledger_defects_fail_with_bounded_evidence(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — abandoned, failed, and silently-empty slices surface."""
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.acs_ingestion_slices
                (dataset, year, geo_level, status, rows_loaded, started_at)
            VALUES
                ('acs5', 2023, 'us', 'failed', 0, NOW()),
                ('acs5', 2023, 'state', 'planned', 0, NULL),
                ('acs5', 2022, 'us', 'success', 0, NOW()),
                ('acs5', 2022, 'state', 'success', 10, NOW()),
                ('acs1', 2023, 'us', 'empty', 0, NOW())
            """
        )
        [outcome] = acs_slice_reconciliation(cursor, {})
        assert outcome.result == "fail"
        assert outcome.observed_count == 3  # failed, planned, success-with-zero
        assert len(outcome.evidence) == 3

        cursor.execute(
            """
            INSERT INTO control.bls_ingestion_slices
                (program, year_start, year_end, status, rows_loaded, started_at)
            VALUES ('la', 2023, 2024, 'success', 25, NOW())
            """
        )
        [outcome] = bls_chunk_reconciliation(cursor, {})
        assert outcome.result == "pass"

        cursor.execute(
            """
            INSERT INTO control.fred_ingestion_slices
                (domain, date_start, date_end, status, rows_loaded, started_at)
            VALUES ('labor', '2024-01-01', '2024-12-31', 'running', 0, NOW())
            """
        )
        outcomes = fred_slice_reconciliation(cursor, {})
        ledger = outcomes[0]
        assert ledger.result == "fail"
        assert ledger.evidence == ["labor:2024-01-01|running"]
    postgres_connection.rollback()


def _seed_probe_capture(
    connection_factory: Callable[[], connection], source_code: str
) -> tuple[str, str]:
    control = CaptureControl(connection_factory, source_code=source_code)
    run_id = control.start_run(watermark={})
    parameters = {"seed": str(uuid4())}
    request = control.start_request(
        run_id=run_id, endpoint="/probe", parameters=parameters
    )
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code=source_code,
        endpoint="/probe",
        request_parameters=parameters,
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "application/json"},
        media_type="application/json",
        payload=json.dumps({"probe": str(uuid4())}).encode(),
        payload_schema_version="probe-v1",
    )
    persist_response_capture(connection_factory, capture)
    control.finish_request(request.request_id, status="captured")
    control.finish_run(run_id, status="success")
    return str(run_id), str(capture.capture_id)


def test_cdc_backward_watermark_ingest_fails(
    postgres_connection_factory: Callable[[], connection],
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — a later ingest at a lower watermark is a regression."""
    source = f"CDCPROBE{uuid4().hex[:10].upper()}"
    run_one, capture_one = _seed_probe_capture(postgres_connection_factory, source)
    run_two, capture_two = _seed_probe_capture(postgres_connection_factory, source)

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.cdc_dataset_release (
                run_id, asset_id, socrata_id, title, release_watermark,
                schema_contract, metadata_capture_id, decision, status,
                captured_row_count, page_count, complete, published_at,
                created_at
            ) VALUES
                (%s, 'cdi', 'abcd-1234', 'probe', 200, '{}'::JSONB, %s,
                 'ingest', 'published', 3, 1, TRUE, NOW(),
                 NOW() - INTERVAL '1 hour'),
                (%s, 'cdi', 'abcd-1234', 'probe', 100, '{}'::JSONB, %s,
                 'ingest', 'captured', 3, 1, TRUE, NULL, NOW())
            """,
            (run_one, capture_one, run_two, capture_two),
        )
        [outcome] = cdc_watermark_monotonicity(cursor, {})
        assert outcome.result == "fail"
        assert outcome.evidence == ["cdi|100"]
    postgres_connection.rollback()


def test_an_offender_count_is_exact_beside_bounded_evidence(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-008 — the count is exact, the evidence is bounded.

    `DATA_QUALITY_OPERATIONS.md` says `control.data_quality_result` holds
    "exact counts, bounded evidence ids" -- two different things -- and its
    operator query selects `observed_count` to judge how bad a failure is.
    The offender queries fetched `EVIDENCE_LIMIT + 1`, one more than the cap
    and written deliberately so truncation could be detected, and the helper
    sliced the extra row away: the count was the evidence's length, so twenty
    bad rows and twenty thousand both recorded 20.
    """
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.acs_ingestion_slices
                (dataset, year, geo_level, state_fips, status, rows_loaded,
                 started_at)
            SELECT 'acs5', 2021, 'county', LPAD(generated::TEXT, 2, '0'),
                   'failed', 0, NOW()
              FROM generate_series(1, 43) AS generated
            """
        )
        [outcome] = acs_slice_reconciliation(cursor, {})
        assert outcome.result == "fail"
        assert outcome.observed_count == 43, (
            "the offender count saturated at the evidence cap: an operator "
            "reading it cannot tell a handful of bad rows from a systemic "
            "failure"
        )
        assert len(outcome.evidence) == EVIDENCE_LIMIT
        # The sample is the rule's own order, not whatever the scan returned.
        assert outcome.evidence == sorted(outcome.evidence)
    postgres_connection.rollback()


def test_nass_ledger_mismatch_and_advanced_partial_slice_fail(
    postgres_connection_factory: Callable[[], connection],
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — preflight drift and a partial slice that advanced."""
    source = f"NASSPROBE{uuid4().hex[:10].upper()}"
    run_id, capture_id = _seed_probe_capture(postgres_connection_factory, source)

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.usda_nass_release (
                run_id, product_id, slice_mode, parser_contract_version,
                extraction_watermark, total_row_count, slice_counts,
                field_signature, decision, status, captured_row_count,
                slice_count, complete
            ) VALUES (
                %s, 'corn_grain', 'recent', 'quickstats-crop-v1',
                '2024-01-01 00:00:00', 10, '{}'::JSONB, '{}'::JSONB,
                'ingest', 'captured', 10, 2, TRUE
            )
            """,
            (run_id,),
        )
        cursor.execute(
            """
            INSERT INTO control.usda_nass_slice (
                run_id, slice_key, product_id, agg_level_desc, year,
                provider_count, captured_row_count, count_capture_id,
                data_capture_id, status
            ) VALUES
                (%s, 'corn_grain|STATE|2024', 'corn_grain', 'STATE', 2024,
                 10, 7, %s, %s, 'captured'),
                (%s, 'corn_grain|COUNTY|2024', 'corn_grain', 'COUNTY', 2024,
                 99, 0, %s, NULL, 'partial')
            """,
            (run_id, capture_id, capture_id, run_id, capture_id),
        )
        [outcome] = nass_slice_ledger(cursor, {})
        assert outcome.result == "fail"
        assert any(entry.startswith("advanced:") for entry in outcome.evidence)
        assert any("corn_grain|STATE|2024" in entry for entry in outcome.evidence)
    postgres_connection.rollback()


def test_pep_sentinel_misclassification_fails(
    postgres_connection_factory: Callable[[], connection],
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — a frozen Census sentinel must classify as sentinel."""
    source = f"PEPPROBE{uuid4().hex[:10].upper()}"
    _, capture_id = _seed_probe_capture(postgres_connection_factory, source)

    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = 'silver_pep'"
            " AND table_name = 'observation_revision'"
            " AND is_nullable = 'NO' AND column_default IS NULL"
        )
        required = {row[0] for row in cursor.fetchall()}
        provided = {
            "capture_id",
            "source_row_index",
            "source_column_index",
            "dataset_code",
            "release_vintage",
            "product_code",
            "metric_code",
            "observation_year",
            "value_source",
            "value_status",
            "parser_version",
            "summary_level",
            "unit",
            "source_header",
        }
        missing = required - provided
        assert not missing, f"seed does not cover NOT NULL columns: {missing}"

        cursor.execute(
            """
            INSERT INTO silver_pep.observation_revision (
                capture_id, source_row_index, source_column_index,
                dataset_code, release_vintage, product_code, metric_code,
                observation_year, value_source, value, value_status,
                parser_version, summary_level, unit, source_header
            ) VALUES (
                %s, 1, 1, 'pep_nst_alldata', 2025, 'NST-EST2025-ALLDATA',
                'POPESTIMATE', 2024, '-999999999', NULL, 'blank',
                'probe-v1', '040', 'persons', 'POPESTIMATE2024'
            )
            """,
            (capture_id,),
        )
        [outcome] = pep_sentinel_conformance(cursor, {})
        assert outcome.result == "fail"
        assert outcome.evidence == [f"{capture_id}|1|1"]
    postgres_connection.rollback()


def test_reference_and_registry_defects_fail(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-004 — incoherent resolutions and dangling publishers surface."""
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO silver_ref.geography_resolution (
                provider_source, provider_dataset, source_geo_type,
                source_code, source_vintage, geo_sk, status
            ) VALUES ('PROBE', 'probe_dataset', 'county', '01001', 2020,
                      NULL, 'resolved')
            """
        )
        [outcome] = reference_resolution_accounting(cursor, {})
        assert outcome.result == "fail"
        assert outcome.evidence == ["PROBE|probe_dataset|01001|resolved"]

        cursor.execute(
            """
            INSERT INTO gold_glossary.publisher_registry (
                source_code, publisher_schema, publisher_view,
                publisher_contract_version
            ) VALUES
                ('PROBE_MISSING', 'gold_probe', 'metric_publisher', 'v1'),
                ('CENSUS_ACS', 'gold_census', 'metric_publisher', 'v1')
            """
        )
        [outcome] = publisher_registry_reconciliation(cursor, {})
        assert outcome.result == "fail"
        assert outcome.evidence == ["PROBE_MISSING|gold_probe|metric_publisher"]
    postgres_connection.rollback()


# ---------------------------------------------------------------------------
# DQ-009 — an offender query is ordered by what the wrapping statement sees
# ---------------------------------------------------------------------------


def test_a_fred_dataset_without_its_series_row_fails_the_rule(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-009 — the reproduction, as a failing-first test.

    DQ-008 moved every rule's `ORDER BY` onto the wrapping statement.
    Fourteen rules wrote positions; this one wrote `dataset.domain,
    dataset.series_id`, naming the relation *inside* the subquery, where the
    wrapper places the clause outside and only `offender` is in scope.
    PostgreSQL refuses it outright, so the rule raised
    `UndefinedTable: missing FROM-clause entry for table "dataset"` the first
    time a FRED dataset had no series row -- the exact condition it exists to
    report -- and an errored assessment is not promotable.

    Three tiers missed it: the unit tier runs no SQL, and this module empties
    `raw_fred.fred_datasets` so the rule answers `not_applicable` before
    `_offenders` is reached. Only the DAG pipeline tier, which seeds the
    warehouse and runs the real rule, reached the statement.
    """
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO raw_fred.fred_datasets
                (domain, series_id, is_available, first_seen_at, last_checked_at)
            VALUES ('labor', 'UNMATCHED_SERIES', TRUE, NOW(), NOW())
            """
        )
        outcomes = fred_slice_reconciliation(cursor, {})

    dataset_outcome = next(
        outcome
        for outcome in outcomes
        if outcome.object_name == "raw_fred.fred_datasets"
    )
    assert dataset_outcome.result == "fail"
    assert dataset_outcome.observed_count == 1
    assert dataset_outcome.evidence == ["labor|UNMATCHED_SERIES"]
    postgres_connection.rollback()


class _ExplainingCursor:
    """Runs every offender statement through the planner, and nothing else.

    A rule reaches `_offenders` only after its configured relation reports
    rows, which is why an empty warehouse proves nothing about the statement:
    every rule here returns `not_applicable` first. This answers each
    preliminary count with one row so the rule proceeds, then hands the
    offender statement to PostgreSQL as an `EXPLAIN` -- which resolves every
    name, type and scope without needing an offender to exist.
    """

    def __init__(self, cursor) -> None:
        self._cursor = cursor
        self.explained: list[str] = []
        self._last_was_offender = False

    def execute(self, sql: str, params=()) -> None:
        statement = str(sql)
        self._last_was_offender = "COUNT(*) OVER () AS offender_total" in statement
        if self._last_was_offender:
            self.explained.append(statement)
            self._cursor.execute(f"EXPLAIN {statement}", params)
            self._cursor.fetchall()
            return
        # Anything else is a preliminary count or a bounded read the rule
        # makes its own decisions from; it runs for real.
        self._cursor.execute(statement, params)

    def fetchall(self):
        if self._last_was_offender:
            # Explained, not executed: there are no offender rows to return,
            # and the rule reports `pass` rather than a fabricated failure.
            return []
        return self._cursor.fetchall()

    def fetchone(self):
        row = self._cursor.fetchone()
        # Every rule gates its offender query behind `SELECT COUNT(*) …`; a
        # zero would return `not_applicable` before the statement is reached.
        if row is not None and len(row) == 1 and row[0] == 0:
            return (1,)
        return row


def test_every_offender_statement_is_one_postgresql_can_run(
    postgres_connection: connection,
) -> None:
    """Covers: DQ-009 — the wrapper contract, proved on every rule.

    DQ-008 proved the wrapper on one rule's exact count and left the other
    fourteen unproved; the one that did not follow its convention raised at
    run time. Seeding an offender for each would mean writing a capture,
    release and geography chain per source; what the defect is about is
    whether each statement can run at all, so each is handed to the planner,
    which resolves every name and scope without an offender existing.

    The rules whose *fail* outcome is separately seeded from real rows are
    the ACS, BLS and FRED ledgers, the FRED dataset join above, USDA NASS's
    slice ledger, Census PEP's sentinel conformance, the CDC watermark, the
    reference accounting and the publisher registry -- the nodes above this
    one. The rest are proved runnable here.
    """
    executors = {**SOURCE_EXECUTORS, **SHARED_RECONCILIATION_EXECUTORS}
    # DQ-SHARED-001 recomputes checksums over a bounded window and uses no
    # offender wrapper, so it contributes no statement.
    assert len(executors) >= 16, f"only {len(executors)} executors found"

    explained: dict[str, int] = {}
    for rule_id, executor in sorted(executors.items()):
        with postgres_connection.cursor() as cursor:
            probe = _ExplainingCursor(cursor)
            executor(probe, {})
            explained[rule_id] = len(probe.explained)
        postgres_connection.rollback()

    # Every rule that carries an offender query reached it, and the total is
    # the number of call sites in the two modules.
    assert sum(explained.values()) >= 19, (
        f"only {sum(explained.values())} offender statements were planned, so "
        f"some rule returned before its own: {explained}"
    )
