"""Real PostgreSQL contract for finalizing an aborted run's control rows.

This reproduces the live finding that blocked the warehouse data-quality plan:
an aborted backfill iteration left ``running`` requests, a capture whose
request never reached ``captured``, and ``preflighted`` USDA NASS slices, so
the completeness and lineage rules reported missing work forever even though a
later successful run captured and published the full registered window.

The decision recorded on 2026-08-31 is that aborted runs finalize their own
control rows, and that finalization runs before an assessment reads them.
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from data_ingestion_toolbox.capture import (
    ABORTED_REQUEST_ERROR,
    CaptureControl,
    ResponseCapture,
    persist_response_capture,
)
from data_ingestion_toolbox.quality.finalization import (
    find_aborted_runs,
    finalize_aborted_runs,
)
from data_ingestion_toolbox.quality.reconciliation import (
    reconcile_requests,
    verify_capture_lineage,
)
from data_ingestion_toolbox.quality.sources import nass_slice_ledger

pytestmark = [pytest.mark.integration, pytest.mark.database]

#: A probe source unique to this module, so a whole-warehouse rule evaluated
#: here reports on this module's rows and not on residue another suite left.
SOURCE_CODE = f"ABORTPROBE{uuid4().hex[:8].upper()}"


def _outcomes(cursor, executor, source_code: str = SOURCE_CODE) -> dict[str, str]:
    scope = {"source_code": source_code}
    return {outcome.object_name: outcome.result for outcome in executor(cursor, scope)}


def _nass_ledger_evidence(cursor, run_id) -> list[str]:
    """Return the ledger rule's evidence entries naming this run."""
    [outcome] = nass_slice_ledger(cursor, {})
    return [entry for entry in outcome.evidence if str(run_id) in entry]


def _start_request(control: CaptureControl, run_id, endpoint: str):
    return control.start_request(
        run_id=run_id, endpoint=endpoint, parameters={"probe": endpoint}
    )


def _capture_for(
    request, run_id, endpoint: str, *, source_code: str = SOURCE_CODE, parameters=None
) -> ResponseCapture:
    return ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code=source_code,
        endpoint=endpoint,
        request_parameters=parameters
        if parameters is not None
        else {"probe": endpoint},
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "application/json"},
        media_type="application/json",
        payload=b'{"probe": true}',
    )


@pytest.fixture
def aborted_run(postgres_connection_factory, request: pytest.FixtureRequest):
    """Commit one aborted run exactly as an interrupted backfill leaves it."""
    control = CaptureControl(postgres_connection_factory, source_code=SOURCE_CODE)
    run_id = control.start_run(watermark={"probe": "abort"})

    with_bytes = _start_request(control, run_id, "/probe/captured")
    persist_response_capture(
        postgres_connection_factory,
        _capture_for(with_bytes, run_id, "/probe/captured"),
    )
    without_bytes = _start_request(control, run_id, "/probe/empty")

    def cleanup() -> None:
        database_connection = postgres_connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(
                    "ALTER TABLE raw_capture.response_capture "
                    "DISABLE TRIGGER response_capture_reject_mutation"
                )
                cursor.execute(
                    "SELECT DISTINCT payload_checksum "
                    "FROM raw_capture.response_capture WHERE run_id = %s",
                    (str(run_id),),
                )
                checksums = [row[0] for row in cursor.fetchall()]
                cursor.execute(
                    "DELETE FROM raw_capture.response_capture WHERE run_id = %s",
                    (str(run_id),),
                )
                cursor.execute(
                    "ALTER TABLE raw_capture.response_capture "
                    "ENABLE TRIGGER response_capture_reject_mutation"
                )
                if checksums:
                    cursor.execute(
                        "ALTER TABLE raw_capture.payload_blob "
                        "DISABLE TRIGGER payload_blob_reject_mutation"
                    )
                    cursor.execute(
                        "DELETE FROM raw_capture.payload_blob AS payload "
                        "WHERE payload.payload_checksum = ANY(%s) AND NOT EXISTS ("
                        "SELECT 1 FROM raw_capture.response_capture AS capture "
                        "WHERE capture.payload_checksum = payload.payload_checksum)",
                        (checksums,),
                    )
                    cursor.execute(
                        "ALTER TABLE raw_capture.payload_blob "
                        "ENABLE TRIGGER payload_blob_reject_mutation"
                    )
                cursor.execute(
                    "DELETE FROM control.ingestion_request WHERE run_id = %s",
                    (str(run_id),),
                )
                cursor.execute(
                    "DELETE FROM control.ingestion_run WHERE run_id = %s",
                    (str(run_id),),
                )
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
    return run_id, with_bytes, without_bytes, control


def test_an_aborted_run_finalizes_its_own_requests(
    aborted_run, postgres_connection_factory
) -> None:
    """Covers: DQ-002 — stopping a run terminalizes the work it abandoned.

    The request whose bytes are already durable finishes as ``captured``,
    because discarding a committed, checksummed payload would lose provider
    evidence. The request that produced nothing finishes as ``failed``.
    """
    run_id, with_bytes, without_bytes, control = aborted_run

    finalization = control.finish_run(run_id, status="failed", error="probe abort")

    assert finalization.captured == 1
    assert finalization.failed == 1
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT request_id, status, last_error, finished_at IS NOT NULL "
                "FROM control.ingestion_request WHERE run_id = %s "
                "ORDER BY endpoint",
                (str(run_id),),
            )
            by_id = {str(row[0]): row[1:] for row in cursor.fetchall()}
            assert by_id[str(with_bytes.request_id)] == ("captured", None, True)
            assert by_id[str(without_bytes.request_id)] == (
                "failed",
                ABORTED_REQUEST_ERROR,
                True,
            )
            # The run itself keeps its honest failure; nothing is disguised.
            cursor.execute(
                "SELECT status, error_summary FROM control.ingestion_run "
                "WHERE run_id = %s",
                (str(run_id),),
            )
            status, summary = cursor.fetchone()
            assert status == "failed"
            assert summary
    finally:
        reader.close()


def test_the_lineage_and_accounting_rules_go_green_after_finalization(
    aborted_run, postgres_connection_factory
) -> None:
    """Covers: DQ-003 — the recorded live lineage/accounting finding clears.

    Before the fix, the aborted run's ``running`` requests and its capture
    whose request never reached ``captured`` failed both shared reconciliation
    rules, and no later successful run over the same partition could clear
    them.
    """
    run_id, _with_bytes, _without_bytes, control = aborted_run

    database_connection = postgres_connection_factory()
    try:
        # Stop the run the way an interrupted process did before this change:
        # the run row is terminal, its requests are not.
        with database_connection.cursor() as cursor:
            cursor.execute(
                "UPDATE control.ingestion_run SET status = 'failed', "
                "finished_at = NOW(), updated_at = NOW() WHERE run_id = %s",
                (str(run_id),),
            )
        database_connection.commit()

        with database_connection.cursor() as cursor:
            lineage = _outcomes(cursor, verify_capture_lineage)
            accounting = _outcomes(cursor, reconcile_requests)
            assert lineage["raw_capture.response_capture"] == "fail"
            assert accounting["control.ingestion_run"] == "fail"
            assert find_aborted_runs(cursor) != []

        report = finalize_aborted_runs(database_connection)
        database_connection.commit()
        assert report.requests_changed == 2

        with database_connection.cursor() as cursor:
            lineage = _outcomes(cursor, verify_capture_lineage)
            accounting = _outcomes(cursor, reconcile_requests)
            assert lineage["control.ingestion_request"] == "pass"
            assert lineage["raw_capture.response_capture"] == "pass"
            assert accounting["control.ingestion_run"] == "pass"
            # A second sweep has nothing left to do.
            assert find_aborted_runs(cursor) == []
        assert finalize_aborted_runs(database_connection).changed == 0
        database_connection.commit()
    finally:
        database_connection.close()
    del control


def test_a_successful_run_is_never_repaired(
    aborted_run, postgres_connection_factory
) -> None:
    """Covers: DQ-003 — an unfinished request under a success stays red.

    Auto-healing that case would erase a real defect: a run that reported
    success while leaving work unresolved is exactly what the accounting rule
    exists to surface.
    """
    run_id, _with_bytes, _without_bytes, control = aborted_run

    control.finish_run(run_id, status="success")

    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            assert find_aborted_runs(cursor) == []
            accounting = _outcomes(cursor, reconcile_requests)
            assert accounting["control.ingestion_run"] == "fail"
        assert finalize_aborted_runs(database_connection).changed == 0
        database_connection.commit()
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM control.ingestion_request "
                "WHERE run_id = %s AND status IN ('planned', 'running')",
                (str(run_id),),
            )
            assert cursor.fetchone()[0] == 2
    finally:
        database_connection.close()


def test_a_run_still_in_flight_is_never_finalized(
    aborted_run, postgres_connection_factory
) -> None:
    """Covers: DQ-002 — a sweep never cancels work that is still running."""
    run_id, _with_bytes, _without_bytes, _control = aborted_run

    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            assert find_aborted_runs(cursor) == []
        assert finalize_aborted_runs(database_connection).changed == 0
        database_connection.commit()
        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM control.ingestion_request "
                "WHERE run_id = %s AND status = 'running'",
                (str(run_id),),
            )
            assert cursor.fetchone()[0] == 2
    finally:
        database_connection.close()


def test_an_aborted_nass_run_retires_only_its_preflighted_slices(
    postgres_connection_factory, request: pytest.FixtureRequest
) -> None:
    """Covers: DQ-004 — a counted-but-never-captured slice becomes terminal.

    An ``over_limit`` slice is already terminal evidence that the release was
    quarantined rather than ingested, so it survives the repair untouched.
    """
    # The NASS ledger rule is whole-warehouse, so this asserts on the exact
    # slices this run owns rather than on a global verdict another suite's
    # residue could flip.
    control = CaptureControl(postgres_connection_factory, source_code="USDA_NASS")
    run_id = control.start_run(watermark={"probe": "nass-abort"})
    count_parameters = {"probe": "count"}
    count_request = control.start_request(
        run_id=run_id, endpoint="/api_GET_COUNTS", parameters=count_parameters
    )
    count_capture = _capture_for(
        count_request,
        run_id,
        "/api_GET_COUNTS",
        source_code="USDA_NASS",
        parameters=count_parameters,
    )
    persist_response_capture(postgres_connection_factory, count_capture)
    control.finish_request(count_request.request_id, status="captured")

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO control.usda_nass_release (
                    run_id, product_id, slice_mode, parser_contract_version,
                    extraction_watermark, total_row_count, slice_counts,
                    field_signature, decision, status, captured_row_count,
                    slice_count, complete
                ) VALUES (
                    %s, 'probe_product', 'recent', '1.0', '2026-01-01', 0,
                    '{}'::JSONB, '{}'::JSONB, 'over_limit_quarantine',
                    'quarantined', 0, 2, FALSE
                )
                """,
                (str(run_id),),
            )
            for slice_key, status in (
                ("probe:NATIONAL:2024", "preflighted"),
                ("probe:STATE:2024", "over_limit"),
            ):
                cursor.execute(
                    """
                    INSERT INTO control.usda_nass_slice (
                        run_id, slice_key, product_id, agg_level_desc, year,
                        provider_count, captured_row_count, count_capture_id,
                        status
                    ) VALUES (%s, %s, 'probe_product', %s, 2024, 10, 0, %s, %s)
                    """,
                    (
                        str(run_id),
                        slice_key,
                        "NATIONAL" if "NATIONAL" in slice_key else "STATE",
                        str(count_capture.capture_id),
                        status,
                    ),
                )
        writer.commit()
    finally:
        writer.close()

    def cleanup() -> None:
        database_connection = postgres_connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM control.usda_nass_slice WHERE run_id = %s",
                    (str(run_id),),
                )
                cursor.execute(
                    "DELETE FROM control.usda_nass_release WHERE run_id = %s",
                    (str(run_id),),
                )
                cursor.execute(
                    "ALTER TABLE raw_capture.response_capture "
                    "DISABLE TRIGGER response_capture_reject_mutation"
                )
                cursor.execute(
                    "DELETE FROM raw_capture.response_capture WHERE run_id = %s",
                    (str(run_id),),
                )
                cursor.execute(
                    "ALTER TABLE raw_capture.response_capture "
                    "ENABLE TRIGGER response_capture_reject_mutation"
                )
                cursor.execute(
                    "ALTER TABLE raw_capture.payload_blob "
                    "DISABLE TRIGGER payload_blob_reject_mutation"
                )
                cursor.execute(
                    "DELETE FROM raw_capture.payload_blob AS payload "
                    "WHERE NOT EXISTS ("
                    "SELECT 1 FROM raw_capture.response_capture AS capture "
                    "WHERE capture.payload_checksum = payload.payload_checksum)"
                )
                cursor.execute(
                    "ALTER TABLE raw_capture.payload_blob "
                    "ENABLE TRIGGER payload_blob_reject_mutation"
                )
                cursor.execute(
                    "DELETE FROM control.ingestion_request WHERE run_id = %s",
                    (str(run_id),),
                )
                cursor.execute(
                    "DELETE FROM control.ingestion_run WHERE run_id = %s",
                    (str(run_id),),
                )
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)
    control.finish_run(run_id, status="failed", error="probe abort")

    database_connection = postgres_connection_factory()
    try:
        with database_connection.cursor() as cursor:
            assert _nass_ledger_evidence(cursor, run_id), (
                "the preflighted slice must be reported before finalization"
            )

        report = finalize_aborted_runs(database_connection, source_code="USDA_NASS")
        database_connection.commit()
        assert report.ledger_rows == {"control.usda_nass_slice": 1}

        with database_connection.cursor() as cursor:
            cursor.execute(
                "SELECT slice_key, status FROM control.usda_nass_slice "
                "WHERE run_id = %s ORDER BY slice_key",
                (str(run_id),),
            )
            assert cursor.fetchall() == [
                ("probe:NATIONAL:2024", "skipped"),
                ("probe:STATE:2024", "over_limit"),
            ]
            assert not _nass_ledger_evidence(cursor, run_id), (
                "the finalized slice must no longer be reported as missing work"
            )
    finally:
        database_connection.close()
