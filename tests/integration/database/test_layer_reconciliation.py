"""Real PostgreSQL lineage/layer reconciliation and publication gating."""

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
from data_ingestion_toolbox.cdc.gold_cdc.publisher import publish_release
from data_ingestion_toolbox.cdc.registry import CDI_ASSET
from data_ingestion_toolbox.cdc.silver_cdc.replay import (
    persist_replay_result,
    replay_captured_run,
)
from data_ingestion_toolbox.cdc.silver_cdc.transform import transform_release
from data_ingestion_toolbox.quality.reconciliation import (
    build_cdc_gate_executors,
    evaluate_publication_gate,
    reconcile_requests,
    verify_capture_checksums,
    verify_capture_lineage,
)
from tests.support.capture_seed import delete_geography, seed_geography
from tests.support.cdc_release import CDC_FIXTURE_DIR, persist_fixture_release

pytestmark = [pytest.mark.integration, pytest.mark.database]

COMMIT_SHA = "89abcdef0123456789abcdef0123456789abcdef"


def _seed_lineage_fixture(
    connection_factory: Callable[[], connection], source_code: str
) -> tuple[str, str]:
    """One healthy run/request/capture chain under an isolated source code."""
    control = CaptureControl(connection_factory, source_code=source_code)
    run_id = control.start_run(watermark={"probe": True})
    request = control.start_request(
        run_id=run_id, endpoint="/probe", parameters={"page": 1}
    )
    capture = ResponseCapture(
        capture_id=uuid4(),
        request_id=request.request_id,
        run_id=run_id,
        source_code=source_code,
        endpoint="/probe",
        request_parameters={"page": 1},
        retrieved_at=datetime.now(timezone.utc),
        http_status=200,
        response_headers={"content-type": "application/json"},
        media_type="application/json",
        payload=b'{"probe": "payload"}',
        payload_schema_version="probe-v1",
    )
    persist_response_capture(connection_factory, capture)
    control.finish_request(request.request_id, status="captured")
    control.finish_run(run_id, status="success")
    return str(run_id), str(capture.capture_id)


def test_injected_lineage_defects_fail_with_bounded_evidence(
    postgres_connection_factory: Callable[[], connection],
) -> None:
    """Covers: DQ-003 — corruption, orphan lineage, and lost work all surface."""
    # Foundation rows are append-only by design, so each execution isolates
    # itself under a fresh source code instead of deleting shared history.
    lineage_source = f"QUALITYPROBE{uuid4().hex[:12].upper()}"
    run_id, capture_id = _seed_lineage_fixture(
        postgres_connection_factory, lineage_source
    )
    scope = {"source_code": lineage_source}

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            healthy = (
                verify_capture_checksums(cursor, scope)
                + verify_capture_lineage(cursor, scope)
                + reconcile_requests(cursor, scope)
            )
            assert {outcome.result for outcome in healthy} == {"pass"}

            # Injected corruption: same-size payload, different bytes. The
            # append-only trigger rightly blocks this, so the test simulates
            # disk-level corruption inside its own never-committed
            # transaction with triggers bypassed.
            cursor.execute("SET session_replication_role = replica")
            cursor.execute(
                """
                UPDATE raw_capture.payload_blob
                   SET payload = OVERLAY(payload PLACING 'X'::BYTEA FROM 3)
                 WHERE payload_checksum = (
                     SELECT payload_checksum
                       FROM raw_capture.response_capture
                      WHERE capture_id = %s
                 )
                """,
                (capture_id,),
            )
            cursor.execute("SET session_replication_role = origin")
            [checksum_outcome] = verify_capture_checksums(cursor, scope)
            assert checksum_outcome.result == "fail"
            assert checksum_outcome.evidence == [capture_id]

            # Injected orphan lineage: a request claims capture, none exists.
            control = CaptureControl(
                postgres_connection_factory, source_code=lineage_source
            )
            orphan_run = control.start_run(watermark={})
            orphan_request = control.start_request(
                run_id=orphan_run, endpoint="/probe", parameters={"page": 2}
            )
            control.finish_request(orphan_request.request_id, status="captured")

            lineage = verify_capture_lineage(cursor, scope)
            by_object = {outcome.object_name: outcome for outcome in lineage}
            failed = by_object["control.ingestion_request"]
            assert failed.result == "fail"
            assert failed.evidence == [str(orphan_request.request_id)]

            # Injected lost work: a run reached a terminal status while a
            # request stayed unfinished. The run row is written directly rather
            # than through finish_run, because finish_run now finalizes the
            # requests an aborted run abandons; this is the state a process
            # killed mid-run leaves behind, which is what the rule must catch.
            planned_request = control.start_request(
                run_id=orphan_run, endpoint="/probe", parameters={"page": 3}
            )
            cursor.execute(
                # GREATEST because this reader transaction opened before the
                # control connection committed the run's started_at.
                "UPDATE control.ingestion_run SET status = 'partial', "
                "finished_at = GREATEST(started_at, NOW()), updated_at = NOW() "
                "WHERE run_id = %s",
                (str(orphan_run),),
            )
            unfinished = reconcile_requests(cursor, scope)
            run_outcome = {outcome.object_name: outcome for outcome in unfinished}[
                "control.ingestion_run"
            ]
            assert run_outcome.result == "fail"
            assert run_outcome.evidence == [str(planned_request.request_id)]
    finally:
        reader.rollback()
        reader.close()


def _cdi_metadata_with_watermark(watermark_delta: int) -> tuple[bytes, str]:
    payload = json.loads((CDC_FIXTURE_DIR / "cdi_metadata.json").read_text())
    payload["rowsUpdatedAt"] = int(payload["rowsUpdatedAt"]) + watermark_delta
    return json.dumps(payload).encode("utf-8"), str(payload["rowsUpdatedAt"])


def _capture_and_conform(
    connection_factory: Callable[[], connection], metadata_payload: bytes
):
    release = persist_fixture_release(
        connection_factory,
        asset=CDI_ASSET,
        metadata_name="cdi_metadata.json",
        observations_name="cdi_observations.json",
        metadata_payload=metadata_payload,
    )
    result = replay_captured_run(
        connection_factory,
        run_id=release.run_id,
        asset=CDI_ASSET,
        release_watermark=release.metadata.release_version,
    )
    persist_replay_result(
        connection_factory,
        run_id=release.run_id,
        asset=CDI_ASSET,
        release_watermark=release.metadata.release_version,
        result=result,
    )
    transform_release(
        connection_factory,
        run_id=release.run_id,
        asset=CDI_ASSET,
        release_watermark=release.metadata.release_version,
    )
    return release


def test_publication_gate_holds_back_a_damaged_release(
    postgres_connection_factory: Callable[[], connection],
    request: pytest.FixtureRequest,
) -> None:
    """Covers: DQ-003 — loss, duplication, and partial publication refuse the
    gate while the prior published release keeps serving."""
    tracked_geo_ids = {"us:1", "state:01", "state:01|county:001"}
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT geo_id FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
                (list(tracked_geo_ids),),
            )
            preexisting = {row[0] for row in cursor.fetchall()}
    finally:
        reader.close()

    def cleanup() -> None:
        database_connection = postgres_connection_factory()
        try:
            with database_connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM control.publisher_ready_event"
                    " WHERE source_code = 'CDC'"
                )
                cursor.execute(
                    "DELETE FROM silver_ref.geography_resolution"
                    " WHERE provider_source = 'CDC'"
                )
                cursor.execute("DELETE FROM silver_cdc.fact_health_observation")
                cursor.execute("DELETE FROM silver_cdc.observation_revision")
                cursor.execute("DELETE FROM silver_cdc.observation_quarantine")
                cursor.execute("DELETE FROM silver_cdc.dim_measure")
                cursor.execute("DELETE FROM silver_cdc.dim_stratum")
                cursor.execute("DELETE FROM silver_cdc.dim_dataset_release")
                cursor.execute("DELETE FROM control.cdc_dataset_release")
                for geo_id in sorted(tracked_geo_ids - preexisting):
                    delete_geography(cursor, geo_id)
            database_connection.commit()
        except BaseException:
            database_connection.rollback()
            raise
        finally:
            database_connection.close()

    request.addfinalizer(cleanup)

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            seed_geography(
                cursor, geo_type="nation", vintage=2020, name="United States"
            )
            seed_geography(
                cursor,
                geo_type="state",
                state_fips="01",
                vintage=2020,
                name="Alabama",
            )
            seed_geography(
                cursor,
                geo_type="county",
                state_fips="01",
                county_fips="001",
                vintage=2020,
                name="Autauga County",
            )
        writer.commit()
    finally:
        writer.close()

    # Release A: healthy, gated, published.
    metadata_a, watermark_a = _cdi_metadata_with_watermark(0)
    release_a = _capture_and_conform(postgres_connection_factory, metadata_a)

    gate_connection = postgres_connection_factory()
    try:
        decision = evaluate_publication_gate(
            gate_connection,
            source_code="CDC",
            code_commit_sha=COMMIT_SHA,
            executors=build_cdc_gate_executors("cdi", watermark_a),
            scope={
                "source_code": "CDC",
                "asset_id": "cdi",
                "release_watermark": watermark_a,
            },
        )
        gate_connection.commit()
    finally:
        gate_connection.close()
    assert decision.publishable, decision.record.failure_summary
    publish_release(
        postgres_connection_factory,
        run_id=release_a.run_id,
        asset_id="cdi",
        release_watermark=watermark_a,
    )

    # Release B: conformed, then damaged before publication.
    metadata_b, watermark_b = _cdi_metadata_with_watermark(+1000)
    release_b = _capture_and_conform(postgres_connection_factory, metadata_b)
    scope_b = {
        "source_code": "CDC",
        "asset_id": "cdi",
        "release_watermark": watermark_b,
    }

    def gate_b() -> tuple[bool, str]:
        database_connection = postgres_connection_factory()
        try:
            verdict = evaluate_publication_gate(
                database_connection,
                source_code="CDC",
                code_commit_sha=COMMIT_SHA,
                executors=build_cdc_gate_executors("cdi", watermark_b),
                scope=scope_b,
            )
            database_connection.commit()
            return verdict.publishable, verdict.record.quality_run_id
        finally:
            database_connection.close()

    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            # Injected loss: one conformed fact disappears.
            cursor.execute(
                """
                DELETE FROM silver_cdc.fact_health_observation
                 WHERE asset_id = 'cdi' AND release_watermark = %s
                   AND source_record_id = (
                       SELECT MIN(source_record_id)
                         FROM silver_cdc.fact_health_observation
                        WHERE asset_id = 'cdi' AND release_watermark = %s
                   )
                """,
                (watermark_b, watermark_b),
            )
        writer.commit()
    finally:
        writer.close()

    publishable, loss_run_id = gate_b()
    assert not publishable

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT observed_count, expected_count, evidence
                  FROM control.data_quality_result
                 WHERE quality_run_id = %s AND rule_id = 'DQ-CDC-003'
                   AND object_name = 'silver_cdc.fact_health_observation'
                """,
                (loss_run_id,),
            )
            observed, expected, evidence = cursor.fetchone()
            assert (observed, expected) == (2, 3)
            assert "facts=2" in evidence
            # The prior published release keeps serving through gold.
            cursor.execute(
                """
                SELECT release_watermark, COUNT(*)
                  FROM gold_cdc.latest_release_observation
                 GROUP BY release_watermark
                """
            )
            assert cursor.fetchall() == [(watermark_a, 3)]
    finally:
        reader.close()

    # Repair the loss (transform is idempotent), then inject duplication.
    transform_release(
        postgres_connection_factory,
        run_id=release_b.run_id,
        asset=CDI_ASSET,
        release_watermark=watermark_b,
    )
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_cdc.fact_health_observation (
                    asset_id, release_watermark, source_record_id,
                    source_run_id, capture_id, source_row_index, measure_id,
                    value_type_id, stratum_id, period_start, period_end,
                    geo_id, geo_sk, geo_type, geography_status, value_source,
                    value, value_status, unit, adjustment_status,
                    confidence_lower, confidence_upper, footnote_code,
                    footnote_text, estimate_method, population_basis,
                    total_population, population_18_plus,
                    transformation_version
                )
                SELECT asset_id, release_watermark, REPEAT('f', 64),
                       source_run_id, capture_id, 9999, measure_id,
                       value_type_id, stratum_id, period_start, period_end,
                       geo_id, geo_sk, geo_type, geography_status,
                       value_source, value, value_status, unit,
                       adjustment_status, confidence_lower, confidence_upper,
                       footnote_code, footnote_text, estimate_method,
                       population_basis, total_population, population_18_plus,
                       transformation_version
                  FROM silver_cdc.fact_health_observation
                 WHERE asset_id = 'cdi' AND release_watermark = %s
                 LIMIT 1
                """,
                (watermark_b,),
            )
        writer.commit()
    finally:
        writer.close()

    publishable, duplication_run_id = gate_b()
    assert not publishable

    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT observed_count, expected_count
                  FROM control.data_quality_result
                 WHERE quality_run_id = %s AND rule_id = 'DQ-CDC-003'
                   AND object_name = 'silver_cdc.fact_health_observation'
                """,
                (duplication_run_id,),
            )
            assert cursor.fetchone() == (4, 3)
    finally:
        reader.close()

    # Remove the duplicate; the gate opens and release B publishes.
    writer = postgres_connection_factory()
    try:
        with writer.cursor() as cursor:
            cursor.execute(
                "DELETE FROM silver_cdc.fact_health_observation"
                " WHERE source_record_id = REPEAT('f', 64)"
            )
        writer.commit()
    finally:
        writer.close()

    publishable, _ = gate_b()
    assert publishable
    publish_release(
        postgres_connection_factory,
        run_id=release_b.run_id,
        asset_id="cdi",
        release_watermark=watermark_b,
    )
    reader = postgres_connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT release_watermark"
                " FROM gold_cdc.latest_release_observation"
            )
            assert cursor.fetchall() == [(watermark_b,)]
    finally:
        reader.close()
