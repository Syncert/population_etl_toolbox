"""Replay one captured run into silver, reconcile it, and publish it.

Everything here reads the committed captures, never the network. One
transaction per run: the measure dimension, the parsed revisions and
quarantine, the geography resolution ledger and the conformed facts land
together or not at all, and the run is only marked ready when the counts
reconcile.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import Json, execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..registry import SaeDataset
from .parse import parse_slice


class SaeReconciliationError(RuntimeError):
    """A replayed run does not account for every captured row."""


class SaePublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _slices(cursor: Any, run_id: UUID) -> list[tuple[str, int, str, str]]:
    cursor.execute(
        """
        SELECT geo_level, estimate_year, capture_id::TEXT, status
        FROM control.census_sae_slice
        WHERE run_id = %s
        ORDER BY geo_level
        """,
        (str(run_id),),
    )
    return list(cursor.fetchall())


def replay_run(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    dataset: SaeDataset,
) -> int:
    """Parse and conform every captured slice of a run; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            slices = _slices(cursor, run_id)
            if not slices:
                raise SaeReconciliationError("run has no captured SAIPE/SAHIE slices")
            execute_values(
                cursor,
                """
                INSERT INTO silver_census_sae.dim_measure (
                    dataset_id, measure_id, measure_label, unit, universe,
                    estimate_method, methodology_url, parser_contract_version
                ) VALUES %s
                ON CONFLICT (dataset_id, measure_id) DO UPDATE SET
                    measure_label = EXCLUDED.measure_label,
                    unit = EXCLUDED.unit,
                    universe = EXCLUDED.universe,
                    estimate_method = EXCLUDED.estimate_method,
                    methodology_url = EXCLUDED.methodology_url,
                    parser_contract_version = EXCLUDED.parser_contract_version,
                    updated_at = NOW()
                """,
                [
                    (
                        dataset.dataset_id,
                        measure.measure_id,
                        measure.label,
                        measure.unit,
                        measure.universe,
                        dataset.estimate_method,
                        dataset.methodology_url,
                        dataset.parser_contract_version,
                    )
                    for measure in dataset.measures
                ],
            )
            expected_revisions = 0
            for geo_level, estimate_year, capture_id, status in slices:
                if status == "empty":
                    continue
                payload = load_captured_payload(connection_factory, UUID(capture_id))
                parsed = parse_slice(
                    dataset,
                    geo_level=geo_level,
                    estimate_year=estimate_year,
                    payload=payload,
                )
                if parsed.estimates:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_census_sae.observation_revision (
                            capture_id, source_row_index, measure_id, run_id,
                            dataset_id, estimate_year, geo_type, geo_source_code,
                            geo_source_label, geo_id, value_source, value,
                            value_status, confidence_lower, confidence_upper,
                            margin_of_error, source_record_id, source_record
                        ) VALUES %s
                        ON CONFLICT (capture_id, source_row_index, measure_id) DO NOTHING
                        """,
                        [
                            (
                                capture_id,
                                item.source_row_index,
                                item.measure_id,
                                str(run_id),
                                dataset.dataset_id,
                                item.estimate_year,
                                item.geo_type,
                                item.geo_source_code,
                                item.geo_source_label,
                                item.geo_id,
                                item.value_source,
                                item.value,
                                item.value_status,
                                item.confidence_lower,
                                item.confidence_upper,
                                item.margin_of_error,
                                item.source_record_id,
                                Json(item.source_record),
                            )
                            for item in parsed.estimates
                        ],
                    )
                if parsed.quarantined:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_census_sae.observation_quarantine (
                            run_id, capture_id, source_row_index, error_code, error_summary
                        ) VALUES %s
                        ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
                        """,
                        [
                            (
                                str(run_id),
                                capture_id,
                                item.source_row_index,
                                item.error_code,
                                item.error_summary,
                            )
                            for item in parsed.quarantined
                        ],
                    )
                payload_rejected = any(
                    item.source_row_index == 0 for item in parsed.quarantined
                )
                quarantined_rows = len(
                    {
                        item.source_row_index
                        for item in parsed.quarantined
                        if item.source_row_index > 0
                    }
                )
                expected_revisions += (parsed.row_count - quarantined_rows) * len(
                    dataset.measures
                )
                cursor.execute(
                    """
                    UPDATE control.census_sae_slice
                       SET captured_row_count = %s,
                           status = %s,
                           updated_at = NOW()
                     WHERE run_id = %s AND geo_level = %s AND status <> 'published'
                    """,
                    (
                        parsed.row_count,
                        "quarantined" if payload_rejected else "silver_ready",
                        str(run_id),
                        geo_level,
                    ),
                )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (revision.geo_type, revision.geo_source_code, revision.estimate_year)
                       %s, revision.dataset_id, revision.geo_type,
                       revision.geo_source_code, revision.geo_source_label,
                       revision.estimate_year, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_census_sae.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.geo_source_code, revision.estimate_year, revision.capture_id
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    source_label = EXCLUDED.source_label,
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                """,
                (SOURCE_CODE, str(run_id)),
            )
            cursor.execute(
                """
                INSERT INTO silver_census_sae.fact_estimate (
                    dataset_id, measure_id, estimate_year, geo_id, capture_id,
                    run_id, retrieved_at, geo_sk, geo_type, geography_status,
                    value_source, value, value_status, confidence_lower,
                    confidence_upper, margin_of_error, source_record_id
                )
                SELECT revision.dataset_id, revision.measure_id, revision.estimate_year,
                       revision.geo_id, revision.capture_id, revision.run_id,
                       capture.retrieved_at, entity.geo_sk, revision.geo_type,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.value_status,
                       revision.confidence_lower, revision.confidence_upper,
                       revision.margin_of_error, revision.source_record_id
                FROM silver_census_sae.observation_revision AS revision
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (dataset_id, measure_id, estimate_year, geo_id, capture_id) DO NOTHING
                """,
                (str(run_id),),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_census_sae.observation_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_census_sae.fact_estimate WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if revisions != expected_revisions or facts != revisions:
                raise SaeReconciliationError(
                    f"run {run_id} reconciles {revisions} revisions and {facts} facts "
                    f"against {expected_revisions} expected"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return facts


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled run's slices through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.census_sae_slice
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                """,
                (str(run_id),),
            )
            cursor.execute(
                """
                SELECT COUNT(*) FILTER (WHERE status IN ('captured')),
                       COUNT(*) FILTER (WHERE status = 'published')
                FROM control.census_sae_slice WHERE run_id = %s
                """,
                (str(run_id),),
            )
            unreplayed, published = cursor.fetchone()
            if unreplayed:
                raise SaePublicationError(
                    f"run {run_id} has slices that were never replayed"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_census_sae"
        )
    return int(published)
