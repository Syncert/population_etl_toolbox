"""Replay one captured County Business Patterns file into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per run: the parsed cells, the quarantine, the geography
resolution ledger and the conformed facts land together or not at all, and
the run is only marked ready when the counts reconcile.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..registry import get_file
from .parse import parse_file


class CbpReconciliationError(RuntimeError):
    """A replayed run does not account for every in-scope captured cell."""


class CbpPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform a captured file; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT file.kind, file.year, file.capture_id::TEXT, capture.retrieved_at
                FROM control.census_cbp_file AS file
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = file.capture_id
                WHERE file.run_id = %s
                """,
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise CbpReconciliationError(
                    "run has no captured County Business Patterns file"
                )
            kind, year, capture_id, _retrieved_at = row
            item = get_file(kind, int(year))
            parsed = parse_file(
                load_captured_payload(connection_factory, UUID(capture_id)), item=item
            )
            if parsed.observations:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_census_cbp.observation_revision (
                        capture_id, source_row_index, measure, run_id, year, naics_code,
                        naics_key, geo_type, geo_source_code, geo_id, value_source, value,
                        value_status, noise_flag, employment_range, source_record_id
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index, measure) DO NOTHING
                    """,
                    [
                        (
                            capture_id,
                            obs.source_row_index,
                            obs.measure,
                            str(run_id),
                            item.year,
                            obs.naics_code,
                            obs.naics_key,
                            obs.geo_type,
                            obs.geo_source_code,
                            obs.geo_id,
                            obs.value_source,
                            obs.value,
                            obs.value_status,
                            obs.noise_flag,
                            obs.employment_range,
                            obs.source_record_id,
                        )
                        for obs in parsed.observations
                    ],
                    page_size=10000,
                )
            if parsed.quarantined:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_census_cbp.observation_quarantine (
                        run_id, capture_id, source_row_index, error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            capture_id,
                            q.source_row_index,
                            q.error_code,
                            q.error_summary,
                        )
                        for q in parsed.quarantined
                    ],
                )
            payload_rejected = any(q.source_row_index == 0 for q in parsed.quarantined)
            cursor.execute(
                """
                UPDATE control.census_cbp_file
                   SET captured_row_count = %s, in_scope_row_count = %s, status = %s, updated_at = NOW()
                 WHERE run_id = %s AND status <> 'published'
                """,
                (
                    parsed.row_count,
                    parsed.in_scope_row_count,
                    "quarantined" if payload_rejected else "silver_ready",
                    str(run_id),
                ),
            )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (revision.geo_type, revision.geo_source_code)
                       %s, %s, revision.geo_type, revision.geo_source_code,
                       NULL, %s, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_census_cbp.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.geo_source_code, revision.capture_id
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                """,
                (SOURCE_CODE, f"cbp_{item.kind}", item.year, str(run_id)),
            )
            cursor.execute(
                """
                INSERT INTO silver_census_cbp.fact_observation (
                    measure, naics_key, geo_id, year, capture_id, run_id, naics_code,
                    retrieved_at, geo_sk, geo_type, geography_status, value_source,
                    value, value_status, noise_flag, employment_range, source_record_id
                )
                SELECT revision.measure, revision.naics_key, revision.geo_id, revision.year,
                       revision.capture_id, revision.run_id, revision.naics_code,
                       capture.retrieved_at, entity.geo_sk, revision.geo_type,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.value_status,
                       revision.noise_flag, revision.employment_range, revision.source_record_id
                FROM silver_census_cbp.observation_revision AS revision
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (measure, naics_key, geo_id, year, capture_id) DO NOTHING
                """,
                (str(run_id),),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_census_cbp.observation_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_census_cbp.fact_observation WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if revisions != len(parsed.observations) or facts != revisions:
                raise CbpReconciliationError(
                    f"run {run_id} reconciles {revisions} revisions and {facts} facts "
                    f"against {len(parsed.observations)} parsed"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return facts


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled file through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.census_cbp_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.census_cbp_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise CbpPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_census_cbp"
        )
    return published
