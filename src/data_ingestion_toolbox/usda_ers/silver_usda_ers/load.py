"""Replay one captured USDA ERS file into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per run: the parsed cells, the quarantine, the geography
resolution ledger and the conformed facts land together or not at all, and
the run is only marked ready when the counts reconcile. A run the capture
found ``unchanged`` replays nothing.
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


class ErsReconciliationError(RuntimeError):
    """A replayed run does not account for every parsed cell."""


class ErsPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform a captured file; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT product || ':' || edition, capture_id::TEXT, status FROM control.usda_ers_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise ErsReconciliationError("run has no captured USDA ERS file")
            key, capture_id, status = row
            if status == "unchanged":
                connection.commit()
                return 0
            item = get_file(key)
            parsed = parse_file(
                load_captured_payload(connection_factory, UUID(capture_id)), item=item
            )
            if parsed.observations:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_usda_ers.observation_revision (
                        capture_id, source_row_index, attribute, run_id, measure, year, fips_code,
                        geo_id, value_source, value, value_status, missing_reason, code_label,
                        source_record_id
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index) DO NOTHING
                    """,
                    [
                        (
                            capture_id,
                            obs.source_row_index,
                            obs.attribute,
                            str(run_id),
                            obs.measure,
                            obs.year,
                            obs.fips_code,
                            obs.geo_id,
                            obs.value_source,
                            obs.value,
                            obs.value_status,
                            obs.missing_reason,
                            obs.code_label,
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
                    INSERT INTO silver_usda_ers.observation_quarantine (
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
            refused = any(q.source_row_index == 0 for q in parsed.quarantined)
            cursor.execute(
                """
                UPDATE control.usda_ers_file
                   SET row_count = %s, in_scope_row_count = %s, status = %s, updated_at = NOW()
                 WHERE run_id = %s AND status = 'captured'
                """,
                (
                    parsed.row_count,
                    parsed.in_scope_row_count,
                    "quarantined" if refused else "silver_ready",
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
                SELECT DISTINCT ON (revision.fips_code)
                       %s, %s, 'county', revision.fips_code, NULL, %s, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_usda_ers.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.fips_code, revision.capture_id
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                """,
                (
                    SOURCE_CODE,
                    f"ers_{item.product}",
                    int(item.edition[:4]),
                    str(run_id),
                ),
            )
            cursor.execute(
                """
                INSERT INTO silver_usda_ers.fact_observation (
                    attribute, geo_id, capture_id, run_id, product, edition, measure, year,
                    retrieved_at, fips_code, geo_sk, geography_status, value_source, value,
                    value_status, missing_reason, code_label, source_record_id
                )
                SELECT revision.attribute, revision.geo_id, revision.capture_id, revision.run_id,
                       file.product, file.edition, revision.measure, revision.year,
                       capture.retrieved_at, revision.fips_code, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.value_status,
                       revision.missing_reason, revision.code_label, revision.source_record_id
                FROM silver_usda_ers.observation_revision AS revision
                JOIN control.usda_ers_file AS file ON file.run_id = revision.run_id
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s AND file.status = 'silver_ready'
                ON CONFLICT (attribute, geo_id, capture_id) DO NOTHING
                """,
                (str(run_id),),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_usda_ers.observation_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_usda_ers.fact_observation WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if revisions != len(parsed.observations) or (
                not refused and facts != revisions
            ):
                raise ErsReconciliationError(
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
                UPDATE control.usda_ers_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.usda_ers_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise ErsPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_usda_ers"
        )
    return published
