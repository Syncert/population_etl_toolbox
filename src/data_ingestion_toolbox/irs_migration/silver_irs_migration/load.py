"""Replay one captured SOI migration file into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per run: the parsed rows, the quarantine, the conformed flows
and the geography resolution ledger land together or not at all, and the
run is only marked ready when every captured row is accounted for as a
flow, a refusal or a quarantined row.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..registry import get_file
from .conform import conform_flows
from .parse import parse_file


class IrsMigrationReconciliationError(RuntimeError):
    """A replayed run does not account for every captured row."""


class IrsMigrationPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _resolved_geographies(cursor: Any, geo_ids: set[str]) -> dict[str, int]:
    if not geo_ids:
        return {}
    cursor.execute(
        "SELECT geo_id, geo_sk FROM silver_ref.dim_geo_entity WHERE geo_id = ANY(%s)",
        (sorted(geo_ids),),
    )
    return {geo_id: int(geo_sk) for geo_id, geo_sk in cursor.fetchall()}


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform a captured file; return the flow count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT file.direction, file.year_pair, file.capture_id::TEXT, capture.retrieved_at
                FROM control.irs_migration_file AS file
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = file.capture_id
                WHERE file.run_id = %s
                """,
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise IrsMigrationReconciliationError(
                    "run has no captured SOI migration file"
                )
            direction, year_pair, capture_id, retrieved_at = row
            item = get_file(direction, year_pair)
            parsed = parse_file(
                load_captured_payload(connection_factory, UUID(capture_id)), item=item
            )
            if parsed.flows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_irs_migration.flow_revision (
                        capture_id, source_row_index, run_id, direction, year_pair,
                        subject_geo_id, category, counterpart_code, counterpart_state_abbr,
                        counterpart_label, counterpart_geo_id, origin_geo_id,
                        destination_geo_id, returns, individuals, agi, value_status,
                        value_source, source_record_id
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index) DO NOTHING
                    """,
                    [
                        (
                            capture_id,
                            flow.source_row_index,
                            str(run_id),
                            item.direction,
                            item.year_pair,
                            flow.subject_geo_id,
                            flow.category,
                            flow.counterpart_code,
                            flow.counterpart_state_abbr,
                            flow.counterpart_label,
                            flow.counterpart_geo_id,
                            flow.origin_geo_id,
                            flow.destination_geo_id,
                            flow.returns,
                            flow.individuals,
                            flow.agi,
                            flow.value_status,
                            flow.value_source,
                            flow.source_record_id,
                        )
                        for flow in parsed.flows
                    ],
                    page_size=10000,
                )
            named = {flow.subject_geo_id for flow in parsed.flows} | {
                geo_id
                for flow in parsed.flows
                for geo_id in (flow.origin_geo_id, flow.destination_geo_id)
                if geo_id is not None
            }
            geo_sk_by_id = _resolved_geographies(cursor, named)
            admitted, refused = conform_flows(parsed.flows, geo_sk_by_id)
            quarantine = [
                (q.source_row_index, q.error_code, q.error_summary)
                for q in parsed.quarantined
            ] + [
                (r.flow.source_row_index, r.error_code, r.error_summary)
                for r in refused
            ]
            if quarantine:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_irs_migration.flow_quarantine (
                        run_id, capture_id, source_row_index, error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
                    """,
                    [
                        (str(run_id), capture_id, index, code, summary)
                        for index, code, summary in quarantine
                    ],
                )
            if admitted:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_irs_migration.fact_flow (
                        direction, year_pair, subject_geo_id, counterpart_code, capture_id,
                        run_id, year1, year2, retrieved_at, category, counterpart_label,
                        subject_geo_sk, origin_geo_id, origin_geo_sk, destination_geo_id,
                        destination_geo_sk, returns, individuals, agi, value_status,
                        value_source, source_record_id
                    ) VALUES %s
                    ON CONFLICT (direction, year_pair, subject_geo_id, counterpart_code, capture_id) DO NOTHING
                    """,
                    [
                        (
                            item.direction,
                            item.year_pair,
                            conformed.flow.subject_geo_id,
                            conformed.flow.counterpart_code,
                            capture_id,
                            str(run_id),
                            item.year1,
                            item.year2,
                            retrieved_at,
                            conformed.flow.category,
                            conformed.flow.counterpart_label,
                            conformed.subject_geo_sk,
                            conformed.flow.origin_geo_id,
                            conformed.origin_geo_sk,
                            conformed.flow.destination_geo_id,
                            conformed.destination_geo_sk,
                            conformed.flow.returns,
                            conformed.flow.individuals,
                            conformed.flow.agi,
                            conformed.flow.value_status,
                            conformed.flow.value_source,
                            conformed.flow.source_record_id,
                        )
                        for conformed in admitted
                    ],
                    page_size=10000,
                )
            payload_rejected = any(q.source_row_index == 0 for q in parsed.quarantined)
            cursor.execute(
                """
                UPDATE control.irs_migration_file
                   SET captured_row_count = %s, parsed_row_count = %s, refused_row_count = %s,
                       status = %s, updated_at = NOW()
                 WHERE run_id = %s AND status <> 'published'
                """,
                (
                    parsed.row_count,
                    len(parsed.flows),
                    len(refused),
                    "quarantined" if payload_rejected else "silver_ready",
                    str(run_id),
                ),
            )
            # One ledger row per county the file names, as either side.
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (named.geo_id)
                       %s, %s, 'county', named.geo_id, named.label, %s, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       %s::UUID,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM (
                    SELECT subject_geo_id AS geo_id, NULL::TEXT AS label
                    FROM silver_irs_migration.flow_revision WHERE run_id = %s
                    UNION ALL
                    SELECT counterpart_geo_id, counterpart_label
                    FROM silver_irs_migration.flow_revision
                    WHERE run_id = %s AND counterpart_geo_id IS NOT NULL
                ) AS named
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = named.geo_id
                ORDER BY named.geo_id, named.label NULLS LAST
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
                (
                    SOURCE_CODE,
                    f"county_{item.direction}",
                    item.year2,
                    capture_id,
                    str(run_id),
                    str(run_id),
                ),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_irs_migration.flow_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_irs_migration.fact_flow WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if (
                revisions != len(parsed.flows)
                or facts != len(admitted)
                or facts + len(refused) != revisions
                or revisions
                + len([q for q in parsed.quarantined if q.source_row_index > 0])
                != parsed.row_count
            ):
                raise IrsMigrationReconciliationError(
                    f"run {run_id} reconciles {revisions} rows, {facts} flows and "
                    f"{len(refused)} refusals against {parsed.row_count} captured"
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
                UPDATE control.irs_migration_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.irs_migration_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise IrsMigrationPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_irs_migration"
        )
    return published
