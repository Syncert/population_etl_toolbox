"""Replay one captured BEA table into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per run: the line dimension, the parsed revisions and
quarantine, the geography resolution ledger and the conformed facts land
together or not at all, and the run is only marked ready when the counts
reconcile.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..capture import PARSER_CONTRACT_VERSION
from ..config import SOURCE_CODE
from ..registry import get_table
from .parse import parse_table

OBSERVATION_BASIS = (
    "BEA regional economic accounts: the county as an economy (income and "
    "production by place of residence or of work, as the line states), not a "
    "survey of residents"
)
METHODOLOGY_URL = (
    "https://www.bea.gov/resources/methodologies/local-area-personal-income"
)


class BeaReconciliationError(RuntimeError):
    """A replayed run does not account for every in-scope captured row."""


class BeaPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform a captured table; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_code, capture_id::TEXT FROM control.bea_table_capture WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise BeaReconciliationError("run has no captured BEA table")
            table_code, capture_id = row
            table = get_table(table_code)
            parsed = parse_table(
                load_captured_payload(connection_factory, UUID(capture_id)), table=table
            )
            lines = {}
            for observation in parsed.observations:
                lines.setdefault(
                    observation.line_code, (observation.description, observation.unit)
                )
            if lines:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_bea.dim_line (
                        table_code, line_code, table_title, description, unit,
                        dollar_basis, observation_basis, methodology_url,
                        parser_contract_version
                    ) VALUES %s
                    ON CONFLICT (table_code, line_code) DO UPDATE SET
                        table_title = EXCLUDED.table_title,
                        description = EXCLUDED.description,
                        unit = EXCLUDED.unit,
                        dollar_basis = EXCLUDED.dollar_basis,
                        observation_basis = EXCLUDED.observation_basis,
                        methodology_url = EXCLUDED.methodology_url,
                        parser_contract_version = EXCLUDED.parser_contract_version,
                        updated_at = NOW()
                    """,
                    [
                        (
                            table.code,
                            line,
                            table.title,
                            description,
                            unit,
                            table.lines[line],
                            OBSERVATION_BASIS,
                            METHODOLOGY_URL,
                            PARSER_CONTRACT_VERSION,
                        )
                        for line, (description, unit) in sorted(lines.items())
                    ],
                )
            if parsed.observations:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_bea.observation_revision (
                        capture_id, source_row_index, year, run_id, table_code,
                        line_code, geo_type, geo_source_code, geo_source_label,
                        geo_id, value_source, value, value_status, source_record_id
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index, year) DO NOTHING
                    """,
                    [
                        (
                            capture_id,
                            obs.source_row_index,
                            obs.year,
                            str(run_id),
                            table.code,
                            obs.line_code,
                            obs.geo_type,
                            obs.geo_source_code,
                            obs.geo_source_label,
                            obs.geo_id,
                            obs.value_source,
                            obs.value,
                            obs.value_status,
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
                    INSERT INTO silver_bea.observation_quarantine (
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
                UPDATE control.bea_table_capture
                   SET member_name = %s, release_date = %s, captured_row_count = %s,
                       in_scope_row_count = %s, status = %s, updated_at = NOW()
                 WHERE run_id = %s AND status <> 'published'
                """,
                (
                    parsed.member,
                    parsed.release_date,
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
                       %s, revision.table_code, revision.geo_type, revision.geo_source_code,
                       revision.geo_source_label, %s, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_bea.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.geo_source_code, revision.capture_id
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
                    parsed.release_date.year if parsed.release_date else None,
                    str(run_id),
                ),
            )
            cursor.execute(
                """
                INSERT INTO silver_bea.fact_observation (
                    table_code, line_code, geo_id, year, capture_id, run_id,
                    release_date, retrieved_at, geo_sk, geo_type, geography_status,
                    value_source, value, value_status, source_record_id
                )
                SELECT revision.table_code, revision.line_code, revision.geo_id, revision.year,
                       revision.capture_id, revision.run_id, %s, capture.retrieved_at,
                       entity.geo_sk, revision.geo_type,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.value_status,
                       revision.source_record_id
                FROM silver_bea.observation_revision AS revision
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (table_code, line_code, geo_id, year, capture_id) DO NOTHING
                """,
                (parsed.release_date, str(run_id)),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_bea.observation_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_bea.fact_observation WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if revisions != len(parsed.observations) or facts != revisions:
                raise BeaReconciliationError(
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
    """Expose a reconciled table through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.bea_table_capture
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.bea_table_capture WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise BeaPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(connection_factory, publisher_schema="gold_bea")
    return published
