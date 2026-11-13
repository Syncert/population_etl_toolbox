"""Replay one captured 2020 Census run into silver, reconcile it, and publish it.

Everything here reads the committed captures, never the network. One
transaction per run: the variable dimension, the parsed revisions and
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
from ..registry import CENSUS_YEAR, DecennialDataset, DecennialSlice
from .parse import parse_slice

#: The silver_ref geography a fact resolves to; a block has none yet, so it
#: is recorded `unsupported` and never served.
_RESOLVED_GEO_TYPES = ("nation", "state", "county", "tract")


class DecennialReconciliationError(RuntimeError):
    """A replayed run does not account for every captured row."""


class DecennialPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _slices(cursor: Any, run_id: UUID) -> list[tuple[str, str, str, str]]:
    cursor.execute(
        """
        SELECT geo_level, scope, capture_id::TEXT, status
        FROM control.census_decennial_slice
        WHERE run_id = %s
        ORDER BY geo_level, scope
        """,
        (str(run_id),),
    )
    return list(cursor.fetchall())


def _upsert_variables(cursor: Any, dataset: DecennialDataset) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_census_decennial.dim_variable (
            dataset_id, variable_id, variable_label, universe, unit,
            parent_variable, methodology_url, parser_contract_version
        ) VALUES %s
        ON CONFLICT (dataset_id, variable_id) DO UPDATE SET
            variable_label = EXCLUDED.variable_label,
            universe = EXCLUDED.universe,
            unit = EXCLUDED.unit,
            parent_variable = EXCLUDED.parent_variable,
            methodology_url = EXCLUDED.methodology_url,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        -- Only a row whose content changed is rewritten (refresh-churn audit).
        WHERE (silver_census_decennial.dim_variable.variable_label, silver_census_decennial.dim_variable.universe, silver_census_decennial.dim_variable.unit, silver_census_decennial.dim_variable.parent_variable, silver_census_decennial.dim_variable.methodology_url, silver_census_decennial.dim_variable.parser_contract_version)
              IS DISTINCT FROM
              (EXCLUDED.variable_label, EXCLUDED.universe, EXCLUDED.unit, EXCLUDED.parent_variable, EXCLUDED.methodology_url, EXCLUDED.parser_contract_version)
        """,
        [
            (
                dataset.dataset_id,
                variable.variable_id,
                variable.label,
                variable.universe,
                variable.unit,
                variable.parent_variable,
                dataset.methodology_url,
                dataset.parser_contract_version,
            )
            for variable in dataset.variables
        ],
    )


def replay_run(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    dataset: DecennialDataset,
) -> int:
    """Parse and conform every captured slice of a run; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            slices = _slices(cursor, run_id)
            if not slices:
                raise DecennialReconciliationError(
                    "run has no captured 2020 Census slices"
                )
            _upsert_variables(cursor, dataset)
            expected_revisions = 0
            for geo_level, scope, capture_id, status in slices:
                if status in ("empty", "unchanged"):
                    continue
                payload = load_captured_payload(connection_factory, UUID(capture_id))
                parsed = parse_slice(
                    dataset, slice_=DecennialSlice(geo_level, scope), payload=payload
                )
                if parsed.counts:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_census_decennial.observation_revision (
                            capture_id, source_row_index, variable_id, run_id,
                            dataset_id, geo_type, geo_source_code, geo_source_label,
                            geo_id, value_source, value, value_status,
                            source_record_id, source_record
                        ) VALUES %s
                        ON CONFLICT (capture_id, source_row_index, variable_id) DO NOTHING
                        """,
                        [
                            (
                                capture_id,
                                item.source_row_index,
                                item.variable_id,
                                str(run_id),
                                dataset.dataset_id,
                                item.geo_type,
                                item.geo_source_code,
                                item.geo_source_label,
                                item.geo_id,
                                item.value_source,
                                item.value,
                                item.value_status,
                                item.source_record_id,
                                Json(item.source_record),
                            )
                            for item in parsed.counts
                        ],
                        page_size=5000,
                    )
                if parsed.quarantined:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_census_decennial.observation_quarantine (
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
                    dataset.variables
                )
                cursor.execute(
                    """
                    UPDATE control.census_decennial_slice
                       SET captured_row_count = %s,
                           status = %s,
                           updated_at = NOW()
                     WHERE run_id = %s AND geo_level = %s AND scope = %s
                       AND status <> 'published'
                    """,
                    (
                        parsed.row_count,
                        "quarantined" if payload_rejected else "silver_ready",
                        str(run_id),
                        geo_level,
                        scope,
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
                       %s, revision.dataset_id, revision.geo_type,
                       revision.geo_source_code, revision.geo_source_label,
                       %s, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN revision.geo_type <> ALL(%s) THEN 'unsupported'
                            WHEN entity.geo_sk IS NULL THEN 'unmapped'
                            ELSE 'resolved' END,
                       CASE WHEN revision.geo_type <> ALL(%s) THEN 'block_geography_not_registered'
                            WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_census_decennial.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity
                  ON entity.geo_id = revision.geo_id
                 AND revision.geo_type = ANY(%s)
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
                -- Only a row whose content changed is rewritten (refresh-churn audit).
                WHERE (silver_ref.geography_resolution.source_label, silver_ref.geography_resolution.geo_sk, silver_ref.geography_resolution.resolution_method, silver_ref.geography_resolution.status, silver_ref.geography_resolution.reason_code)
                      IS DISTINCT FROM
                      (EXCLUDED.source_label, EXCLUDED.geo_sk, EXCLUDED.resolution_method, EXCLUDED.status, EXCLUDED.reason_code)
                """,
                (
                    SOURCE_CODE,
                    CENSUS_YEAR,
                    list(_RESOLVED_GEO_TYPES),
                    list(_RESOLVED_GEO_TYPES),
                    list(_RESOLVED_GEO_TYPES),
                    str(run_id),
                ),
            )
            cursor.execute(
                """
                INSERT INTO silver_census_decennial.fact_count (
                    dataset_id, variable_id, geo_id, capture_id, run_id,
                    retrieved_at, geo_sk, geo_type, geography_status,
                    value_source, value, value_status, source_record_id
                )
                SELECT revision.dataset_id, revision.variable_id, revision.geo_id,
                       revision.capture_id, revision.run_id, capture.retrieved_at,
                       entity.geo_sk, revision.geo_type,
                       CASE WHEN revision.geo_type <> ALL(%s) THEN 'unsupported'
                            WHEN entity.geo_sk IS NULL THEN 'unmapped'
                            ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.value_status,
                       revision.source_record_id
                FROM silver_census_decennial.observation_revision AS revision
                JOIN raw_capture.response_capture AS capture
                  ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity
                  ON entity.geo_id = revision.geo_id
                 AND revision.geo_type = ANY(%s)
                WHERE revision.run_id = %s
                ON CONFLICT (dataset_id, variable_id, geo_id, capture_id) DO NOTHING
                """,
                (list(_RESOLVED_GEO_TYPES), list(_RESOLVED_GEO_TYPES), str(run_id)),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_census_decennial.observation_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_census_decennial.fact_count WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if revisions != expected_revisions or facts != revisions:
                raise DecennialReconciliationError(
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
                UPDATE control.census_decennial_slice
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                """,
                (str(run_id),),
            )
            cursor.execute(
                """
                SELECT COUNT(*) FILTER (WHERE status = 'captured'),
                       COUNT(*) FILTER (WHERE status = 'published')
                FROM control.census_decennial_slice WHERE run_id = %s
                """,
                (str(run_id),),
            )
            unreplayed, published = cursor.fetchone()
            if unreplayed:
                raise DecennialPublicationError(
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
            connection_factory, publisher_schema="gold_census_decennial"
        )
    return int(published)
