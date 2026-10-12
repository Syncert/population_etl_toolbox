"""Replay one captured Building Permits run into silver, reconcile it, publish it.

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

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..capture import PARSER_CONTRACT_VERSION
from ..config import SOURCE_CODE
from ..registry import LAYOUTS, MEASURES, STRUCTURE_TYPES, BpsSlice
from .parse import parse_file

OBSERVATION_BASIS = (
    "authorized by building permits: housing units a permit allows, "
    "not started or completed"
)
METHODOLOGY_URL = "https://www.census.gov/construction/bps/methodology.html"


class BpsReconciliationError(RuntimeError):
    """A replayed run does not account for every in-scope captured row."""


class BpsPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def slice_from_row(slice_key: str, frequency: str, year: int, month: int) -> BpsSlice:
    if slice_key.startswith("place:"):
        return BpsSlice("place", frequency, year, month, slice_key.split(":", 1)[1])
    return BpsSlice(slice_key, frequency, year, month)


def _values_per_row(item: BpsSlice) -> int:
    measures = [
        m
        for m in MEASURES
        if m[0] != "valuation" or LAYOUTS[item.kind].registers_valuation
    ]
    return len(measures) * len(STRUCTURE_TYPES)


def _upsert_dimension(cursor: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_census_bps.dim_measure (
            measure_id, structure_type, measure_label, structure_label, unit,
            observation_basis, methodology_url, parser_contract_version
        ) VALUES %s
        ON CONFLICT (measure_id, structure_type) DO UPDATE SET
            measure_label = EXCLUDED.measure_label,
            structure_label = EXCLUDED.structure_label,
            unit = EXCLUDED.unit,
            observation_basis = EXCLUDED.observation_basis,
            methodology_url = EXCLUDED.methodology_url,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        """,
        [
            (
                measure_id,
                structure_type,
                label,
                structure_label,
                unit,
                OBSERVATION_BASIS,
                METHODOLOGY_URL,
                PARSER_CONTRACT_VERSION,
            )
            for measure_id, label, unit in MEASURES
            for structure_type, structure_label in STRUCTURE_TYPES
        ],
    )


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform every captured file of a run; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT slice_key, frequency, year, month, capture_id::TEXT, status
                FROM control.census_bps_slice WHERE run_id = %s ORDER BY slice_key
                """,
                (str(run_id),),
            )
            slices = list(cursor.fetchall())
            if not slices:
                raise BpsReconciliationError(
                    "run has no captured Building Permits files"
                )
            _upsert_dimension(cursor)
            expected = 0
            for slice_key, frequency, year, month, capture_id, status in slices:
                if status == "empty":
                    continue
                item = slice_from_row(slice_key, frequency, year, month)
                parsed = parse_file(
                    load_captured_payload(connection_factory, UUID(capture_id)),
                    item=item,
                )
                if parsed.observations:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_census_bps.observation_revision (
                            capture_id, source_row_index, measure_id, structure_type,
                            run_id, slice_key, frequency, geo_type, geo_source_code,
                            geo_source_label, geo_id, period_start, period_end,
                            value_source, value, reported_value, value_status,
                            months_reported, source_record_id
                        ) VALUES %s
                        ON CONFLICT (capture_id, source_row_index, measure_id, structure_type) DO NOTHING
                        """,
                        [
                            (
                                capture_id,
                                obs.source_row_index,
                                obs.measure_id,
                                obs.structure_type,
                                str(run_id),
                                slice_key,
                                frequency,
                                obs.geo_type,
                                obs.geo_source_code,
                                obs.geo_source_label,
                                obs.geo_id,
                                obs.period_start,
                                obs.period_end,
                                obs.value_source,
                                obs.value,
                                obs.reported_value,
                                obs.value_status,
                                obs.months_reported,
                                obs.source_record_id,
                            )
                            for obs in parsed.observations
                        ],
                        page_size=5000,
                    )
                if parsed.quarantined:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_census_bps.observation_quarantine (
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
                payload_rejected = any(
                    q.source_row_index == 0 for q in parsed.quarantined
                )
                expected += parsed.in_scope_row_count * _values_per_row(item)
                cursor.execute(
                    """
                    UPDATE control.census_bps_slice
                       SET captured_row_count = %s, in_scope_row_count = %s,
                           status = %s, updated_at = NOW()
                     WHERE run_id = %s AND slice_key = %s AND status <> 'published'
                    """,
                    (
                        parsed.row_count,
                        parsed.in_scope_row_count,
                        "quarantined" if payload_rejected else "silver_ready",
                        str(run_id),
                        slice_key,
                    ),
                )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (revision.geo_type, revision.geo_source_code, EXTRACT(YEAR FROM revision.period_start))
                       %s, revision.slice_key, revision.geo_type, revision.geo_source_code,
                       revision.geo_source_label, EXTRACT(YEAR FROM revision.period_start)::INTEGER,
                       entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_census_bps.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.geo_source_code, EXTRACT(YEAR FROM revision.period_start), revision.capture_id
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
                INSERT INTO silver_census_bps.fact_observation (
                    measure_id, structure_type, frequency, geo_id, period_start,
                    capture_id, run_id, retrieved_at, period_end, geo_sk, geo_type,
                    geography_status, value_source, value, reported_value,
                    value_status, months_reported, source_record_id
                )
                SELECT revision.measure_id, revision.structure_type, revision.frequency,
                       revision.geo_id, revision.period_start, revision.capture_id,
                       revision.run_id, capture.retrieved_at, revision.period_end,
                       entity.geo_sk, revision.geo_type,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.reported_value,
                       revision.value_status, revision.months_reported, revision.source_record_id
                FROM silver_census_bps.observation_revision AS revision
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (measure_id, structure_type, frequency, geo_id, period_start, capture_id) DO NOTHING
                """,
                (str(run_id),),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_census_bps.observation_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_census_bps.fact_observation WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if revisions != expected or facts != revisions:
                raise BpsReconciliationError(
                    f"run {run_id} reconciles {revisions} revisions and {facts} facts against {expected} expected"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return facts


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled run's files through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.census_bps_slice
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                """,
                (str(run_id),),
            )
            cursor.execute(
                """
                SELECT COUNT(*) FILTER (WHERE status = 'captured'),
                       COUNT(*) FILTER (WHERE status = 'published')
                FROM control.census_bps_slice WHERE run_id = %s
                """,
                (str(run_id),),
            )
            unreplayed, published = cursor.fetchone()
            if unreplayed:
                raise BpsPublicationError(
                    f"run {run_id} has files that were never replayed"
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
            connection_factory, publisher_schema="gold_census_bps"
        )
    return int(published)
