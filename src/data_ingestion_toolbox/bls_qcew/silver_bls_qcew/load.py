"""Replay one captured QCEW run into silver, reconcile it, and publish it.

Everything here reads the committed captures, never the network. One
transaction per run: the dimensions, the parsed revisions and quarantine,
the geography resolution ledger and the conformed facts land together or
not at all, and the run is only marked ready when the counts reconcile.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..capture import PARSER_CONTRACT_VERSION
from ..config import SOURCE_CODE
from ..registry import INDUSTRIES, MEASURES, get_industry, measures_for
from .parse import parse_slice

OBSERVATION_BASIS = (
    "establishment-based: jobs located in the area, from unemployment-insurance "
    "records (QCEW), not residents employed (LAUS)"
)
METHODOLOGY_URL = "https://www.bls.gov/opub/hom/cew/home.htm"


class QcewReconciliationError(RuntimeError):
    """A replayed run does not account for every in-scope captured row."""


class QcewPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _upsert_dimensions(cursor: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_bls_qcew.dim_measure (
            measure_id, measure_label, unit, period_kind, observation_basis,
            methodology_url, parser_contract_version
        ) VALUES %s
        ON CONFLICT (measure_id) DO UPDATE SET
            measure_label = EXCLUDED.measure_label,
            unit = EXCLUDED.unit,
            period_kind = EXCLUDED.period_kind,
            observation_basis = EXCLUDED.observation_basis,
            methodology_url = EXCLUDED.methodology_url,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        """,
        [
            (
                measure.measure_id,
                measure.label,
                measure.unit,
                measure.period_kind,
                OBSERVATION_BASIS,
                METHODOLOGY_URL,
                PARSER_CONTRACT_VERSION,
            )
            for measure in MEASURES
        ],
    )
    execute_values(
        cursor,
        """
        INSERT INTO silver_bls_qcew.dim_industry (industry_code, industry_title, industry_level)
        VALUES %s
        ON CONFLICT (industry_code) DO UPDATE SET
            industry_title = EXCLUDED.industry_title,
            industry_level = EXCLUDED.industry_level,
            updated_at = NOW()
        """,
        [
            (
                industry.code,
                industry.title,
                "total" if industry.code == "10" else "sector",
            )
            for industry in INDUSTRIES
        ],
    )


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform every captured slice of a run; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT industry_code, year, period, capture_id::TEXT, status
                FROM control.bls_qcew_slice WHERE run_id = %s ORDER BY industry_code
                """,
                (str(run_id),),
            )
            slices = list(cursor.fetchall())
            if not slices:
                raise QcewReconciliationError("run has no captured QCEW slices")
            _upsert_dimensions(cursor)
            expected = 0
            for industry_code, year, period, capture_id, status in slices:
                if status == "empty":
                    continue
                industry = get_industry(industry_code)
                parsed = parse_slice(
                    load_captured_payload(connection_factory, UUID(capture_id)),
                    year=year,
                    period=period,
                    industry=industry,
                )
                if parsed.observations:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_bls_qcew.observation_revision (
                            capture_id, source_row_index, measure_id, month_index,
                            run_id, year, period, industry_code, own_code,
                            agglvl_code, geo_type, geo_source_code, geo_id,
                            period_start, period_end, value_source, value,
                            value_status, disclosure_code, source_record_id
                        ) VALUES %s
                        ON CONFLICT (capture_id, source_row_index, measure_id, month_index) DO NOTHING
                        """,
                        [
                            (
                                capture_id,
                                item.source_row_index,
                                item.measure_id,
                                item.month_index,
                                str(run_id),
                                year,
                                period,
                                item.industry_code,
                                item.own_code,
                                item.agglvl_code,
                                item.geo_type,
                                item.geo_source_code,
                                item.geo_id,
                                item.period_start,
                                item.period_end,
                                item.value_source,
                                item.value,
                                item.value_status,
                                item.disclosure_code,
                                item.source_record_id,
                            )
                            for item in parsed.observations
                        ],
                        page_size=5000,
                    )
                if parsed.quarantined:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_bls_qcew.observation_quarantine (
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
                values_per_row = sum(
                    len(measure.columns) for measure in measures_for(period)
                )
                expected += (
                    parsed.in_scope_row_count - quarantined_rows
                ) * values_per_row
                cursor.execute(
                    """
                    UPDATE control.bls_qcew_slice
                       SET captured_row_count = %s, in_scope_row_count = %s,
                           status = %s, updated_at = NOW()
                     WHERE run_id = %s AND industry_code = %s AND status <> 'published'
                    """,
                    (
                        parsed.row_count,
                        parsed.in_scope_row_count,
                        "quarantined" if payload_rejected else "silver_ready",
                        str(run_id),
                        industry_code,
                    ),
                )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (revision.geo_type, revision.geo_source_code, revision.year)
                       %s, 'qcew', revision.geo_type, revision.geo_source_code, NULL,
                       revision.year, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_bls_qcew.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.geo_source_code, revision.year, revision.capture_id
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
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
                INSERT INTO silver_bls_qcew.fact_observation (
                    measure_id, industry_code, own_code, geo_id, period_start,
                    capture_id, run_id, retrieved_at, period_end, year, period,
                    geo_sk, geo_type, geography_status, value_source, value,
                    value_status, disclosure_code, source_record_id
                )
                SELECT revision.measure_id, revision.industry_code, revision.own_code,
                       revision.geo_id, revision.period_start, revision.capture_id,
                       revision.run_id, capture.retrieved_at, revision.period_end,
                       revision.year, revision.period, entity.geo_sk, revision.geo_type,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.value_status,
                       revision.disclosure_code, revision.source_record_id
                FROM silver_bls_qcew.observation_revision AS revision
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (measure_id, industry_code, own_code, geo_id, period_start, capture_id) DO NOTHING
                """,
                (str(run_id),),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_bls_qcew.observation_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_bls_qcew.fact_observation WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if revisions != expected or facts != revisions:
                raise QcewReconciliationError(
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
    """Expose a reconciled run's slices through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.bls_qcew_slice
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                """,
                (str(run_id),),
            )
            cursor.execute(
                """
                SELECT COUNT(*) FILTER (WHERE status = 'captured'),
                       COUNT(*) FILTER (WHERE status = 'published')
                FROM control.bls_qcew_slice WHERE run_id = %s
                """,
                (str(run_id),),
            )
            unreplayed, published = cursor.fetchone()
            if unreplayed:
                raise QcewPublicationError(
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
            connection_factory, publisher_schema="gold_bls_qcew"
        )
    return int(published)
