"""Replay one captured SOI income file into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per file: the dimensions, the parsed revisions and quarantine,
the geography resolution ledger and the conformed facts land together or
not at all, and the file is only marked ready when the counts reconcile.
A file the parser refused whole is marked ``quarantined`` with no facts, so
the next scheduled run asks for it again. A read recorded ``unchanged``
repeats the last published file and replays and publishes nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..capture import PARSER_CONTRACT_VERSION
from ..config import SOURCE_CODE
from ..registry import AGI_BANDS, MEASURES, SoiFile
from .parse import parse_file

PAGE_SIZE = 5000


class IrsSoiReconciliationError(RuntimeError):
    """A replayed file does not account for every captured row."""


class IrsSoiPublicationError(RuntimeError):
    """A file has not reached the silver publication gate."""


def _file(cursor: Any, run_id: UUID) -> tuple[SoiFile, str, str]:
    cursor.execute(
        """
        SELECT file_kind, tax_year, capture_id::TEXT, status
        FROM control.irs_soi_file
        WHERE run_id = %s
        """,
        (str(run_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise IrsSoiReconciliationError(f"run {run_id} captured no SOI income file")
    kind, tax_year, capture_id, status = row
    return SoiFile(str(kind), int(tax_year)), str(capture_id), str(status)


def _write_dimensions(cursor: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_irs_soi.dim_measure (
            measure_id, soi_column, measure_label, unit, parser_contract_version
        ) VALUES %s
        ON CONFLICT (measure_id) DO UPDATE SET
            soi_column = EXCLUDED.soi_column,
            measure_label = EXCLUDED.measure_label,
            unit = EXCLUDED.unit,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        """,
        [
            (
                measure.measure_id,
                measure.column,
                measure.label,
                measure.unit,
                PARSER_CONTRACT_VERSION,
            )
            for measure in MEASURES
        ],
    )
    execute_values(
        cursor,
        """
        INSERT INTO silver_irs_soi.dim_agi_band (
            agi_band, band_label, lower_dollars, upper_dollars
        ) VALUES %s
        ON CONFLICT (agi_band) DO UPDATE SET
            band_label = EXCLUDED.band_label,
            lower_dollars = EXCLUDED.lower_dollars,
            upper_dollars = EXCLUDED.upper_dollars,
            updated_at = NOW()
        """,
        [
            (band.code, band.label, band.lower_dollars, band.upper_dollars)
            for band in AGI_BANDS.values()
        ],
    )


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform one captured file; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            item, capture_id, status = _file(cursor, run_id)
            if status == "unchanged":
                # The bytes repeat the last published file (utility.unchanged).
                connection.commit()
                return 0
            if status == "published":
                cursor.execute(
                    "SELECT COUNT(*) FROM silver_irs_soi.fact_observation WHERE run_id = %s",
                    (str(run_id),),
                )
                return int(cursor.fetchone()[0])
            _write_dimensions(cursor)
            payload = load_captured_payload(connection_factory, UUID(capture_id))
            parsed = parse_file(item, payload)
            if parsed.observations:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_irs_soi.observation_revision (
                        capture_id, source_row_index, measure_id, run_id,
                        file_kind, tax_year, agi_stub, agi_band, geo_type,
                        geo_source_code, geo_source_label, geo_id,
                        value_source, value, value_status, source_record_id
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index, measure_id) DO NOTHING
                    """,
                    [
                        (
                            capture_id,
                            obs.source_row_index,
                            obs.measure_id,
                            str(run_id),
                            item.kind,
                            item.tax_year,
                            obs.agi_stub,
                            obs.agi_band,
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
                    page_size=PAGE_SIZE,
                )
            if parsed.quarantined:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_irs_soi.observation_quarantine (
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
            quarantined_rows = (
                parsed.row_count
                if parsed.rejected
                else len({q.source_row_index for q in parsed.quarantined})
            )
            expected = (parsed.row_count - quarantined_rows) * len(parsed.measures_read)
            if parsed.rejected:
                expected = 0
            cursor.execute(
                """
                UPDATE control.irs_soi_file
                   SET captured_row_count = %s,
                       quarantined_row_count = %s,
                       status = %s,
                       updated_at = NOW()
                 WHERE run_id = %s AND status <> 'published'
                """,
                (
                    parsed.row_count,
                    quarantined_rows,
                    "quarantined" if parsed.rejected else "silver_ready",
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
                       %s, revision.file_kind, revision.geo_type,
                       revision.geo_source_code, revision.geo_source_label,
                       revision.tax_year, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_irs_soi.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.geo_source_code
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
                INSERT INTO silver_irs_soi.fact_observation (
                    file_kind, metric_key, tax_year, geo_id, capture_id, run_id,
                    measure_id, agi_band, agi_stub, retrieved_at, geo_sk,
                    geo_type, geography_status, value_source, value,
                    value_status, source_record_id
                )
                SELECT revision.file_kind,
                       revision.measure_id || CASE WHEN revision.agi_band = 'all' THEN ''
                                                   ELSE ':' || revision.agi_band END,
                       revision.tax_year, revision.geo_id, revision.capture_id,
                       revision.run_id, revision.measure_id, revision.agi_band,
                       revision.agi_stub, capture.retrieved_at, entity.geo_sk,
                       revision.geo_type,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.value_status,
                       revision.source_record_id
                FROM silver_irs_soi.observation_revision AS revision
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (file_kind, metric_key, tax_year, geo_id, capture_id) DO NOTHING
                """,
                (str(run_id),),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_irs_soi.observation_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_irs_soi.fact_observation WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if revisions != expected or facts != revisions:
                raise IrsSoiReconciliationError(
                    f"run {run_id} reconciles {revisions} revisions and {facts} facts "
                    f"against {expected} expected"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return facts


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled file through the gold views; return 1 when published."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.irs_soi_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                """,
                (str(run_id),),
            )
            cursor.execute(
                "SELECT status FROM control.irs_soi_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise IrsSoiPublicationError(f"run {run_id} was never replayed")
            published = row[0] == "published"
            if published:
                # Prevention list: statistics follow every load.
                cursor.execute("ANALYZE silver_irs_soi.fact_observation")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(connection_factory, publisher_schema="gold_irs_soi")
    return int(published)
