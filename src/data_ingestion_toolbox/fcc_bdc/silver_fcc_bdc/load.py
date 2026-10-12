"""Replay one captured FCC vintage into silver, reconcile it, and publish it.

Everything here reads the committed captures, never the network. One
transaction per run: every file's kept rows, the quarantine and the
geography resolution ledger land together or not at all. A run the capture
found ``unchanged`` replays nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..registry import CENSUS_PLACE, OTHER_GEOGRAPHIES, SummaryFile
from .parse import parse_summary


class BdcReconciliationError(RuntimeError):
    """A replayed run does not account for every parsed row."""


class BdcPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform every file of a captured vintage; return the rows written."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT as_of_date, status FROM control.fcc_bdc_read WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise BdcReconciliationError("run has no captured FCC vintage")
            as_of_date, status = row
            if status == "unchanged":
                connection.commit()
                return 0
            cursor.execute(
                """
                SELECT subcategory, state_fips, file_id, file_name, capture_id::TEXT
                FROM control.fcc_bdc_file WHERE run_id = %s ORDER BY slice_key
                """,
                (str(run_id),),
            )
            files = cursor.fetchall()
            parsed_rows = 0
            total_rows = 0
            refused = False
            for subcategory, state_fips, file_id, file_name, capture_id in files:
                item = SummaryFile(
                    as_of_date
                    if isinstance(as_of_date, date)
                    else date.fromisoformat(str(as_of_date)),
                    OTHER_GEOGRAPHIES
                    if subcategory == "other_geographies"
                    else CENSUS_PLACE,
                    state_fips,
                    int(file_id),
                    file_name,
                )
                parsed = parse_summary(
                    load_captured_payload(connection_factory, UUID(capture_id)),
                    item=item,
                )
                total_rows += parsed.row_count
                parsed_rows += len(parsed.rows)
                refused = refused or any(
                    q.source_row_index == 0 for q in parsed.quarantined
                )
                if parsed.rows:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_fcc_bdc.availability_row (
                            run_id, geo_id, technology, capture_id, source_row_index, geography_type,
                            geography_id, geo_sk, geography_status, total_units, speed_02_02, speed_10_1,
                            speed_25_3, speed_100_20, speed_250_25, speed_1000_100, value_source,
                            value_status, missing_reason
                        )
                        SELECT incoming.run_id::UUID, incoming.geo_id, incoming.technology,
                               incoming.capture_id::UUID, incoming.source_row_index::INTEGER,
                               incoming.geography_type, incoming.geography_id, entity.geo_sk,
                               CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                               incoming.total_units::INTEGER, incoming.s1::NUMERIC, incoming.s2::NUMERIC,
                               incoming.s3::NUMERIC, incoming.s4::NUMERIC, incoming.s5::NUMERIC,
                               incoming.s6::NUMERIC, incoming.value_source, incoming.value_status,
                               incoming.missing_reason
                        FROM (VALUES %s) AS incoming (
                            run_id, geo_id, technology, capture_id, source_row_index, geography_type,
                            geography_id, total_units, s1, s2, s3, s4, s5, s6, value_source, value_status,
                            missing_reason
                        )
                        LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = incoming.geo_id
                        ON CONFLICT (run_id, geo_id, technology) DO NOTHING
                        """,
                        [
                            (
                                str(run_id),
                                r.geo_id,
                                r.technology,
                                capture_id,
                                r.source_row_index,
                                r.geography_type,
                                r.geography_id,
                                r.total_units,
                                *r.shares,
                                r.value_source,
                                r.value_status,
                                r.missing_reason,
                            )
                            for r in parsed.rows
                        ],
                        page_size=5000,
                    )
                if parsed.quarantined:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_fcc_bdc.quarantine (
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
            cursor.execute(
                """
                UPDATE control.fcc_bdc_read
                   SET row_count = %s, kept_row_count = %s, status = %s, updated_at = NOW()
                 WHERE run_id = %s AND status = 'captured'
                """,
                (
                    total_rows,
                    parsed_rows,
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
                SELECT DISTINCT ON (availability.geography_type, availability.geography_id)
                       %s, 'bdc_fixed_summary', availability.geography_type,
                       availability.geography_id, NULL, EXTRACT(YEAR FROM read.as_of_date)::INTEGER,
                       availability.geo_sk,
                       CASE WHEN availability.geo_sk IS NOT NULL THEN 'exact_code' END,
                       availability.capture_id, availability.geography_status,
                       CASE WHEN availability.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_fcc_bdc.availability_row AS availability
                JOIN control.fcc_bdc_read AS read USING (run_id)
                WHERE availability.run_id = %s
                ORDER BY availability.geography_type, availability.geography_id, availability.capture_id
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
                "SELECT COUNT(*) FROM silver_fcc_bdc.availability_row WHERE run_id = %s",
                (str(run_id),),
            )
            written = int(cursor.fetchone()[0])
            if written != parsed_rows:
                raise BdcReconciliationError(
                    f"run {run_id} wrote {written} rows against {parsed_rows} parsed"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return written


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled vintage through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.fcc_bdc_read
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.fcc_bdc_read WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise BdcPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(connection_factory, publisher_schema="gold_fcc_bdc")
    return published
