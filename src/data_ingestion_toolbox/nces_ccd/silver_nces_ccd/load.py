"""Replay one captured CCD or EDGE file into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per run: the parsed rows, the quarantine and, for a geocode
file, the geography resolution ledger land together or not at all. A run the
capture found ``unchanged`` replays nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..registry import FILES
from .parse import parse_file


class CcdReconciliationError(RuntimeError):
    """A replayed run does not account for every parsed row."""


class CcdPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _write_locations(cursor: Any, run_id: str, capture_id: str, parsed: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_nces_ccd.school_location (
            run_id, ncessch, leaid, capture_id, source_row_index, operating_state_fips,
            state_fips, county_fips, latitude, longitude, geo_id, geo_sk, geography_status
        )
        SELECT incoming.run_id::UUID, incoming.ncessch, incoming.leaid, incoming.capture_id::UUID,
               incoming.source_row_index::INTEGER, incoming.operating_state_fips, incoming.state_fips,
               incoming.county_fips, incoming.latitude::NUMERIC, incoming.longitude::NUMERIC,
               incoming.geo_id, entity.geo_sk,
               CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END
        FROM (VALUES %s) AS incoming (
            run_id, ncessch, leaid, capture_id, source_row_index, operating_state_fips,
            state_fips, county_fips, latitude, longitude, geo_id
        )
        LEFT JOIN silver_ref.dim_geo_entity AS entity
          ON entity.geo_id = incoming.geo_id AND entity.geo_type = 'county'
        ON CONFLICT (run_id, ncessch) DO NOTHING
        """,
        [
            (
                run_id,
                row.ncessch,
                row.leaid,
                capture_id,
                row.source_row_index,
                row.operating_state_fips,
                row.state_fips,
                row.county_fips,
                row.latitude,
                row.longitude,
                f"state:{row.state_fips}|county:{row.county_fips[2:]}",
            )
            for row in parsed.locations
        ],
        page_size=10000,
    )
    cursor.execute(
        """
        INSERT INTO silver_ref.geography_resolution (
            provider_source, provider_dataset, source_geo_type,
            source_code, source_label, source_vintage, geo_sk,
            resolution_method, evidence_capture_id, status, reason_code
        )
        SELECT DISTINCT ON (location.county_fips)
               %s, 'edge_geocode_publicsch', 'county',
               location.county_fips, NULL, LEFT(file.school_year, 4)::INTEGER, location.geo_sk,
               CASE WHEN location.geo_sk IS NOT NULL THEN 'exact_code' END,
               location.capture_id, location.geography_status,
               CASE WHEN location.geo_sk IS NULL THEN 'canonical_geography_absent' END
        FROM silver_nces_ccd.school_location AS location
        JOIN control.nces_ccd_file AS file USING (run_id)
        WHERE location.run_id = %s
        ORDER BY location.county_fips, location.capture_id
        ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
        DO UPDATE SET
            geo_sk = EXCLUDED.geo_sk,
            resolution_method = EXCLUDED.resolution_method,
            evidence_capture_id = EXCLUDED.evidence_capture_id,
            status = EXCLUDED.status,
            reason_code = EXCLUDED.reason_code,
            resolved_at = NOW()
        """,
        (SOURCE_CODE, run_id),
    )


def _write_directory(cursor: Any, run_id: str, capture_id: str, parsed: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_nces_ccd.school_directory (
            run_id, ncessch, leaid, capture_id, source_row_index, operating_state_fips,
            school_status, school_type, charter, school_level
        ) VALUES %s
        ON CONFLICT (run_id, ncessch) DO NOTHING
        """,
        [
            (
                run_id,
                row.ncessch,
                row.leaid,
                capture_id,
                row.source_row_index,
                row.operating_state_fips,
                row.status,
                row.school_type,
                row.charter,
                row.level,
            )
            for row in parsed.directory
        ],
        page_size=10000,
    )


def _write_counts(cursor: Any, run_id: str, capture_id: str, parsed: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_nces_ccd.school_count (
            run_id, ncessch, measure, leaid, capture_id, source_row_index, operating_state_fips,
            value_source, value, value_status, missing_reason, dms_flag
        ) VALUES %s
        ON CONFLICT (run_id, ncessch, measure) DO NOTHING
        """,
        [
            (
                run_id,
                row.ncessch,
                row.measure,
                row.leaid,
                capture_id,
                row.source_row_index,
                row.operating_state_fips,
                row.value_source,
                row.value,
                row.value_status,
                row.missing_reason,
                row.dms_flag,
            )
            for row in parsed.counts
        ],
        page_size=10000,
    )


_KEPT = {
    "geocode": ("silver_nces_ccd.school_location", "locations"),
    "directory": ("silver_nces_ccd.school_directory", "directory"),
}


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform a captured file; return the rows written."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT file_stem, capture_id::TEXT, status FROM control.nces_ccd_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise CcdReconciliationError("run has no captured NCES file")
            stem, capture_id, status = row
            if status == "unchanged":
                connection.commit()
                return 0
            item = next(
                (candidate for candidate in FILES if candidate.stem == stem), None
            )
            if item is None:
                raise CcdReconciliationError(f"{stem} is not a registered NCES file")
            parsed = parse_file(
                load_captured_payload(connection_factory, UUID(capture_id)), item=item
            )
            if parsed.locations:
                _write_locations(cursor, str(run_id), capture_id, parsed)
            if parsed.directory:
                _write_directory(cursor, str(run_id), capture_id, parsed)
            if parsed.counts:
                _write_counts(cursor, str(run_id), capture_id, parsed)
            if parsed.quarantined:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_nces_ccd.quarantine (
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
            relation, attribute = _KEPT.get(
                item.component.name, ("silver_nces_ccd.school_count", "counts")
            )
            expected = len(getattr(parsed, attribute))
            kept_rows = len(
                {getattr(row, "source_row_index") for row in getattr(parsed, attribute)}
            )
            refused = any(q.source_row_index == 0 for q in parsed.quarantined)
            cursor.execute(
                """
                UPDATE control.nces_ccd_file
                   SET row_count = %s, kept_row_count = %s, status = %s, updated_at = NOW()
                 WHERE run_id = %s AND status = 'captured'
                """,
                (
                    parsed.row_count,
                    kept_rows,
                    "quarantined" if refused else "silver_ready",
                    str(run_id),
                ),
            )
            cursor.execute(
                f"SELECT COUNT(*) FROM {relation} WHERE run_id = %s", (str(run_id),)
            )
            written = int(cursor.fetchone()[0])
            if written != expected:
                raise CcdReconciliationError(
                    f"run {run_id} wrote {written} rows to {relation} against {expected} parsed"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return written


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled file through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.nces_ccd_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.nces_ccd_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise CcdPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_nces_ccd"
        )
    return published
