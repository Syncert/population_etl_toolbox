"""Replay one captured FEMA read into silver, reconcile it, and publish it.

Everything here reads the committed pages, never the network. One
transaction per run: the parsed rows, the quarantine, the geography
resolution ledger and the facts land together or not at all. An NRI read
the capture found ``unchanged`` replays nothing; a declarations read adds
only the revisions (``id``, ``hash``) not already kept.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..client import FemaPayloadError, read_page
from ..config import SOURCE_CODE, FemaConfig
from ..registry import NRI
from .parse import parse_declarations, parse_nri


class FemaReconciliationError(RuntimeError):
    """A replayed run does not account for every parsed row."""


class FemaPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def replay_run(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    config: FemaConfig | None = None,
) -> int:
    """Parse and conform a captured read; return the rows written."""
    runtime = config or FemaConfig()
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT stream, status FROM control.fema_nri_run WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise FemaReconciliationError("run has no captured FEMA read")
            stream, status = row
            if status == "unchanged":
                connection.commit()
                return 0
            cursor.execute(
                """
                SELECT page_index, capture_id::TEXT FROM control.fema_nri_page
                WHERE run_id = %s ORDER BY page_index
                """,
                (str(run_id),),
            )
            pages = cursor.fetchall()
            page_size = (
                runtime.nri_page_size
                if stream == NRI
                else runtime.declaration_page_size
            )
            quarantine: list[tuple] = []
            nri_rows: list[tuple] = []
            declaration_rows: list[tuple] = []
            seen: set[str] = set()
            records_total = 0
            refused = False
            for page_index, capture_id in pages:
                payload = load_captured_payload(connection_factory, UUID(capture_id))
                try:
                    records, _more = read_page(
                        stream,
                        payload,
                        f"{stream}:page:{page_index}",
                        page_size=page_size,
                    )
                except FemaPayloadError as error:
                    refused = True
                    quarantine.append(
                        (
                            str(run_id),
                            capture_id,
                            0,
                            error.code,
                            f"page refused: {error.code}",
                        )
                    )
                    continue
                records_total += len(records)
                offset = page_index * page_size
                if stream == NRI:
                    observations, rejected = parse_nri(
                        records, offset=offset, seen=seen
                    )
                    nri_rows.extend(
                        (
                            str(run_id),
                            obs.geo_id,
                            obs.field,
                            obs.measure,
                            capture_id,
                            obs.stcofips,
                            obs.county_type,
                            obs.nri_version,
                            obs.value_source,
                            obs.value,
                            obs.value_status,
                            obs.missing_reason,
                            obs.rating,
                            obs.source_record_id,
                        )
                        for obs in observations
                    )
                else:
                    declarations, rejected = parse_declarations(records, offset=offset)
                    declaration_rows.extend(
                        (
                            item.declaration_id,
                            item.revision_hash,
                            str(run_id),
                            capture_id,
                            item.record_index,
                            item.declaration_string,
                            item.disaster_number,
                            item.declaration_type,
                            item.declaration_date,
                            item.incident_type,
                            item.state_fips,
                            item.county_fips,
                            item.place_code,
                            item.designated_area,
                            item.last_refresh,
                            item.geo_id,
                        )
                        for item in declarations
                    )
                quarantine.extend(
                    (
                        str(run_id),
                        capture_id,
                        q.record_index,
                        q.error_code,
                        q.error_summary,
                    )
                    for q in rejected
                )
            if quarantine:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_fema_nri.quarantine (
                        run_id, capture_id, record_index, error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (capture_id, record_index, error_code) DO NOTHING
                    """,
                    quarantine,
                )
            versions = {row[7] for row in nri_rows}
            if stream == NRI and len(versions) > 1:
                raise FemaReconciliationError(
                    f"run {run_id} mixes NRI versions {sorted(map(str, versions))}"
                )
            written = 0
            if nri_rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_fema_nri.nri_fact (
                        run_id, geo_id, field, measure, capture_id, stcofips, county_type, nri_version,
                        geo_sk, geography_status, value_source, value, value_status, missing_reason,
                        rating, source_record_id
                    )
                    SELECT incoming.run_id::UUID, incoming.geo_id, incoming.field, incoming.measure,
                           incoming.capture_id::UUID, incoming.stcofips, incoming.county_type,
                           incoming.nri_version, entity.geo_sk,
                           CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                           incoming.value_source, incoming.value::NUMERIC, incoming.value_status,
                           incoming.missing_reason, incoming.rating, incoming.source_record_id
                    FROM (VALUES %s) AS incoming (
                        run_id, geo_id, field, measure, capture_id, stcofips, county_type, nri_version,
                        value_source, value, value_status, missing_reason, rating, source_record_id
                    )
                    LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = incoming.geo_id
                    ON CONFLICT (run_id, geo_id, field) DO NOTHING
                    """,
                    nri_rows,
                    page_size=10000,
                )
                cursor.execute(
                    "SELECT COUNT(*) FROM silver_fema_nri.nri_fact WHERE run_id = %s",
                    (str(run_id),),
                )
                written = int(cursor.fetchone()[0])
                if written != len(nri_rows):
                    raise FemaReconciliationError(
                        f"run {run_id} wrote {written} NRI facts against {len(nri_rows)}"
                    )
            if declaration_rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_fema_nri.declaration_revision (
                        declaration_id, revision_hash, run_id, capture_id, record_index, declaration_string,
                        disaster_number, declaration_type, declaration_date, incident_type, state_fips,
                        county_fips, place_code, designated_area, last_refresh, geo_id, geo_sk,
                        geography_status
                    )
                    SELECT incoming.declaration_id, incoming.revision_hash, incoming.run_id::UUID,
                           incoming.capture_id::UUID, incoming.record_index::INTEGER,
                           incoming.declaration_string, incoming.disaster_number::INTEGER,
                           incoming.declaration_type, incoming.declaration_date::DATE,
                           incoming.incident_type, incoming.state_fips, incoming.county_fips,
                           incoming.place_code, incoming.designated_area,
                           incoming.last_refresh::TIMESTAMPTZ, incoming.geo_id, entity.geo_sk,
                           CASE
                               WHEN incoming.geo_id IS NULL THEN 'area'
                               WHEN entity.geo_sk IS NULL THEN 'unmapped'
                               ELSE 'resolved'
                           END
                    FROM (VALUES %s) AS incoming (
                        declaration_id, revision_hash, run_id, capture_id, record_index, declaration_string,
                        disaster_number, declaration_type, declaration_date, incident_type, state_fips,
                        county_fips, place_code, designated_area, last_refresh, geo_id
                    )
                    LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = incoming.geo_id
                    ON CONFLICT (declaration_id, revision_hash) DO NOTHING
                    """,
                    declaration_rows,
                    page_size=10000,
                )
                cursor.execute(
                    "SELECT COUNT(*) FROM silver_fema_nri.declaration_revision WHERE run_id = %s",
                    (str(run_id),),
                )
                written = int(cursor.fetchone()[0])
            # The ledger's vintage: the NRI version's year ("December 2025"),
            # or the newest declaration refresh's.
            vintage = max(
                [
                    int(str(version)[-4:])
                    for version in versions
                    if str(version)[-4:].isdigit()
                ]
                + [row[14].year for row in declaration_rows]
                or [0]
            )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (resolved.code)
                       %s, %s, 'county', resolved.code, NULL, %s, resolved.geo_sk,
                       CASE WHEN resolved.geo_sk IS NOT NULL THEN 'exact_code' END,
                       resolved.capture_id,
                       CASE WHEN resolved.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN resolved.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM (
                    SELECT stcofips AS code, geo_sk, capture_id FROM silver_fema_nri.nri_fact WHERE run_id = %s
                    UNION ALL
                    SELECT state_fips || county_fips, geo_sk, capture_id
                    FROM silver_fema_nri.declaration_revision
                    WHERE run_id = %s AND geography_status <> 'area'
                ) AS resolved
                ORDER BY resolved.code, resolved.capture_id
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                """,
                (SOURCE_CODE, f"fema_{stream}", vintage, str(run_id), str(run_id)),
            )
            cursor.execute(
                """
                UPDATE control.fema_nri_run
                   SET status = %s, record_count = %s, updated_at = NOW()
                 WHERE run_id = %s AND status = 'captured'
                """,
                (
                    "quarantined" if refused else "silver_ready",
                    records_total,
                    str(run_id),
                ),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return written


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled read through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.fema_nri_run
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.fema_nri_run WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] in ("captured", "capturing"):
                raise FemaPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_fema_nri"
        )
    return published
