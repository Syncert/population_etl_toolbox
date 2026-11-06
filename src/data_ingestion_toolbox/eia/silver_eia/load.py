"""Replay captured EIA pages into silver, and publish a reconciled read.

A read's every page is parsed from its stored bytes. Each price is kept as a
revision row and conformed into the fact under its capture, so a week EIA
revises is a second row beside the first. A state resolves through the USPS
code the shared reference's Census Gazetteer carries; the nation, PADDs and
cities through their canonical ids. Every area is ledgered in
``silver_ref.geography_resolution``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from .parse import parse_page


_REVISION_COLUMNS = (
    "capture_id, row_index, run_id, series_id, week_start, duoarea, area_name, "
    "product, product_name, units, geo_type, geo_id, state_usps, value_source, "
    "value, value_status, source_record_id"
)

_INCOMING_TABLE_SQL = """
    DROP TABLE IF EXISTS pg_temp.eia_incoming_revision;
    CREATE TEMP TABLE eia_incoming_revision
        (LIKE silver_eia.price_revision INCLUDING DEFAULTS) ON COMMIT DROP
"""

#: Every routine read goes back eight weeks so a revised week is read again,
#: and seven of those weeks are usually what the warehouse already holds:
#: 4,176 of 75,864 revision rows (and as many fact rows) on OFFICE_LAB repeated
#: the previous read of the same week (refresh-churn audit, 2026-11-06). A
#: price is stored again only when its value or status differs from the
#: newest stored revision of that series and week; the served price is the
#: newest stored one, so a skipped repeat changes nothing served.
_STORE_NEW_REVISIONS_SQL = f"""
    INSERT INTO silver_eia.price_revision ({_REVISION_COLUMNS})
    SELECT {_REVISION_COLUMNS}
    FROM eia_incoming_revision AS incoming
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT stored.value, stored.value_status
            FROM silver_eia.price_revision AS stored
            WHERE stored.series_id = incoming.series_id
              AND stored.week_start = incoming.week_start
              AND stored.capture_id <> incoming.capture_id
            ORDER BY stored.created_at DESC, stored.capture_id DESC
            LIMIT 1
        ) AS newest
        WHERE newest.value IS NOT DISTINCT FROM incoming.value
          AND newest.value_status = incoming.value_status
    )
    ON CONFLICT (capture_id, row_index) DO NOTHING
"""


class EiaReconciliationError(RuntimeError):
    """A replayed read does not account for every captured row."""


class EiaPublicationError(RuntimeError):
    """A read has not reached the silver publication gate."""


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform one captured read; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT page.capture_id::TEXT
                FROM control.eia_page AS page
                WHERE page.run_id = %s
                ORDER BY page.page_index
                """,
                (str(run_id),),
            )
            captures = [row[0] for row in cursor.fetchall()]
            if not captures:
                raise EiaReconciliationError(f"run {run_id} has no captured EIA page")
            parsed_rows = rejected = 0
            payload_rejected = False
            cursor.execute(_INCOMING_TABLE_SQL)
            for capture_id in captures:
                page = parse_page(
                    load_captured_payload(connection_factory, UUID(capture_id))
                )
                parsed_rows += page.row_count
                rejected += sum(1 for q in page.quarantined if q.row_index >= 0)
                payload_rejected = payload_rejected or any(
                    q.row_index < 0 for q in page.quarantined
                )
                if page.prices:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO eia_incoming_revision (
                            capture_id, row_index, run_id, series_id, week_start,
                            duoarea, area_name, product, product_name, units,
                            geo_type, geo_id, state_usps, value_source, value,
                            value_status, source_record_id
                        ) VALUES %s
                        """,
                        [
                            (
                                capture_id,
                                price.row_index,
                                str(run_id),
                                price.series_id,
                                price.week_start,
                                price.duoarea,
                                price.area_name,
                                price.product,
                                price.product_name,
                                price.units,
                                price.geo_type,
                                price.geo_id,
                                price.state_usps,
                                price.value_source,
                                price.value,
                                price.value_status,
                                price.source_record_id,
                            )
                            for price in page.prices
                        ],
                        page_size=5000,
                    )
                if page.quarantined:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_eia.observation_quarantine (
                            run_id, capture_id, row_index, error_code, error_summary
                        ) VALUES %s
                        ON CONFLICT (capture_id, row_index, error_code) DO NOTHING
                        """,
                        [
                            (
                                str(run_id),
                                capture_id,
                                q.row_index,
                                q.error_code,
                                q.error_summary,
                            )
                            for q in page.quarantined
                        ],
                    )
            cursor.execute(_STORE_NEW_REVISIONS_SQL)
            cursor.execute(
                """
                SELECT COUNT(*) FROM eia_incoming_revision AS incoming
                WHERE NOT EXISTS (
                    SELECT 1 FROM silver_eia.price_revision AS stored
                    WHERE stored.capture_id = incoming.capture_id
                      AND stored.row_index = incoming.row_index
                )
                """
            )
            repeated = int(cursor.fetchone()[0])
            # The area each row names, by its code: a state through the USPS
            # code of its current reference version, anything else by its id.
            cursor.execute(
                """
                WITH state_by_usps AS (
                    SELECT DISTINCT ON (version.usps) version.usps, entity.geo_id
                    FROM silver_ref.dim_geo_entity_version AS version
                    JOIN silver_ref.dim_geo_entity AS entity USING (geo_sk)
                    WHERE entity.geo_type = 'state' AND version.usps IS NOT NULL
                    ORDER BY version.usps, version.geography_vintage DESC, version.ingested_at DESC
                )
                UPDATE silver_eia.price_revision AS revision
                   SET geo_id = state_by_usps.geo_id
                  FROM state_by_usps
                 WHERE revision.run_id = %s
                   AND revision.geo_type = 'state'
                   AND revision.geo_id IS NULL
                   AND state_by_usps.usps = revision.state_usps
                """,
                (str(run_id),),
            )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (revision.geo_type, revision.duoarea)
                       %s, 'petroleum_pri_gnd', revision.geo_type, revision.duoarea,
                       revision.area_name, EXTRACT(YEAR FROM revision.week_start)::INTEGER,
                       entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_eia.price_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.duoarea, revision.week_start DESC
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    source_label = EXCLUDED.source_label,
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                -- Only a row whose content changed is rewritten
                -- (refresh-churn audit, 2026-11-06).
                WHERE (silver_ref.geography_resolution.source_label, silver_ref.geography_resolution.geo_sk, silver_ref.geography_resolution.resolution_method, silver_ref.geography_resolution.status, silver_ref.geography_resolution.reason_code)
                      IS DISTINCT FROM
                      (EXCLUDED.source_label, EXCLUDED.geo_sk, EXCLUDED.resolution_method, EXCLUDED.status, EXCLUDED.reason_code)
                """,
                (SOURCE_CODE, str(run_id)),
            )
            cursor.execute(
                """
                INSERT INTO silver_eia.fact_retail_price (
                    series_id, week_start, capture_id, run_id, retrieved_at,
                    product, duoarea, area_name, geo_type, geo_id, geo_sk,
                    geography_status, value_source, value, value_status, source_record_id
                )
                SELECT revision.series_id, revision.week_start, revision.capture_id,
                       revision.run_id, capture.retrieved_at, revision.product,
                       revision.duoarea, revision.area_name, revision.geo_type,
                       revision.geo_id, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.value_status,
                       revision.source_record_id
                FROM silver_eia.price_revision AS revision
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (series_id, week_start, capture_id) DO NOTHING
                """,
                (str(run_id),),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_eia.price_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_eia.fact_retail_price WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            # A row repeating the stored week is accounted for, not stored.
            if revisions + repeated + rejected != parsed_rows or facts != revisions:
                raise EiaReconciliationError(
                    f"run {run_id} reconciles {revisions} revisions, {repeated} "
                    f"repeats of a stored week, {rejected} set aside "
                    f"and {facts} facts against {parsed_rows} captured rows"
                )
            cursor.execute(
                """
                UPDATE control.eia_read
                   SET parsed_row_count = %s, repeated_row_count = %s,
                       status = %s, updated_at = NOW()
                 WHERE run_id = %s AND status <> 'published'
                """,
                (
                    parsed_rows,
                    repeated,
                    "quarantined" if payload_rejected else "silver_ready",
                    str(run_id),
                ),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return facts


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled read through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.eia_read
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.eia_read WHERE run_id = %s", (str(run_id),)
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise EiaPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(connection_factory, publisher_schema="gold_eia")
    return published
