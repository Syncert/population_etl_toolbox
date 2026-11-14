"""Replay one captured NDCP release into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per workbook: the parsed prices, the quarantine, the geography
resolution ledger and the conformed facts land together or not at all, and
the read is only marked ready when the counts reconcile.

A price that repeats the newest stored price of its county, study year and
measure (the same number, status and imputation code) is not stored again
(CHURN RULE): a changed workbook writes only the prices that changed, and a
read recorded ``unchanged`` replays nothing at all. A workbook the parser
refused whole is marked ``quarantined`` with no facts, so the next scheduled
run reads it again.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..registry import get_release
from .parse import parse_capture

PAGE_SIZE = 10000

_COLUMNS = (
    "capture_id, source_row_index, metric_key, run_id, release, care_type, "
    "age_group, study_year, fips_code, county_name, geo_id, value_source, value, "
    "value_status, missing_reason, imputed, imputation_code, source_record_id"
)

_INCOMING_TABLE_SQL = """
    DROP TABLE IF EXISTS pg_temp.dol_childcare_incoming;
    CREATE TEMP TABLE dol_childcare_incoming
        (LIKE silver_dol_childcare.price_revision INCLUDING DEFAULTS) ON COMMIT DROP
"""

_STORE_CHANGED_PRICES_SQL = f"""
    INSERT INTO silver_dol_childcare.price_revision ({_COLUMNS})
    SELECT {_COLUMNS}
    FROM dol_childcare_incoming AS incoming
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT stored.value, stored.value_status, stored.imputation_code
            FROM silver_dol_childcare.price_revision AS stored
            WHERE stored.metric_key = incoming.metric_key
              AND stored.geo_id = incoming.geo_id
              AND stored.study_year = incoming.study_year
              AND stored.capture_id <> incoming.capture_id
            ORDER BY stored.created_at DESC, stored.capture_id DESC
            LIMIT 1
        ) AS newest
        WHERE newest.value IS NOT DISTINCT FROM incoming.value
          AND newest.value_status = incoming.value_status
          AND newest.imputation_code IS NOT DISTINCT FROM incoming.imputation_code
    )
    ON CONFLICT (capture_id, source_row_index, metric_key) DO NOTHING
"""


class ChildcareReconciliationError(RuntimeError):
    """A replayed workbook does not account for every parsed price."""


class ChildcarePublicationError(RuntimeError):
    """A workbook has not reached the silver publication gate."""


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform one captured workbook; return the fact count."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT release, capture_id::TEXT, status
                FROM control.dol_childcare_file WHERE run_id = %s
                """,
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise ChildcareReconciliationError(
                    f"run {run_id} captured no NDCP workbook"
                )
            release_key, capture_id, status = row
            if status == "unchanged":
                # The bytes repeat the last published workbook (utility.unchanged).
                connection.commit()
                return 0
            if status == "published":
                cursor.execute(
                    "SELECT COUNT(*) FROM silver_dol_childcare.fact_price WHERE run_id = %s",
                    (str(run_id),),
                )
                return int(cursor.fetchone()[0])
            release = get_release(str(release_key))
            parsed = parse_capture(
                load_captured_payload(connection_factory, UUID(capture_id)),
                release=release,
            )
            cursor.execute(_INCOMING_TABLE_SQL)
            if parsed.prices:
                execute_values(
                    cursor,
                    f"INSERT INTO dol_childcare_incoming ({_COLUMNS}) VALUES %s",
                    [
                        (
                            capture_id,
                            price.source_row_index,
                            price.metric_key,
                            str(run_id),
                            release.key,
                            price.care_type,
                            price.age_group,
                            price.study_year,
                            price.fips_code,
                            price.county_name or None,
                            price.geo_id,
                            price.value_source,
                            price.value,
                            price.value_status,
                            price.missing_reason,
                            price.imputed,
                            price.imputation_code,
                            price.source_record_id,
                        )
                        for price in parsed.prices
                    ],
                    page_size=PAGE_SIZE,
                )
            if parsed.quarantined:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_dol_childcare.observation_quarantine (
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
            cursor.execute(_STORE_CHANGED_PRICES_SQL)
            cursor.execute(
                "SELECT COUNT(*) FROM silver_dol_childcare.price_revision WHERE run_id = %s",
                (str(run_id),),
            )
            revisions = int(cursor.fetchone()[0])
            repeated = len(parsed.prices) - revisions
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (revision.fips_code)
                       %s, revision.release, 'county', revision.fips_code,
                       revision.county_name, %s, entity.geo_sk,
                       CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                       revision.capture_id,
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       CASE WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_dol_childcare.price_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.fips_code, revision.study_year DESC
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    source_label = EXCLUDED.source_label,
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                -- Only a row whose content changed is rewritten (CHURN RULE).
                WHERE (silver_ref.geography_resolution.source_label, silver_ref.geography_resolution.geo_sk, silver_ref.geography_resolution.resolution_method, silver_ref.geography_resolution.status, silver_ref.geography_resolution.reason_code)
                      IS DISTINCT FROM
                      (EXCLUDED.source_label, EXCLUDED.geo_sk, EXCLUDED.resolution_method, EXCLUDED.status, EXCLUDED.reason_code)
                """,
                (SOURCE_CODE, release.last_year, str(run_id)),
            )
            cursor.execute(
                """
                INSERT INTO silver_dol_childcare.fact_price (
                    metric_key, geo_id, study_year, capture_id, run_id, release,
                    care_type, age_group, retrieved_at, fips_code, county_name,
                    geo_sk, geo_type, geography_status, value_source, value,
                    value_status, missing_reason, imputed, imputation_code,
                    source_record_id
                )
                SELECT revision.metric_key, revision.geo_id, revision.study_year,
                       revision.capture_id, revision.run_id, revision.release,
                       revision.care_type, revision.age_group, capture.retrieved_at,
                       revision.fips_code, revision.county_name, entity.geo_sk,
                       'county',
                       CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                       revision.value_source, revision.value, revision.value_status,
                       revision.missing_reason, revision.imputed,
                       revision.imputation_code, revision.source_record_id
                FROM silver_dol_childcare.price_revision AS revision
                JOIN raw_capture.response_capture AS capture ON capture.capture_id = revision.capture_id
                LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (metric_key, geo_id, study_year, capture_id) DO NOTHING
                """,
                (str(run_id),),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_dol_childcare.fact_price WHERE run_id = %s",
                (str(run_id),),
            )
            facts = int(cursor.fetchone()[0])
            if facts != revisions or revisions + repeated != len(parsed.prices):
                raise ChildcareReconciliationError(
                    f"run {run_id} reconciles {revisions} revisions, {repeated} "
                    f"repeats and {facts} facts against {len(parsed.prices)} parsed"
                )
            quarantined_rows = len(
                {
                    q.source_row_index
                    for q in parsed.quarantined
                    if q.source_row_index > 0
                }
            )
            cursor.execute(
                """
                UPDATE control.dol_childcare_file
                   SET captured_row_count = %s,
                       quarantined_row_count = %s,
                       repeated_price_count = %s,
                       status = %s,
                       updated_at = NOW()
                 WHERE run_id = %s AND status <> 'published'
                """,
                (
                    parsed.row_count,
                    quarantined_rows,
                    repeated,
                    "quarantined" if parsed.rejected else "silver_ready",
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
    """Expose a reconciled workbook through the gold views; return 1 when published."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.dol_childcare_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.dol_childcare_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise ChildcarePublicationError(f"run {run_id} was never replayed")
            if published:
                # Prevention list: statistics follow every load.
                cursor.execute("ANALYZE silver_dol_childcare.fact_price")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_dol_childcare"
        )
    return published
