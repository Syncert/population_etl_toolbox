"""Transactional CDC silver conformance and geography resolution."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from ..registry import CdcAsset


class CdcReconciliationError(RuntimeError):
    """A captured release cannot be marked ready for publication."""


def transform_release(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    asset: CdcAsset,
    release_watermark: str,
) -> int:
    """Conform one replayed release atomically and enforce reconciliation."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_cdc.dim_dataset_release (
                    asset_id, release_watermark, socrata_id, title,
                    methodology_url, geography_basis, parser_contract_version,
                    estimate_method, population_basis, metadata_capture_id,
                    source_run_id, source_record_count, quarantine_count, status
                )
                SELECT release.asset_id, release.release_watermark::TEXT,
                       release.socrata_id, release.title, %s, %s, %s, %s, %s,
                       release.metadata_capture_id, release.run_id,
                       release.captured_row_count,
                       (SELECT COUNT(*)
                          FROM silver_cdc.observation_quarantine AS quarantine
                         WHERE quarantine.run_id = release.run_id),
                       'replaying'
                FROM control.cdc_dataset_release AS release
                WHERE release.run_id = %s
                  AND release.asset_id = %s
                  AND release.complete
                ON CONFLICT (asset_id, release_watermark) DO UPDATE SET
                    source_run_id = EXCLUDED.source_run_id,
                    source_record_count = EXCLUDED.source_record_count,
                    quarantine_count = EXCLUDED.quarantine_count,
                    status = CASE
                        WHEN silver_cdc.dim_dataset_release.status = 'published'
                        THEN 'published' ELSE 'replaying' END,
                    updated_at = NOW()
                """,
                (
                    asset.methodology_url,
                    asset.geography_basis,
                    asset.parser_contract_version,
                    asset.estimate_method,
                    asset.population_basis,
                    str(run_id),
                    asset.asset_id,
                ),
            )
            if cursor.rowcount != 1:
                raise CdcReconciliationError(
                    "CDC release is absent, quarantined, or incomplete"
                )
            cursor.execute(
                """
                INSERT INTO silver_cdc.dim_measure (
                    asset_id, measure_id, value_type_id, measure_label, topic,
                    value_type_label, unit, adjustment_status,
                    estimate_method, population_basis
                )
                SELECT DISTINCT asset_id, measure_id, value_type_id,
                       measure_label, topic, value_type_label, unit,
                       adjustment_status, estimate_method, population_basis
                FROM silver_cdc.observation_revision
                WHERE run_id = %s
                ON CONFLICT (asset_id, measure_id, value_type_id) DO UPDATE SET
                    measure_label = EXCLUDED.measure_label,
                    topic = EXCLUDED.topic,
                    value_type_label = EXCLUDED.value_type_label,
                    unit = EXCLUDED.unit,
                    adjustment_status = EXCLUDED.adjustment_status,
                    estimate_method = EXCLUDED.estimate_method,
                    population_basis = EXCLUDED.population_basis,
                    updated_at = NOW()
                """,
                (str(run_id),),
            )
            cursor.execute(
                """
                INSERT INTO silver_cdc.dim_stratum (stratum_id, strata)
                SELECT DISTINCT stratum_id, strata
                FROM silver_cdc.observation_revision
                WHERE run_id = %s
                ON CONFLICT (stratum_id) DO NOTHING
                """,
                (str(run_id),),
            )
            geography_vintage = 2020 if asset.asset_id == "places_county" else None
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (
                    revision.geo_type, revision.geo_source_code,
                    COALESCE(%s, revision.period_end)
                )
                    'CDC', revision.asset_id, revision.geo_type,
                    revision.geo_source_code, revision.geo_source_label,
                    COALESCE(%s, revision.period_end), entity.geo_sk,
                    CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                    revision.capture_id,
                    CASE WHEN revision.geo_type = 'unsupported' THEN 'unsupported'
                         WHEN entity.geo_sk IS NULL THEN 'unmapped'
                         ELSE 'resolved' END,
                    CASE WHEN revision.geo_type = 'unsupported'
                         THEN 'unsupported_provider_code'
                         WHEN entity.geo_sk IS NULL
                         THEN 'canonical_geography_absent' END
                FROM silver_cdc.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity
                  ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.geo_source_code,
                         COALESCE(%s, revision.period_end), revision.capture_id
                ON CONFLICT (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_vintage
                ) DO UPDATE SET
                    source_label = EXCLUDED.source_label,
                    geo_sk = EXCLUDED.geo_sk,
                    resolution_method = EXCLUDED.resolution_method,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                """,
                (
                    geography_vintage,
                    geography_vintage,
                    str(run_id),
                    geography_vintage,
                ),
            )
            cursor.execute(
                """
                INSERT INTO silver_cdc.fact_health_observation (
                    asset_id, release_watermark, source_record_id, source_run_id,
                    capture_id, source_row_index, measure_id, value_type_id,
                    stratum_id, period_start, period_end, geo_id, geo_sk,
                    geo_type, geography_status, value_source, value,
                    value_status, unit, adjustment_status, confidence_lower,
                    confidence_upper, footnote_code, footnote_text,
                    estimate_method, population_basis, total_population,
                    population_18_plus, transformation_version
                )
                SELECT revision.asset_id, revision.release_watermark,
                       revision.source_record_id, revision.run_id,
                       revision.capture_id, revision.source_row_index,
                       revision.measure_id, revision.value_type_id,
                       revision.stratum_id, revision.period_start,
                       revision.period_end, revision.geo_id, entity.geo_sk,
                       revision.geo_type,
                       CASE WHEN revision.geo_type = 'unsupported' THEN 'unsupported'
                            WHEN entity.geo_sk IS NULL THEN 'unmapped'
                            ELSE 'resolved' END,
                       revision.value_source, revision.value,
                       revision.value_status, revision.unit,
                       revision.adjustment_status, revision.confidence_lower,
                       revision.confidence_upper, revision.footnote_code,
                       revision.footnote_text, revision.estimate_method,
                       revision.population_basis, revision.total_population,
                       revision.population_18_plus, %s
                FROM silver_cdc.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity
                  ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (
                    asset_id, release_watermark, source_record_id
                ) DO NOTHING
                """,
                (asset.parser_contract_version, str(run_id)),
            )
            cursor.execute(
                """
                SELECT release.captured_row_count,
                       (SELECT COUNT(*)
                          FROM silver_cdc.observation_revision
                         WHERE run_id = release.run_id),
                       (SELECT COUNT(*)
                          FROM silver_cdc.observation_quarantine
                         WHERE run_id = release.run_id),
                       (SELECT COUNT(*)
                          FROM silver_cdc.fact_health_observation
                         WHERE asset_id = release.asset_id
                           AND release_watermark = release.release_watermark::TEXT)
                FROM control.cdc_dataset_release AS release
                WHERE release.run_id = %s
                """,
                (str(run_id),),
            )
            captured, revisions, quarantined, facts = cursor.fetchone()
            if captured != revisions + quarantined or facts != revisions:
                raise CdcReconciliationError("CDC release row reconciliation failed")
            cursor.execute(
                """
                UPDATE silver_cdc.dim_dataset_release
                   SET status = CASE WHEN status = 'published'
                                     THEN status ELSE 'silver_ready' END,
                       reconciled_at = NOW(), updated_at = NOW()
                 WHERE asset_id = %s AND release_watermark = %s
                """,
                (asset.asset_id, release_watermark),
            )
            cursor.execute(
                """
                UPDATE control.cdc_dataset_release
                   SET status = 'silver_ready', updated_at = NOW()
                 WHERE run_id = %s
                """,
                (str(run_id),),
            )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return int(facts)
