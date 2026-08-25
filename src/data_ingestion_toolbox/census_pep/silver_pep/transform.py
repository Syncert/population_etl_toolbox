"""Conform capture-scoped PEP revisions into source-faithful silver facts."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from data_ingestion_toolbox.census_pep.config import CONFIG

if TYPE_CHECKING:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def _get_hook() -> PostgresHook:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=CONFIG.postgres_conn_id)


def transform_pep_to_silver(hook: PostgresHook | None = None) -> int:
    """Insert immutable facts while retaining unmapped/unsupported outcomes."""
    hook = hook or _get_hook()
    with hook.get_conn() as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO silver_pep.dim_measure (
                        metric_code, display_name, unit, is_component, allows_negative
                    )
                    SELECT DISTINCT metric_code,
                        INITCAP(REPLACE(metric_code, '_', ' ')), unit,
                        metric_code NOT IN ('ESTIMATESBASE', 'POPESTIMATE'),
                        metric_code NOT IN (
                            'ESTIMATESBASE', 'POPESTIMATE', 'BIRTHS', 'DEATHS'
                        )
                    FROM silver_pep.observation_revision
                    ON CONFLICT (metric_code) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        unit = EXCLUDED.unit,
                        is_component = EXCLUDED.is_component,
                        allows_negative = EXCLUDED.allows_negative,
                        updated_at = NOW()
                    """
                )
                cursor.execute(
                    """
                    INSERT INTO silver_pep.release_load (
                        capture_id, dataset_code, release_vintage, product_code,
                        source_record_count, observation_count,
                        completeness_status, completeness_reason
                    )
                    SELECT capture_id, MIN(dataset_code), MIN(release_vintage),
                        MIN(product_code), COUNT(DISTINCT source_row_index), COUNT(*),
                        CASE WHEN BOOL_OR(
                            (dataset_code = 'pep_nst_alldata' AND summary_level = '010')
                            OR (dataset_code = 'pep_county_alldata' AND summary_level = '050')
                            OR (dataset_code = 'pep_subcounty' AND summary_level = '162')
                        ) THEN 'complete' ELSE 'incomplete' END,
                        CASE WHEN BOOL_OR(
                            (dataset_code = 'pep_nst_alldata' AND summary_level = '010')
                            OR (dataset_code = 'pep_county_alldata' AND summary_level = '050')
                            OR (dataset_code = 'pep_subcounty' AND summary_level = '162')
                        ) THEN NULL ELSE 'required principal summary level is absent' END
                    FROM silver_pep.observation_revision
                    GROUP BY capture_id
                    ON CONFLICT (capture_id) DO UPDATE SET
                        source_record_count = EXCLUDED.source_record_count,
                        observation_count = EXCLUDED.observation_count,
                        completeness_status = EXCLUDED.completeness_status,
                        completeness_reason = EXCLUDED.completeness_reason,
                        validated_at = NOW()
                    """
                )
                cursor.execute(
                    """
                    WITH raw_source_geographies AS (
                        SELECT DISTINCT capture_id, dataset_code, release_vintage,
                            name_source,
                            CASE WHEN summary_level = '010' THEN 'nation'
                                 WHEN summary_level = '040' THEN 'state'
                                 WHEN summary_level = '050' THEN 'county'
                                 WHEN summary_level = '162' THEN 'place'
                                 ELSE 'unsupported' END AS geo_type,
                            CASE WHEN summary_level = '010' THEN 'us:1'
                                 WHEN summary_level = '040' THEN 'state:' || state_fips_source
                                 WHEN summary_level = '050' THEN 'state:' || state_fips_source || '|county:' || county_fips_source
                                 WHEN summary_level = '162' THEN 'state:' || state_fips_source || '|place:' || place_fips_source END AS candidate_geo_id,
                            CASE WHEN summary_level = '010' THEN '1'
                                 WHEN summary_level = '040' THEN state_fips_source
                                 WHEN summary_level = '050' THEN state_fips_source || county_fips_source
                                 WHEN summary_level = '162' THEN state_fips_source || place_fips_source
                                 ELSE summary_level || ':' || COALESCE(state_fips_source, '') || ':' || COALESCE(county_fips_source, '') || ':' || COALESCE(place_fips_source, '') END AS source_geo_code
                        FROM silver_pep.observation_revision
                    ), source_geographies AS (
                        SELECT DISTINCT ON (
                            source.dataset_code, source.geo_type,
                            source.source_geo_code, source.release_vintage
                        ) source.*
                        FROM raw_source_geographies AS source
                        JOIN raw_capture.response_capture AS capture USING (capture_id)
                        ORDER BY source.dataset_code, source.geo_type,
                            source.source_geo_code, source.release_vintage,
                            capture.retrieved_at DESC, source.capture_id DESC
                    )
                    INSERT INTO silver_ref.geography_resolution (
                        provider_source, provider_dataset, source_geo_type,
                        source_code, source_label, source_vintage, geo_sk,
                        resolution_method, evidence_capture_id, status, reason_code
                    )
                    SELECT 'CENSUS_PEP', source.dataset_code, source.geo_type,
                        source.source_geo_code, source.name_source,
                        source.release_vintage, entity.geo_sk,
                        CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                        source.capture_id,
                        CASE WHEN source.geo_type = 'unsupported' THEN 'unsupported'
                             WHEN entity.geo_sk IS NULL THEN 'unmapped'
                             ELSE 'resolved' END,
                        CASE WHEN source.geo_type = 'unsupported' THEN 'unsupported_summary_level'
                             WHEN entity.geo_sk IS NULL THEN 'canonical_geography_absent' END
                    FROM source_geographies AS source
                    LEFT JOIN silver_ref.dim_geo_entity AS entity
                      ON entity.geo_id = source.candidate_geo_id
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
                    """
                )
                cursor.execute(
                    """
                    WITH source_rows AS (
                        SELECT revision.*,
                            CASE
                                WHEN summary_level = '010' THEN 'nation'
                                WHEN summary_level = '040' THEN 'state'
                                WHEN summary_level = '050' THEN 'county'
                                WHEN summary_level = '162' THEN 'place'
                                ELSE 'unsupported'
                            END AS geo_type,
                            CASE
                                WHEN summary_level = '010' THEN 'us:1'
                                WHEN summary_level = '040' THEN 'state:' || state_fips_source
                                WHEN summary_level = '050' THEN 'state:' || state_fips_source || '|county:' || county_fips_source
                                WHEN summary_level = '162' THEN 'state:' || state_fips_source || '|place:' || place_fips_source
                            END AS candidate_geo_id,
                            CASE
                                WHEN summary_level = '010' THEN '1'
                                WHEN summary_level = '040' THEN state_fips_source
                                WHEN summary_level = '050' THEN state_fips_source || county_fips_source
                                WHEN summary_level = '162' THEN state_fips_source || place_fips_source
                                ELSE COALESCE(state_fips_source, '') || ':' || COALESCE(county_fips_source, '') || ':' || COALESCE(place_fips_source, '')
                            END AS source_geo_code
                        FROM silver_pep.observation_revision AS revision
                        WHERE value_status = 'valid' AND value IS NOT NULL
                    )
                    INSERT INTO silver_pep.fact_population_estimate (
                        capture_id, source_row_index, source_column_index,
                        dataset_code, release_vintage, product_code, metric_code,
                        observation_year, estimate_date, geo_id, geo_sk, geo_type,
                        geography_basis_date, resolution_status, summary_level,
                        source_geo_code, source_name, functional_status_source,
                        value_source, value, unit
                    )
                    SELECT source.capture_id, source.source_row_index,
                        source.source_column_index, source.dataset_code,
                        source.release_vintage, source.product_code,
                        source.metric_code, source.observation_year,
                        MAKE_DATE(source.observation_year, 7, 1),
                        source.candidate_geo_id, entity.geo_sk, source.geo_type,
                        release.geography_basis_date,
                        CASE WHEN source.geo_type = 'unsupported' THEN 'unsupported'
                             WHEN entity.geo_sk IS NULL THEN 'unmapped'
                             ELSE 'resolved' END,
                        source.summary_level, source.source_geo_code,
                        source.name_source, source.functional_status_source,
                        source.value_source, source.value, source.unit
                    FROM source_rows AS source
                    JOIN silver_pep.pep_release AS release
                      ON release.dataset_code = source.dataset_code
                     AND release.vintage_year = source.release_vintage
                     AND release.product_code = source.product_code
                    LEFT JOIN silver_ref.dim_geo_entity AS entity
                      ON entity.geo_id = source.candidate_geo_id
                    ON CONFLICT (capture_id, source_row_index, source_column_index)
                    DO NOTHING
                    """
                )
                inserted = max(cursor.rowcount, 0)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
    logger.info("Census PEP silver transform inserted %d facts", inserted)
    return inserted
