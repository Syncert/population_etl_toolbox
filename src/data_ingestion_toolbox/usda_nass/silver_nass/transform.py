"""Transactional USDA NASS silver conformance and geography resolution.

Capture loading, revision persistence, and the atomic conformance step live
here so the pure parsers in :mod:`values` and :mod:`dimensions` stay free of
I/O. Publication is gated on exact reconciliation: every captured row must
become either a conformed fact or an explicit quarantine row.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import Json, execute_values

from ..registry import NassProduct
from .values import (
    CapturedSlicePayload,
    NassObservation,
    NassReplayError,
    ReplayResult,
    replay_slices,
)


class NassReconciliationError(RuntimeError):
    """A captured release cannot be marked ready for publication."""


def load_captured_slices(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    product: NassProduct,
) -> list[CapturedSlicePayload]:
    """Load exact slice bytes and their recorded control state for replay."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT slice_key, status, data_capture_id
                FROM control.usda_nass_slice
                WHERE run_id = %s AND product_id = %s
                ORDER BY slice_key
                """,
                (str(run_id), product.product_id),
            )
            recorded = cursor.fetchall()
            cursor.execute(
                """
                SELECT slice.data_capture_id, slice.slice_key,
                       slice.agg_level_desc, slice.year, slice.provider_count,
                       slice.captured_row_count, blob.payload,
                       blob.payload_checksum
                FROM control.usda_nass_slice AS slice
                JOIN raw_capture.response_capture AS capture
                  ON capture.capture_id = slice.data_capture_id
                JOIN raw_capture.payload_blob AS blob USING (payload_checksum)
                WHERE slice.run_id = %s
                  AND slice.product_id = %s
                  AND capture.source_code = 'USDA_NASS'
                ORDER BY slice.slice_key
                """,
                (str(run_id), product.product_id),
            )
            rows = cursor.fetchall()
    finally:
        database_connection.close()

    if not recorded:
        raise NassReplayError("USDA NASS run recorded no registered slices")
    incomplete = [
        slice_key
        for slice_key, status, capture_id in recorded
        if status not in {"captured", "empty"}
        or (status == "captured" and capture_id is None)
    ]
    if incomplete:
        raise NassReplayError(
            f"USDA NASS release has unusable slices: {sorted(incomplete)}"
        )
    return [
        CapturedSlicePayload(
            capture_id=capture_id,
            slice_key=slice_key,
            agg_level_desc=agg_level_desc,
            year=int(year),
            provider_count=int(provider_count),
            captured_row_count=int(captured_row_count),
            payload=bytes(payload),
            payload_checksum=checksum,
        )
        for (
            capture_id,
            slice_key,
            agg_level_desc,
            year,
            provider_count,
            captured_row_count,
            payload,
            checksum,
        ) in rows
    ]


def replay_captured_run(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    product: NassProduct,
    release_watermark: str,
) -> ReplayResult:
    """Replay one complete run solely from durable capture bytes."""
    slices = load_captured_slices(connection_factory, run_id=run_id, product=product)
    if not slices:
        # Every registered slice was empty. That is a real provider outcome and
        # must not masquerade as a publishable release.
        raise NassReplayError("USDA NASS release captured no observation payloads")
    return replay_slices(product, slices, release_watermark=release_watermark)


def _revision_row(
    observation: NassObservation,
    *,
    run_id: UUID,
) -> tuple[object, ...]:
    return (
        str(observation.capture_id),
        observation.source_row_index,
        str(run_id),
        observation.product_id,
        observation.release_watermark,
        observation.slice_key,
        observation.source_record_id,
        Json(observation.source_row),
        observation.commodity.commodity_sk,
        observation.commodity.sector_desc,
        observation.commodity.group_desc,
        observation.commodity.commodity_desc,
        observation.commodity.class_desc,
        observation.commodity.prodn_practice_desc,
        observation.commodity.util_practice_desc,
        observation.statistic.statistic_sk,
        observation.statistic.source_desc,
        observation.statistic.statisticcat_desc,
        observation.statistic.short_desc,
        observation.statistic.unit_desc,
        observation.statistic.freq_desc,
        observation.statistic.value_kind,
        observation.statistic.calculation_basis,
        observation.statistic.additive_behavior,
        observation.statistic.additive_behavior_known,
        observation.domain.domain_sk,
        observation.domain.domain_desc,
        observation.domain.domaincat_desc,
        observation.geography.geo_type,
        observation.geography.geo_id,
        observation.geography.geo_source_code,
        observation.geography.agg_level_desc,
        observation.geography.state_fips,
        observation.geography.county_fips,
        observation.geography.location_desc,
        observation.geography.state_alpha,
        observation.geography.state_name,
        observation.geography.county_name,
        observation.geography.asd_code,
        observation.geography.region_desc,
        observation.geography.watershed_code,
        observation.period.year,
        observation.period.begin_code,
        observation.period.end_code,
        observation.period.reference_period_desc,
        observation.period.week_ending,
        observation.value_source,
        observation.value,
        observation.value_status,
        observation.suppression_code,
        observation.cv_source,
        observation.cv_value,
        observation.cv_status,
        observation.cv_symbol,
        observation.load_time,
    )


def persist_replay_result(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    product: NassProduct,
    release_watermark: str,
    result: ReplayResult,
) -> None:
    """Persist capture-scoped source revisions and explicit quarantine rows."""
    if result.input_count != len(result.observations) + len(result.quarantined):
        raise NassReplayError("USDA NASS replay reconciliation failed before write")
    revisions = [
        _revision_row(observation, run_id=run_id) for observation in result.observations
    ]
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            if revisions:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_nass.observation_revision (
                        capture_id, source_row_index, run_id, product_id,
                        release_watermark, slice_key, source_record_id,
                        source_record, commodity_sk, sector_desc, group_desc,
                        commodity_desc, class_desc, prodn_practice_desc,
                        util_practice_desc, statistic_sk, source_desc,
                        statisticcat_desc, short_desc, unit_desc, freq_desc,
                        value_kind, calculation_basis, additive_behavior,
                        additive_behavior_known, domain_sk, domain_desc,
                        domaincat_desc, geo_type, geo_id, geo_source_code,
                        agg_level_desc,
                        state_fips, county_fips, location_desc, state_alpha,
                        state_name, county_name, asd_code, region_desc,
                        watershed_code, year, begin_code, end_code,
                        reference_period_desc, week_ending, value_source, value,
                        value_status, suppression_code, cv_source, cv_value,
                        cv_status, cv_symbol, load_time
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index) DO NOTHING
                    """,
                    revisions,
                    page_size=500,
                )
            if result.quarantined:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_nass.observation_quarantine (
                        run_id, product_id, release_watermark, slice_key,
                        source_row_index, error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (
                        run_id, product_id, release_watermark, slice_key,
                        source_row_index, error_code
                    ) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            product.product_id,
                            release_watermark,
                            item.slice_key,
                            item.source_row_index,
                            item.error_code,
                            item.error_summary,
                        )
                        for item in result.quarantined
                    ],
                    page_size=500,
                )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()


def transform_release(
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    product: NassProduct,
    release_watermark: str,
) -> int:
    """Conform one replayed release atomically and enforce reconciliation."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO silver_nass.dim_dataset_release (
                    product_id, release_watermark, label, source_desc,
                    slice_mode, methodology_url, parser_contract_version,
                    incremental_field, release_expectation, registered_years,
                    source_run_id, source_record_count, quarantine_count,
                    slice_count, status
                )
                SELECT release.product_id, release.extraction_watermark,
                       %s, %s, release.slice_mode, %s, %s, %s, %s, %s::JSONB,
                       release.run_id, release.captured_row_count,
                       (SELECT COUNT(*)
                          FROM silver_nass.observation_quarantine AS quarantine
                         WHERE quarantine.run_id = release.run_id),
                       release.slice_count, 'replaying'
                FROM control.usda_nass_release AS release
                WHERE release.run_id = %s
                  AND release.product_id = %s
                  AND release.complete
                ON CONFLICT (product_id, release_watermark) DO UPDATE SET
                    source_run_id = EXCLUDED.source_run_id,
                    source_record_count = EXCLUDED.source_record_count,
                    quarantine_count = EXCLUDED.quarantine_count,
                    slice_count = EXCLUDED.slice_count,
                    slice_mode = EXCLUDED.slice_mode,
                    status = CASE
                        WHEN silver_nass.dim_dataset_release.status = 'published'
                        THEN 'published' ELSE 'replaying' END,
                    updated_at = NOW()
                """,
                (
                    product.label,
                    product.source_desc,
                    product.methodology_url,
                    product.parser_contract_version,
                    product.incremental_field,
                    product.release_expectation,
                    json.dumps(list(product.years("full"))),
                    str(run_id),
                    product.product_id,
                ),
            )
            if cursor.rowcount != 1:
                raise NassReconciliationError(
                    "USDA NASS release is absent, quarantined, or incomplete"
                )
            cursor.execute(
                """
                INSERT INTO silver_nass.dim_commodity (
                    commodity_sk, sector_desc, group_desc, commodity_desc,
                    class_desc, prodn_practice_desc, util_practice_desc
                )
                SELECT DISTINCT commodity_sk, sector_desc, group_desc,
                       commodity_desc, class_desc, prodn_practice_desc,
                       util_practice_desc
                FROM silver_nass.observation_revision
                WHERE run_id = %s
                ON CONFLICT (commodity_sk) DO NOTHING
                """,
                (str(run_id),),
            )
            cursor.execute(
                """
                INSERT INTO silver_nass.dim_statistic (
                    statistic_sk, source_desc, statisticcat_desc, short_desc,
                    unit_desc, freq_desc, value_kind, calculation_basis,
                    additive_behavior, additive_behavior_known
                )
                SELECT DISTINCT statistic_sk, source_desc, statisticcat_desc,
                       short_desc, unit_desc, freq_desc, value_kind,
                       calculation_basis, additive_behavior,
                       additive_behavior_known
                FROM silver_nass.observation_revision
                WHERE run_id = %s
                ON CONFLICT (statistic_sk) DO UPDATE SET
                    value_kind = EXCLUDED.value_kind,
                    calculation_basis = EXCLUDED.calculation_basis,
                    additive_behavior = EXCLUDED.additive_behavior,
                    additive_behavior_known = EXCLUDED.additive_behavior_known,
                    updated_at = NOW()
                """,
                (str(run_id),),
            )
            cursor.execute(
                """
                INSERT INTO silver_nass.dim_domain (
                    domain_sk, domain_desc, domaincat_desc
                )
                SELECT DISTINCT domain_sk, domain_desc, domaincat_desc
                FROM silver_nass.observation_revision
                WHERE run_id = %s
                ON CONFLICT (domain_sk) DO NOTHING
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
                SELECT DISTINCT ON (
                    revision.geo_type, revision.geo_source_code, revision.year
                )
                    'USDA_NASS', revision.product_id, revision.geo_type,
                    revision.geo_source_code, revision.location_desc,
                    revision.year, entity.geo_sk,
                    CASE WHEN entity.geo_sk IS NOT NULL THEN 'exact_code' END,
                    revision.capture_id,
                    CASE WHEN revision.geo_type = 'unsupported' THEN 'unsupported'
                         WHEN entity.geo_sk IS NULL THEN 'unmapped'
                         ELSE 'resolved' END,
                    CASE WHEN revision.geo_type = 'unsupported'
                         THEN 'unsupported_aggregate_level'
                         WHEN entity.geo_sk IS NULL
                         THEN 'canonical_geography_absent' END
                FROM silver_nass.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity
                  ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ORDER BY revision.geo_type, revision.geo_source_code,
                         revision.year, revision.capture_id
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
                (str(run_id),),
            )
            cursor.execute(
                """
                INSERT INTO silver_nass.fact_crop_observation (
                    product_id, release_watermark, source_record_id,
                    source_run_id, capture_id, source_row_index, slice_key,
                    commodity_sk, statistic_sk, domain_sk, geo_id, geo_sk,
                    geo_type, geography_status, geo_source_code,
                    agg_level_desc, location_desc,
                    state_fips, county_fips, year, freq_desc, begin_code,
                    end_code, reference_period_desc, week_ending, value_source,
                    value, value_status, suppression_code, unit_desc,
                    cv_source, cv_value, cv_status, cv_symbol, load_time,
                    source_desc, transformation_version
                )
                SELECT revision.product_id, revision.release_watermark,
                       revision.source_record_id, revision.run_id,
                       revision.capture_id, revision.source_row_index,
                       revision.slice_key, revision.commodity_sk,
                       revision.statistic_sk, revision.domain_sk,
                       revision.geo_id, entity.geo_sk, revision.geo_type,
                       CASE WHEN revision.geo_type = 'unsupported' THEN 'unsupported'
                            WHEN entity.geo_sk IS NULL THEN 'unmapped'
                            ELSE 'resolved' END,
                       revision.geo_source_code, revision.agg_level_desc,
                       revision.location_desc,
                       revision.state_fips, revision.county_fips, revision.year,
                       revision.freq_desc, revision.begin_code,
                       revision.end_code, revision.reference_period_desc,
                       revision.week_ending, revision.value_source,
                       revision.value, revision.value_status,
                       revision.suppression_code, revision.unit_desc,
                       revision.cv_source, revision.cv_value,
                       revision.cv_status, revision.cv_symbol,
                       revision.load_time, revision.source_desc, %s
                FROM silver_nass.observation_revision AS revision
                LEFT JOIN silver_ref.dim_geo_entity AS entity
                  ON entity.geo_id = revision.geo_id
                WHERE revision.run_id = %s
                ON CONFLICT (
                    product_id, release_watermark, source_record_id
                ) DO NOTHING
                """,
                (product.parser_contract_version, str(run_id)),
            )
            cursor.execute(
                """
                SELECT release.captured_row_count,
                       (SELECT COUNT(*)
                          FROM silver_nass.observation_revision
                         WHERE run_id = release.run_id),
                       (SELECT COUNT(*)
                          FROM silver_nass.observation_quarantine
                         WHERE run_id = release.run_id),
                       (SELECT COUNT(*)
                          FROM silver_nass.fact_crop_observation
                         WHERE product_id = release.product_id
                           AND release_watermark = release.extraction_watermark)
                FROM control.usda_nass_release AS release
                WHERE release.run_id = %s
                """,
                (str(run_id),),
            )
            captured, revisions_count, quarantined, facts = cursor.fetchone()
            if captured != revisions_count + quarantined or facts != revisions_count:
                raise NassReconciliationError(
                    "USDA NASS release row reconciliation failed"
                )
            cursor.execute(
                """
                UPDATE silver_nass.dim_dataset_release
                   SET status = CASE WHEN status = 'published'
                                     THEN status ELSE 'silver_ready' END,
                       reconciled_at = NOW(), updated_at = NOW()
                 WHERE product_id = %s AND release_watermark = %s
                """,
                (product.product_id, release_watermark),
            )
            cursor.execute(
                """
                UPDATE control.usda_nass_release
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
