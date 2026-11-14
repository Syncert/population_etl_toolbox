"""Replay one captured CDC case surveillance snapshot into silver and publish it.

A replay reads every slice of the snapshot in order, stores each group CDC's
API returned (``silver_cdc_covid_cases.slice_group``), and writes the
snapshot's counts by county, state and nation once
(``silver_cdc_covid_cases.area_month``). A record is placed on the county its
published county code names when that county is in the shared geography and
of the record's state; otherwise it counts in its state and the nation only.
The replay is one transaction and commits only when every group of every
slice is stored or set aside and the records add up to what was captured.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..registry import MEASURES, PARSER_CONTRACT_VERSION
from .parse import parse_slice

PUBLISHER_SCHEMA = "gold_cdc_covid_cases"


class CdcCovidReconciliationError(RuntimeError):
    """A replay that cannot account for every captured record."""


#: Each snapshot's counts by county, state and nation, month, age and measure.
_AREA_SQL = """
    WITH placed AS (
        SELECT grp.state_fips, grp.case_month, grp.age_key, grp.hosp_yn,
               grp.death_yn, grp.record_count, county.geo_id AS county_geo_id
        FROM silver_cdc_covid_cases.slice_group AS grp
        LEFT JOIN silver_ref.dim_geo_entity AS county
          ON county.geo_type = 'county'
         AND LEFT(grp.county_fips_code, 2) = grp.state_fips
         AND county.geo_id = 'state:' || LEFT(grp.county_fips_code, 2)
                             || '|county:' || RIGHT(grp.county_fips_code, 3)
        WHERE grp.snapshot_id = %(snapshot_id)s
    ),
    aged AS (
        SELECT state_fips, case_month, age_key, hosp_yn, death_yn, record_count,
               county_geo_id
        FROM placed
        UNION ALL
        SELECT state_fips, case_month, 'all_ages', hosp_yn, death_yn, record_count,
               county_geo_id
        FROM placed
    ),
    areas AS (
        SELECT county_geo_id AS geo_id, 'county' AS geo_type, case_month, age_key,
               hosp_yn, death_yn, record_count, FALSE AS unplaced
        FROM aged WHERE county_geo_id IS NOT NULL
        UNION ALL
        SELECT 'state:' || state_fips, 'state', case_month, age_key, hosp_yn,
               death_yn, record_count, county_geo_id IS NULL
        FROM aged WHERE state_fips IS NOT NULL
        UNION ALL
        SELECT 'us:1', 'nation', case_month, age_key, hosp_yn, death_yn,
               record_count, county_geo_id IS NULL
        FROM aged
    ),
    summed AS (
        SELECT geo_id, geo_type, case_month, age_key,
               SUM(record_count) AS case_records,
               COALESCE(SUM(record_count) FILTER (WHERE hosp_yn = 'Yes'), 0) AS hospitalized,
               COALESCE(SUM(record_count) FILTER (
                   WHERE hosp_yn IS NULL OR hosp_yn NOT IN ('Yes', 'No')), 0) AS hosp_unknown,
               COALESCE(SUM(record_count) FILTER (WHERE death_yn = 'Yes'), 0) AS died,
               COALESCE(SUM(record_count) FILTER (
                   WHERE death_yn IS NULL OR death_yn NOT IN ('Yes', 'No')), 0) AS death_unknown,
               COALESCE(SUM(record_count) FILTER (WHERE unplaced), 0) AS unplaced
        FROM areas
        GROUP BY geo_id, geo_type, case_month, age_key
    )
    INSERT INTO silver_cdc_covid_cases.area_month (
        snapshot_id, measure_id, age_key, case_month, geo_id, geo_sk, geo_type,
        geography_status, value, value_status, case_record_count,
        unplaced_record_count, source_record_id
    )
    SELECT %(snapshot_id)s, measure.measure_id, summed.age_key, summed.case_month,
           summed.geo_id, entity.geo_sk, summed.geo_type,
           CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
           measure.value, 'valid', summed.case_records, summed.unplaced,
           md5(%(snapshot_id)s || '|' || measure.measure_id || '|' || summed.age_key
               || '|' || summed.geo_id || '|' || summed.case_month::TEXT)
    FROM summed
    CROSS JOIN LATERAL (VALUES
        ('case_records', summed.case_records),
        ('hospitalized_records', summed.hospitalized),
        ('hospitalization_unknown_records', summed.hosp_unknown),
        ('death_records', summed.died),
        ('death_unknown_records', summed.death_unknown)
    ) AS measure (measure_id, value)
    LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = summed.geo_id
"""


def _upsert_measures(cursor: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_cdc_covid_cases.dim_measure (
            measure_id, measure_label, basis, parser_contract_version
        ) VALUES %s
        ON CONFLICT (measure_id) DO UPDATE SET
            measure_label = EXCLUDED.measure_label,
            basis = EXCLUDED.basis,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        WHERE (silver_cdc_covid_cases.dim_measure.measure_label,
               silver_cdc_covid_cases.dim_measure.basis,
               silver_cdc_covid_cases.dim_measure.parser_contract_version)
              IS DISTINCT FROM
              (EXCLUDED.measure_label, EXCLUDED.basis, EXCLUDED.parser_contract_version)
        """,
        [
            (measure.measure_id, measure.label, measure.basis, PARSER_CONTRACT_VERSION)
            for measure in MEASURES
        ],
    )


def _slices(cursor: Any, snapshot_id: UUID) -> list[tuple]:
    cursor.execute(
        """
        SELECT slice_index, run_id::TEXT, capture_id::TEXT, res_state, case_month,
               group_count, record_count
        FROM control.cdc_covid_cases_slice
        WHERE snapshot_id = %s ORDER BY slice_index
        """,
        (str(snapshot_id),),
    )
    return list(cursor.fetchall())


def _clear(cursor: Any, snapshot_id: UUID) -> None:
    for relation in ("area_month", "slice_group", "quarantine"):
        cursor.execute(
            f"DELETE FROM silver_cdc_covid_cases.{relation} WHERE snapshot_id = %s",
            (str(snapshot_id),),
        )


def _quarantine_snapshot(cursor: Any, snapshot_id: UUID) -> None:
    cursor.execute(
        """
        UPDATE control.cdc_covid_cases_snapshot
           SET status = 'quarantined', updated_at = NOW()
         WHERE snapshot_id = %s
        """,
        (str(snapshot_id),),
    )


def replay_snapshot(factory: Callable[[], Any], *, snapshot_id: UUID) -> int:
    """Parse and conform one captured snapshot; return the records stored."""
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, captured_record_count, group_count
                FROM control.cdc_covid_cases_snapshot WHERE snapshot_id = %s
                """,
                (str(snapshot_id),),
            )
            found = cursor.fetchone()
            if found is None:
                raise CdcCovidReconciliationError(f"no snapshot {snapshot_id}")
            status, captured_records, captured_groups = found
            if status != "captured":
                return 0
            _upsert_measures(cursor)
            _clear(cursor, snapshot_id)
            kept = set_aside_records = groups_read = 0
            for (
                slice_index,
                run_id,
                capture_id,
                res_state,
                case_month,
                group_count,
                _records,
            ) in _slices(cursor, snapshot_id):
                if capture_id is None:
                    raise CdcCovidReconciliationError(
                        f"snapshot {snapshot_id} slice {slice_index} was never captured"
                    )
                payload = load_captured_payload(factory, UUID(capture_id))
                parsed = parse_slice(
                    payload, res_state=res_state, case_month=case_month
                )
                if parsed.quarantined:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_cdc_covid_cases.quarantine (
                            snapshot_id, run_id, capture_id, source_row_index,
                            record_count, error_code, error_summary
                        ) VALUES %s
                        ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
                        """,
                        [
                            (
                                str(snapshot_id),
                                run_id,
                                capture_id,
                                item.source_row_index,
                                item.record_count,
                                item.error_code,
                                item.error_summary,
                            )
                            for item in parsed.quarantined
                        ],
                    )
                if any(item.source_row_index == 0 for item in parsed.quarantined) or (
                    parsed.group_count != group_count
                ):
                    _quarantine_snapshot(cursor, snapshot_id)
                    connection.commit()
                    return 0
                if parsed.groups:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO silver_cdc_covid_cases.slice_group (
                            snapshot_id, slice_index, source_row_index, capture_id,
                            res_state, state_fips, case_month, county_fips_code,
                            age_group, age_key, hosp_yn, death_yn, record_count
                        ) VALUES %s
                        """,
                        [
                            (
                                str(snapshot_id),
                                slice_index,
                                group.source_row_index,
                                capture_id,
                                res_state,
                                parsed.state_fips,
                                parsed.case_month,
                                group.county_fips_code,
                                group.age_group,
                                group.age_key,
                                group.hosp_yn,
                                group.death_yn,
                                group.record_count,
                            )
                            for group in parsed.groups
                        ],
                        page_size=10_000,
                    )
                kept += sum(group.record_count for group in parsed.groups)
                set_aside_records += sum(
                    item.record_count or 0 for item in parsed.quarantined
                )
                groups_read += len(parsed.groups) + len(parsed.quarantined)
            if groups_read != captured_groups or (
                kept + set_aside_records != captured_records
            ):
                raise CdcCovidReconciliationError(
                    f"snapshot {snapshot_id} stored {kept} and set aside "
                    f"{set_aside_records} of {captured_records} records"
                )
            cursor.execute("ANALYZE silver_cdc_covid_cases.slice_group")
            cursor.execute(_AREA_SQL, {"snapshot_id": str(snapshot_id)})
            cursor.execute(
                """
                UPDATE control.cdc_covid_cases_snapshot
                   SET kept_record_count = %s, status = 'silver_ready', updated_at = NOW()
                 WHERE snapshot_id = %s
                """,
                (kept, str(snapshot_id)),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return kept


def publish_snapshot(factory: Callable[[], Any], *, snapshot_id: UUID) -> int:
    """Expose a reconciled snapshot through the gold views; return 1 if published."""
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.cdc_covid_cases_snapshot
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE snapshot_id = %s AND status = 'silver_ready'
                """,
                (str(snapshot_id),),
            )
            published = cursor.rowcount
            if published:
                cursor.execute("ANALYZE silver_cdc_covid_cases.area_month")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(factory, publisher_schema=PUBLISHER_SCHEMA)
    return published
