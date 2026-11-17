"""Replay one captured HHS hospital capacity snapshot into silver and publish it.

A replay reads every page of the snapshot in order, stores each registered
figure of each hospital week as published, keeps each hospital as its newest
week describes it, places it on the county its published FIPS code names,
and writes the snapshot's area sums once (``silver_hhs_hospital_capacity.
area_week``). The replay is one transaction and commits only when every row
of every page is stored or set aside.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..registry import MEASURES, PARSER_CONTRACT_VERSION
from .parse import FacilityWeek, parse_page

PUBLISHER_SCHEMA = "gold_hhs_hospital_capacity"


class HhsReconciliationError(RuntimeError):
    """A replay that cannot account for every captured row."""


_COUNTIES_SQL = """
    SELECT geo_id, geo_sk FROM silver_ref.dim_geo_entity
    WHERE geo_type = 'county' AND geo_id = ANY(%s)
"""

#: Each snapshot's sums by county, state and nation, for every week and figure.
_AREA_SQL = """
    WITH counted AS (
        SELECT facility.state_fips, facility.county_geo_id, value.measure_id,
               value.collection_week, value.value, value.value_status
        FROM silver_hhs_hospital_capacity.facility_week_value AS value
        JOIN silver_hhs_hospital_capacity.facility AS facility
          ON facility.snapshot_id = value.snapshot_id
         AND facility.hospital_pk = value.hospital_pk
        WHERE value.snapshot_id = %(snapshot_id)s
    ),
    areas AS (
        SELECT measure_id, collection_week, county_geo_id AS geo_id, 'county' AS geo_type,
               SUM(value) AS value, COUNT(*) AS facilities,
               COUNT(*) FILTER (WHERE value_status = 'valid') AS reporting,
               COUNT(*) FILTER (WHERE value_status = 'suppressed') AS suppressed,
               COUNT(*) FILTER (WHERE value_status = 'missing') AS missing,
               0 AS unplaced
        FROM counted WHERE county_geo_id IS NOT NULL
        GROUP BY measure_id, collection_week, county_geo_id
        UNION ALL
        SELECT measure_id, collection_week, 'state:' || state_fips, 'state', SUM(value), COUNT(*),
               COUNT(*) FILTER (WHERE value_status = 'valid'),
               COUNT(*) FILTER (WHERE value_status = 'suppressed'),
               COUNT(*) FILTER (WHERE value_status = 'missing'),
               COUNT(*) FILTER (WHERE county_geo_id IS NULL)
        FROM counted WHERE state_fips IS NOT NULL
        GROUP BY measure_id, collection_week, state_fips
        UNION ALL
        SELECT measure_id, collection_week, 'us:1', 'nation', SUM(value), COUNT(*),
               COUNT(*) FILTER (WHERE value_status = 'valid'),
               COUNT(*) FILTER (WHERE value_status = 'suppressed'),
               COUNT(*) FILTER (WHERE value_status = 'missing'),
               COUNT(*) FILTER (WHERE county_geo_id IS NULL)
        FROM counted
        GROUP BY measure_id, collection_week
    )
    INSERT INTO silver_hhs_hospital_capacity.area_week (
        snapshot_id, measure_id, collection_week, geo_id, geo_type, geo_sk,
        geography_status, value, value_status, facility_count,
        reporting_facility_count, suppressed_count, missing_count,
        unplaced_facility_count, source_record_id
    )
    SELECT %(snapshot_id)s, areas.measure_id, areas.collection_week, areas.geo_id,
           areas.geo_type, entity.geo_sk,
           CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
           areas.value,
           CASE WHEN areas.reporting > 0 THEN 'valid'
                WHEN areas.suppressed > 0 THEN 'suppressed'
                ELSE 'missing' END,
           areas.facilities, areas.reporting, areas.suppressed, areas.missing,
           areas.unplaced,
           md5(%(snapshot_id)s || '|' || areas.measure_id || '|' || areas.geo_id
               || '|' || areas.collection_week::TEXT)
    FROM areas
    LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = areas.geo_id
"""


@dataclass
class _Facility:
    newest: FacilityWeek
    first_week: date
    last_week: date
    weeks: int


def _upsert_measures(cursor: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_hhs_hospital_capacity.dim_measure (
            measure_id, source_column, coverage_column, measure_label, unit, basis,
            parser_contract_version
        ) VALUES %s
        ON CONFLICT (measure_id) DO UPDATE SET
            source_column = EXCLUDED.source_column,
            coverage_column = EXCLUDED.coverage_column,
            measure_label = EXCLUDED.measure_label,
            unit = EXCLUDED.unit,
            basis = EXCLUDED.basis,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        WHERE (silver_hhs_hospital_capacity.dim_measure.source_column,
               silver_hhs_hospital_capacity.dim_measure.coverage_column,
               silver_hhs_hospital_capacity.dim_measure.measure_label,
               silver_hhs_hospital_capacity.dim_measure.unit,
               silver_hhs_hospital_capacity.dim_measure.basis,
               silver_hhs_hospital_capacity.dim_measure.parser_contract_version)
              IS DISTINCT FROM
              (EXCLUDED.source_column, EXCLUDED.coverage_column, EXCLUDED.measure_label,
               EXCLUDED.unit, EXCLUDED.basis, EXCLUDED.parser_contract_version)
        """,
        [
            (
                measure.measure_id,
                measure.column,
                measure.coverage_column,
                measure.label,
                measure.unit,
                measure.basis,
                PARSER_CONTRACT_VERSION,
            )
            for measure in MEASURES
        ],
    )


def _pages(cursor: Any, snapshot_id: UUID) -> list[tuple[str, str, int, int]]:
    cursor.execute(
        """
        SELECT run_id::TEXT, capture_id::TEXT, row_offset, row_count
        FROM control.hhs_hospital_capacity_page
        WHERE snapshot_id = %s ORDER BY page_index
        """,
        (str(snapshot_id),),
    )
    return list(cursor.fetchall())


def _quarantine(
    cursor: Any, snapshot_id: UUID, run_id: str, capture_id: str, items: Any
) -> None:
    if not items:
        return
    execute_values(
        cursor,
        """
        INSERT INTO silver_hhs_hospital_capacity.quarantine (
            snapshot_id, run_id, capture_id, source_row_index, error_code, error_summary
        ) VALUES %s
        ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
        """,
        [
            (
                str(snapshot_id),
                run_id,
                capture_id,
                item.source_row_index,
                item.error_code,
                item.error_summary,
            )
            for item in items
        ],
    )


def _store_values(
    cursor: Any, snapshot_id: UUID, capture_id: str, rows: tuple[FacilityWeek, ...]
) -> None:
    values = [
        (
            str(snapshot_id),
            row.hospital_pk,
            row.collection_week,
            measure_id,
            capture_id,
            row.source_row_index,
            figure.source,
            figure.value,
            figure.status,
            figure.coverage,
        )
        for row in rows
        for measure_id, figure in row.figures.items()
    ]
    if values:
        execute_values(
            cursor,
            """
            INSERT INTO silver_hhs_hospital_capacity.facility_week_value (
                snapshot_id, hospital_pk, collection_week, measure_id, capture_id,
                source_row_index, value_source, value, value_status, coverage_days
            ) VALUES %s
            """,
            values,
            page_size=10_000,
        )


def _store_facilities(
    cursor: Any, snapshot_id: UUID, facilities: dict[str, _Facility]
) -> None:
    codes = sorted(
        {
            f"state:{item.newest.fips_code[:2]}|county:{item.newest.fips_code[2:]}"
            for item in facilities.values()
            if item.newest.fips_code
        }
    )
    cursor.execute(_COUNTIES_SQL, (codes,))
    counties = dict(cursor.fetchall())
    rows = []
    for hospital_pk, item in sorted(facilities.items()):
        row = item.newest
        geo_id = geo_sk = None
        if row.fips_code is None:
            status = "not_listed"
        else:
            candidate = f"state:{row.fips_code[:2]}|county:{row.fips_code[2:]}"
            if candidate in counties and row.fips_code[:2] == row.state_fips:
                geo_id, geo_sk, status = candidate, counties[candidate], "resolved"
            else:
                status = "unmapped"
        rows.append(
            (
                str(snapshot_id),
                hospital_pk,
                row.ccn,
                row.hospital_name,
                row.street_address,
                row.city,
                row.zip_code,
                row.state_code,
                row.state_fips,
                row.hospital_subtype,
                row.fips_code,
                row.is_metro_micro,
                row.longitude,
                row.latitude,
                geo_id,
                geo_sk,
                status,
                item.first_week,
                item.last_week,
                item.weeks,
            )
        )
    if rows:
        execute_values(
            cursor,
            """
            INSERT INTO silver_hhs_hospital_capacity.facility (
                snapshot_id, hospital_pk, ccn, hospital_name, street_address, city,
                zip_code, state_code, state_fips, hospital_subtype, fips_code,
                is_metro_micro, longitude, latitude, county_geo_id, county_geo_sk,
                county_status, first_week, last_week, week_count
            ) VALUES %s
            """,
            rows,
            page_size=5000,
        )


def _clear(cursor: Any, snapshot_id: UUID) -> None:
    for relation in ("area_week", "facility_week_value", "facility", "quarantine"):
        cursor.execute(
            f"DELETE FROM silver_hhs_hospital_capacity.{relation} WHERE snapshot_id = %s",
            (str(snapshot_id),),
        )


def replay_snapshot(factory: Callable[[], Any], *, snapshot_id: UUID) -> int:
    """Parse and conform one captured snapshot; return the hospital weeks stored."""
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT status FROM control.hhs_hospital_capacity_snapshot WHERE snapshot_id = %s",
                (str(snapshot_id),),
            )
            found = cursor.fetchone()
            if found is None:
                raise HhsReconciliationError(f"no snapshot {snapshot_id}")
            if found[0] != "captured":
                return 0
            _upsert_measures(cursor)
            _clear(cursor, snapshot_id)
            seen: set[tuple[str, date]] = set()
            facilities: dict[str, _Facility] = {}
            stored = set_aside = total = 0
            for run_id, capture_id, offset, _row_count in _pages(cursor, snapshot_id):
                payload = load_captured_payload(factory, UUID(capture_id))
                parsed = parse_page(payload, first_row_index=offset + 1, seen=seen)
                _quarantine(cursor, snapshot_id, run_id, capture_id, parsed.quarantined)
                if any(item.source_row_index == 0 for item in parsed.quarantined):
                    cursor.execute(
                        """
                        UPDATE control.hhs_hospital_capacity_snapshot
                           SET status = 'quarantined', updated_at = NOW()
                         WHERE snapshot_id = %s
                        """,
                        (str(snapshot_id),),
                    )
                    connection.commit()
                    return 0
                _store_values(cursor, snapshot_id, capture_id, parsed.rows)
                for row in parsed.rows:
                    item = facilities.get(row.hospital_pk)
                    if item is None:
                        facilities[row.hospital_pk] = _Facility(
                            row, row.collection_week, row.collection_week, 1
                        )
                        continue
                    item.weeks += 1
                    item.first_week = min(item.first_week, row.collection_week)
                    if row.collection_week > item.last_week:
                        item.last_week = row.collection_week
                        item.newest = row
                stored += len(parsed.rows)
                set_aside += len(parsed.quarantined)
                total += parsed.row_count
            if stored + set_aside != total:
                raise HhsReconciliationError(
                    f"snapshot {snapshot_id} stored {stored} and set aside {set_aside} "
                    f"of {total} rows"
                )
            _store_facilities(cursor, snapshot_id, facilities)
            cursor.execute("ANALYZE silver_hhs_hospital_capacity.facility")
            cursor.execute("ANALYZE silver_hhs_hospital_capacity.facility_week_value")
            cursor.execute(_AREA_SQL, {"snapshot_id": str(snapshot_id)})
            cursor.execute(
                """
                UPDATE control.hhs_hospital_capacity_snapshot
                   SET row_count = %s, kept_row_count = %s, status = 'silver_ready',
                       updated_at = NOW()
                 WHERE snapshot_id = %s
                """,
                (total, stored, str(snapshot_id)),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return stored


def publish_snapshot(factory: Callable[[], Any], *, snapshot_id: UUID) -> int:
    """Expose a reconciled snapshot through the gold views; return 1 if published."""
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.hhs_hospital_capacity_snapshot
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE snapshot_id = %s AND status = 'silver_ready'
                """,
                (str(snapshot_id),),
            )
            published = cursor.rowcount
            if published:
                cursor.execute("ANALYZE silver_hhs_hospital_capacity.area_week")
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
