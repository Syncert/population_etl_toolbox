"""Replay one captured CMS file into silver and publish it.

A replay parses the capture, places each cost report on a county by name,
stores every facility row and figure as published, and writes the file's
area sums once (``silver_cms_ltc.area_total``), all in one transaction. The
run commits only when every data row is either stored or set aside.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload
from ..registry import get_series
from .parse import parse_file, place_county

PUBLISHER_SCHEMA = "gold_cms_ltc"


class CmsReconciliationError(RuntimeError):
    """A replay that cannot account for every captured row."""


_COUNTIES_SQL = """
    SELECT entity.geo_id, entity.geo_sk, entity.state_fips, attribute.name
    FROM silver_ref.dim_geo_entity AS entity
    JOIN LATERAL (
        SELECT version.name
        FROM silver_ref.dim_geo_entity_version AS version
        WHERE version.geo_sk = entity.geo_sk
        ORDER BY version.geography_vintage DESC, version.ingested_at DESC
        LIMIT 1
    ) AS attribute ON TRUE
    WHERE entity.geo_type = 'county' AND entity.state_fips = ANY(%s)
"""

#: One facility per certification number: a cost-report facility filing two
#: reports counts by the one whose fiscal year ends last (then the higher
#: report number); a CMS-671 row is already one per facility, placed by the
#: newest published cost report that put it on a county.
_FACILITY_SQL = {
    "cost_report": """
        CREATE TEMP TABLE cms_ltc_facility ON COMMIT DROP AS
        SELECT DISTINCT ON (report.ccn)
               report.ccn, report.report_key, report.state_fips,
               report.county_geo_id, report.county_geo_sk
        FROM silver_cms_ltc.facility_report AS report
        WHERE report.run_id = %(run_id)s
        ORDER BY report.ccn, report.fiscal_year_end DESC NULLS LAST,
                 LENGTH(report.report_key) DESC, report.report_key DESC
    """,
    "characteristics": """
        CREATE TEMP TABLE cms_ltc_facility ON COMMIT DROP AS
        SELECT report.ccn, report.report_key, report.state_fips,
               placement.county_geo_id, placement.county_geo_sk
        FROM silver_cms_ltc.facility_report AS report
        LEFT JOIN LATERAL (
            SELECT cost.county_geo_id, cost.county_geo_sk
            FROM silver_cms_ltc.facility_report AS cost
            JOIN control.cms_ltc_file AS file ON file.run_id = cost.run_id
            WHERE cost.ccn = report.ccn
              AND cost.series_id = 'cost_report'
              AND cost.county_status = 'resolved'
              AND file.status = 'published'
            ORDER BY file.period_start DESC, cost.fiscal_year_end DESC NULLS LAST,
                     LENGTH(cost.report_key) DESC, cost.report_key DESC
            LIMIT 1
        ) AS placement ON TRUE
        WHERE report.run_id = %(run_id)s
    """,
}

_AREA_SQL = """
    WITH counted AS (
        SELECT facility.ccn, facility.state_fips, facility.county_geo_id,
               facility.county_geo_sk, count.measure_id, count.value
        FROM cms_ltc_facility AS facility
        JOIN silver_cms_ltc.facility_count AS count
          ON count.run_id = %(run_id)s
         AND count.report_key = facility.report_key
        WHERE count.value_status = 'valid'
    ),
    areas AS (
        SELECT measure_id, county_geo_id AS geo_id, 'county' AS geo_type,
               SUM(value) AS value, COUNT(*) AS facility_count, 0 AS unplaced
        FROM counted WHERE county_geo_id IS NOT NULL
        GROUP BY measure_id, county_geo_id
        UNION ALL
        SELECT measure_id, 'state:' || state_fips, 'state', SUM(value), COUNT(*),
               COUNT(*) FILTER (WHERE county_geo_id IS NULL)
        FROM counted WHERE state_fips IS NOT NULL
        GROUP BY measure_id, state_fips
        UNION ALL
        SELECT measure_id, 'us:1', 'nation', SUM(value), COUNT(*),
               COUNT(*) FILTER (WHERE county_geo_id IS NULL)
        FROM counted
        GROUP BY measure_id
    )
    INSERT INTO silver_cms_ltc.area_total (
        run_id, measure_id, geo_id, geo_type, geo_sk, value, facility_count,
        unplaced_facility_count, geography_status, source_record_id
    )
    SELECT %(run_id)s, areas.measure_id, areas.geo_id, areas.geo_type, entity.geo_sk,
           areas.value, areas.facility_count, areas.unplaced,
           CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
           md5(%(run_id)s || '|' || areas.measure_id || '|' || areas.geo_id)
    FROM areas
    LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = areas.geo_id
"""


def _file(cursor: Any, run_id: UUID) -> tuple[str, str, str]:
    cursor.execute(
        "SELECT series_id, capture_id::TEXT, status FROM control.cms_ltc_file WHERE run_id = %s",
        (str(run_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise CmsReconciliationError(f"run {run_id} has no captured CMS file")
    return row


def _counties(
    cursor: Any, state_codes: set[str]
) -> dict[str, list[tuple[str, int, str]]]:
    cursor.execute(_COUNTIES_SQL, (sorted(state_codes),))
    counties: dict[str, list[tuple[str, int, str]]] = {}
    for geo_id, geo_sk, state_fips, name in cursor.fetchall():
        counties.setdefault(state_fips, []).append((geo_id, geo_sk, name))
    return counties


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform one captured file; return the facility rows stored."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            series_id, capture_id, status = _file(cursor, run_id)
            if status == "unchanged":
                return 0
            series = get_series(series_id)
            _upsert_measures(cursor, series)
            payload = load_captured_payload(connection_factory, UUID(capture_id))
            parsed = parse_file(series, payload)
            if any(item.source_row_index == 0 for item in parsed.quarantined):
                _quarantine(cursor, run_id, capture_id, parsed.quarantined)
                cursor.execute(
                    "UPDATE control.cms_ltc_file SET status = 'quarantined', text_encoding = %s, updated_at = NOW() WHERE run_id = %s",
                    (parsed.encoding, str(run_id)),
                )
                connection.commit()
                return 0
            counties = (
                _counties(
                    cursor, {row.state_fips for row in parsed.rows if row.state_fips}
                )
                if series.county_column
                else {}
            )
            report_rows = []
            count_rows = []
            for row in parsed.rows:
                if series.county_column:
                    county_status, geo_id, geo_sk = place_county(
                        row.county_name, row.state_fips, counties
                    )
                else:
                    county_status, geo_id, geo_sk = "not_listed", None, None
                report_rows.append(
                    (
                        str(run_id),
                        row.report_key,
                        series.series_id,
                        row.ccn,
                        capture_id,
                        row.source_row_index,
                        row.facility_name,
                        row.street_address,
                        row.city,
                        row.state_code,
                        row.state_fips,
                        row.zip_code,
                        row.county_name,
                        row.fiscal_year_begin,
                        row.fiscal_year_end,
                        row.ownership_type,
                        row.hospital_based,
                        row.certification_date,
                        geo_id,
                        geo_sk,
                        county_status,
                    )
                )
                for measure_id, (published, value) in row.counts.items():
                    count_rows.append(
                        (
                            str(run_id),
                            row.report_key,
                            capture_id,
                            measure_id,
                            published,
                            value,
                            "valid" if value is not None else "missing",
                        )
                    )
            if report_rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_cms_ltc.facility_report (
                        run_id, report_key, series_id, ccn, capture_id, source_row_index,
                        facility_name, street_address, city, state_code, state_fips,
                        zip_code, county_name, fiscal_year_begin, fiscal_year_end,
                        ownership_type, hospital_based, certification_date,
                        county_geo_id, county_geo_sk, county_status
                    ) VALUES %s
                    ON CONFLICT (run_id, report_key) DO NOTHING
                    """,
                    report_rows,
                    page_size=5000,
                )
            if count_rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_cms_ltc.facility_count (
                        run_id, report_key, capture_id, measure_id, value_source, value,
                        value_status
                    ) VALUES %s
                    ON CONFLICT (run_id, report_key, measure_id) DO NOTHING
                    """,
                    count_rows,
                    page_size=5000,
                )
            _quarantine(cursor, run_id, capture_id, parsed.quarantined)
            set_aside = len({item.source_row_index for item in parsed.quarantined})
            cursor.execute(
                "SELECT COUNT(*) FROM silver_cms_ltc.facility_report WHERE run_id = %s",
                (str(run_id),),
            )
            stored = int(cursor.fetchone()[0])
            if stored + set_aside != parsed.row_count:
                raise CmsReconciliationError(
                    f"run {run_id} stored {stored} and set aside {set_aside} "
                    f"of {parsed.row_count} rows"
                )
            cursor.execute("ANALYZE silver_cms_ltc.facility_report")
            cursor.execute(_FACILITY_SQL[series.series_id], {"run_id": str(run_id)})
            cursor.execute("ANALYZE cms_ltc_facility")
            cursor.execute(
                "DELETE FROM silver_cms_ltc.area_total WHERE run_id = %s",
                (str(run_id),),
            )
            cursor.execute(_AREA_SQL, {"run_id": str(run_id)})
            cursor.execute(
                """
                UPDATE control.cms_ltc_file
                   SET row_count = %s, kept_row_count = %s, text_encoding = %s,
                       status = 'silver_ready', updated_at = NOW()
                 WHERE run_id = %s AND status <> 'published'
                """,
                (parsed.row_count, stored, parsed.encoding, str(run_id)),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return stored


def _upsert_measures(cursor: Any, series: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_cms_ltc.dim_measure (
            series_id, measure_id, source_column, measure_label, unit, basis,
            accrual, parser_contract_version
        ) VALUES %s
        ON CONFLICT (series_id, measure_id) DO UPDATE SET
            source_column = EXCLUDED.source_column,
            measure_label = EXCLUDED.measure_label,
            unit = EXCLUDED.unit,
            basis = EXCLUDED.basis,
            accrual = EXCLUDED.accrual,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        WHERE (silver_cms_ltc.dim_measure.source_column, silver_cms_ltc.dim_measure.measure_label,
               silver_cms_ltc.dim_measure.unit, silver_cms_ltc.dim_measure.basis,
               silver_cms_ltc.dim_measure.accrual, silver_cms_ltc.dim_measure.parser_contract_version)
              IS DISTINCT FROM
              (EXCLUDED.source_column, EXCLUDED.measure_label, EXCLUDED.unit, EXCLUDED.basis,
               EXCLUDED.accrual, EXCLUDED.parser_contract_version)
        """,
        [
            (
                series.series_id,
                measure.measure_id,
                measure.column,
                measure.label,
                measure.unit,
                measure.basis,
                series.accrual,
                series.parser_contract_version,
            )
            for measure in series.measures
        ],
    )


def _quarantine(cursor: Any, run_id: UUID, capture_id: str, items: Any) -> None:
    if not items:
        return
    execute_values(
        cursor,
        """
        INSERT INTO silver_cms_ltc.quarantine (
            run_id, capture_id, source_row_index, error_code, error_summary
        ) VALUES %s
        ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
        """,
        [
            (
                str(run_id),
                capture_id,
                item.source_row_index,
                item.error_code,
                item.error_summary,
            )
            for item in items
        ],
    )


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled file through the gold views; return 1 if published."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.cms_ltc_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                """,
                (str(run_id),),
            )
            published = cursor.rowcount
            if published:
                cursor.execute("ANALYZE silver_cms_ltc.area_total")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema=PUBLISHER_SCHEMA
        )
    return published
