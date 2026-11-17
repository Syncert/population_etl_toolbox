"""Replay one captured IPEDS file into silver and publish it.

A directory (HD) replay stores every institution as published and places it
on the county its published county code names. An IC or EF A replay stores
every registered figure as published and writes the file's area sums once
(``silver_nces_ipeds.area_total``), placing each institution by the same
year's newest published directory file. Each replay is one transaction and
commits only when every data row is stored, set aside, or (EF A) of a level
the registry does not read.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..registry import MEASURES, get_component
from .parse import parse_file

PUBLISHER_SCHEMA = "gold_nces_ipeds"


class IpedsReconciliationError(RuntimeError):
    """A replay that cannot account for every captured row."""


_COUNTIES_SQL = """
    SELECT geo_id, geo_sk FROM silver_ref.dim_geo_entity
    WHERE geo_type = 'county' AND geo_id = ANY(%s)
"""

#: Each institution of the file, placed by the same year's newest published
#: directory file (none when that year has no published directory).
_PLACED_SQL = """
    CREATE TEMP TABLE nces_ipeds_placed ON COMMIT DROP AS
    SELECT DISTINCT value.unitid, place.state_fips, place.county_geo_id
    FROM silver_nces_ipeds.institution_value AS value
    LEFT JOIN LATERAL (
        SELECT institution.state_fips, institution.county_geo_id
        FROM silver_nces_ipeds.institution AS institution
        JOIN control.nces_ipeds_file AS directory ON directory.run_id = institution.run_id
        WHERE institution.unitid = value.unitid
          AND institution.collection_year = value.collection_year
          AND directory.status = 'published'
        ORDER BY directory.published_at DESC
        LIMIT 1
    ) AS place ON TRUE
    WHERE value.run_id = %(run_id)s
"""

_AREA_SQL = """
    WITH counted AS (
        SELECT placed.unitid, placed.state_fips, placed.county_geo_id,
               value.measure_id, value.collection_year, value.value
        FROM nces_ipeds_placed AS placed
        JOIN silver_nces_ipeds.institution_value AS value
          ON value.run_id = %(run_id)s AND value.unitid = placed.unitid
        WHERE value.value_status = 'valid'
    ),
    areas AS (
        SELECT measure_id, collection_year, county_geo_id AS geo_id, 'county' AS geo_type,
               SUM(value) AS value, COUNT(*) AS institutions, 0 AS unplaced
        FROM counted WHERE county_geo_id IS NOT NULL
        GROUP BY measure_id, collection_year, county_geo_id
        UNION ALL
        SELECT measure_id, collection_year, 'state:' || state_fips, 'state', SUM(value), COUNT(*),
               COUNT(*) FILTER (WHERE county_geo_id IS NULL)
        FROM counted WHERE state_fips IS NOT NULL
        GROUP BY measure_id, collection_year, state_fips
        UNION ALL
        SELECT measure_id, collection_year, 'us:1', 'nation', SUM(value), COUNT(*),
               COUNT(*) FILTER (WHERE county_geo_id IS NULL)
        FROM counted
        GROUP BY measure_id, collection_year
    )
    INSERT INTO silver_nces_ipeds.area_total (
        run_id, measure_id, collection_year, geo_id, geo_type, geo_sk, value,
        institution_count, unplaced_institution_count, geography_status, source_record_id
    )
    SELECT %(run_id)s, areas.measure_id, areas.collection_year, areas.geo_id, areas.geo_type,
           entity.geo_sk, areas.value, areas.institutions, areas.unplaced,
           CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
           md5(%(run_id)s || '|' || areas.measure_id || '|' || areas.geo_id)
    FROM areas
    LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = areas.geo_id
"""


def _file(cursor: Any, run_id: UUID) -> tuple[str, int, str | None, str]:
    cursor.execute(
        """
        SELECT component_id, collection_year, capture_id::TEXT, status
        FROM control.nces_ipeds_file WHERE run_id = %s
        """,
        (str(run_id),),
    )
    row = cursor.fetchone()
    if row is None:
        raise IpedsReconciliationError(f"run {run_id} has no IPEDS file")
    return row


def _upsert_measures(cursor: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_nces_ipeds.dim_measure (
            measure_id, component_id, source_column, source_level, measure_label,
            unit, basis, parser_contract_version
        ) VALUES %s
        ON CONFLICT (measure_id) DO UPDATE SET
            component_id = EXCLUDED.component_id,
            source_column = EXCLUDED.source_column,
            source_level = EXCLUDED.source_level,
            measure_label = EXCLUDED.measure_label,
            unit = EXCLUDED.unit,
            basis = EXCLUDED.basis,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        WHERE (silver_nces_ipeds.dim_measure.component_id, silver_nces_ipeds.dim_measure.source_column,
               silver_nces_ipeds.dim_measure.source_level, silver_nces_ipeds.dim_measure.measure_label,
               silver_nces_ipeds.dim_measure.unit, silver_nces_ipeds.dim_measure.basis,
               silver_nces_ipeds.dim_measure.parser_contract_version)
              IS DISTINCT FROM
              (EXCLUDED.component_id, EXCLUDED.source_column, EXCLUDED.source_level,
               EXCLUDED.measure_label, EXCLUDED.unit, EXCLUDED.basis,
               EXCLUDED.parser_contract_version)
        """,
        [
            (
                measure.measure_id,
                measure.component_id,
                measure.column,
                measure.level,
                measure.label,
                measure.unit,
                measure.basis,
                get_component(measure.component_id).parser_contract_version,
            )
            for measure in MEASURES
        ],
    )


def _quarantine(cursor: Any, run_id: UUID, capture_id: str, items: Any) -> None:
    if not items:
        return
    execute_values(
        cursor,
        """
        INSERT INTO silver_nces_ipeds.quarantine (
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


def _store_directory(
    cursor: Any, run_id: UUID, year: int, capture_id: str, parsed: Any
) -> int:
    codes = sorted(
        {
            f"state:{row.county_code[:2]}|county:{row.county_code[2:]}"
            for row in parsed.institutions
            if row.county_code
        }
    )
    cursor.execute(_COUNTIES_SQL, (codes,))
    counties = dict(cursor.fetchall())
    rows = []
    for row in parsed.institutions:
        geo_id = geo_sk = None
        if row.county_code is None:
            status = "not_listed"
        else:
            candidate = f"state:{row.county_code[:2]}|county:{row.county_code[2:]}"
            if candidate in counties and row.county_code[:2] == row.state_fips:
                geo_id, geo_sk, status = candidate, counties[candidate], "resolved"
            else:
                status = "unmapped"
        rows.append(
            (
                str(run_id),
                row.unitid,
                year,
                capture_id,
                row.source_row_index,
                row.institution_name,
                row.city,
                row.state_code,
                row.state_fips,
                row.zip_code,
                row.county_code,
                row.county_name,
                row.longitude,
                row.latitude,
                row.sector,
                row.active_code,
                geo_id,
                geo_sk,
                status,
            )
        )
    if rows:
        execute_values(
            cursor,
            """
            INSERT INTO silver_nces_ipeds.institution (
                run_id, unitid, collection_year, capture_id, source_row_index,
                institution_name, city, state_code, state_fips, zip_code, county_code,
                county_name, longitude, latitude, sector, active_code, county_geo_id,
                county_geo_sk, county_status
            ) VALUES %s
            ON CONFLICT (run_id, unitid) DO NOTHING
            """,
            rows,
            page_size=5000,
        )
    cursor.execute(
        "SELECT COUNT(*) FROM silver_nces_ipeds.institution WHERE run_id = %s",
        (str(run_id),),
    )
    return int(cursor.fetchone()[0])


def _store_figures(
    cursor: Any, run_id: UUID, year: int, capture_id: str, parsed: Any
) -> int:
    rows = [
        (
            str(run_id),
            figure.unitid,
            figure.measure_id,
            year,
            capture_id,
            figure.source_row_index,
            figure.value_source,
            figure.value,
            figure.value_status,
            figure.imputation_code,
        )
        for figure in parsed.figures
    ]
    if rows:
        execute_values(
            cursor,
            """
            INSERT INTO silver_nces_ipeds.institution_value (
                run_id, unitid, measure_id, collection_year, capture_id,
                source_row_index, value_source, value, value_status, imputation_code
            ) VALUES %s
            ON CONFLICT (run_id, unitid, measure_id) DO NOTHING
            """,
            rows,
            page_size=5000,
        )
    cursor.execute(
        """
        SELECT COUNT(DISTINCT source_row_index)
        FROM silver_nces_ipeds.institution_value WHERE run_id = %s
        """,
        (str(run_id),),
    )
    stored = int(cursor.fetchone()[0])
    cursor.execute("ANALYZE silver_nces_ipeds.institution_value")
    cursor.execute(_PLACED_SQL, {"run_id": str(run_id)})
    cursor.execute("ANALYZE nces_ipeds_placed")
    cursor.execute(
        "DELETE FROM silver_nces_ipeds.area_total WHERE run_id = %s", (str(run_id),)
    )
    cursor.execute(_AREA_SQL, {"run_id": str(run_id)})
    return stored


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform one captured file; return the source rows stored."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            component_id, year, capture_id, status = _file(cursor, run_id)
            if status in ("unchanged", "not_published"):
                return 0
            component = get_component(component_id)
            _upsert_measures(cursor)
            payload = load_captured_payload(connection_factory, UUID(capture_id))
            parsed = parse_file(component, payload)
            if any(item.source_row_index == 0 for item in parsed.quarantined):
                _quarantine(cursor, run_id, capture_id, parsed.quarantined)
                cursor.execute(
                    """
                    UPDATE control.nces_ipeds_file
                       SET status = 'quarantined', csv_member = %s, updated_at = NOW()
                     WHERE run_id = %s
                    """,
                    (parsed.csv_member, str(run_id)),
                )
                connection.commit()
                return 0
            if component_id == "HD":
                stored = _store_directory(cursor, run_id, year, capture_id, parsed)
            else:
                stored = _store_figures(cursor, run_id, year, capture_id, parsed)
            _quarantine(cursor, run_id, capture_id, parsed.quarantined)
            set_aside = len({item.source_row_index for item in parsed.quarantined})
            if stored + set_aside + parsed.skipped_row_count != parsed.row_count:
                raise IpedsReconciliationError(
                    f"run {run_id} stored {stored}, set aside {set_aside} and skipped "
                    f"{parsed.skipped_row_count} of {parsed.row_count} rows"
                )
            cursor.execute(
                """
                UPDATE control.nces_ipeds_file
                   SET row_count = %s, kept_row_count = %s, csv_member = %s,
                       status = 'silver_ready', updated_at = NOW()
                 WHERE run_id = %s AND status <> 'published'
                """,
                (parsed.row_count, stored, parsed.csv_member, str(run_id)),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return stored


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled file through the gold views; return 1 if published."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.nces_ipeds_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                """,
                (str(run_id),),
            )
            published = cursor.rowcount
            if published:
                cursor.execute("ANALYZE silver_nces_ipeds.institution")
                cursor.execute("ANALYZE silver_nces_ipeds.area_total")
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
