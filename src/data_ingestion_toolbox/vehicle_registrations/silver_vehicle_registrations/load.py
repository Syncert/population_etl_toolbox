"""Replay one captured vehicle registrations release into silver and publish it.

A replay parses every document the release captured anew (an ``unchanged``
document is not read again), keeps each printed figure, and writes a figure
only when the publisher's text for it is new or differs from the newest
published one (CHURN RULE). A file whose layout is not the registered one is
set aside by name and writes nothing. The replay is one transaction and
commits only when every line of every parsed document is stored, counted as
a repeat, or set aside.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..registry import (
    MEASURES,
    MV1_CONTRACT_VERSION,
    RPT28_CONTRACT_VERSION,
    WISDOT_COUNTIES,
    county_geo_id,
)
from .parse_mv1 import parse_mv1
from .parse_rpt28 import parse_rpt28


PUBLISHER_SCHEMA = "gold_vehicle_registrations"


class VehicleRegistrationsReconciliationError(RuntimeError):
    """A replay that cannot account for every captured line."""


_STAGE_SQL = """
    CREATE TEMP TABLE wisdot_stage (
        data_year SMALLINT, table_part TEXT, county_code TEXT, line_key TEXT,
        vehicle_type TEXT, row_kind TEXT, county_name TEXT, cvt_name TEXT,
        cvt_code TEXT, cvt_kind TEXT, capture_id UUID, page_number INTEGER,
        source_row_index INTEGER, value_source TEXT, value BIGINT, value_status TEXT
    ) ON COMMIT DROP;
    CREATE TEMP TABLE mv1_stage (
        data_year SMALLINT, area_name TEXT, vehicle_class TEXT, ownership TEXT,
        source_label TEXT, state_fips TEXT, footnote_marks TEXT[], capture_id UUID,
        source_row_index INTEGER, value_source TEXT, value NUMERIC, value_status TEXT
    ) ON COMMIT DROP;
"""

_WISDOT_CHANGED_SQL = """
    INSERT INTO silver_vehicle_registrations.wisdot_count (
        data_year, table_part, county_code, line_key, vehicle_type, release_id,
        row_kind, county_name, cvt_name, cvt_code, cvt_kind, capture_id,
        page_number, source_row_index, value_source, value, value_status
    )
    SELECT stage.data_year, stage.table_part, stage.county_code, stage.line_key,
           stage.vehicle_type, %(release_id)s, stage.row_kind, stage.county_name,
           stage.cvt_name, stage.cvt_code, stage.cvt_kind, stage.capture_id,
           stage.page_number, stage.source_row_index, stage.value_source,
           stage.value, stage.value_status
    FROM wisdot_stage AS stage
    LEFT JOIN LATERAL (
        SELECT TRUE AS found, value.value_source
        FROM silver_vehicle_registrations.wisdot_count AS value
        JOIN control.vehicle_registrations_release AS release
          ON release.release_id = value.release_id
        WHERE release.status = 'published'
          AND value.data_year = stage.data_year
          AND value.table_part = stage.table_part
          AND value.county_code = stage.county_code
          AND value.line_key = stage.line_key
          AND value.vehicle_type = stage.vehicle_type
        ORDER BY release.published_at DESC
        LIMIT 1
    ) AS newest ON TRUE
    WHERE newest.found IS NULL OR newest.value_source IS DISTINCT FROM stage.value_source
"""

_MV1_CHANGED_SQL = """
    INSERT INTO silver_vehicle_registrations.fhwa_mv1_figure (
        data_year, area_name, vehicle_class, ownership, release_id, source_label,
        state_fips, footnote_marks, capture_id, source_row_index, value_source,
        value, value_status
    )
    SELECT stage.data_year, stage.area_name, stage.vehicle_class, stage.ownership,
           %(release_id)s, stage.source_label, stage.state_fips, stage.footnote_marks,
           stage.capture_id, stage.source_row_index, stage.value_source, stage.value,
           stage.value_status
    FROM mv1_stage AS stage
    LEFT JOIN LATERAL (
        SELECT TRUE AS found, value.value_source, value.source_label
        FROM silver_vehicle_registrations.fhwa_mv1_figure AS value
        JOIN control.vehicle_registrations_release AS release
          ON release.release_id = value.release_id
        WHERE release.status = 'published'
          AND value.data_year = stage.data_year
          AND value.area_name = stage.area_name
          AND value.vehicle_class = stage.vehicle_class
          AND value.ownership = stage.ownership
        ORDER BY release.published_at DESC
        LIMIT 1
    ) AS newest ON TRUE
    WHERE newest.found IS NULL
       OR (newest.value_source, newest.source_label)
          IS DISTINCT FROM (stage.value_source, stage.source_label)
"""


def _upsert_counties(cursor: Any) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO silver_vehicle_registrations.wisdot_county (
            county_code, county_name, geo_id
        ) VALUES %s
        ON CONFLICT (county_code) DO UPDATE SET
            county_name = EXCLUDED.county_name,
            geo_id = EXCLUDED.geo_id,
            updated_at = NOW()
        WHERE (silver_vehicle_registrations.wisdot_county.county_name,
               silver_vehicle_registrations.wisdot_county.geo_id)
              IS DISTINCT FROM (EXCLUDED.county_name, EXCLUDED.geo_id)
        """,
        [
            (code, name, county_geo_id(fips))
            for code, (name, fips) in sorted(WISDOT_COUNTIES.items())
        ],
    )


def _upsert_measures(cursor: Any) -> None:
    contracts = {
        "wisdot_rpt28": RPT28_CONTRACT_VERSION,
        "fhwa_mv1": MV1_CONTRACT_VERSION,
    }
    execute_values(
        cursor,
        """
        INSERT INTO silver_vehicle_registrations.dim_measure (
            measure_id, publisher, source_column, measure_label, unit, basis,
            parser_contract_version
        ) VALUES %s
        ON CONFLICT (measure_id) DO UPDATE SET
            publisher = EXCLUDED.publisher,
            source_column = EXCLUDED.source_column,
            measure_label = EXCLUDED.measure_label,
            unit = EXCLUDED.unit,
            basis = EXCLUDED.basis,
            parser_contract_version = EXCLUDED.parser_contract_version,
            updated_at = NOW()
        WHERE (silver_vehicle_registrations.dim_measure.publisher,
               silver_vehicle_registrations.dim_measure.source_column,
               silver_vehicle_registrations.dim_measure.measure_label,
               silver_vehicle_registrations.dim_measure.unit,
               silver_vehicle_registrations.dim_measure.basis,
               silver_vehicle_registrations.dim_measure.parser_contract_version)
              IS DISTINCT FROM
              (EXCLUDED.publisher, EXCLUDED.source_column, EXCLUDED.measure_label,
               EXCLUDED.unit, EXCLUDED.basis, EXCLUDED.parser_contract_version)
        """,
        [
            (
                m.measure_id,
                m.publisher,
                m.source_column,
                m.label,
                "vehicles",
                m.basis,
                contracts[m.publisher],
            )
            for m in MEASURES
        ],
    )


def _set_aside(
    cursor: Any, release_id: str, run_id: str, capture_id: str, items: tuple
) -> None:
    if not items:
        return
    execute_values(
        cursor,
        """
        INSERT INTO silver_vehicle_registrations.quarantine (
            release_id, run_id, capture_id, source_row_index, error_code, error_summary
        ) VALUES %s
        ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
        """,
        [
            (
                release_id,
                run_id,
                capture_id,
                item.source_row_index,
                item.error_code,
                item.error_summary,
            )
            for item in items
        ],
    )


def _finish_document(
    cursor: Any,
    release_id: str,
    key: str,
    *,
    status: str,
    rows: int = 0,
    kept: int = 0,
    repeated: int = 0,
    disagreements: list | None = None,
    footnotes: list | None = None,
) -> None:
    cursor.execute(
        """
        UPDATE control.vehicle_registrations_document
           SET status = %s, row_count = %s, kept_row_count = %s,
               repeated_row_count = %s, disagreements = %s::JSONB, footnotes = %s::JSONB
         WHERE release_id = %s AND document_key = %s
        """,
        (
            status,
            rows,
            kept,
            repeated,
            json.dumps(disagreements or []),
            json.dumps(footnotes or []),
            release_id,
            key,
        ),
    )


def _replay_rpt28(
    cursor: Any,
    payload: bytes,
    *,
    year: int,
    capture_id: str,
    release_id: str,
    run_id: str,
    key: str,
) -> None:
    parsed = parse_rpt28(payload)
    whole = [item for item in parsed.quarantined if item.source_row_index == 0]
    if not whole and parsed.data_year != year:
        from .parse_rpt28 import SetAside

        whole = [
            SetAside(
                0,
                "year_disagrees",
                f"the report reads year {parsed.data_year}, its link {year}",
            )
        ]
    if whole:
        _set_aside(cursor, release_id, run_id, capture_id, tuple(whole))
        _finish_document(cursor, release_id, key, status="set_aside")
        return
    if (
        len(parsed.lines) + parsed.repeated_count + len(parsed.quarantined)
        != parsed.row_count
    ):
        raise VehicleRegistrationsReconciliationError(
            f"{key}: {len(parsed.lines)} kept, {parsed.repeated_count} repeated and "
            f"{len(parsed.quarantined)} set aside of {parsed.row_count} lines"
        )
    _set_aside(cursor, release_id, run_id, capture_id, parsed.quarantined)
    values = [
        (
            year,
            line.table_part,
            line.county_code,
            line.line_key,
            column,
            line.row_kind,
            line.county_name,
            line.cvt_name,
            line.cvt_code,
            line.cvt_kind,
            capture_id,
            line.page_number,
            line.source_row_index,
            cell.source,
            cell.value,
            cell.status,
        )
        for line in parsed.lines
        for column, cell in line.cells.items()
    ]
    execute_values(
        cursor,
        """
        INSERT INTO wisdot_stage (
            data_year, table_part, county_code, line_key, vehicle_type, row_kind,
            county_name, cvt_name, cvt_code, cvt_kind, capture_id, page_number,
            source_row_index, value_source, value, value_status
        ) VALUES %s
        """,
        values,
        page_size=10_000,
    )
    _finish_document(
        cursor,
        release_id,
        key,
        status="parsed",
        rows=parsed.row_count,
        kept=len(parsed.lines),
        repeated=parsed.repeated_count,
        disagreements=[
            {
                "check": check,
                "county_code": code,
                "column": column,
                "printed": printed,
                "summed": summed,
            }
            for check, code, column, printed, summed in parsed.disagreements
        ],
    )


def _replay_mv1(
    cursor: Any,
    payload: bytes,
    *,
    year: int,
    capture_id: str,
    release_id: str,
    run_id: str,
    key: str,
) -> None:
    parsed = parse_mv1(payload)
    whole = [item for item in parsed.quarantined if item.source_row_index == 0]
    if whole:
        _set_aside(cursor, release_id, run_id, capture_id, tuple(whole))
        _finish_document(cursor, release_id, key, status="set_aside")
        return
    if len(parsed.rows) + len(parsed.quarantined) != parsed.row_count:
        raise VehicleRegistrationsReconciliationError(
            f"{key}: {len(parsed.rows)} kept and {len(parsed.quarantined)} set aside "
            f"of {parsed.row_count} rows"
        )
    _set_aside(cursor, release_id, run_id, capture_id, parsed.quarantined)
    values = [
        (
            year,
            row.area_name,
            figure.vehicle_class,
            figure.ownership,
            row.source_label,
            row.fips or None,
            list(row.footnote_marks),
            capture_id,
            row.source_row_index,
            figure.source,
            figure.value,
            figure.status,
        )
        for row in parsed.rows
        for figure in row.figures
    ]
    execute_values(
        cursor,
        """
        INSERT INTO mv1_stage (
            data_year, area_name, vehicle_class, ownership, source_label, state_fips,
            footnote_marks, capture_id, source_row_index, value_source, value, value_status
        ) VALUES %s
        """,
        values,
        page_size=10_000,
    )
    _finish_document(
        cursor,
        release_id,
        key,
        status="parsed",
        rows=parsed.row_count,
        kept=len(parsed.rows),
        footnotes=[{"mark": mark, "text": text} for mark, text in parsed.footnotes],
    )


def replay_release(factory: Callable[[], Any], *, release_id: UUID) -> int:
    """Parse and conform one captured release; return the figures written."""
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, run_id::TEXT FROM control.vehicle_registrations_release
                WHERE release_id = %s
                """,
                (str(release_id),),
            )
            found = cursor.fetchone()
            if found is None:
                raise VehicleRegistrationsReconciliationError(
                    f"no release {release_id}"
                )
            status, run_id = found
            if status != "captured":
                return 0
            _upsert_counties(cursor)
            _upsert_measures(cursor)
            for relation in ("wisdot_count", "fhwa_mv1_figure", "quarantine"):
                cursor.execute(
                    f"DELETE FROM silver_vehicle_registrations.{relation} WHERE release_id = %s",
                    (str(release_id),),
                )
            cursor.execute(_STAGE_SQL)
            cursor.execute(
                """
                SELECT document_key, document_kind, data_year, capture_id::TEXT
                FROM control.vehicle_registrations_document
                WHERE release_id = %s AND document_kind <> 'index'
                  AND status IN ('captured', 'parsed', 'set_aside')
                ORDER BY document_key
                """,
                (str(release_id),),
            )
            for key, kind, year, capture_id in list(cursor.fetchall()):
                payload = load_captured_payload(factory, UUID(capture_id))
                replay = _replay_rpt28 if kind == "rpt28" else _replay_mv1
                replay(
                    cursor,
                    payload,
                    year=year,
                    capture_id=capture_id,
                    release_id=str(release_id),
                    run_id=run_id,
                    key=key,
                )
            cursor.execute("ANALYZE wisdot_stage")
            cursor.execute("ANALYZE mv1_stage")
            cursor.execute(_WISDOT_CHANGED_SQL, {"release_id": str(release_id)})
            changed = cursor.rowcount
            cursor.execute(_MV1_CHANGED_SQL, {"release_id": str(release_id)})
            changed += cursor.rowcount
            cursor.execute(
                """
                UPDATE control.vehicle_registrations_release
                   SET changed_value_count = %s, status = 'silver_ready', updated_at = NOW()
                 WHERE release_id = %s
                """,
                (changed, str(release_id)),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return changed


def publish_release(factory: Callable[[], Any], *, release_id: UUID) -> int:
    """Expose a reconciled release through the gold views; return 1 if published."""
    connection = factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.vehicle_registrations_release
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE release_id = %s AND status = 'silver_ready'
                """,
                (str(release_id),),
            )
            published = cursor.rowcount
            if published:
                cursor.execute("ANALYZE silver_vehicle_registrations.wisdot_count")
                cursor.execute("ANALYZE silver_vehicle_registrations.fhwa_mv1_figure")
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
