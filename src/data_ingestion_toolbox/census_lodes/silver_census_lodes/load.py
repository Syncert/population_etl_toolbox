"""Replay one captured LODES state-year into silver, reconcile it, and publish it.

Everything here reads the committed captures, never the network. One
transaction per run: the county sums, the county-pair flows, the quarantine
and the geography resolution ledger land together or not at all. A run the
capture found ``unchanged`` replays nothing.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..registry import OD_MAIN, RAC, STATE_FIPS, WAC, LodesFile, column_available
from .parse import parse_area, parse_flows


class LodesReconciliationError(RuntimeError):
    """A replayed run does not account for its captured files."""


class LodesPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _county(code: str) -> str:
    return f"state:{code[:2]}|county:{code[2:]}"


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Aggregate a captured state-year; return the number of county rows written."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT state, year, status FROM control.census_lodes_slice WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise LodesReconciliationError("run has no LODES slice")
            state, year, status = row
            if status == "unchanged":
                connection.commit()
                return 0
            cursor.execute(
                """
                SELECT family, file_name, capture_id::TEXT FROM control.census_lodes_file
                WHERE run_id = %s AND status = 'captured' ORDER BY family
                """,
                (str(run_id),),
            )
            captured = cursor.fetchall()
            area_rows: list[tuple] = []
            flow_rows: list[tuple] = []
            quarantine: list[tuple] = []
            refused = False
            for family, _name, capture_id in captured:
                item = LodesFile(state, family, int(year))
                payload = load_captured_payload(connection_factory, UUID(capture_id))
                if family in (RAC, WAC):
                    parsed = parse_area(payload, item=item)
                    for county, sums in parsed.totals.items():
                        for column, value in sums.items():
                            available = column_available(column, year=int(year))
                            area_rows.append(
                                (
                                    str(run_id),
                                    family,
                                    column,
                                    _county(county),
                                    int(year),
                                    capture_id,
                                    parsed.blocks[county],
                                    str(value),
                                    value if available else None,
                                    "valid" if available else "not_available",
                                )
                            )
                else:
                    parsed = parse_flows(payload, item=item)
                    part = "main" if family == OD_MAIN else "aux"
                    for (home, work), jobs in parsed.flows.items():
                        flow_rows.append(
                            (
                                str(run_id),
                                part,
                                _county(home),
                                _county(work),
                                int(year),
                                capture_id,
                                jobs,
                            )
                        )
                refused = refused or any(
                    q.source_row_index == 0 for q in parsed.quarantined
                )
                quarantine.extend(
                    (
                        str(run_id),
                        capture_id,
                        q.source_row_index,
                        q.error_code,
                        q.error_summary,
                    )
                    for q in parsed.quarantined
                )
                cursor.execute(
                    """
                    UPDATE control.census_lodes_file SET row_count = %s, quarantined_count = %s
                    WHERE run_id = %s AND family = %s
                    """,
                    (parsed.row_count, len(parsed.quarantined), str(run_id), family),
                )
            if quarantine:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_census_lodes.quarantine (
                        run_id, capture_id, source_row_index, error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
                    """,
                    quarantine,
                )
            if area_rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_census_lodes.fact_area (
                        run_id, family, column_code, geo_id, year, capture_id, geo_sk,
                        geography_status, block_count, value_source, value, value_status
                    )
                    SELECT incoming.run_id::UUID, incoming.family, incoming.column_code,
                           incoming.geo_id, incoming.year::INTEGER, incoming.capture_id::UUID,
                           entity.geo_sk,
                           CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
                           incoming.block_count::INTEGER, incoming.value_source,
                           incoming.value::BIGINT, incoming.value_status
                    FROM (VALUES %s) AS incoming (
                        run_id, family, column_code, geo_id, year, capture_id, block_count,
                        value_source, value, value_status
                    )
                    LEFT JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_id = incoming.geo_id
                    ON CONFLICT (run_id, family, column_code, geo_id) DO NOTHING
                    """,
                    area_rows,
                    page_size=10000,
                )
            if flow_rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_census_lodes.fact_flow (
                        run_id, part, home_geo_id, work_geo_id, year, capture_id,
                        home_geo_sk, work_geo_sk, jobs
                    )
                    SELECT incoming.run_id::UUID, incoming.part, incoming.home_geo_id,
                           incoming.work_geo_id, incoming.year::INTEGER, incoming.capture_id::UUID,
                           home.geo_sk, work.geo_sk, incoming.jobs::BIGINT
                    FROM (VALUES %s) AS incoming (
                        run_id, part, home_geo_id, work_geo_id, year, capture_id, jobs
                    )
                    LEFT JOIN silver_ref.dim_geo_entity AS home ON home.geo_id = incoming.home_geo_id
                    LEFT JOIN silver_ref.dim_geo_entity AS work ON work.geo_id = incoming.work_geo_id
                    ON CONFLICT (run_id, part, home_geo_id, work_geo_id) DO NOTHING
                    """,
                    flow_rows,
                    page_size=10000,
                )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type, source_code,
                    source_label, source_vintage, geo_sk, resolution_method,
                    evidence_capture_id, status, reason_code
                )
                SELECT DISTINCT ON (area.geo_id)
                       %s, 'lodes', 'county', area.geo_id, NULL, area.year, area.geo_sk,
                       CASE WHEN area.geo_sk IS NOT NULL THEN 'exact_code' END,
                       area.capture_id,
                       area.geography_status,
                       CASE WHEN area.geo_sk IS NULL THEN 'canonical_geography_absent' END
                FROM silver_census_lodes.fact_area AS area
                WHERE area.run_id = %s
                ORDER BY area.geo_id, area.capture_id
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET geo_sk = EXCLUDED.geo_sk, resolution_method = EXCLUDED.resolution_method,
                              evidence_capture_id = EXCLUDED.evidence_capture_id,
                              status = EXCLUDED.status, reason_code = EXCLUDED.reason_code,
                              resolved_at = NOW()
                """,
                (SOURCE_CODE, str(run_id)),
            )
            cursor.execute(
                "SELECT COUNT(*) FROM silver_census_lodes.fact_area WHERE run_id = %s",
                (str(run_id),),
            )
            areas = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT COUNT(*) FROM silver_census_lodes.fact_flow WHERE run_id = %s",
                (str(run_id),),
            )
            flows = int(cursor.fetchone()[0])
            if areas != len(area_rows) or flows != len(flow_rows):
                raise LodesReconciliationError(
                    f"run {run_id} wrote {areas} county sums and {flows} flows against "
                    f"{len(area_rows)} and {len(flow_rows)} aggregated"
                )
            cursor.execute(
                """
                UPDATE control.census_lodes_slice SET status = %s, updated_at = NOW()
                WHERE run_id = %s AND status = 'captured'
                """,
                ("quarantined" if refused else "silver_ready", str(run_id)),
            )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return areas + flows


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled state-year through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.census_lodes_slice
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.census_lodes_slice WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise LodesPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_census_lodes"
        )
    return published


__all__ = ["STATE_FIPS", "publish_run", "replay_run"]
