"""Replay the captured normals archive into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per run: the stations, their county assignment, the normals and
the quarantine land together or not at all. A run the capture found
``unchanged`` replays nothing.

NCEI gives each station coordinates, not a county. The county is assigned
here: the county boundary of the newest loaded vintage that covers the point,
and that vintage is recorded on the run and on every station.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from .parse import parse_archive


class NormalsReconciliationError(RuntimeError):
    """A replayed run does not account for every parsed station or normal."""


class NormalsPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _record_id(station_id: str, variable: str, capture_id: str) -> str:
    return hashlib.md5(f"{station_id}|{variable}|{capture_id}".encode()).hexdigest()


_ASSIGN_COUNTIES = """
WITH vintage AS (
    SELECT MAX(geometry.boundary_vintage) AS boundary_vintage
    FROM silver_ref.dim_geo_geometry_version AS geometry
    JOIN silver_ref.dim_geo_entity AS entity ON entity.geo_sk = geometry.geo_sk
    WHERE entity.geo_type = 'county' AND geometry.is_valid
),
matched AS (
    SELECT station.station_id,
           COUNT(DISTINCT entity.geo_sk) AS counties,
           MIN(entity.geo_sk) AS geo_sk,
           MIN(entity.geo_id) AS geo_id
    FROM silver_noaa_normals.station AS station
    CROSS JOIN vintage
    JOIN silver_ref.dim_geo_geometry_version AS geometry
      ON geometry.boundary_vintage = vintage.boundary_vintage
     AND geometry.is_valid
     AND ST_Covers(geometry.geom, ST_SetSRID(ST_MakePoint(station.longitude, station.latitude), 4326))
    JOIN silver_ref.dim_geo_entity AS entity
      ON entity.geo_sk = geometry.geo_sk AND entity.geo_type = 'county'
    WHERE station.run_id = %(run_id)s
    GROUP BY station.station_id
),
assigned AS (
    SELECT station.station_id, vintage.boundary_vintage, COALESCE(matched.counties, 0) AS counties,
           matched.geo_sk, matched.geo_id
    FROM silver_noaa_normals.station AS station
    CROSS JOIN vintage
    LEFT JOIN matched ON matched.station_id = station.station_id
    WHERE station.run_id = %(run_id)s
)
UPDATE silver_noaa_normals.station AS station
   SET boundary_vintage = assigned.boundary_vintage,
       geo_sk = CASE WHEN assigned.counties = 1 THEN assigned.geo_sk END,
       geo_id = CASE WHEN assigned.counties = 1 THEN assigned.geo_id END,
       geography_status = CASE
           WHEN assigned.counties = 1 THEN 'resolved'
           WHEN assigned.counties > 1 THEN 'ambiguous'
           ELSE 'unmapped'
       END,
       geography_reason = CASE
           WHEN assigned.counties = 1 THEN NULL
           WHEN assigned.counties > 1 THEN 'on_county_boundary'
           WHEN assigned.boundary_vintage IS NULL THEN 'no_county_boundaries'
           ELSE 'outside_counties'
       END
FROM assigned
WHERE station.run_id = %(run_id)s AND station.station_id = assigned.station_id
"""


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform the captured archive; return the normals written."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT capture_id::TEXT, status FROM control.noaa_normals_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise NormalsReconciliationError("run has no captured normals archive")
            capture_id, status = row
            if status == "unchanged":
                connection.commit()
                return 0
            parsed = parse_archive(
                load_captured_payload(connection_factory, UUID(capture_id))
            )
            if parsed.stations:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_noaa_normals.station (
                        run_id, station_id, capture_id, source_row_index, latitude, longitude,
                        elevation_m, station_name, geography_status, geography_reason
                    ) VALUES %s
                    ON CONFLICT (run_id, station_id) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            station.station_id,
                            capture_id,
                            station.member_index,
                            station.latitude,
                            station.longitude,
                            station.elevation_m,
                            station.name,
                            "unmapped",
                            "no_county_boundaries",
                        )
                        for station in parsed.stations
                    ],
                    page_size=5000,
                )
                cursor.execute(_ASSIGN_COUNTIES, {"run_id": str(run_id)})
            if parsed.observations:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_noaa_normals.station_normal (
                        run_id, station_id, variable, measure, capture_id, value_source, value,
                        value_status, missing_reason, measurement_flag, completeness_flag, years,
                        source_record_id
                    ) VALUES %s
                    ON CONFLICT (run_id, station_id, variable) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            obs.station_id,
                            obs.variable,
                            obs.measure,
                            capture_id,
                            obs.value_source,
                            obs.value,
                            obs.value_status,
                            obs.missing_reason,
                            obs.measurement_flag,
                            obs.completeness_flag,
                            obs.years,
                            _record_id(obs.station_id, obs.variable, capture_id),
                        )
                        for obs in parsed.observations
                    ],
                    page_size=10000,
                )
            if parsed.quarantined:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_noaa_normals.quarantine (
                        run_id, capture_id, source_row_index, error_code, error_summary
                    ) VALUES %s
                    ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            capture_id,
                            q.member_index,
                            q.error_code,
                            q.error_summary,
                        )
                        for q in parsed.quarantined
                    ],
                )
            refused = any(q.member_index == 0 for q in parsed.quarantined)
            cursor.execute(
                """
                UPDATE control.noaa_normals_file
                   SET station_file_count = %s, station_count = %s, status = %s, updated_at = NOW(),
                       boundary_vintage = (
                           SELECT MAX(boundary_vintage) FROM silver_noaa_normals.station WHERE run_id = %s
                       )
                 WHERE run_id = %s AND status = 'captured'
                """,
                (
                    parsed.member_count,
                    len(parsed.stations),
                    "quarantined" if refused else "silver_ready",
                    str(run_id),
                    str(run_id),
                ),
            )
            cursor.execute(
                """
                INSERT INTO silver_ref.geography_resolution (
                    provider_source, provider_dataset, source_geo_type,
                    source_code, source_label, source_vintage, geo_sk,
                    resolution_method, evidence_capture_id, status, reason_code
                )
                SELECT %s, 'normals_annualseasonal_1991_2020', 'station',
                       station.station_id, NULL, COALESCE(station.boundary_vintage, 0), station.geo_sk,
                       NULL, station.capture_id,
                       CASE WHEN station.geography_status = 'resolved' THEN 'resolved'
                            WHEN station.geography_status = 'ambiguous' THEN 'ambiguous'
                            ELSE 'unmapped' END,
                       station.geography_reason
                FROM silver_noaa_normals.station AS station
                WHERE station.run_id = %s
                ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
                DO UPDATE SET
                    geo_sk = EXCLUDED.geo_sk,
                    evidence_capture_id = EXCLUDED.evidence_capture_id,
                    status = EXCLUDED.status,
                    reason_code = EXCLUDED.reason_code,
                    resolved_at = NOW()
                """,
                (SOURCE_CODE, str(run_id)),
            )
            cursor.execute(
                """
                SELECT (SELECT COUNT(*) FROM silver_noaa_normals.station WHERE run_id = %s),
                       (SELECT COUNT(*) FROM silver_noaa_normals.station_normal WHERE run_id = %s)
                """,
                (str(run_id), str(run_id)),
            )
            stations, normals = (int(value) for value in cursor.fetchone())
            if stations != len(parsed.stations) or normals != len(parsed.observations):
                raise NormalsReconciliationError(
                    f"run {run_id} wrote {stations} stations and {normals} normals against "
                    f"{len(parsed.stations)} and {len(parsed.observations)} parsed"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return normals


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a reconciled archive through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.noaa_normals_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING run_id
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT status FROM control.noaa_normals_file WHERE run_id = %s",
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None or row[0] == "captured":
                raise NormalsPublicationError(f"run {run_id} was never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_noaa_normals"
        )
    return published
