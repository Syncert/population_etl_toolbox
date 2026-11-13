"""Replay the captured normals archive into silver, reconcile it, and publish it.

Everything here reads the committed capture, never the network. One
transaction per run: the stations, their county assignment, the normals and
the quarantine land together or not at all. A run the capture found
``unchanged`` replays nothing.

NCEI gives each station coordinates, not a county. The county is assigned
here: the county boundary of the newest loaded vintage that covers the point,
or, for a point no boundary covers, the nearest county within
``NEAREST_COUNTY_CAP_METRES`` (recorded as ``nearest_county`` with its
distance). That vintage is recorded on the run and on every station.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..registry import ANNUAL, PRODUCTS
from .parse import parse_archive


class NormalsReconciliationError(RuntimeError):
    """A replayed run does not account for every parsed station or normal."""


class NormalsPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _record_id(
    station_id: str, variable: str, capture_id: str, month: int | None = None
) -> str:
    key = f"{station_id}|{variable}|{capture_id}"
    if month is not None:
        key += f"|{month:02d}"
    return hashlib.md5(key.encode()).hexdigest()


#: County shapes of the newest boundary vintage, filtered to counties before any
#: spatial test, in an indexed and analysed temporary table: the shared geometry
#: table also holds every tract, place and block group shape.
_STAGE_COUNTY_SHAPES = (
    """
    CREATE TEMPORARY TABLE normals_county_shape ON COMMIT DROP AS
    SELECT entity.geo_sk, entity.geo_id, geometry.boundary_vintage, geometry.geom
    FROM silver_ref.dim_geo_entity AS entity
    JOIN silver_ref.dim_geo_geometry_version AS geometry ON geometry.geo_sk = entity.geo_sk
    WHERE entity.geo_type = 'county'
      AND geometry.is_valid
      AND geometry.boundary_vintage = (
          SELECT MAX(newest.boundary_vintage)
          FROM silver_ref.dim_geo_geometry_version AS newest
          JOIN silver_ref.dim_geo_entity AS county ON county.geo_sk = newest.geo_sk
          WHERE county.geo_type = 'county' AND newest.is_valid
      )
    """,
    "CREATE INDEX ON normals_county_shape USING GIST (geom)",
    "ANALYZE normals_county_shape",
    """
    CREATE TEMPORARY TABLE normals_station_point ON COMMIT DROP AS
    SELECT station.station_id,
           ST_SetSRID(ST_MakePoint(station.longitude, station.latitude), 4326) AS point,
           (SELECT MAX(shape.boundary_vintage) FROM normals_county_shape AS shape)
               AS boundary_vintage
    FROM silver_noaa_normals.station AS station
    WHERE station.run_id = %(run_id)s
    """,
    "ANALYZE normals_station_point",
)

#: A station no county shape covers is placed on the nearest county only when
#: that county's shape is within this many metres of the station: the 1:500k
#: shapes follow a generalised shoreline, so a pier, point or harbour station
#: can sit just off them while standing in the county. Nothing farther is
#: placed (Nick, 2026-11-12).
NEAREST_COUNTY_CAP_METRES = 1000

#: The nearest county shape of every station no shape covers, when it lies
#: within the cap. The five nearest shapes by the index are measured on the
#: spheroid and the closest is taken.
_STAGE_NEAREST = """
CREATE TEMPORARY TABLE normals_nearest ON COMMIT DROP AS
SELECT point.station_id, nearest.geo_sk, nearest.geo_id, ROUND(nearest.metres::NUMERIC, 1) AS metres
FROM normals_station_point AS point
CROSS JOIN LATERAL (
    SELECT candidate.geo_sk, candidate.geo_id,
           ST_Distance(candidate.geom::geography, point.point::geography) AS metres
    FROM (
        SELECT shape.geo_sk, shape.geo_id, shape.geom
        FROM normals_county_shape AS shape
        ORDER BY shape.geom <-> point.point
        LIMIT 5
    ) AS candidate
    ORDER BY 3, 1
    LIMIT 1
) AS nearest
WHERE NOT EXISTS (
    SELECT 1 FROM normals_county_shape AS shape WHERE ST_Covers(shape.geom, point.point)
)
  AND nearest.metres <= %(cap)s
"""

#: Each station point probes the county shape index; a point on a shared
#: boundary is covered by more than one county and is not placed. Only a
#: station whose placement changed is rewritten (churn rule).
_ASSIGN_COUNTIES = """
WITH matched AS (
    SELECT point.station_id,
           COUNT(DISTINCT shape.geo_sk) AS counties,
           MIN(shape.geo_sk) AS geo_sk,
           MIN(shape.geo_id) AS geo_id
    FROM normals_station_point AS point
    JOIN normals_county_shape AS shape ON ST_Covers(shape.geom, point.point)
    GROUP BY point.station_id
),
assigned AS (
    SELECT point.station_id, point.boundary_vintage,
           COALESCE(matched.counties, 0) AS counties,
           COALESCE(matched.geo_sk, nearest.geo_sk) AS geo_sk,
           COALESCE(matched.geo_id, nearest.geo_id) AS geo_id,
           nearest.metres AS nearest_metres
    FROM normals_station_point AS point
    LEFT JOIN matched ON matched.station_id = point.station_id
    LEFT JOIN normals_nearest AS nearest ON nearest.station_id = point.station_id
),
placed AS (
    SELECT assigned.station_id, assigned.boundary_vintage,
           CASE WHEN assigned.counties = 1 OR assigned.nearest_metres IS NOT NULL
                THEN assigned.geo_sk END AS geo_sk,
           CASE WHEN assigned.counties = 1 OR assigned.nearest_metres IS NOT NULL
                THEN assigned.geo_id END AS geo_id,
           CASE
               WHEN assigned.counties = 1 OR assigned.nearest_metres IS NOT NULL THEN 'resolved'
               WHEN assigned.counties > 1 THEN 'ambiguous'
               ELSE 'unmapped'
           END AS geography_status,
           CASE
               WHEN assigned.counties = 1 OR assigned.nearest_metres IS NOT NULL THEN NULL
               WHEN assigned.counties > 1 THEN 'on_county_boundary'
               WHEN assigned.boundary_vintage IS NULL THEN 'no_county_boundaries'
               ELSE 'outside_counties'
           END AS geography_reason,
           CASE
               WHEN assigned.counties = 1 THEN 'inside_county'
               WHEN assigned.nearest_metres IS NOT NULL THEN 'nearest_county'
           END AS placement_method,
           assigned.nearest_metres AS placement_distance_m
    FROM assigned
)
UPDATE silver_noaa_normals.station AS station
   SET boundary_vintage = placed.boundary_vintage,
       geo_sk = placed.geo_sk,
       geo_id = placed.geo_id,
       geography_status = placed.geography_status,
       geography_reason = placed.geography_reason,
       placement_method = placed.placement_method,
       placement_distance_m = placed.placement_distance_m
FROM placed
WHERE station.run_id = %(run_id)s AND station.station_id = placed.station_id
  AND (station.boundary_vintage, station.geo_sk, station.geo_id, station.geography_status,
       station.geography_reason, station.placement_method, station.placement_distance_m)
      IS DISTINCT FROM
      (placed.boundary_vintage, placed.geo_sk, placed.geo_id, placed.geography_status,
       placed.geography_reason, placed.placement_method, placed.placement_distance_m)
"""


def _place_stations(cursor: Any, run_id: UUID | str) -> None:
    """Assign every station of a run to a county of the newest boundary vintage."""
    parameters = {"run_id": str(run_id), "cap": NEAREST_COUNTY_CAP_METRES}
    for statement in _STAGE_COUNTY_SHAPES:
        cursor.execute(statement, parameters)
    cursor.execute(_STAGE_NEAREST, parameters)
    cursor.execute(_ASSIGN_COUNTIES, parameters)
    cursor.execute(
        "DROP TABLE normals_nearest, normals_station_point, normals_county_shape"
    )


def place_published_stations(connection_factory: Callable[[], Any]) -> list[UUID]:
    """Place again the stations of every published run placed before the current rule.

    A run whose archive is unchanged replays nothing, so a placement rule
    added later reaches its stations here: a run with a station that has no
    placement method yet (placed before nearest-county placement) is placed
    again, one transaction per run, rewriting only stations that change.
    """
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT file.run_id::TEXT
                FROM control.noaa_normals_file AS file
                WHERE file.status = 'published'
                  AND EXISTS (
                      SELECT 1 FROM silver_noaa_normals.station AS station
                      WHERE station.run_id = file.run_id
                        AND station.geography_status = 'resolved'
                        AND station.placement_method IS NULL
                  )
                ORDER BY file.run_id
                """
            )
            runs = [UUID(row[0]) for row in cursor.fetchall()]
        reader.commit()
    finally:
        reader.close()
    for run_id in runs:
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                _place_stations(cursor, run_id)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
    return runs


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Parse and conform the captured archive; return the normals written."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT capture_id::TEXT, status, product
                FROM control.noaa_normals_file WHERE run_id = %s
                """,
                (str(run_id),),
            )
            row = cursor.fetchone()
            if row is None:
                raise NormalsReconciliationError("run has no captured normals archive")
            capture_id, status, product = row
            if status == "unchanged":
                connection.commit()
                return 0
            monthly = product != ANNUAL
            normal_table = (
                "silver_noaa_normals.station_monthly_normal"
                if monthly
                else "silver_noaa_normals.station_normal"
            )
            parsed = parse_archive(
                load_captured_payload(connection_factory, UUID(capture_id)), product
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
                _place_stations(cursor, run_id)
            if parsed.observations and monthly:
                execute_values(
                    cursor,
                    """
                    INSERT INTO silver_noaa_normals.station_monthly_normal (
                        run_id, station_id, variable, month, measure, capture_id, value_source,
                        value, value_status, missing_reason, measurement_flag, completeness_flag,
                        years, source_record_id
                    ) VALUES %s
                    ON CONFLICT (run_id, station_id, variable, month) DO NOTHING
                    """,
                    [
                        (
                            str(run_id),
                            obs.station_id,
                            obs.variable,
                            obs.month,
                            obs.measure,
                            capture_id,
                            obs.value_source,
                            obs.value,
                            obs.value_status,
                            obs.missing_reason,
                            obs.measurement_flag,
                            obs.completeness_flag,
                            obs.years,
                            _record_id(
                                obs.station_id, obs.variable, capture_id, obs.month
                            ),
                        )
                        for obs in parsed.observations
                    ],
                    page_size=10000,
                )
            elif parsed.observations:
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
                SELECT %s, %s, 'station',
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
                -- Only a row whose content changed is rewritten
                -- (refresh-churn audit, 2026-11-06).
                WHERE (silver_ref.geography_resolution.geo_sk, silver_ref.geography_resolution.status, silver_ref.geography_resolution.reason_code)
                      IS DISTINCT FROM
                      (EXCLUDED.geo_sk, EXCLUDED.status, EXCLUDED.reason_code)
                """,
                (SOURCE_CODE, PRODUCTS[product].dataset, str(run_id)),
            )
            cursor.execute(
                f"""
                SELECT (SELECT COUNT(*) FROM silver_noaa_normals.station WHERE run_id = %s),
                       (SELECT COUNT(*) FROM {normal_table} WHERE run_id = %s)
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


def settle_unreplayed_runs(connection_factory: Callable[[], Any]) -> list[UUID]:
    """Finish every captured archive an earlier attempt left unreplayed.

    A run that captured and then failed before replaying (the task retries,
    the database stops, the machine reboots) stayed ``captured`` for good:
    each retry read the archive again under a new run, so nothing ever came
    back for the first one and DQ-NOAA-002 reported it unreplayed. They are
    settled oldest first, before the next read. A run whose bytes equal the
    published archive is ``unchanged`` -- what the capture would have
    recorded had that archive been published when it read -- and any other is
    replayed and published like a fresh read, so a changed archive is never
    passed over. Returns the runs settled.
    """
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id::TEXT, payload_checksum
                  FROM control.noaa_normals_file
                 WHERE status = 'captured'
                 ORDER BY created_at, run_id
                """
            )
            orphans = cursor.fetchall()
        connection.commit()
    finally:
        connection.close()

    settled: list[UUID] = []
    for run_id, checksum in orphans:
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE control.noaa_normals_file
                       SET status = 'unchanged', updated_at = NOW()
                     WHERE run_id = %s
                       AND status = 'captured'
                       AND payload_checksum = (
                           SELECT published.payload_checksum
                             FROM control.noaa_normals_file AS published
                            WHERE published.status = 'published'
                              AND published.product = control.noaa_normals_file.product
                            ORDER BY published.published_at DESC LIMIT 1
                       )
                    """,
                    (run_id,),
                )
                unchanged = cursor.rowcount == 1
            connection.commit()
        finally:
            connection.close()
        if not unchanged:
            replay_run(connection_factory, run_id=UUID(run_id))
            publish_run(connection_factory, run_id=UUID(run_id))
        settled.append(UUID(run_id))
    return settled
