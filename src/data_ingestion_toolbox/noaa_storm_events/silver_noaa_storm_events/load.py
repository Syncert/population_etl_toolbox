"""Replay captured Storm Events files into silver, place events, count, and publish.

Everything here reads the committed capture, never the network. One
transaction per file: its quarantine, the events (or zone rows) it changed or
added, the superseded and withdrawn events, the counties every changed event
is counted in and the recomputed counts of its year land together or not at
all.

NCEI rewrites the current year's file every month and an earlier year only
when it revises it, so most reads change a few thousand events. Only those
are written (the churn rule): the file is staged in an indexed, analysed
temporary table and compared with the current rows by event id; an event
whose published row is the same is counted, not rewritten. The same holds
for placements and counts.

Zone (``Z``) events are placed per Nick's decision of 2026-11-11: counted in
every county of the zone only when the event's zone code and zone name both
match a published NWS zone-county file, the file valid on the event's begin
date first, then the others newest first. Every other zone event is
``zone_unmatched`` and appears in its state's coverage note, never on a
county.
"""

from __future__ import annotations

import hashlib
import io
from collections.abc import Callable, Iterable, Iterator
from typing import Any
from uuid import UUID

from data_ingestion_toolbox.capture import load_captured_payload

from ..client import decompress_details
from ..config import SOURCE_CODE
from ..registry import NATION_STATE_FIPS, STATE_FIPS_TO_USPS, zone_file_valid_from
from .parse import (
    ParsedDetails,
    ParsedZones,
    StormQuarantine,
    parse_details_file,
    parse_zone_file,
)


class StormEventsReconciliationError(RuntimeError):
    """A replayed file does not account for every parsed event."""


class StormEventsPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _copy_text(value: object) -> str:
    """One field in COPY text format: NULL as ``\\N``, specials escaped."""
    if value is None:
        return "\\N"
    text = str(value)
    return (
        text.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


class _RowStream(io.RawIOBase):
    """A file-like view of tab-separated rows, read by COPY without listing them."""

    def __init__(self, rows: Iterator[str]) -> None:
        self._rows = rows
        self._buffer = b""

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:  # type: ignore[override]
        while size < 0 or len(self._buffer) < size:
            try:
                self._buffer += next(self._rows).encode()
            except StopIteration:
                break
        if size < 0:
            chunk, self._buffer = self._buffer, b""
        else:
            chunk, self._buffer = self._buffer[:size], self._buffer[size:]
        return chunk


def _rows(values: Iterable[tuple]) -> Iterator[str]:
    for row in values:
        yield "\t".join(_copy_text(value) for value in row) + "\n"


def _record_id(event_id: int, capture_id: str) -> str:
    return hashlib.md5(f"{event_id}|{capture_id}".encode()).hexdigest()


def _write_quarantine(
    cursor: Any, run_id: UUID, capture_id: str, quarantined: list[StormQuarantine]
) -> None:
    if not quarantined:
        return
    from psycopg2.extras import execute_values

    execute_values(
        cursor,
        """
        INSERT INTO silver_noaa_storm_events.quarantine (
            run_id, capture_id, source_row_index, error_code, error_summary
        ) VALUES %s
        ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
        """,
        [
            (str(run_id), capture_id, item.line, item.error_code, item.error_summary)
            for item in quarantined
        ],
    )


# ---------------------------------------------------------------------------
# Zone-county files
# ---------------------------------------------------------------------------

_ZONE_STAGE = """
CREATE TEMPORARY TABLE storm_zone_incoming (
    state TEXT NOT NULL,
    zone TEXT NOT NULL,
    cwa TEXT NOT NULL,
    zone_name TEXT NOT NULL,
    zone_name_key TEXT NOT NULL,
    county_name TEXT NOT NULL,
    county_fips5 TEXT NOT NULL,
    time_zone TEXT NOT NULL,
    source_line INTEGER NOT NULL
) ON COMMIT DROP
"""


def _replay_zone_file(
    cursor: Any,
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    file_name: str,
    capture_id: str,
) -> tuple[int, int, int]:
    """Stage and write one zone-county file; return (rows, quarantined, written)."""
    parsed = ParsedZones()
    raw = load_captured_payload(connection_factory, UUID(capture_id))
    cursor.execute(_ZONE_STAGE)
    cursor.copy_expert(
        "COPY storm_zone_incoming FROM STDIN WITH (FORMAT text)",
        _RowStream(
            _rows(
                (
                    row.state,
                    row.zone,
                    row.cwa,
                    row.zone_name,
                    row.zone_name_key,
                    row.county_name,
                    row.county_fips5,
                    row.time_zone,
                    row.line,
                )
                for row in parse_zone_file(raw, parsed=parsed)
            )
        ),
    )
    cursor.execute(
        """
        INSERT INTO silver_noaa_storm_events.zone_county (
            zone_file, valid_from, state, zone, cwa, zone_name, zone_name_key, county_name,
            county_fips5, time_zone, geo_id, geo_sk, geography_status, run_id, capture_id,
            source_line
        )
        SELECT %(file)s, %(valid_from)s, incoming.state, incoming.zone, incoming.cwa,
               incoming.zone_name, incoming.zone_name_key, incoming.county_name,
               incoming.county_fips5, incoming.time_zone, entity.geo_id, entity.geo_sk,
               CASE WHEN entity.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
               %(run_id)s, %(capture_id)s, incoming.source_line
        FROM storm_zone_incoming AS incoming
        LEFT JOIN silver_ref.dim_geo_entity AS entity
          ON entity.geo_type = 'county'
         AND entity.geo_id = 'state:' || LEFT(incoming.county_fips5, 2)
                             || '|county:' || RIGHT(incoming.county_fips5, 3)
        ON CONFLICT (zone_file, state, zone, county_fips5) DO UPDATE SET
            cwa = EXCLUDED.cwa,
            zone_name = EXCLUDED.zone_name,
            zone_name_key = EXCLUDED.zone_name_key,
            county_name = EXCLUDED.county_name,
            time_zone = EXCLUDED.time_zone,
            geo_id = EXCLUDED.geo_id,
            geo_sk = EXCLUDED.geo_sk,
            geography_status = EXCLUDED.geography_status,
            run_id = EXCLUDED.run_id,
            capture_id = EXCLUDED.capture_id,
            source_line = EXCLUDED.source_line
        WHERE (silver_noaa_storm_events.zone_county.zone_name,
               silver_noaa_storm_events.zone_county.county_name,
               silver_noaa_storm_events.zone_county.geo_sk)
              IS DISTINCT FROM (EXCLUDED.zone_name, EXCLUDED.county_name, EXCLUDED.geo_sk)
        """,
        {
            "file": file_name,
            "valid_from": zone_file_valid_from(file_name),
            "run_id": str(run_id),
            "capture_id": capture_id,
        },
    )
    written = cursor.rowcount
    _write_quarantine(cursor, run_id, capture_id, parsed.quarantined)
    cursor.execute(
        """
        INSERT INTO silver_ref.geography_resolution (
            provider_source, provider_dataset, source_geo_type, source_code, source_label,
            source_vintage, geo_sk, resolution_method, evidence_capture_id, status, reason_code
        )
        SELECT DISTINCT ON (zone.county_fips5)
               %s, 'nws_zone_county', 'county', zone.county_fips5, zone.county_name, 0,
               zone.geo_sk,
               CASE WHEN zone.geo_sk IS NULL THEN NULL ELSE 'provider_crosswalk' END,
               %s, zone.geography_status,
               CASE WHEN zone.geo_sk IS NULL THEN 'county_not_in_shared_geography' END
        FROM silver_noaa_storm_events.zone_county AS zone
        WHERE zone.zone_file = %s
        ORDER BY zone.county_fips5
        ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
        DO UPDATE SET
            geo_sk = EXCLUDED.geo_sk,
            resolution_method = EXCLUDED.resolution_method,
            evidence_capture_id = EXCLUDED.evidence_capture_id,
            status = EXCLUDED.status,
            reason_code = EXCLUDED.reason_code,
            resolved_at = NOW()
        WHERE (silver_ref.geography_resolution.geo_sk, silver_ref.geography_resolution.status)
              IS DISTINCT FROM (EXCLUDED.geo_sk, EXCLUDED.status)
        """,
        (SOURCE_CODE, capture_id, file_name),
    )
    return parsed.line_count, len(parsed.quarantined), written


# ---------------------------------------------------------------------------
# Details files
# ---------------------------------------------------------------------------

_EVENT_COLUMNS = (
    "event_id",
    "episode_id",
    "year",
    "begin_date",
    "end_date",
    "begin_time",
    "end_time",
    "begin_date_time",
    "end_date_time",
    "cz_timezone",
    "state_name",
    "state_fips",
    "event_type",
    "event_type_key",
    "cz_type",
    "cz_fips",
    "cz_name",
    "cz_name_key",
    "wfo",
    "injuries_direct",
    "injuries_indirect",
    "deaths_direct",
    "deaths_indirect",
    "damage_property_published",
    "damage_property_usd",
    "damage_property_status",
    "damage_crops_published",
    "damage_crops_usd",
    "damage_crops_status",
    "source",
    "magnitude",
    "magnitude_type",
    "flood_cause",
    "category",
    "tor_f_scale",
    "tor_length",
    "tor_width",
    "begin_location",
    "end_location",
    "begin_lat",
    "begin_lon",
    "end_lat",
    "end_lon",
    "episode_narrative",
    "event_narrative",
    "data_source",
    "row_hash",
    "source_line",
    "source_record_id",
)

_EVENT_COLUMN_LIST = ", ".join(_EVENT_COLUMNS)

#: The staged file: the event columns' types, none of their constraints.
_EVENT_STAGE = f"""
CREATE TEMPORARY TABLE storm_incoming ON COMMIT DROP AS
SELECT {_EVENT_COLUMN_LIST} FROM silver_noaa_storm_events.event WITH NO DATA
"""


def _event_rows(events: Iterable[Any], capture_id: str) -> Iterator[tuple]:
    for event in events:
        yield (
            event.event_id,
            event.episode_id,
            event.year,
            event.begin_date.isoformat(),
            event.end_date.isoformat() if event.end_date else None,
            event.begin_time,
            event.end_time,
            event.begin_date_time,
            event.end_date_time,
            event.cz_timezone,
            event.state_name,
            event.state_fips,
            event.event_type,
            event.event_type_key,
            event.cz_type,
            event.cz_fips,
            event.cz_name,
            event.cz_name_key,
            event.wfo,
            event.injuries_direct,
            event.injuries_indirect,
            event.deaths_direct,
            event.deaths_indirect,
            event.damage_property.published,
            event.damage_property.usd,
            event.damage_property.status,
            event.damage_crops.published,
            event.damage_crops.usd,
            event.damage_crops.status,
            event.source,
            event.magnitude,
            event.magnitude_type,
            event.flood_cause,
            event.category,
            event.tor_f_scale,
            event.tor_length,
            event.tor_width,
            event.begin_location,
            event.end_location,
            event.begin_lat,
            event.begin_lon,
            event.end_lat,
            event.end_lon,
            event.episode_narrative,
            event.event_narrative,
            event.data_source,
            event.row_hash,
            event.line,
            _record_id(event.event_id, capture_id),
        )


_REVISION_INSERT = """
INSERT INTO silver_noaa_storm_events.event_revision (
    event_id, year, event_type, state_fips, cz_type, cz_fips, event_status, row_hash,
    run_id, capture_id, source_line, source_record_id, created_at, superseded_by_run_id,
    change_kind
)
SELECT current.event_id, current.year, current.event_type, current.state_fips,
       current.cz_type, current.cz_fips, current.event_status, current.row_hash,
       current.run_id, current.capture_id, current.source_line, current.source_record_id,
       current.created_at, %(run_id)s, %(change_kind)s
FROM silver_noaa_storm_events.event AS current
JOIN {source} AS changed USING (event_id)
ON CONFLICT DO NOTHING
"""


def _replay_details_file(
    cursor: Any,
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    capture_id: str,
    data_year: int,
) -> tuple[int, int, int, int, bool]:
    """Stage, compare and write one year; return (rows, quarantined, written, withdrawn, refused)."""
    parsed = ParsedDetails()
    csv_bytes = decompress_details(
        load_captured_payload(connection_factory, UUID(capture_id))
    )
    cursor.execute(_EVENT_STAGE)
    cursor.copy_expert(
        f"COPY storm_incoming ({_EVENT_COLUMN_LIST}) FROM STDIN WITH (FORMAT text)",
        _RowStream(
            _rows(
                _event_rows(
                    parse_details_file(csv_bytes, file_year=data_year, parsed=parsed),
                    capture_id,
                )
            )
        ),
    )
    refused = any(item.line == 0 for item in parsed.quarantined)
    cursor.execute("CREATE INDEX ON storm_incoming (event_id)")
    cursor.execute("ANALYZE storm_incoming")
    cursor.execute("SET LOCAL enable_nestloop = off")
    cursor.execute(
        """
        CREATE TEMPORARY TABLE storm_changed ON COMMIT DROP AS
        SELECT incoming.event_id, (current.event_id IS NOT NULL) AS replaces
        FROM storm_incoming AS incoming
        LEFT JOIN silver_noaa_storm_events.event AS current USING (event_id)
        WHERE current.event_id IS NULL
           OR current.row_hash <> incoming.row_hash
           OR current.event_status <> 'current'
        """
    )
    # Events of the year an earlier file published that this file does not
    # hold. A refused file withdraws nothing.
    cursor.execute(
        """
        CREATE TEMPORARY TABLE storm_withdrawn ON COMMIT DROP AS
        SELECT current.event_id
        FROM silver_noaa_storm_events.event AS current
        WHERE current.year = %(year)s AND current.event_status = 'current'
          AND NOT %(refused)s
          AND NOT EXISTS (
              SELECT 1 FROM storm_incoming AS incoming WHERE incoming.event_id = current.event_id
          )
        """,
        {"year": data_year, "refused": refused},
    )
    cursor.execute("SET LOCAL enable_nestloop = on")
    cursor.execute("ANALYZE storm_changed")
    cursor.execute(
        _REVISION_INSERT.format(
            source="(SELECT event_id FROM storm_changed WHERE replaces)"
        ),
        {"run_id": str(run_id), "change_kind": "replaced"},
    )
    cursor.execute(
        _REVISION_INSERT.format(source="storm_withdrawn"),
        {"run_id": str(run_id), "change_kind": "withdrawn"},
    )
    update_columns = ",\n            ".join(
        f"{column} = EXCLUDED.{column}"
        for column in _EVENT_COLUMNS
        if column != "event_id"
    )
    cursor.execute(
        f"""
        INSERT INTO silver_noaa_storm_events.event (
            {_EVENT_COLUMN_LIST}, event_status, placement_status, run_id, capture_id
        )
        SELECT {", ".join("incoming." + column for column in _EVENT_COLUMNS)},
               'current',
               -- Set by the placement step below, in the same transaction.
               CASE incoming.cz_type WHEN 'M' THEN 'marine' WHEN 'Z' THEN 'zone_unmatched'
                    ELSE 'county_unmapped' END,
               %(run_id)s, %(capture_id)s
        FROM storm_incoming AS incoming
        JOIN storm_changed AS changed USING (event_id)
        ON CONFLICT (event_id) DO UPDATE SET
            {update_columns},
            event_status = 'current',
            run_id = EXCLUDED.run_id,
            capture_id = EXCLUDED.capture_id,
            created_at = NOW()
        """,
        {"run_id": str(run_id), "capture_id": capture_id},
    )
    written = cursor.rowcount
    cursor.execute(
        """
        UPDATE silver_noaa_storm_events.event AS current
           SET event_status = 'withdrawn', run_id = %(run_id)s, created_at = NOW()
          FROM storm_withdrawn AS withdrawn
         WHERE current.event_id = withdrawn.event_id
        """,
        {"run_id": str(run_id)},
    )
    withdrawn = cursor.rowcount
    _write_quarantine(cursor, run_id, capture_id, parsed.quarantined)
    cursor.execute(
        """
        CREATE TEMPORARY TABLE storm_target ON COMMIT DROP AS
        SELECT event_id FROM storm_changed
        UNION
        SELECT event_id FROM storm_withdrawn
        """
    )
    place_events(cursor, years=[data_year])
    cursor.execute(
        """
        SELECT COUNT(*) FROM storm_incoming AS incoming
        JOIN silver_noaa_storm_events.event AS current USING (event_id)
        WHERE current.row_hash = incoming.row_hash AND current.event_status = 'current'
        """
    )
    present = int(cursor.fetchone()[0])
    cursor.execute("SELECT COUNT(*) FROM storm_incoming")
    staged = int(cursor.fetchone()[0])
    if (
        present != staged
        or staged + len(parsed.quarantined) - (1 if refused else 0) != parsed.row_count
    ):
        raise StormEventsReconciliationError(
            f"run {run_id} year {data_year}: {present} of {staged} staged events are current; "
            f"{parsed.row_count} rows read, {len(parsed.quarantined)} quarantined"
        )
    return parsed.row_count, len(parsed.quarantined), written, withdrawn, refused


# ---------------------------------------------------------------------------
# Placement and counts
# ---------------------------------------------------------------------------


def _usps_table(cursor: Any) -> None:
    cursor.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS storm_state_usps "
        "(state_fips TEXT PRIMARY KEY, usps TEXT NOT NULL, in_nation BOOLEAN NOT NULL) "
        "ON COMMIT DROP"
    )
    cursor.execute("TRUNCATE storm_state_usps")
    from psycopg2.extras import execute_values

    execute_values(
        cursor,
        "INSERT INTO storm_state_usps (state_fips, usps, in_nation) VALUES %s",
        [
            (fips, usps, fips in NATION_STATE_FIPS)
            for fips, usps in sorted(STATE_FIPS_TO_USPS.items())
        ],
    )


def place_events(cursor: Any, *, years: Iterable[int] | None = None) -> None:
    """Place every event in ``storm_target`` and recount the years it touches.

    ``storm_target`` (one ``event_id`` column) must exist in the transaction.
    The years recounted are the target events' years plus ``years``.
    """
    _usps_table(cursor)
    cursor.execute("ANALYZE storm_target")
    # The published zone-county files, each with whether it is valid on a date.
    cursor.execute(
        """
        CREATE TEMPORARY TABLE storm_zone_file ON COMMIT DROP AS
        SELECT DISTINCT file.file_name AS zone_file, zone.valid_from
        FROM control.noaa_storm_events_file AS file
        JOIN silver_noaa_storm_events.zone_county AS zone ON zone.zone_file = file.file_name
        WHERE file.file_kind = 'zone_county' AND file.status IN ('silver_ready', 'published')
        """
    )
    cursor.execute("SET LOCAL enable_nestloop = off")
    # The zone file each zone event is matched with: the file valid on the
    # begin date first, then the others newest first; code AND name must match.
    cursor.execute(
        """
        CREATE TEMPORARY TABLE storm_zone_match ON COMMIT DROP AS
        SELECT DISTINCT ON (event.event_id) event.event_id, candidate.zone_file
        FROM storm_target AS target
        JOIN silver_noaa_storm_events.event AS event USING (event_id)
        JOIN storm_state_usps AS usps ON usps.state_fips = event.state_fips
        JOIN (
            SELECT DISTINCT zone.zone_file, zone.state, zone.zone, zone.zone_name_key,
                   zone.valid_from
            FROM silver_noaa_storm_events.zone_county AS zone
            JOIN storm_zone_file AS file USING (zone_file)
        ) AS candidate
          ON candidate.state = usps.usps
         AND candidate.zone = event.cz_fips
         AND candidate.zone_name_key = event.cz_name_key
        WHERE event.cz_type = 'Z' AND event.event_status = 'current'
        ORDER BY event.event_id,
                 (candidate.valid_from = (
                     SELECT MAX(valid.valid_from) FROM storm_zone_file AS valid
                     WHERE valid.valid_from <= event.begin_date
                 )) IS TRUE DESC,
                 candidate.valid_from DESC
        """
    )
    cursor.execute(
        """
        CREATE TEMPORARY TABLE storm_place_desired ON COMMIT DROP AS
        SELECT event.event_id, entity.geo_id, entity.geo_sk, event.year,
               'storm_events_' || event.event_type_key AS metric_key,
               'county_reported'::TEXT AS basis
        FROM storm_target AS target
        JOIN silver_noaa_storm_events.event AS event USING (event_id)
        JOIN silver_ref.dim_geo_entity AS entity
          ON entity.geo_type = 'county'
         AND entity.geo_id = 'state:' || event.state_fips || '|county:' || event.cz_fips
        WHERE event.cz_type = 'C' AND event.event_status = 'current'
        UNION ALL
        SELECT DISTINCT event.event_id, zone.geo_id, zone.geo_sk, event.year,
               'storm_events_' || event.event_type_key, 'zone_reported'
        FROM storm_zone_match AS matched
        JOIN silver_noaa_storm_events.event AS event USING (event_id)
        JOIN storm_state_usps AS usps ON usps.state_fips = event.state_fips
        JOIN silver_noaa_storm_events.zone_county AS zone
          ON zone.zone_file = matched.zone_file
         AND zone.state = usps.usps
         AND zone.zone = event.cz_fips
        WHERE zone.geography_status = 'resolved'
        """
    )
    cursor.execute("CREATE INDEX ON storm_place_desired (event_id, geo_id)")
    cursor.execute("ANALYZE storm_place_desired")
    cursor.execute(
        """
        CREATE TEMPORARY TABLE storm_years ON COMMIT DROP AS
        SELECT DISTINCT event.year
        FROM storm_target AS target
        JOIN silver_noaa_storm_events.event AS event USING (event_id)
        UNION
        SELECT DISTINCT place.year
        FROM storm_target AS target
        JOIN silver_noaa_storm_events.event_place AS place USING (event_id)
        UNION
        SELECT UNNEST(%s::INTEGER[])
        """,
        (list(years or []),),
    )
    cursor.execute(
        """
        DELETE FROM silver_noaa_storm_events.event_place AS place
        USING storm_target AS target
        WHERE place.event_id = target.event_id
          AND NOT EXISTS (
              SELECT 1 FROM storm_place_desired AS desired
              WHERE desired.event_id = place.event_id AND desired.geo_id = place.geo_id
                AND desired.year = place.year AND desired.metric_key = place.metric_key
                AND desired.basis = place.basis
          )
        """
    )
    cursor.execute(
        """
        INSERT INTO silver_noaa_storm_events.event_place (
            event_id, geo_id, geo_sk, year, metric_key, basis
        )
        SELECT desired.event_id, desired.geo_id, desired.geo_sk, desired.year,
               desired.metric_key, desired.basis
        FROM storm_place_desired AS desired
        ON CONFLICT (event_id, geo_id) DO UPDATE SET
            geo_sk = EXCLUDED.geo_sk,
            year = EXCLUDED.year,
            metric_key = EXCLUDED.metric_key,
            basis = EXCLUDED.basis
        WHERE (silver_noaa_storm_events.event_place.geo_sk,
               silver_noaa_storm_events.event_place.year,
               silver_noaa_storm_events.event_place.metric_key,
               silver_noaa_storm_events.event_place.basis)
              IS DISTINCT FROM (EXCLUDED.geo_sk, EXCLUDED.year, EXCLUDED.metric_key, EXCLUDED.basis)
        """
    )
    cursor.execute(
        """
        UPDATE silver_noaa_storm_events.event AS event
           SET placement_status = placed.placement_status, zone_file = placed.zone_file
          FROM (
              SELECT event.event_id,
                     CASE
                         WHEN event.cz_type = 'M' THEN 'marine'
                         WHEN event.cz_type = 'Z' AND matched.zone_file IS NOT NULL THEN 'zone_matched'
                         WHEN event.cz_type = 'Z' THEN 'zone_unmatched'
                         WHEN entity.geo_sk IS NOT NULL THEN 'county'
                         ELSE 'county_unmapped'
                     END AS placement_status,
                     CASE WHEN event.cz_type = 'Z' THEN matched.zone_file END AS zone_file
              FROM storm_target AS target
              JOIN silver_noaa_storm_events.event AS event USING (event_id)
              LEFT JOIN storm_zone_match AS matched USING (event_id)
              LEFT JOIN silver_ref.dim_geo_entity AS entity
                ON event.cz_type = 'C' AND entity.geo_type = 'county'
               AND entity.geo_id = 'state:' || event.state_fips || '|county:' || event.cz_fips
          ) AS placed
         WHERE event.event_id = placed.event_id
           AND (event.placement_status, event.zone_file)
               IS DISTINCT FROM (placed.placement_status, placed.zone_file)
        """
    )
    cursor.execute("SET LOCAL enable_nestloop = on")
    _recount_years(cursor)
    _recount_states(cursor)


#: The impact measures: the event column each sums.
_IMPACTS = (
    ("storm_event_deaths_direct", "deaths_direct"),
    ("storm_event_deaths_indirect", "deaths_indirect"),
    ("storm_event_injuries_direct", "injuries_direct"),
    ("storm_event_injuries_indirect", "injuries_indirect"),
    ("storm_event_property_damage", "damage_property_usd"),
    ("storm_event_crop_damage", "damage_crops_usd"),
)


def _recount_years(cursor: Any) -> None:
    """Recompute ``place_year`` for every year in ``storm_years``; write only changes."""
    impacts = ", ".join(
        f"('{metric}', event.{column}::NUMERIC)" for metric, column in _IMPACTS
    )
    cursor.execute(
        f"""
        CREATE TEMPORARY TABLE storm_place_year ON COMMIT DROP AS
        WITH current_event AS (
            SELECT event.*
            FROM silver_noaa_storm_events.event AS event
            JOIN storm_years USING (year)
            WHERE event.event_status = 'current'
        ),
        county_count AS (
            SELECT place.metric_key, 'county'::TEXT AS geo_type, place.geo_id,
                   MIN(place.geo_sk) AS geo_sk, place.year, MIN(event.event_type) AS event_type,
                   COUNT(*)::NUMERIC AS value, COUNT(*)::INTEGER AS events_counted,
                   COUNT(*) FILTER (WHERE place.basis = 'county_reported')::INTEGER AS county_reported,
                   COUNT(*) FILTER (WHERE place.basis = 'zone_reported')::INTEGER AS zone_reported
            FROM silver_noaa_storm_events.event_place AS place
            JOIN storm_years USING (year)
            JOIN current_event AS event USING (event_id)
            GROUP BY place.metric_key, place.geo_id, place.year
        ),
        county_total AS (
            SELECT 'storm_events_all_types'::TEXT, 'county'::TEXT, geo_id, MIN(geo_sk), year,
                   NULL::TEXT, SUM(value), SUM(events_counted)::INTEGER,
                   SUM(county_reported)::INTEGER, SUM(zone_reported)::INTEGER
            FROM county_count
            GROUP BY geo_id, year
        ),
        county_impact AS (
            SELECT impact.metric, 'county'::TEXT, place.geo_id, MIN(place.geo_sk), place.year,
                   NULL::TEXT, SUM(impact.amount),
                   COUNT(impact.amount)::INTEGER, COUNT(impact.amount)::INTEGER, 0
            FROM silver_noaa_storm_events.event_place AS place
            JOIN storm_years USING (year)
            JOIN current_event AS event USING (event_id)
            CROSS JOIN LATERAL (VALUES {impacts}) AS impact(metric, amount)
            WHERE place.basis = 'county_reported' AND impact.amount IS NOT NULL
            GROUP BY impact.metric, place.geo_id, place.year
        ),
        area_event AS (
            SELECT event.*, area.geo_type, area.geo_id, area.geo_sk
            FROM current_event AS event
            JOIN storm_state_usps AS usps ON usps.state_fips = event.state_fips
            JOIN silver_ref.dim_geo_entity AS area
              ON (area.geo_type = 'state' AND area.geo_id = 'state:' || event.state_fips)
              OR (usps.in_nation AND area.geo_type = 'nation' AND area.geo_id = 'us:1')
        ),
        area_count AS (
            SELECT 'storm_events_' || event_type_key, geo_type, geo_id, MIN(geo_sk), year,
                   MIN(event_type), COUNT(*)::NUMERIC, COUNT(*)::INTEGER,
                   COUNT(*) FILTER (WHERE cz_type = 'C')::INTEGER,
                   COUNT(*) FILTER (WHERE cz_type = 'Z')::INTEGER
            FROM area_event
            GROUP BY event_type_key, geo_type, geo_id, year
        ),
        area_total AS (
            SELECT 'storm_events_all_types', geo_type, geo_id, MIN(geo_sk), year, NULL::TEXT,
                   COUNT(*)::NUMERIC, COUNT(*)::INTEGER,
                   COUNT(*) FILTER (WHERE cz_type = 'C')::INTEGER,
                   COUNT(*) FILTER (WHERE cz_type = 'Z')::INTEGER
            FROM area_event
            GROUP BY geo_type, geo_id, year
        ),
        area_impact AS (
            SELECT impact.metric, event.geo_type, event.geo_id, MIN(event.geo_sk), event.year,
                   NULL::TEXT, SUM(impact.amount), COUNT(impact.amount)::INTEGER,
                   COUNT(impact.amount) FILTER (WHERE event.cz_type = 'C')::INTEGER,
                   COUNT(impact.amount) FILTER (WHERE event.cz_type = 'Z')::INTEGER
            FROM area_event AS event
            CROSS JOIN LATERAL (VALUES {impacts}) AS impact(metric, amount)
            WHERE impact.amount IS NOT NULL
            GROUP BY impact.metric, event.geo_type, event.geo_id, event.year
        )
        SELECT * FROM county_count
        UNION ALL SELECT * FROM county_total
        UNION ALL SELECT * FROM county_impact
        UNION ALL SELECT * FROM area_count
        UNION ALL SELECT * FROM area_total
        UNION ALL SELECT * FROM area_impact
        """
    )
    cursor.execute("ANALYZE storm_place_year")
    cursor.execute(
        """
        INSERT INTO silver_noaa_storm_events.event_type (
            event_type_key, event_type, first_seen_run_id
        )
        SELECT SUBSTRING(metric_key FROM 14), MIN(event_type), (SELECT run_id FROM storm_run)
        FROM storm_place_year
        WHERE event_type IS NOT NULL
        GROUP BY metric_key
        ON CONFLICT (event_type_key) DO NOTHING
        """
    )
    cursor.execute(
        """
        DELETE FROM silver_noaa_storm_events.place_year AS stored
        USING storm_years AS touched
        WHERE stored.year = touched.year
          AND NOT EXISTS (
              SELECT 1 FROM storm_place_year AS desired
              WHERE desired.metric_key = stored.metric_key AND desired.geo_id = stored.geo_id
                AND desired.year = stored.year
          )
        """
    )
    cursor.execute(
        """
        INSERT INTO silver_noaa_storm_events.place_year (
            metric_key, geo_type, geo_id, geo_sk, year, event_type, value, events_counted,
            county_reported, zone_reported, run_id
        )
        SELECT desired.metric_key, desired.geo_type, desired.geo_id, desired.geo_sk,
               desired.year, desired.event_type, desired.value, desired.events_counted,
               desired.county_reported, desired.zone_reported,
               (SELECT run_id FROM storm_run)
        FROM storm_place_year AS desired
        ON CONFLICT (metric_key, geo_id, year) DO UPDATE SET
            geo_type = EXCLUDED.geo_type,
            geo_sk = EXCLUDED.geo_sk,
            event_type = EXCLUDED.event_type,
            value = EXCLUDED.value,
            events_counted = EXCLUDED.events_counted,
            county_reported = EXCLUDED.county_reported,
            zone_reported = EXCLUDED.zone_reported,
            run_id = EXCLUDED.run_id,
            updated_at = NOW()
        WHERE (silver_noaa_storm_events.place_year.geo_sk,
               silver_noaa_storm_events.place_year.event_type,
               silver_noaa_storm_events.place_year.value,
               silver_noaa_storm_events.place_year.events_counted,
               silver_noaa_storm_events.place_year.county_reported,
               silver_noaa_storm_events.place_year.zone_reported)
              IS DISTINCT FROM (EXCLUDED.geo_sk, EXCLUDED.event_type, EXCLUDED.value,
                                EXCLUDED.events_counted, EXCLUDED.county_reported,
                                EXCLUDED.zone_reported)
        """
    )


def _recount_states(cursor: Any) -> None:
    """Recompute ``state_year`` for every year in ``storm_years``; write only changes."""
    cursor.execute(
        """
        CREATE TEMPORARY TABLE storm_state_year ON COMMIT DROP AS
        SELECT event.state_fips, event.year, MIN(event.state_name) AS state_name,
               COUNT(*)::INTEGER AS events_reported,
               COUNT(*) FILTER (WHERE event.placement_status = 'county')::INTEGER
                   AS county_events_placed,
               COUNT(*) FILTER (WHERE event.placement_status = 'county_unmapped')::INTEGER
                   AS county_events_unplaced,
               COUNT(*) FILTER (WHERE event.placement_status = 'zone_matched')::INTEGER
                   AS zone_events_placed,
               COUNT(*) FILTER (WHERE event.placement_status = 'zone_unmatched')::INTEGER
                   AS zone_events_unplaced,
               COUNT(*) FILTER (WHERE event.placement_status = 'marine')::INTEGER
                   AS marine_events
        FROM silver_noaa_storm_events.event AS event
        JOIN storm_years USING (year)
        WHERE event.event_status = 'current'
        GROUP BY event.state_fips, event.year
        """
    )
    cursor.execute(
        """
        DELETE FROM silver_noaa_storm_events.state_year AS stored
        USING storm_years AS touched
        WHERE stored.year = touched.year
          AND NOT EXISTS (
              SELECT 1 FROM storm_state_year AS desired
              WHERE desired.state_fips = stored.state_fips AND desired.year = stored.year
          )
        """
    )
    cursor.execute(
        """
        INSERT INTO silver_noaa_storm_events.state_year (
            state_fips, year, state_name, events_reported, county_events_placed,
            county_events_unplaced, zone_events_placed, zone_events_unplaced, marine_events,
            run_id
        )
        SELECT desired.*, (SELECT run_id FROM storm_run) FROM storm_state_year AS desired
        ON CONFLICT (state_fips, year) DO UPDATE SET
            state_name = EXCLUDED.state_name,
            events_reported = EXCLUDED.events_reported,
            county_events_placed = EXCLUDED.county_events_placed,
            county_events_unplaced = EXCLUDED.county_events_unplaced,
            zone_events_placed = EXCLUDED.zone_events_placed,
            zone_events_unplaced = EXCLUDED.zone_events_unplaced,
            marine_events = EXCLUDED.marine_events,
            run_id = EXCLUDED.run_id,
            updated_at = NOW()
        WHERE (silver_noaa_storm_events.state_year.state_name,
               silver_noaa_storm_events.state_year.events_reported,
               silver_noaa_storm_events.state_year.county_events_placed,
               silver_noaa_storm_events.state_year.county_events_unplaced,
               silver_noaa_storm_events.state_year.zone_events_placed,
               silver_noaa_storm_events.state_year.zone_events_unplaced,
               silver_noaa_storm_events.state_year.marine_events)
              IS DISTINCT FROM (EXCLUDED.state_name, EXCLUDED.events_reported,
                                EXCLUDED.county_events_placed, EXCLUDED.county_events_unplaced,
                                EXCLUDED.zone_events_placed, EXCLUDED.zone_events_unplaced,
                                EXCLUDED.marine_events)
        """
    )


def _set_run(cursor: Any, run_id: UUID) -> None:
    cursor.execute(
        "CREATE TEMPORARY TABLE IF NOT EXISTS storm_run (run_id UUID NOT NULL) ON COMMIT DROP"
    )
    cursor.execute("TRUNCATE storm_run")
    cursor.execute("INSERT INTO storm_run (run_id) VALUES (%s)", (str(run_id),))


def _replace_zone_events(cursor: Any) -> None:
    """Re-place every current zone event after a zone-county file changed."""
    cursor.execute(
        """
        CREATE TEMPORARY TABLE storm_target ON COMMIT DROP AS
        SELECT event_id FROM silver_noaa_storm_events.event
        WHERE cz_type = 'Z' AND event_status = 'current'
        """
    )
    cursor.execute("CREATE INDEX ON storm_target (event_id)")
    place_events(cursor)


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


def _run_files(connection_factory: Callable[[], Any], run_id: UUID) -> list[tuple]:
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT file_kind, file_name, capture_id::TEXT, data_year
                FROM control.noaa_storm_events_file
                WHERE run_id = %s AND status = 'captured'
                -- Zone files first, so this run's events are placed with them.
                ORDER BY file_kind DESC, stamp, data_year, file_name
                """,
                (str(run_id),),
            )
            files = cursor.fetchall()
        reader.commit()
    finally:
        reader.close()
    return files


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Replay every captured file of a run; return the events and zone rows written."""
    files = _run_files(connection_factory, run_id)
    total = 0
    zones_changed = False
    for file_kind, file_name, capture_id, data_year in files:
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                _set_run(cursor, run_id)
                if file_kind == "zone_county":
                    rows, quarantined, written = _replay_zone_file(
                        cursor,
                        connection_factory,
                        run_id=run_id,
                        file_name=file_name,
                        capture_id=capture_id,
                    )
                    withdrawn, refused = 0, rows == 0
                    zones_changed = zones_changed or written > 0
                else:
                    if zones_changed:
                        # Placed with the new zone files before this year's events.
                        _replace_zone_events(cursor)
                        cursor.execute("DROP TABLE storm_target")
                        for table in (
                            "storm_zone_file",
                            "storm_zone_match",
                            "storm_place_desired",
                            "storm_years",
                            "storm_place_year",
                            "storm_state_year",
                        ):
                            cursor.execute(f"DROP TABLE {table}")
                        zones_changed = False
                    rows, quarantined, written, withdrawn, refused = (
                        _replay_details_file(
                            cursor,
                            connection_factory,
                            run_id=run_id,
                            capture_id=capture_id,
                            data_year=int(data_year),
                        )
                    )
                cursor.execute(
                    """
                    UPDATE control.noaa_storm_events_file
                       SET row_count = %s, quarantined_count = %s, written_count = %s,
                           withdrawn_count = %s, status = %s, updated_at = NOW()
                     WHERE run_id = %s AND file_name = %s AND status = 'captured'
                    """,
                    (
                        rows,
                        quarantined,
                        written,
                        withdrawn,
                        "quarantined" if refused else "silver_ready",
                        str(run_id),
                        file_name,
                    ),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        total += written
    if zones_changed:
        # A run that read new zone files and no details file.
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                _set_run(cursor, run_id)
                _replace_zone_events(cursor)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
    return total


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a run's reconciled files through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.noaa_storm_events_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING file_name
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT COUNT(*) FROM control.noaa_storm_events_file "
                "WHERE run_id = %s AND status = 'captured'",
                (str(run_id),),
            )
            if int(cursor.fetchone()[0]):
                raise StormEventsPublicationError(
                    f"run {run_id} has files never replayed"
                )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_noaa_storm_events"
        )
    return published


def settle_unreplayed_runs(connection_factory: Callable[[], Any]) -> list[UUID]:
    """Finish, oldest first, every run an earlier attempt captured but never replayed."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id::TEXT, MIN(created_at)
                  FROM control.noaa_storm_events_file
                 WHERE status = 'captured'
                 GROUP BY run_id
                 ORDER BY MIN(created_at), run_id
                """
            )
            runs = [UUID(row[0]) for row in cursor.fetchall()]
        reader.commit()
    finally:
        reader.close()
    for run_id in runs:
        replay_run(connection_factory, run_id=run_id)
        publish_run(connection_factory, run_id=run_id)
    return runs
