"""Replay captured nClimDiv county files into silver, reconcile, and publish.

Everything here reads the committed capture, never the network. One
transaction per county file: its quarantine, the months it changed or added,
the superseded values and the file's counts land together or not at all.

NCEI rewrites the last two calendar years every month and leaves the rest of
a period of record alone until a version change, so a monthly read changes
a few hundred thousand of a file's five million values. Only those are
written (the churn rule): the incoming file is staged in an indexed,
analysed temporary table and compared with the current rows by key; a month
whose published text and county are the same is counted, not rewritten.
"""

from __future__ import annotations

import hashlib
import io
from collections.abc import Callable, Iterator
from typing import Any
from uuid import UUID

from data_ingestion_toolbox.capture import load_captured_payload

from ..config import SOURCE_CODE
from ..registry import ELEMENTS_BY_FILE
from .parse import MonthValue, ParsedFile, parse_county_file


class ClimdivReconciliationError(RuntimeError):
    """A replayed file does not account for every parsed value."""


class ClimdivPublicationError(RuntimeError):
    """A run has not reached the silver publication gate."""


def _record_id(value: MonthValue, measure: str, capture_id: str) -> str:
    key = (
        f"{value.ncei_state_code}{value.county_fips}|{measure}|{value.year}|"
        f"{value.month:02d}|{capture_id}"
    )
    return hashlib.md5(key.encode()).hexdigest()


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


_STAGE = """
CREATE TEMPORARY TABLE climdiv_incoming (
    ncei_state_code TEXT NOT NULL,
    county_fips TEXT NOT NULL,
    measure TEXT NOT NULL,
    year INTEGER NOT NULL,
    month SMALLINT NOT NULL,
    state_fips TEXT NOT NULL,
    value_source TEXT NOT NULL,
    value NUMERIC,
    value_status TEXT NOT NULL,
    missing_reason TEXT,
    source_line INTEGER NOT NULL,
    source_record_id TEXT NOT NULL
) ON COMMIT DROP
"""

_RESOLVE = """
CREATE TEMPORARY TABLE climdiv_county ON COMMIT DROP AS
SELECT codes.ncei_state_code, codes.county_fips, codes.state_fips,
       entity.geo_id, entity.geo_sk
FROM (
    SELECT DISTINCT ncei_state_code, county_fips, state_fips FROM climdiv_incoming
) AS codes
LEFT JOIN silver_ref.dim_geo_entity AS entity
  ON entity.geo_type = 'county'
 AND entity.geo_id = 'state:' || codes.state_fips || '|county:' || codes.county_fips
"""

#: Rows whose published text or county changed, and rows that are new.
_COMPARE = """
CREATE TEMPORARY TABLE climdiv_change ON COMMIT DROP AS
SELECT incoming.*, county.geo_id, county.geo_sk,
       CASE WHEN county.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END AS geography_status,
       (current.ncei_state_code IS NOT NULL) AS replaces
FROM climdiv_incoming AS incoming
JOIN climdiv_county AS county USING (ncei_state_code, county_fips)
LEFT JOIN silver_noaa_climdiv.county_month AS current
  ON current.ncei_state_code = incoming.ncei_state_code
 AND current.county_fips = incoming.county_fips
 AND current.measure = incoming.measure
 AND current.year = incoming.year
 AND current.month = incoming.month
WHERE current.ncei_state_code IS NULL
   OR (current.value_source, current.geo_sk) IS DISTINCT FROM (incoming.value_source, county.geo_sk)
"""


def _replay_file(
    cursor: Any,
    connection_factory: Callable[[], Any],
    *,
    run_id: UUID,
    file_element: str,
    capture_id: str,
    release_stamp: str,
) -> tuple[int, int, int, bool]:
    """Stage, compare and write one file; return (lines, values, written, refused)."""
    element = ELEMENTS_BY_FILE[file_element]
    parsed = ParsedFile()
    counted = {"values": 0}

    def rows() -> Iterator[str]:
        for value in parse_county_file(
            load_captured_payload(connection_factory, UUID(capture_id)),
            element,
            last_year=int(release_stamp[:4]),
            parsed=parsed,
        ):
            counted["values"] += 1
            yield (
                "\t".join(
                    (
                        value.ncei_state_code,
                        value.county_fips,
                        element.measure,
                        str(value.year),
                        str(value.month),
                        value.state_fips,
                        value.value_source,
                        "\\N" if value.value is None else str(value.value),
                        value.value_status,
                        value.missing_reason or "\\N",
                        str(value.line),
                        _record_id(value, element.measure, capture_id),
                    )
                )
                + "\n"
            )

    cursor.execute(_STAGE)
    cursor.copy_expert(
        "COPY climdiv_incoming FROM STDIN WITH (FORMAT text)", _RowStream(rows())
    )
    cursor.execute(
        "CREATE INDEX ON climdiv_incoming (ncei_state_code, county_fips, measure, year, month)"
    )
    cursor.execute("ANALYZE climdiv_incoming")
    cursor.execute(_RESOLVE)
    cursor.execute("ANALYZE climdiv_county")
    # A hash join over five million staged rows, never a nested loop.
    cursor.execute("SET LOCAL enable_nestloop = off")
    cursor.execute(_COMPARE)
    cursor.execute("SET LOCAL enable_nestloop = on")
    cursor.execute(
        """
        INSERT INTO silver_noaa_climdiv.county_month_revision (
            ncei_state_code, county_fips, measure, year, month, state_fips, geo_id, geo_sk,
            geography_status, value_source, value, value_status, missing_reason, run_id,
            capture_id, source_line, source_record_id, created_at, superseded_by_run_id
        )
        SELECT current.ncei_state_code, current.county_fips, current.measure, current.year,
               current.month, current.state_fips, current.geo_id, current.geo_sk,
               current.geography_status, current.value_source, current.value,
               current.value_status, current.missing_reason, current.run_id,
               current.capture_id, current.source_line, current.source_record_id,
               current.created_at, %(run_id)s
        FROM silver_noaa_climdiv.county_month AS current
        JOIN climdiv_change AS change USING (ncei_state_code, county_fips, measure, year, month)
        WHERE change.replaces
        ON CONFLICT DO NOTHING
        """,
        {"run_id": str(run_id)},
    )
    cursor.execute(
        """
        INSERT INTO silver_noaa_climdiv.county_month (
            ncei_state_code, county_fips, measure, year, month, state_fips, geo_id, geo_sk,
            geography_status, value_source, value, value_status, missing_reason, run_id,
            capture_id, source_line, source_record_id
        )
        SELECT change.ncei_state_code, change.county_fips, change.measure, change.year,
               change.month, change.state_fips, change.geo_id, change.geo_sk,
               change.geography_status, change.value_source, change.value, change.value_status,
               change.missing_reason, %(run_id)s, %(capture_id)s, change.source_line,
               change.source_record_id
        FROM climdiv_change AS change
        ON CONFLICT (ncei_state_code, county_fips, measure, year, month) DO UPDATE SET
            state_fips = EXCLUDED.state_fips,
            geo_id = EXCLUDED.geo_id,
            geo_sk = EXCLUDED.geo_sk,
            geography_status = EXCLUDED.geography_status,
            value_source = EXCLUDED.value_source,
            value = EXCLUDED.value,
            value_status = EXCLUDED.value_status,
            missing_reason = EXCLUDED.missing_reason,
            run_id = EXCLUDED.run_id,
            capture_id = EXCLUDED.capture_id,
            source_line = EXCLUDED.source_line,
            source_record_id = EXCLUDED.source_record_id,
            created_at = NOW()
        """,
        {"run_id": str(run_id), "capture_id": capture_id},
    )
    written = cursor.rowcount
    if parsed.quarantined:
        from psycopg2.extras import execute_values

        execute_values(
            cursor,
            """
            INSERT INTO silver_noaa_climdiv.quarantine (
                run_id, capture_id, source_row_index, error_code, error_summary
            ) VALUES %s
            ON CONFLICT (capture_id, source_row_index, error_code) DO NOTHING
            """,
            [
                (
                    str(run_id),
                    capture_id,
                    item.line,
                    item.error_code,
                    item.error_summary,
                )
                for item in parsed.quarantined
            ],
        )
    cursor.execute(
        """
        INSERT INTO silver_ref.geography_resolution (
            provider_source, provider_dataset, source_geo_type, source_code, source_label,
            source_vintage, geo_sk, resolution_method, evidence_capture_id, status, reason_code
        )
        SELECT %s, 'nclimdiv_county', 'county', county.ncei_state_code || county.county_fips,
               NULL, 0, county.geo_sk, CASE WHEN county.geo_sk IS NULL THEN NULL ELSE 'provider_crosswalk' END,
               %s, CASE WHEN county.geo_sk IS NULL THEN 'unmapped' ELSE 'resolved' END,
               CASE WHEN county.geo_sk IS NULL THEN 'county_not_in_shared_geography' END
        FROM climdiv_county AS county
        ON CONFLICT (provider_source, provider_dataset, source_geo_type, source_code, source_vintage)
        DO UPDATE SET
            geo_sk = EXCLUDED.geo_sk,
            resolution_method = EXCLUDED.resolution_method,
            evidence_capture_id = EXCLUDED.evidence_capture_id,
            status = EXCLUDED.status,
            reason_code = EXCLUDED.reason_code,
            resolved_at = NOW()
        -- Only a row whose content changed is rewritten (churn rule).
        WHERE (silver_ref.geography_resolution.geo_sk, silver_ref.geography_resolution.status)
              IS DISTINCT FROM (EXCLUDED.geo_sk, EXCLUDED.status)
        """,
        (SOURCE_CODE, capture_id),
    )
    cursor.execute(
        """
        SELECT COUNT(*) FROM climdiv_incoming AS incoming
        JOIN silver_noaa_climdiv.county_month AS current
          USING (ncei_state_code, county_fips, measure, year, month)
        WHERE current.value_source = incoming.value_source
        """
    )
    present = int(cursor.fetchone()[0])
    if present != counted["values"]:
        raise ClimdivReconciliationError(
            f"run {run_id} {file_element}: {present} of {counted['values']} parsed values "
            "are current after the replay"
        )
    refused = any(item.line == 0 for item in parsed.quarantined)
    return parsed.line_count, counted["values"], written, refused


def replay_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Replay every captured file of a run; return the monthly values written."""
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT file_element, capture_id::TEXT, release_stamp
                FROM control.noaa_climdiv_file
                WHERE run_id = %s AND status = 'captured'
                ORDER BY file_element
                """,
                (str(run_id),),
            )
            files = cursor.fetchall()
        reader.commit()
    finally:
        reader.close()
    total = 0
    for file_element, capture_id, release_stamp in files:
        connection = connection_factory()
        try:
            with connection.cursor() as cursor:
                lines, values, written, refused = _replay_file(
                    cursor,
                    connection_factory,
                    run_id=run_id,
                    file_element=file_element,
                    capture_id=capture_id,
                    release_stamp=release_stamp,
                )
                cursor.execute(
                    """
                    UPDATE control.noaa_climdiv_file
                       SET line_count = %s, value_count = %s, written_count = %s,
                           status = %s, updated_at = NOW()
                     WHERE run_id = %s AND file_element = %s AND status = 'captured'
                    """,
                    (
                        lines,
                        values,
                        written,
                        "quarantined" if refused else "silver_ready",
                        str(run_id),
                        file_element,
                    ),
                )
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
        total += written
    return total


def publish_run(connection_factory: Callable[[], Any], *, run_id: UUID) -> int:
    """Expose a run's reconciled files through the gold views."""
    connection = connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE control.noaa_climdiv_file
                   SET status = 'published', published_at = NOW(), updated_at = NOW()
                 WHERE run_id = %s AND status = 'silver_ready'
                RETURNING file_element
                """,
                (str(run_id),),
            )
            published = len(cursor.fetchall())
            cursor.execute(
                "SELECT COUNT(*) FROM control.noaa_climdiv_file WHERE run_id = %s AND status = 'captured'",
                (str(run_id),),
            )
            if int(cursor.fetchone()[0]):
                raise ClimdivPublicationError(f"run {run_id} has files never replayed")
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    if published:
        from data_ingestion_toolbox.glossary import emit_latest_publisher_ready

        emit_latest_publisher_ready(
            connection_factory, publisher_schema="gold_noaa_climdiv"
        )
    return published


def settle_unreplayed_runs(connection_factory: Callable[[], Any]) -> list[UUID]:
    """Finish, oldest first, every run an earlier attempt captured but never replayed.

    Settled before the next read, so an older release is never replayed over
    a newer one.
    """
    reader = connection_factory()
    try:
        with reader.cursor() as cursor:
            cursor.execute(
                """
                SELECT run_id::TEXT, MIN(created_at)
                  FROM control.noaa_climdiv_file
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
