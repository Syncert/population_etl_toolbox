"""Offline FRED response-capture replay into source-shaped silver revisions."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload
from data_ingestion_toolbox.normalization import NumericParseError, parse_decimal


class FredCapturePayloadError(ValueError):
    """A captured FRED payload cannot be replayed under the declared contract."""


def _source_text(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _parse_date(value: object, *, field: str, required: bool) -> date | None:
    source_value = _source_text(value)
    if source_value in (None, ""):
        if required:
            raise FredCapturePayloadError(f"FRED observation is missing {field}")
        return None
    try:
        return date.fromisoformat(source_value)
    except ValueError as exc:
        raise FredCapturePayloadError(f"FRED observation has invalid {field}") from exc


def parse_captured_observations(payload: bytes) -> list[dict[str, object]]:
    """Parse exact captured bytes while retaining every source representation."""
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FredCapturePayloadError("FRED capture is not valid JSON") from exc
    if not isinstance(document, dict):
        raise FredCapturePayloadError("FRED response must be an object")
    observations = document.get("observations")
    if not isinstance(observations, list):
        raise FredCapturePayloadError("FRED observations must be a list")

    parsed: list[dict[str, object]] = []
    for index, observation in enumerate(observations):
        if not isinstance(observation, Mapping):
            raise FredCapturePayloadError("FRED observation must be an object")
        observation_date_source = _source_text(observation.get("date"))
        value_source = _source_text(observation.get("value"))
        realtime_start_source = _source_text(observation.get("realtime_start"))
        realtime_end_source = _source_text(observation.get("realtime_end"))

        observation_date = _parse_date(
            observation_date_source, field="date", required=True
        )
        realtime_start = _parse_date(
            realtime_start_source, field="realtime_start", required=False
        )
        realtime_end = _parse_date(
            realtime_end_source, field="realtime_end", required=False
        )

        value: Decimal | None = None
        if value_source in (None, "", "."):
            value_status = "missing"
        else:
            try:
                value = parse_decimal(value_source)
            except NumericParseError:
                value_status = "invalid"
            else:
                value_status = "valid" if value is not None else "missing"

        parsed.append(
            {
                "observation_index": index,
                "observation_date_source": observation_date_source,
                "value_source": value_source,
                "realtime_start_source": realtime_start_source,
                "realtime_end_source": realtime_end_source,
                "observation_date": observation_date,
                "value": value,
                "value_status": value_status,
                "realtime_start": realtime_start,
                "realtime_end": realtime_end,
            }
        )
    return parsed


_REVISION_COLUMNS = (
    "capture_id, observation_index, domain, series_id, observation_date_source, "
    "value_source, realtime_start_source, realtime_end_source, observation_date, "
    "value, value_status, realtime_start, realtime_end"
)

_INCOMING_TABLE_SQL = """
    DROP TABLE IF EXISTS pg_temp.fred_incoming_revision;
    CREATE TEMP TABLE fred_incoming_revision
        (LIKE silver_fred.observation_revision INCLUDING DEFAULTS) ON COMMIT DROP
"""

#: Every monthly run re-reads each series' last two years so a revised value
#: is read again, and most of the answer repeats what is stored: 8,713 of
#: 62,876 revision rows on OFFICE_LAB repeated the previous read of the same
#: observation (refresh-churn audit, 2026-11-06). The realtime period moves on
#: every read, so a repeat that was stored also moved the silver fact and the
#: served rows. An observation is stored again only when its value or status
#: differs from the newest stored revision of it; the transform serves the
#: newest revision, so a skipped repeat changes nothing served.
_STORE_NEW_REVISIONS_SQL = f"""
    INSERT INTO silver_fred.observation_revision ({_REVISION_COLUMNS})
    SELECT {_REVISION_COLUMNS}
    FROM fred_incoming_revision AS incoming
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT stored.value_source, stored.value_status
            FROM silver_fred.observation_revision AS stored
            WHERE stored.domain IS NOT DISTINCT FROM incoming.domain
              AND stored.series_id = incoming.series_id
              AND stored.observation_date = incoming.observation_date
              AND stored.capture_id <> incoming.capture_id
            ORDER BY stored.realtime_start DESC NULLS LAST,
                     stored.parsed_at DESC, stored.capture_id DESC
            LIMIT 1
        ) AS newest
        WHERE newest.value_source IS NOT DISTINCT FROM incoming.value_source
          AND newest.value_status = incoming.value_status
    )
    ON CONFLICT (capture_id, observation_index) DO NOTHING
"""


def replay_fred_capture(
    connection_factory: Callable[[], Any],
    *,
    capture_id: UUID,
    series_id: str,
    domain: str | None,
) -> int:
    """Rebuild source-shaped silver revisions using only a stored capture."""
    payload = load_captured_payload(connection_factory, capture_id)
    observations = parse_captured_observations(payload)
    if not observations:
        return 0

    records = [
        (
            str(capture_id),
            item["observation_index"],
            domain,
            series_id,
            item["observation_date_source"],
            item["value_source"],
            item["realtime_start_source"],
            item["realtime_end_source"],
            item["observation_date"],
            item["value"],
            item["value_status"],
            item["realtime_start"],
            item["realtime_end"],
        )
        for item in observations
    ]
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(_INCOMING_TABLE_SQL)
            execute_values(
                cursor,
                f"INSERT INTO fred_incoming_revision ({_REVISION_COLUMNS}) VALUES %s",
                records,
                page_size=1000,
            )
            cursor.execute(_STORE_NEW_REVISIONS_SQL)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return len(records)
