"""Offline BLS response-capture replay into source-shaped silver revisions."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload
from data_ingestion_toolbox.normalization import NumericParseError, parse_decimal


class BlsCapturePayloadError(ValueError):
    """A captured BLS payload cannot be replayed under the declared contract."""


def parse_captured_observations(payload: bytes) -> list[dict[str, object]]:
    """Parse captured JSON and retain exact observation representations."""
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BlsCapturePayloadError("BLS capture is not valid JSON") from exc
    if not isinstance(document, dict):
        raise BlsCapturePayloadError("BLS response must be an object")
    if document.get("status") != "REQUEST_SUCCEEDED":
        raise BlsCapturePayloadError("BLS capture does not have successful status")
    results = document.get("Results")
    if not isinstance(results, Mapping):
        raise BlsCapturePayloadError("BLS Results must be an object")
    series_list = results.get("series")
    if not isinstance(series_list, list):
        raise BlsCapturePayloadError("BLS Results.series must be a list")

    parsed: list[dict[str, object]] = []
    observation_index = 0
    for series in series_list:
        if not isinstance(series, Mapping):
            raise BlsCapturePayloadError("BLS series entry must be an object")
        series_id = series.get("seriesID")
        data = series.get("data")
        if not series_id or not isinstance(data, list):
            raise BlsCapturePayloadError("BLS series is missing seriesID or data")
        for observation in data:
            if not isinstance(observation, Mapping):
                raise BlsCapturePayloadError("BLS observation must be an object")
            year_source = str(observation.get("year", ""))
            period_source = str(observation.get("period", ""))
            value_source = (
                None
                if observation.get("value") is None
                else str(observation.get("value"))
            )
            latest_source = observation.get("latest")
            try:
                year = int(year_source)
            except ValueError as exc:
                raise BlsCapturePayloadError(
                    "BLS observation has invalid year"
                ) from exc
            if not period_source:
                raise BlsCapturePayloadError("BLS observation is missing period")
            try:
                value = parse_decimal(value_source)
            except NumericParseError:
                value = None
                value_status = "invalid"
            else:
                value_status = "valid" if value is not None else "missing"
            is_latest = (
                latest_source.strip().lower() == "true"
                if isinstance(latest_source, str)
                else bool(latest_source)
            )
            parsed.append(
                {
                    "observation_index": observation_index,
                    "series_id": str(series_id),
                    "year_source": year_source,
                    "period_source": period_source,
                    "period_name_source": str(observation.get("periodName", "")),
                    "value_source": value_source,
                    "latest_source": (
                        None if latest_source is None else str(latest_source)
                    ),
                    "footnotes_source": json.dumps(
                        observation.get("footnotes", []),
                        separators=(",", ":"),
                    ),
                    "year": year,
                    "period": period_source,
                    "period_name": str(observation.get("periodName", "")),
                    "value": value,
                    "value_status": value_status,
                    "is_latest": is_latest,
                }
            )
            observation_index += 1
    return parsed


_REVISION_COLUMNS = (
    "capture_id, observation_index, program, series_id, "
    "year_source, period_source, period_name_source, "
    "value_source, latest_source, footnotes_source, "
    "year, period, period_name, value, value_status, is_latest"
)

_INCOMING_TABLE_SQL = """
    CREATE TEMP TABLE bls_incoming_revision
        (LIKE silver_bls.observation_revision INCLUDING DEFAULTS)
        ON COMMIT DROP
"""

#: BLS answers carry a response time, so every read is new bytes, yet most of
#: an answer repeats what an earlier read already stored: 5.68M of 7.87M
#: revision rows on OFFICE_LAB (2026-11-05) equalled another revision of the
#: same observation. An observation is stored again only when it differs
#: from the newest stored revision of that observation (by any source token,
#: the `latest` flag included); the transform serves the newest revision, so
#: a skipped identical row changes nothing served. The comparison uses
#: `bls_observation_revision_current_idx` and one capture probe per row.
_STORE_NEW_REVISIONS_SQL = f"""
    INSERT INTO silver_bls.observation_revision ({_REVISION_COLUMNS})
    SELECT {_REVISION_COLUMNS}
    FROM bls_incoming_revision AS incoming
    WHERE NOT EXISTS (
        SELECT 1
        FROM (
            SELECT stored.period_name_source, stored.value_source,
                   stored.latest_source, stored.footnotes_source
            FROM silver_bls.observation_revision AS stored
            JOIN raw_capture.response_capture AS capture USING (capture_id)
            WHERE stored.program = incoming.program
              AND stored.series_id = incoming.series_id
              AND stored.year = incoming.year
              AND stored.period = incoming.period
              AND stored.capture_id <> incoming.capture_id
            ORDER BY capture.retrieved_at DESC, stored.capture_id DESC
            LIMIT 1
        ) AS newest
        WHERE newest.period_name_source
                  IS NOT DISTINCT FROM incoming.period_name_source
          AND newest.value_source IS NOT DISTINCT FROM incoming.value_source
          AND newest.latest_source IS NOT DISTINCT FROM incoming.latest_source
          AND newest.footnotes_source
                  IS NOT DISTINCT FROM incoming.footnotes_source
    )
    ON CONFLICT (capture_id, observation_index) DO NOTHING
"""


def replay_bls_capture(
    connection_factory: Callable[[], Any],
    *,
    capture_id: UUID,
    program: str,
) -> int:
    """Replay a stored BLS response without any provider request."""
    observations = parse_captured_observations(
        load_captured_payload(connection_factory, capture_id)
    )
    if not observations:
        return 0
    records = [
        (
            str(capture_id),
            item["observation_index"],
            program,
            item["series_id"],
            item["year_source"],
            item["period_source"],
            item["period_name_source"],
            item["value_source"],
            item["latest_source"],
            item["footnotes_source"],
            item["year"],
            item["period"],
            item["period_name"],
            item["value"],
            item["value_status"],
            item["is_latest"],
        )
        for item in observations
    ]
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(_INCOMING_TABLE_SQL)
            execute_values(
                cursor,
                f"INSERT INTO bls_incoming_revision ({_REVISION_COLUMNS}) VALUES %s",
                records,
                page_size=5000,
            )
            cursor.execute(_STORE_NEW_REVISIONS_SQL)
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return len(records)
