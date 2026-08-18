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
        raise FredCapturePayloadError(
            f"FRED observation has invalid {field}"
        ) from exc


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
            execute_values(
                cursor,
                """
                INSERT INTO silver_fred.observation_revision (
                    capture_id, observation_index, domain, series_id,
                    observation_date_source, value_source,
                    realtime_start_source, realtime_end_source,
                    observation_date, value, value_status,
                    realtime_start, realtime_end
                ) VALUES %s
                ON CONFLICT (capture_id, observation_index) DO NOTHING
                """,
                records,
                page_size=1000,
            )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return len(records)
