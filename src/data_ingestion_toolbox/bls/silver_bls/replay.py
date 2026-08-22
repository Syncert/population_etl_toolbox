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
            execute_values(
                cursor,
                """
                INSERT INTO silver_bls.observation_revision (
                    capture_id, observation_index, program, series_id,
                    year_source, period_source, period_name_source,
                    value_source, latest_source, footnotes_source,
                    year, period, period_name, value, value_status, is_latest
                ) VALUES %s
                ON CONFLICT (capture_id, observation_index) DO NOTHING
                """,
                records,
                page_size=5000,
            )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()
    return len(records)
