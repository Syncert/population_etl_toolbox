"""Offline Census ACS capture replay into source-shaped silver values."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload
from data_ingestion_toolbox.normalization import NumericParseError, parse_decimal

CENSUS_NULL_SENTINELS = {
    "-222222222",
    "-333333333",
    "-555555555",
    "-666666666",
    "-888888888",
    "-999999999",
}


class CensusCapturePayloadError(ValueError):
    """A captured Census payload violates its registered response contract."""


def parse_captured_values(
    payload: bytes,
    *,
    dataset: str,
    year: int,
    geo_level: str,
) -> list[dict[str, object]]:
    """Unpivot a captured array in silver while preserving headers and strings."""
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CensusCapturePayloadError("Census capture is not valid JSON") from exc
    if not isinstance(document, list) or not document:
        raise CensusCapturePayloadError("Census response must be a non-empty array")
    header = document[0]
    records = document[1:]
    if not isinstance(header, list) or not all(
        isinstance(item, str) for item in header
    ):
        raise CensusCapturePayloadError("Census header must be an array of strings")
    if len(set(header)) != len(header):
        raise CensusCapturePayloadError("Census response contains duplicate headers")
    if any(
        not isinstance(record, list) or len(record) != len(header) for record in records
    ):
        raise CensusCapturePayloadError(
            "Census response row length does not match header"
        )
    required_geographies = {
        "us": {"us"},
        "state": {"state"},
        "county": {"state", "county"},
    }
    if geo_level not in required_geographies:
        raise CensusCapturePayloadError("unsupported Census geography level")
    missing = required_geographies[geo_level] - set(header)
    if missing:
        raise CensusCapturePayloadError(
            f"Census response missing geography columns: {sorted(missing)}"
        )

    geo_columns = {"us", "state", "county"}
    variable_indexes = [
        index for index, name in enumerate(header) if name not in geo_columns
    ]
    parsed: list[dict[str, object]] = []
    for row_index, record in enumerate(records):
        source_row = dict(zip(header, record))
        state_source = _text(source_row.get("state"))
        county_source = _text(source_row.get("county"))
        us_source = _text(source_row.get("us"))
        for column_index in variable_indexes:
            variable_name = header[column_index]
            value_source = _text(record[column_index])
            if value_source is None:
                value_status = "absent"
                value = None
            elif value_source == "":
                value_status = "blank"
                value = None
            elif value_source in CENSUS_NULL_SENTINELS:
                value_status = "sentinel"
                value = None
            else:
                try:
                    value = parse_decimal(value_source)
                except NumericParseError:
                    value_status = "invalid"
                    value = None
                else:
                    value_status = "valid" if value is not None else "invalid"
            parsed.append(
                {
                    "source_row_index": row_index,
                    "source_column_index": column_index,
                    "source_header": variable_name,
                    "dataset": dataset,
                    "year": year,
                    "geo_level": geo_level,
                    "us_source": us_source,
                    "state_fips_source": state_source,
                    "county_fips_source": county_source,
                    "variable_name": variable_name,
                    "table_id": variable_name.split("_", 1)[0],
                    "measure_type": variable_name[-1:] or None,
                    "value_source": value_source,
                    "value": value,
                    "value_status": value_status,
                }
            )
    return parsed


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def replay_census_capture(
    connection_factory: Callable[[], Any],
    *,
    capture_id: UUID,
    dataset: str,
    year: int,
    geo_level: str,
) -> int:
    """Replay one stored Census response without network access."""
    values = parse_captured_values(
        load_captured_payload(connection_factory, capture_id),
        dataset=dataset,
        year=year,
        geo_level=geo_level,
    )
    if not values:
        return 0
    records = [
        (
            str(capture_id),
            item["source_row_index"],
            item["source_column_index"],
            item["source_header"],
            item["dataset"],
            item["year"],
            item["geo_level"],
            item["us_source"],
            item["state_fips_source"],
            item["county_fips_source"],
            item["variable_name"],
            item["table_id"],
            item["measure_type"],
            item["value_source"],
            item["value"],
            item["value_status"],
        )
        for item in values
    ]
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO silver_census.observation_revision (
                    capture_id, source_row_index, source_column_index,
                    source_header, dataset, year, geo_level, us_source,
                    state_fips_source, county_fips_source, variable_name,
                    table_id, measure_type, value_source, value, value_status
                ) VALUES %s
                ON CONFLICT (capture_id, source_row_index, source_column_index)
                DO NOTHING
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
