"""
Census PEP silver replay — unpivot captured JSON into ``silver_pep.observation_revision``.

The PEP API returns a JSON array with a header row followed by data rows,
identical in structure to the ACS API responses already handled by
``census_acs.silver_census.replay``.  This module reuses the same sentinel
values and parsing strategy while targeting the PEP-specific schema.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload
from data_ingestion_toolbox.normalization import NumericParseError, parse_decimal

# Census null / suppressed value sentinels (shared with ACS replay)
_CENSUS_NULL_SENTINELS = frozenset({
    "-222222222",
    "-333333333",
    "-555555555",
    "-666666666",
    "-888888888",
    "-999999999",
})


class PepCapturePayloadError(ValueError):
    """A captured PEP payload violates its registered response contract."""


def _text(value: Any) -> str | None:
    return None if value is None else str(value)


# ---------------------------------------------------------------------------
# PEP-specific validation rules
# ---------------------------------------------------------------------------

_PEP_REQUIRED_GEO = {
    "us": {"us"},
    "state": {"state", "name"},
    "county": {"state", "county"},
    "place": {"state", "place"},
    "division": {"state", "diviston"},  # intentional: source may use "division"
    "region": {"region"},
}


def parse_captured_pep_values(
    payload: bytes,
    *,
    year: int,
    file_type: str,
) -> list[dict[str, Any]]:
    """Unpivot a captured PEP JSON array into silver-layer records.

    Parameters
    ----------
    payload:
        Raw ``bytes`` from ``raw_capture.payload_blob``.
    year:
        Calendar year the capture represents (extracted from the API URL).
    file_type:
        PEP file type: ``"ansfile"`` or ``"intlfile"``.

    Returns
    -------
    list[dict[str, Any]]
        One dict per (geography, variable) combination.
    """
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PepCapturePayloadError("PEP capture is not valid JSON") from exc

    if not isinstance(document, list) or not document:
        raise PepCapturePayloadError("PEP response must be a non-empty array")

    header = document[0]
    records = document[1:]

    if not isinstance(header, list) or not all(isinstance(item, str) for item in header):
        raise PepCapturePayloadError("PEP header must be an array of strings")

    if len(set(header)) != len(header):
        raise PepCapturePayloadError("PEP response contains duplicate headers")

    if any(not isinstance(record, list) or len(record) != len(header) for record in records):
        raise PepCapturePayloadError("PEP response row length does not match header")

    # Identify geography columns (PEP always includes 'state'; 'us' or 'county' or 'place' depending on file)
    geo_columns = {"us", "state", "county", "place", "name", "region", "division"}
    variable_indexes = [i for i, name in enumerate(header) if name not in geo_columns]

    parsed: list[dict[str, Any]] = []
    for row_index, record in enumerate(records):
        source_row = dict(zip(header, record))
        state_source = _text(source_row.get("state"))
        county_source = _text(source_row.get("county"))
        us_source = _text(source_row.get("us"))
        place_source = _text(source_row.get("place"))
        name_source = _text(source_row.get("name"))

        for column_index in variable_indexes:
            variable_name = header[column_index]
            value_source = _text(record[column_index])

            if value_source is None or value_source == "":
                value_status = "absent"
                value = None
            elif value_source in _CENSUS_NULL_SENTINELS:
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

            parsed.append({
                "source_row_index": row_index,
                "source_column_index": column_index,
                "source_header": variable_name,
                "year": year,
                "file_type": file_type,
                "state_fips_source": state_source,
                "county_fips_source": county_source,
                "place_fips_source": place_source,
                "name_source": name_source,
                "us_source": us_source,
                "variable_name": variable_name,
                "value_source": value_source,
                "value": value,
                "value_status": value_status,
            })

    return parsed


# ---------------------------------------------------------------------------
# Replay into silver layer
# ---------------------------------------------------------------------------

def replay_pep_capture(
    connection_factory: Callable[[], Any],
    *,
    capture_id: UUID,
    year: int,
    file_type: str,
) -> int:
    """Replay one stored PEP response into ``silver_pep.observation_revision``.

    Parameters
    ----------
    connection_factory:
        Callable returning a psycopg2-style database connection.
    capture_id:
        UUID of the captured response in ``raw_capture.response_capture``.
    year:
        Calendar year of the capture.
    file_type:
        PEP file type (``"ansfile"`` / ``"intlfile"``).

    Returns
    -------
    int
        Number of rows inserted into ``silver_pep.observation_revision``.
    """
    values = parse_captured_pep_values(
        load_captured_payload(connection_factory, capture_id),
        year=year,
        file_type=file_type,
    )
    if not values:
        return 0

    records = [
        (
            str(capture_id),
            item["source_row_index"],
            item["source_column_index"],
            item["source_header"],
            item["year"],
            item["file_type"],
            item["state_fips_source"],
            item["county_fips_source"],
            item["place_fips_source"],
            item["name_source"],
            item["us_source"],
            item["variable_name"],
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
                INSERT INTO silver_pep.observation_revision (
                    capture_id, source_row_index, source_column_index,
                    source_header, year, file_type,
                    state_fips_source, county_fips_source,
                    place_fips_source, name_source, us_source,
                    variable_name, value_source, value, value_status
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
