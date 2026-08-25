"""Offline replay of captured Census PEP bulk CSV releases."""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Callable
from typing import Any
from uuid import UUID

from psycopg2.extras import execute_values

from data_ingestion_toolbox.capture import load_captured_payload
from data_ingestion_toolbox.census_pep.config import CONFIG, PEPRelease
from data_ingestion_toolbox.normalization import NumericParseError, parse_decimal

_CENSUS_NULL_SENTINELS = frozenset(
    {
        "-222222222",
        "-333333333",
        "-555555555",
        "-666666666",
        "-888888888",
        "-999999999",
    }
)

_RATE_METRICS = frozenset(
    {
        "RBIRTH",
        "RDEATH",
        "RNATURALCHG",
        "RINTERNATIONALMIG",
        "RDOMESTICMIG",
        "RNETMIG",
    }
)

_REQUIRED_SOURCE_COLUMNS = {
    "pep_nst_alldata": frozenset({"SUMLEV", "REGION", "DIVISION", "STATE", "NAME"}),
    "pep_county_alldata": frozenset({"SUMLEV", "STATE", "COUNTY", "STNAME", "CTYNAME"}),
    "pep_subcounty": frozenset(
        {
            "SUMLEV",
            "STATE",
            "COUNTY",
            "PLACE",
            "COUSUB",
            "CONCIT",
            "FUNCSTAT",
            "NAME",
            "STNAME",
        }
    ),
}


class PepCapturePayloadError(ValueError):
    """A captured PEP payload violates its registered response contract."""


def _parse_document(
    payload: bytes, *, text_encoding: str
) -> tuple[list[str], list[list[str]]]:
    if not payload:
        raise PepCapturePayloadError("PEP capture is empty")
    try:
        source = payload.decode(text_encoding)
    except UnicodeDecodeError as exc:
        raise PepCapturePayloadError(
            f"PEP capture is not valid {text_encoding} CSV"
        ) from exc
    if not source.strip():
        raise PepCapturePayloadError("PEP capture is empty")

    try:
        document = list(csv.reader(io.StringIO(source, newline=""), strict=True))
    except csv.Error as exc:
        raise PepCapturePayloadError("PEP capture is not valid CSV") from exc
    if not document or not document[0]:
        raise PepCapturePayloadError("PEP CSV header is empty")

    header = document[0]
    records = document[1:]
    if any(not column.strip() for column in header):
        raise PepCapturePayloadError("PEP CSV header contains a blank column")
    if len(header) != len(set(header)):
        raise PepCapturePayloadError("PEP CSV header contains duplicate columns")
    if not records:
        raise PepCapturePayloadError("PEP CSV contains no data rows")
    if any(len(record) != len(header) for record in records):
        raise PepCapturePayloadError("PEP CSV row length does not match header")
    return header, records


def _metric_columns(
    header: list[str],
    release: PEPRelease,
) -> list[tuple[int, str, int]]:
    dataset = CONFIG.datasets.get(release.dataset_code)
    if dataset is None:
        raise ValueError(f"unknown registered PEP dataset: {release.dataset_code}")
    required = _REQUIRED_SOURCE_COLUMNS[release.dataset_code]
    missing = required - set(header)
    if missing:
        raise PepCapturePayloadError(
            "PEP CSV is missing required columns: " + ", ".join(sorted(missing))
        )

    family_pattern = "|".join(
        re.escape(family) for family in sorted(dataset.variables, key=len, reverse=True)
    )
    pattern = re.compile(rf"^({family_pattern})_?(\d{{4}})$")
    metrics: list[tuple[int, str, int]] = []
    for index, column in enumerate(header):
        match = pattern.fullmatch(column)
        if match is None:
            continue
        observation_year = int(match.group(2))
        if not (
            release.observation_start_year
            <= observation_year
            <= release.observation_end_year
        ):
            raise PepCapturePayloadError(
                f"PEP metric column is outside release range: {column}"
            )
        metrics.append((index, match.group(1), observation_year))
    if not metrics:
        raise PepCapturePayloadError("PEP CSV contains no registered metric column")
    return metrics


def _value(value_source: str) -> tuple[Any, str]:
    if value_source == "":
        return None, "blank"
    if value_source in _CENSUS_NULL_SENTINELS:
        return None, "sentinel"
    try:
        value = parse_decimal(value_source)
    except NumericParseError:
        return None, "invalid"
    if value is None:
        return None, "invalid"
    return value, "valid"


def parse_captured_pep_values(
    payload: bytes,
    *,
    release: PEPRelease,
) -> list[dict[str, Any]]:
    """Unpivot source-shaped PEP CSV bytes into revision records."""
    dataset = CONFIG.datasets[release.dataset_code]
    header, records = _parse_document(payload, text_encoding=dataset.text_encoding)
    metrics = _metric_columns(header, release)

    parsed: list[dict[str, Any]] = []
    for row_index, record in enumerate(records):
        source_row = dict(zip(header, record))
        summary_level = source_row.get("SUMLEV")
        if summary_level not in dataset.summary_levels:
            raise PepCapturePayloadError(
                f"PEP CSV contains unregistered summary level: {summary_level}"
            )
        for column_index, metric_code, observation_year in metrics:
            value_source = record[column_index]
            value, value_status = _value(value_source)
            parsed.append(
                {
                    "source_row_index": row_index,
                    "source_column_index": column_index,
                    "source_header": header[column_index],
                    "dataset_code": release.dataset_code,
                    "release_vintage": release.vintage_year,
                    "product_code": release.product_code,
                    "observation_year": observation_year,
                    "metric_code": metric_code,
                    "unit": (
                        "per_1000_population"
                        if metric_code in _RATE_METRICS
                        else "persons"
                    ),
                    "summary_level": summary_level,
                    "region_code_source": source_row.get("REGION"),
                    "division_code_source": source_row.get("DIVISION"),
                    "state_fips_source": source_row.get("STATE"),
                    "county_fips_source": source_row.get("COUNTY"),
                    "place_fips_source": source_row.get("PLACE"),
                    "county_subdivision_source": source_row.get("COUSUB"),
                    "consolidated_city_source": source_row.get("CONCIT"),
                    "functional_status_source": source_row.get("FUNCSTAT"),
                    "name_source": source_row.get("NAME") or source_row.get("CTYNAME"),
                    "state_name_source": source_row.get("STNAME"),
                    "value_source": value_source,
                    "value": value,
                    "value_status": value_status,
                }
            )
    return parsed


def validate_release_completeness(
    values: list[dict[str, Any]],
    *,
    release: PEPRelease,
) -> None:
    """Reject bulk files that cannot represent a complete production release."""
    source_rows = {
        (item["source_row_index"], item["summary_level"], item["state_fips_source"])
        for item in values
    }
    levels = {item[1] for item in source_rows}
    state_codes = {item[2] for item in source_rows if item[2] not in (None, "00")}
    requirements = {
        "pep_nst_alldata": ("040", 50, 50),
        "pep_county_alldata": ("050", 50, 3000),
        "pep_subcounty": ("162", 50, 18000),
    }
    required_level, minimum_states, minimum_rows = requirements[release.dataset_code]
    matching_rows = {row[0] for row in source_rows if row[1] == required_level}
    if (
        required_level not in levels
        or len(state_codes) < minimum_states
        or len(matching_rows) < minimum_rows
    ):
        raise PepCapturePayloadError(
            "PEP release is incomplete: "
            f"required summary level {required_level}, at least {minimum_states} "
            f"states and {minimum_rows} principal rows; found "
            f"{len(state_codes)} states and {len(matching_rows)} principal rows"
        )


def replay_pep_capture(
    connection_factory: Callable[[], Any],
    *,
    capture_id: UUID,
    release: PEPRelease,
    require_complete: bool = False,
) -> int:
    """Replay one stored PEP release into its capture-scoped silver revision."""
    values = parse_captured_pep_values(
        load_captured_payload(connection_factory, capture_id),
        release=release,
    )
    if require_complete:
        validate_release_completeness(values, release=release)
    if not values:
        return 0

    columns = (
        "source_row_index",
        "source_column_index",
        "source_header",
        "dataset_code",
        "release_vintage",
        "product_code",
        "observation_year",
        "metric_code",
        "unit",
        "summary_level",
        "region_code_source",
        "division_code_source",
        "state_fips_source",
        "county_fips_source",
        "place_fips_source",
        "county_subdivision_source",
        "consolidated_city_source",
        "functional_status_source",
        "name_source",
        "state_name_source",
        "value_source",
        "value",
        "value_status",
    )
    records = [
        (str(capture_id), *(item[column] for column in columns)) for item in values
    ]

    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            inserted = execute_values(
                cursor,
                """
                INSERT INTO silver_pep.observation_revision (
                    capture_id, source_row_index, source_column_index,
                    source_header, dataset_code, release_vintage, product_code,
                    observation_year, metric_code, unit, summary_level,
                    region_code_source, division_code_source, state_fips_source,
                    county_fips_source, place_fips_source,
                    county_subdivision_source, consolidated_city_source,
                    functional_status_source, name_source, state_name_source,
                    value_source, value, value_status
                ) VALUES %s
                ON CONFLICT (capture_id, source_row_index, source_column_index)
                DO NOTHING
                RETURNING 1
                """,
                records,
                page_size=5000,
                fetch=True,
            )
        database_connection.commit()
    except BaseException:
        database_connection.rollback()
        raise
    finally:
        database_connection.close()

    return len(inserted)
