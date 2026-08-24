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


def _parse_document(payload: bytes) -> tuple[list[str], list[list[str]]]:
    if not payload:
        raise PepCapturePayloadError("PEP capture is empty")
    try:
        source = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise PepCapturePayloadError("PEP capture is not valid UTF-8 CSV") from exc
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
    header, records = _parse_document(payload)
    metrics = _metric_columns(header, release)

    parsed: list[dict[str, Any]] = []
    for row_index, record in enumerate(records):
        source_row = dict(zip(header, record))
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
                    "summary_level": source_row.get("SUMLEV"),
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


def replay_pep_capture(
    connection_factory: Callable[[], Any],
    *,
    capture_id: UUID,
    release: PEPRelease,
) -> int:
    """Replay one stored PEP release into its capture-scoped silver revision."""
    values = parse_captured_pep_values(
        load_captured_payload(connection_factory, capture_id),
        release=release,
    )
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
            execute_values(
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
