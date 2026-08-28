"""Pure CDC Socrata metadata parsing and release-change decisions."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .registry import CdcAsset


class CdcMetadataError(ValueError):
    """A metadata response cannot support a safe release decision."""


class MetadataDecision(StrEnum):
    """Typed outcome of comparing provider metadata with accepted state."""

    UNCHANGED = "unchanged"
    INGEST = "ingest"
    SCHEMA_CHANGE_QUARANTINE = "schema_change_quarantine"
    DATASET_REPLACEMENT_QUARANTINE = "dataset_replacement_quarantine"
    BACKWARD_WATERMARK_QUARANTINE = "backward_watermark_quarantine"


@dataclass(frozen=True)
class CdcMetadata:
    """Allowlisted metadata required to identify and version one asset."""

    socrata_id: str
    title: str
    watermark: int
    columns: tuple[tuple[str, str], ...]
    row_count: int | None = None
    license_id: str | None = None

    @property
    def release_version(self) -> str:
        return str(self.watermark)


def _column_contract(value: object) -> dict[str, str]:
    if not isinstance(value, list):
        raise CdcMetadataError("CDC metadata columns must be a list")
    columns: dict[str, str] = {}
    for item in value:
        if isinstance(item, list) and len(item) == 2:
            name, data_type = item
        elif isinstance(item, dict):
            name = item.get("fieldName")
            data_type = item.get("dataTypeName")
        else:
            raise CdcMetadataError("CDC metadata contains an invalid column")
        if not isinstance(name, str) or not isinstance(data_type, str):
            raise CdcMetadataError("CDC metadata column identity is invalid")
        if name in columns:
            raise CdcMetadataError("CDC metadata contains a duplicate column")
        columns[name] = data_type
    return columns


def parse_metadata(payload: bytes, asset: CdcAsset) -> CdcMetadata:
    """Parse only fields required for contract and release comparison."""
    try:
        source = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CdcMetadataError("CDC metadata is not valid JSON") from exc
    if not isinstance(source, dict):
        raise CdcMetadataError("CDC metadata must be a JSON object")
    socrata_id = source.get("id")
    title = source.get("name")
    watermark = source.get(asset.release_field)
    if not isinstance(socrata_id, str) or not isinstance(title, str):
        raise CdcMetadataError("CDC metadata identity fields are missing")
    if not isinstance(watermark, int) or watermark < 0:
        raise CdcMetadataError("CDC metadata watermark is invalid")
    available = _column_contract(source.get("columns"))
    consumed = tuple(
        (column.name, available[column.name])
        for column in asset.expected_columns
        if column.name in available
    )
    row_count = source.get("rowsCount")
    if row_count is not None and (not isinstance(row_count, int) or row_count < 0):
        raise CdcMetadataError("CDC metadata row count is invalid")
    license_id = source.get("licenseId")
    return CdcMetadata(
        socrata_id=socrata_id,
        title=title,
        watermark=watermark,
        columns=consumed,
        row_count=row_count,
        license_id=license_id if isinstance(license_id, str) else None,
    )


def decide_metadata(
    asset: CdcAsset,
    current: CdcMetadata,
    previous: CdcMetadata | None,
) -> MetadataDecision:
    """Return the safe next action without performing I/O."""
    if current.socrata_id != asset.socrata_id:
        return MetadataDecision.DATASET_REPLACEMENT_QUARANTINE
    expected = tuple(
        (column.name, column.data_type) for column in asset.expected_columns
    )
    if current.columns != expected:
        return MetadataDecision.SCHEMA_CHANGE_QUARANTINE
    if previous is None:
        return MetadataDecision.INGEST
    if current.socrata_id != previous.socrata_id:
        return MetadataDecision.DATASET_REPLACEMENT_QUARANTINE
    if current.watermark < previous.watermark:
        return MetadataDecision.BACKWARD_WATERMARK_QUARANTINE
    if current.watermark == previous.watermark:
        return MetadataDecision.UNCHANGED
    return MetadataDecision.INGEST


def load_latest_accepted_metadata(
    connection_factory: Callable[[], Any],
    asset: CdcAsset,
) -> CdcMetadata | None:
    """Load the latest safe metadata contract for release comparison."""
    database_connection = connection_factory()
    try:
        with database_connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT socrata_id, title, release_watermark, schema_contract,
                       provider_row_count, license_id
                FROM control.cdc_dataset_release
                WHERE asset_id = %s
                  AND decision IN ('ingest', 'unchanged')
                  AND status IN ('captured', 'silver_ready', 'published')
                ORDER BY release_watermark DESC, created_at DESC
                LIMIT 1
                """,
                (asset.asset_id,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return CdcMetadata(
            socrata_id=row[0],
            title=row[1],
            watermark=int(row[2]),
            columns=tuple(tuple(item) for item in row[3]),
            row_count=row[4],
            license_id=row[5],
        )
    finally:
        database_connection.close()
