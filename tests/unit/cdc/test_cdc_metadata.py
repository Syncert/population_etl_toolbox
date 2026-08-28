"""CDC metadata parsing and release-decision contracts."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from data_ingestion_toolbox.cdc.metadata import (
    MetadataDecision,
    decide_metadata,
    parse_metadata,
)
from data_ingestion_toolbox.cdc.registry import CDI_ASSET

pytestmark = pytest.mark.unit

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "cdc"


def _fixture() -> bytes:
    return (FIXTURE_DIR / "cdi_metadata.json").read_bytes()


def test_registered_metadata_is_parsed_without_unconsumed_fields() -> None:
    """Covers: ETL-038 — CDC metadata freezes only consumed contract fields."""
    metadata = parse_metadata(_fixture(), CDI_ASSET)

    assert metadata.socrata_id == "hksd-2xuw"
    assert metadata.watermark == 1780605223
    assert metadata.columns == tuple(
        (column.name, column.data_type) for column in CDI_ASSET.expected_columns
    )


def test_release_decisions_quarantine_unsafe_metadata_changes() -> None:
    """Covers: RES-002 — CDC schema, identity, and watermark drift are typed."""
    current = parse_metadata(_fixture(), CDI_ASSET)

    assert decide_metadata(CDI_ASSET, current, None) is MetadataDecision.INGEST
    assert decide_metadata(CDI_ASSET, current, current) is MetadataDecision.UNCHANGED
    assert (
        decide_metadata(
            CDI_ASSET,
            current,
            replace(current, watermark=current.watermark + 1),
        )
        is MetadataDecision.BACKWARD_WATERMARK_QUARANTINE
    )


def test_missing_consumed_column_is_schema_change_quarantine() -> None:
    """Covers: RES-002 — missing consumed CDC fields cannot reach parsing."""
    source = _fixture().replace(b'["datavalue", "number"],', b"")
    metadata = parse_metadata(source, CDI_ASSET)

    assert (
        decide_metadata(CDI_ASSET, metadata, None)
        is MetadataDecision.SCHEMA_CHANGE_QUARANTINE
    )
