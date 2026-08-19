"""BLS metadata synchronization failure contracts."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.bls import metadata
from data_ingestion_toolbox.bls.config import LAUS_COUNTY_PARENT_FIPS

pytestmark = pytest.mark.unit


def test_laus_county_scope_includes_puerto_rico_and_excludes_unassigned_fips() -> None:
    """Covers: ETL-001 — LAUS planning uses valid county-parent geographies."""
    assert len(LAUS_COUNTY_PARENT_FIPS) == 52
    assert len(set(LAUS_COUNTY_PARENT_FIPS)) == 52
    assert "72" in LAUS_COUNTY_PARENT_FIPS
    assert "52" not in LAUS_COUNTY_PARENT_FIPS


def test_fetch_metadata_propagates_download_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-026 — BLS metadata download failures remain visible."""
    def fail_download(_url: str):
        raise RuntimeError("download failed")

    monkeypatch.setattr(metadata, "read_bls_tsv", fail_download)

    with pytest.raises(RuntimeError, match="download failed"):
        metadata.fetch_bls_metadata("ce")


def test_series_sync_rejects_empty_metadata_before_opening_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-026 — empty BLS metadata cannot produce a green sync."""
    monkeypatch.setattr(metadata, "fetch_bls_metadata", lambda _program: ([], {}))
    monkeypatch.setattr(
        metadata,
        "_get_pg_connection",
        lambda: pytest.fail("database must not be opened for empty metadata"),
    )

    with pytest.raises(RuntimeError, match="no series metadata"):
        metadata.sync_bls_series_metadata("ce")
