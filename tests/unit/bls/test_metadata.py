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


def _refused(status: int):
    import requests

    response = requests.Response()
    response.status_code = status

    def refuse(_program: str):
        raise requests.HTTPError(f"{status} Forbidden", response=response)

    return refuse


def test_a_series_list_refused_for_now_keeps_the_one_stored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-124 — a 403 from download.bls.gov keeps the stored list."""
    monkeypatch.setattr(metadata, "sync_bls_series_metadata", _refused(403))
    monkeypatch.setattr(metadata, "_stored_series_count", lambda program: 812)
    assert metadata.sync_or_keep_bls_series_metadata("la") == 812


def test_a_series_list_refused_before_any_was_stored_fails_the_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-124 — a program never synced is never planned without its series."""
    monkeypatch.setattr(metadata, "sync_bls_series_metadata", _refused(403))
    monkeypatch.setattr(metadata, "_stored_series_count", lambda program: 0)
    with pytest.raises(RuntimeError, match="none is stored yet; retry the run later"):
        metadata.sync_or_keep_bls_series_metadata("cx")


def test_a_list_that_is_not_refused_but_wrong_still_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-124 — only a refusal for now falls back; a 404 or a bad list fails."""
    monkeypatch.setattr(metadata, "sync_bls_series_metadata", _refused(404))
    monkeypatch.setattr(
        metadata,
        "_stored_series_count",
        lambda program: pytest.fail("a 404 must not fall back to the stored list"),
    )
    with pytest.raises(Exception, match="404"):
        metadata.sync_or_keep_bls_series_metadata("la")

    def empty(_program: str):
        raise RuntimeError("BLS returned no series metadata")

    monkeypatch.setattr(metadata, "sync_bls_series_metadata", empty)
    with pytest.raises(RuntimeError, match="no series metadata"):
        metadata.sync_or_keep_bls_series_metadata("la")


def test_every_download_bls_gov_client_names_the_project_not_a_placeholder() -> None:
    """Covers: ETL-124 — the metadata and provider-area clients send one honest User-Agent."""
    from data_ingestion_toolbox.silver_ref.provider_areas import BLS_HEADERS

    assert BLS_HEADERS["User-Agent"] == metadata.UA
    assert "example.com" not in metadata.UA
