"""Covers: ETL-122 — the scheduled Return A load's registry, gate and refusals.

The registry periods are the data years 2015 through last year; a year is
read again only when its bytes, its parser contract or the county reference
changed since it was loaded; and only the newest year's "not found" means
"not published yet".
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from data_ingestion_toolbox.fbi_ucr.return_a import fetch
from data_ingestion_toolbox.fbi_ucr.return_a.load import (
    OBSERVATION,
    PARSER_VERSION,
    PARTICIPATION,
    RELATIONSHIP,
    CROSSWALK,
    _LedgerYear,
    unchanged,
)
from data_ingestion_toolbox.fbi_ucr.return_a.registry import (
    FIRST_YEAR,
    registered_years,
)

pytestmark = pytest.mark.unit

LOADED = datetime(2026, 10, 31, 12, tzinfo=UTC)
CHECKSUM = "a" * 64


def test_the_registry_periods_are_every_year_from_2015_through_last_year() -> None:
    """Covers: ETL-122 — the registry periods are 2015 through last year."""
    assert FIRST_YEAR == 2015
    assert registered_years(2026) == list(range(2025, 2014, -1))
    assert registered_years(2027)[:2] == [2026, 2025]
    assert registered_years(2027)[-1] == 2015
    assert registered_years()[0] == datetime.now(UTC).year - 1


def test_a_year_is_unchanged_only_with_the_same_bytes_contract_and_counties() -> None:
    """Covers: ETL-122 — the repeats gate, narrowed to counties."""
    ledger = _LedgerYear(CHECKSUM, PARSER_VERSION, LOADED)
    assert unchanged(ledger, CHECKSUM, LOADED - timedelta(days=30))
    assert unchanged(ledger, CHECKSUM, None)
    # Never loaded: always read.
    assert not unchanged(None, CHECKSUM, None)
    # A revised file.
    assert not unchanged(ledger, "b" * 64, None)
    # Loaded under another parser contract.
    assert not unchanged(
        _LedgerYear(CHECKSUM, "fbi-return-a-v0", LOADED), CHECKSUM, None
    )
    # A county added since could resolve a code the load left unresolved.
    assert not unchanged(ledger, CHECKSUM, LOADED + timedelta(seconds=1))


def test_lineage_is_never_what_makes_a_row_differ() -> None:
    """Covers: ETL-122 — an identical row read from another capture or line is not rewritten."""
    for table in (PARTICIPATION, OBSERVATION):
        assert set(table.lineage) == {
            "source_run_id",
            "capture_id",
            "source_row_index",
            "transformation_version",
        }
        assert not set(table.lineage) & set(table.compare)
        assert table.names_line
        assert "release_key" in table.key
    assert RELATIONSHIP.lineage == ("evidence_capture_id",)
    assert "evidence_capture_id" not in RELATIONSHIP.compare
    assert not CROSSWALK.names_line
    for table in (CROSSWALK, RELATIONSHIP, PARTICIPATION, OBSERVATION):
        assert not set(table.key) & set(table.compare)


class _Client:
    """Stands in for ``httpx.Client``: the signed URL, then the file's answer."""

    def __init__(self, status: int) -> None:
        self.status = status

    def get(self, url: str, **_keywords: object) -> httpx.Response:
        if url == fetch.SIGNED_URL_ENDPOINT:
            return httpx.Response(200, text="https://example.test/reta-2026.zip")
        return httpx.Response(self.status, text="<Error>NoSuchKey</Error>")


def _client(status: int) -> httpx.Client:
    return _Client(status)  # type: ignore[return-value]


@pytest.mark.parametrize("status", [403, 404])
def test_a_missing_file_is_refused_once_with_its_status(status: int) -> None:
    """Covers: ETL-122 — a missing file's refusal carries its status, no URL."""
    with pytest.raises(fetch.ReturnAFetchError) as caught:
        fetch.fetch_year(2026, client=_client(status))
    assert caught.value.status_code == status
    assert status in fetch.NOT_PUBLISHED_STATUSES
    assert "example.test" not in str(caught.value)


def test_a_server_error_is_not_read_as_not_published(monkeypatch) -> None:
    """Covers: ETL-122 — a 5xx is a failure, never "not published"."""
    monkeypatch.setattr(fetch, "BACKOFF_SECONDS", 0.0)
    with pytest.raises(fetch.ReturnAFetchError) as caught:
        fetch.fetch_year(2026, client=_client(503))
    assert caught.value.status_code is None
