"""Isolated live contract checks for the registered LEHD LODES8 files.

One small state's newest registered year: its version file names the
registered format, its checksum list names every registered file, and the
residence file matches its listed checksum and the columns this adapter
reads. They take no credential.

Covers: EXT-021
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.census_lodes.client import (
    LodesFetchError,
    LodesIntegrityError,
    fetch,
    parse_checksums,
    parse_version,
    verify,
)
from data_ingestion_toolbox.census_lodes.config import LodesConfig
from data_ingestion_toolbox.census_lodes.registry import (
    FORMAT_VERSION,
    YEARS,
    checksum_path,
    files_for,
    version_path,
)
from data_ingestion_toolbox.census_lodes.silver_census_lodes.parse import parse_area
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)
STATE = "de"
NEWEST = YEARS[-1]
CONFIG = LodesConfig(min_spacing_seconds=1.0, max_attempts=2)


def _get(path: str) -> bytes:
    response, _result = observe_external_call(
        f"census_lodes:{path}", lambda: fetch(path, config=CONFIG), logger=LOGGER
    )
    return response.raw_bytes


def test_the_version_checksums_and_residence_file_answer_as_registered() -> None:
    """Covers: EXT-021 — format version, listed files and the verified residence file."""
    _vintage, format_version = parse_version(
        _get(version_path(STATE)).decode("latin-1")
    )
    assert format_version == FORMAT_VERSION
    listed = parse_checksums(_get(checksum_path(STATE)).decode("latin-1"))
    files = files_for(STATE, NEWEST)
    assert {item.name for item in files} <= set(listed)
    residence = files[0]
    raw = _get(residence.path)
    verify(raw, residence.path, expected=listed[residence.name])
    parsed = parse_area(raw, item=residence)
    assert parsed.quarantined == ()
    assert set(parsed.totals) == {"10001", "10003", "10005"}


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            LodesFetchError("/de/version.txt", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            LodesFetchError("/de/version.txt", code="non_retryable_http", status=404),
            "contract-regression",
        ),
        (
            LodesIntegrityError("/de/rac/x.csv.gz", code="checksum_mismatch"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-021 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
