"""Isolated live contract checks for the registered SOI county and state income files.

One file of each layout: the newest county CSVs and state file, the 2010
zipped CSVs, and the oldest and newest per-state workbook zips. Each must
answer at its registered path and parse whole, with every state's total
and its band scheme as registered. They take no credential.

Covers: EXT-032
"""

from __future__ import annotations

import logging

import httpx
import pytest

from data_ingestion_toolbox.irs_soi.client import (
    IrsSoiFetchError,
    IrsSoiPayloadError,
    fetch_file,
)
from data_ingestion_toolbox.irs_soi.config import IrsSoiConfig
from data_ingestion_toolbox.irs_soi.registry import (
    ALL_RETURNS,
    COUNTY_AGI,
    band_scheme,
    get_file,
)
from data_ingestion_toolbox.irs_soi.silver_irs_soi.parse import parse_file
from tests.support.external import classify_external_failure, observe_external_call

pytestmark = [pytest.mark.external, pytest.mark.slow]

LOGGER = logging.getLogger(__name__)


@pytest.mark.parametrize(
    ("key", "min_rows"),
    [
        ("county_total:2023", 3000),
        ("county_agi:2023", 20000),
        ("state:2023", 500),
        ("county_total:2010", 3000),
        ("county_total:2009", 3000),
        ("county_total:1989", 3000),
    ],
)
def test_each_registered_layout_answers_and_parses_whole(
    key: str, min_rows: int
) -> None:
    """Covers: EXT-032 — path, layout and band scheme answer as registered."""
    item = get_file(key)
    config = IrsSoiConfig(min_spacing_seconds=1.0, max_attempts=2)
    response, _result = observe_external_call(
        f"irs_soi:{item.path}",
        lambda: fetch_file(item, config=config),
        logger=LOGGER,
    )
    parsed = parse_file(item, response.raw_bytes)
    assert not parsed.rejected, parsed.quarantined[:3]
    assert parsed.row_count >= min_rows
    # SOI's own rows: a handful of duplicates or blanks, never a tenth.
    assert len(parsed.quarantined) < parsed.row_count / 10
    states = {obs.geo_id for obs in parsed.observations if obs.geo_type == "state"}
    assert len(states) >= 51
    bands = {obs.agi_band for obs in parsed.observations}
    expected = set(band_scheme(item.kind, item.tax_year).values())
    if item.kind == COUNTY_AGI:
        expected -= {ALL_RETURNS}
    assert bands == expected


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            IrsSoiFetchError("/23incyallnoagi.csv", code="retry_exhausted", status=503),
            "upstream-unavailable",
        ),
        (httpx.ConnectTimeout("timed out"), "upstream-unavailable"),
        (
            IrsSoiFetchError(
                "/23incyallnoagi.csv", code="non_retryable_http", status=404
            ),
            "contract-regression",
        ),
        (
            IrsSoiPayloadError("/23incyallnoagi.csv", code="not_a_csv"),
            "contract-regression",
        ),
    ],
)
def test_outages_classify_apart_from_contract_changes(
    error: BaseException, expected: str
) -> None:
    """Covers: EXT-032 — an outage is not reported as a contract regression."""
    assert classify_external_failure(error) == expected
