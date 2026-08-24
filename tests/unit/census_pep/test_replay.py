"""Offline replay contracts for captured Census PEP bulk CSV bytes."""

from __future__ import annotations

from pathlib import Path

import pytest

from data_ingestion_toolbox.census_pep.config import CONFIG, PEPRelease
from data_ingestion_toolbox.census_pep.silver_pep.replay import (
    PepCapturePayloadError,
    parse_captured_pep_values,
)

pytestmark = pytest.mark.unit

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "census_pep"


def _release(dataset_code: str, vintage_year: int) -> PEPRelease:
    return next(
        release
        for release in CONFIG.releases
        if release.dataset_code == dataset_code and release.vintage_year == vintage_year
    )


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def test_current_and_prior_replay_keep_distinct_revision_keys() -> None:
    """Covers: ETL-004 — PEP replay separates vintage and observation year."""
    current = parse_captured_pep_values(
        _fixture("nst_2025.csv"),
        release=_release("pep_nst_alldata", 2025),
    )
    prior = parse_captured_pep_values(
        _fixture("nst_2024.csv"),
        release=_release("pep_nst_alldata", 2024),
    )

    current_2024 = next(
        row
        for row in current
        if row["metric_code"] == "POPESTIMATE" and row["observation_year"] == 2024
    )
    prior_2024 = next(
        row
        for row in prior
        if row["metric_code"] == "POPESTIMATE" and row["observation_year"] == 2024
    )

    assert current_2024["release_vintage"] == 2025
    assert prior_2024["release_vintage"] == 2024
    assert current_2024["value_source"] == "340003797"
    assert prior_2024["value_source"] == "340110988"
    assert current_2024["value"] != prior_2024["value"]
    assert current_2024["unit"] == "persons"


def test_subcounty_replay_retains_authoritative_place_codes() -> None:
    """Covers: ETL-004 — PEP replay retains incorporated-place identity."""
    rows = parse_captured_pep_values(
        _fixture("subcounty_2025.csv"),
        release=_release("pep_subcounty", 2025),
    )
    estimate = next(
        row
        for row in rows
        if row["metric_code"] == "POPESTIMATE" and row["observation_year"] == 2025
    )

    assert estimate["summary_level"] == "162"
    assert estimate["state_fips_source"] == "01"
    assert estimate["place_fips_source"] == "00124"
    assert estimate["name_source"] == "Abbeville city"
    assert estimate["value_source"] == "2378"


def test_rate_metrics_have_explicit_rate_unit() -> None:
    """Covers: ETL-004 — PEP rate fields cannot masquerade as counts."""
    rows = parse_captured_pep_values(
        _fixture("nst_2025.csv"),
        release=_release("pep_nst_alldata", 2025),
    )
    rate = next(
        row
        for row in rows
        if row["metric_code"] == "RNETMIG" and row["observation_year"] == 2025
    )

    assert rate["unit"] == "per_1000_population"
    assert str(rate["value"]) == "3.7026195511"


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"", "empty"),
        (b"SUMLEV,SUMLEV,POPESTIMATE2025\n010,010,1\n", "duplicate"),
        (b"SUMLEV,NAME,POPESTIMATE2025\n010,United States,1,extra\n", "row length"),
        (
            b"SUMLEV,REGION,DIVISION,STATE,NAME\n010,0,0,00,United States\n",
            "metric column",
        ),
    ],
)
def test_malformed_bulk_csv_is_rejected(payload: bytes, message: str) -> None:
    """Covers: ETL-005 — Malformed PEP CSV fails deterministically."""
    with pytest.raises(PepCapturePayloadError, match=message):
        parse_captured_pep_values(
            payload,
            release=_release("pep_nst_alldata", 2025),
        )


def test_sentinel_and_invalid_values_are_not_coerced_to_zero() -> None:
    """Covers: ETL-006 — PEP missing and invalid values retain status."""
    payload = (
        b"SUMLEV,REGION,DIVISION,STATE,NAME,POPESTIMATE2025,BIRTHS2025\n"
        b"010,0,0,00,United States,-999999999,not-a-number\n"
    )

    rows = parse_captured_pep_values(
        payload,
        release=_release("pep_nst_alldata", 2025),
    )
    by_metric = {row["metric_code"]: row for row in rows}

    assert by_metric["POPESTIMATE"]["value"] is None
    assert by_metric["POPESTIMATE"]["value_status"] == "sentinel"
    assert by_metric["BIRTHS"]["value"] is None
    assert by_metric["BIRTHS"]["value_status"] == "invalid"
