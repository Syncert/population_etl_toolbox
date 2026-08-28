"""Scheduled external-contract credential policy."""

from __future__ import annotations

import pytest

from tests.support.external import (
    REQUIRED_SCHEDULED_CREDENTIALS,
    validate_scheduled_credentials,
)

pytestmark = pytest.mark.unit


def test_scheduled_external_credentials_accept_all_configured_keys() -> None:
    """Covers: EXT-006 — scheduled evidence accepts all required credentials."""
    validate_scheduled_credentials(
        {
            "CENSUS_API_KEY": "census-secret",
            "BLS_API_KEY": "bls-secret",
            "FRED_API_KEY": "fred-secret",
            "FBI_CDE_API_KEY": "fbi-secret",
            "USDA_NASS_API_KEY": "nass-secret",
        }
    )


def test_scheduled_external_credentials_name_missing_keys_without_values() -> None:
    """Covers: EXT-006 — scheduled failures name keys without exposing values."""
    with pytest.raises(RuntimeError) as error:
        validate_scheduled_credentials(
            {
                "CENSUS_API_KEY": "census-secret",
                "BLS_API_KEY": " ",
                "FRED_API_KEY": "fred-secret",
                "FBI_CDE_API_KEY": "fbi-secret",
                "USDA_NASS_API_KEY": "nass-secret",
            }
        )

    assert str(error.value) == (
        "missing required scheduled external credentials: BLS_API_KEY"
    )
    assert "census-secret" not in str(error.value)
    assert "fred-secret" not in str(error.value)


def test_scheduled_external_credentials_cover_every_credentialed_source() -> None:
    """Covers: EXT-006 — no credentialed source is left out of the tier.

    A source whose key is missing from this tuple would skip silently in the
    scheduled run instead of failing it, which is exactly how a source drops
    out of live contract coverage without anyone noticing.
    """
    assert set(REQUIRED_SCHEDULED_CREDENTIALS) == {
        "CENSUS_API_KEY",
        "BLS_API_KEY",
        "FRED_API_KEY",
        "FBI_CDE_API_KEY",
        "USDA_NASS_API_KEY",
    }
