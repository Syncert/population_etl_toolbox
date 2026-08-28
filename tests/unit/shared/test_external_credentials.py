"""Scheduled external-contract credential policy."""

from __future__ import annotations

import pytest

from tests.support.external import validate_scheduled_credentials

pytestmark = pytest.mark.unit


def test_scheduled_external_credentials_accept_all_configured_keys() -> None:
    """Covers: EXT-006 — scheduled evidence accepts all required credentials."""
    validate_scheduled_credentials(
        {
            "CENSUS_API_KEY": "census-secret",
            "BLS_API_KEY": "bls-secret",
            "FRED_API_KEY": "fred-secret",
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
            }
        )

    assert str(error.value) == (
        "missing required scheduled external credentials: BLS_API_KEY"
    )
    assert "census-secret" not in str(error.value)
    assert "fred-secret" not in str(error.value)
