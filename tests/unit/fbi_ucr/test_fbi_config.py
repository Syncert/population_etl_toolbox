"""FBI UCR configuration and credential-handling contracts."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from data_ingestion_toolbox.fbi_ucr.config import (
    API_KEY_ENVIRONMENT_VARIABLE,
    CDE_BASE_PATH,
    CDE_BASE_URL,
    CDE_SERVER_URL,
    CONFIG,
    FbiUcrConfig,
    target_database,
)

pytestmark = pytest.mark.unit


def test_frozen_official_base_url_matches_the_documented_surface() -> None:
    """Covers: ETL-030 — the server and mutable base path stay explicit."""
    assert CDE_SERVER_URL == "https://api.usa.gov/crime/fbi/cde"
    assert CDE_BASE_PATH == "/LATEST"
    assert CDE_BASE_URL == "https://api.usa.gov/crime/fbi/cde/LATEST"


def test_configuration_defaults_validate_without_io() -> None:
    """Covers: ETL-030 — defaults load with no credential and no I/O."""
    assert CONFIG.postgres_conn_id == "public_data"
    assert not CONFIG.has_api_key
    assert target_database() == "public_data"


def test_credential_is_read_only_from_the_named_environment_secret(
    monkeypatch,
) -> None:
    """Covers: ETL-030, EXT-006 — the key comes from its documented name."""
    monkeypatch.delenv(API_KEY_ENVIRONMENT_VARIABLE, raising=False)
    assert not FbiUcrConfig.from_environment().has_api_key

    monkeypatch.setenv(API_KEY_ENVIRONMENT_VARIABLE, "configured-key")
    assert FbiUcrConfig.from_environment().cde_api_key == "configured-key"
    assert API_KEY_ENVIRONMENT_VARIABLE == "FBI_CDE_API_KEY"


def test_empty_postgres_connection_id_is_rejected() -> None:
    """Covers: ETL-030 — an empty warehouse connection ID is rejected."""
    with pytest.raises(ValidationError, match="postgres_conn_id must not be empty"):
        FbiUcrConfig(postgres_conn_id="   ")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_attempts", 0),
        ("silver_max_active_tis", 0),
        ("request_timeout_seconds", 0.0),
        ("min_spacing_seconds", -1.0),
    ],
)
def test_invalid_transport_sizing_is_rejected_without_leaking_the_key(
    field: str, value: object
) -> None:
    """Covers: ETL-030 — invalid sizing fails and never echoes the secret."""
    secret = "must-not-appear"

    with pytest.raises(ValidationError) as caught:
        FbiUcrConfig(**{field: value, "cde_api_key": secret})

    assert secret not in str(caught.value)
