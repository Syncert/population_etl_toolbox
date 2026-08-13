"""Cross-source ingestion configuration validation contracts."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from data_ingestion_toolbox.bls.config import BlsConfig
from data_ingestion_toolbox.census_acs.config import AcsConfig
from data_ingestion_toolbox.fred.config import FredConfig

pytestmark = pytest.mark.unit


def test_source_configuration_defaults_are_valid() -> None:
    """Covers: ETL-030 — all source configuration defaults validate."""
    assert AcsConfig().postgres_conn_id
    assert BlsConfig().programs
    assert FredConfig().configured_series_by_domain()


@pytest.mark.parametrize("config_type", [AcsConfig, BlsConfig, FredConfig])
def test_missing_postgres_connection_id_is_rejected(config_type: type) -> None:
    """Covers: ETL-030 — every source rejects an empty connection ID."""
    with pytest.raises(ValidationError, match="postgres_conn_id must not be empty"):
        config_type(postgres_conn_id="   ")


@pytest.mark.parametrize(
    ("config_type", "field_name"),
    [
        (AcsConfig, "datasets"),
        (AcsConfig, "curated_tables"),
        (BlsConfig, "programs"),
        (BlsConfig, "curated_by_program"),
        (FredConfig, "domains"),
        (FredConfig, "curated_series_ids"),
        (FredConfig, "curated_by_domain"),
    ],
)
def test_empty_configured_scope_is_rejected(config_type: type, field_name: str) -> None:
    """Covers: ETL-030 — empty configured ingestion scopes are rejected."""
    with pytest.raises(ValidationError, match="scope must not be empty"):
        config_type(
            **{
                field_name: []
                if field_name != "curated_by_program"
                and field_name != "curated_by_domain"
                else {}
            }
        )


def test_duplicate_fred_domain_ownership_is_rejected() -> None:
    """Covers: ETL-030 — duplicate FRED series ownership is rejected."""
    config = FredConfig(
        domains=["one", "two"],
        curated_series_ids=["SERIES"],
        curated_by_domain={"one": ["SERIES"], "two": ["SERIES"]},
    )
    with pytest.raises(ValueError, match="exactly one domain"):
        config.configured_series_by_domain()


@pytest.mark.parametrize(
    ("config_type", "field_name"),
    [
        (AcsConfig, "census_api_global_concurrency"),
        (BlsConfig, "bls_api_series_chunk_size"),
        (BlsConfig, "bls_api_year_chunk_size"),
        (FredConfig, "fred_api_series_chunk_size"),
    ],
)
def test_invalid_batch_and_concurrency_sizes_are_rejected(
    config_type: type, field_name: str
) -> None:
    """Covers: ETL-030 — nonpositive batch and concurrency sizes fail."""
    secret = "must-not-appear"
    key_field = {
        AcsConfig: "census_api_key",
        BlsConfig: "bls_api_key",
        FredConfig: "fred_api_key",
    }[config_type]
    with pytest.raises(ValidationError) as caught:
        config_type(**{field_name: 0, key_field: secret})
    assert secret not in str(caught.value)
