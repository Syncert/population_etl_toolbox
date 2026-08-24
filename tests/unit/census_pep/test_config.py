"""Census PEP adapter configuration contracts."""

from __future__ import annotations

import os
import pytest

from data_ingestion_toolbox.census_pep import config

pytestmark = pytest.mark.unit


def test_config_has_curated_datasets() -> None:
    """Covers: PEP-001.2 — default config includes curated datasets."""
    assert len(config.CONFIG.datasets) > 0
    assert "pepprst2020" in config.CONFIG.datasets
    assert "pecp2020" in config.CONFIG.datasets
    assert "gvc2020" in config.CONFIG.datasets


def test_config_pepdataset_immutable() -> None:
    """Covers: PEP-001.2 — PEPDataset is immutable after construction."""
    ds = config.PEPDataset(
        code="test_ds",
        title="Test Dataset",
        release_status="active",
    )
    with pytest.raises(AttributeError, match="immutable"):
        ds.code = "changed"  # type: ignore[attr-defined]


def test_config_pepdataset_hash_equality() -> None:
    """Covers: PEP-001.2 — PEPDataset hash is based on code."""
    ds1 = config.PEPDataset(code="dup", title="Dup")
    ds2 = config.PEPDataset(code="dup", title="Dup different title")
    assert hash(ds1) == hash(ds2)
    assert ds1 == ds2


def test_config_with_api_key_returns_new_instance() -> None:
    """Covers: PEP-001.2 — with_api_key returns a new config with key set."""
    new_config = config.CONFIG.with_api_key("test-key-123")
    assert new_config.has_api_key
    assert new_config.get_api_key() == "test-key-123"


def test_config_get_api_key_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Covers: PEP-001.2 — get_api_key falls back to environment variable."""
    monkeypatch.setenv(config.CENSUS_API_KEY_ENV, "env-key-456")
    env_config = config.PEPConfig()
    assert env_config.has_api_key
    assert env_config.get_api_key() == "env-key-456"


def test_config_get_api_key_raises_without_key() -> None:
    """Covers: PEP-001.2 — get_api_key raises when no key available."""
    # Ensure the env var is not set
    env_key = config.CENSUS_API_KEY_ENV
    if env_key in os.environ:
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.delenv(env_key, raising=False)

    no_key_config = config.PEPConfig(_api_key=None)
    assert not no_key_config.has_api_key
    with pytest.raises(ValueError, match=env_key):
        no_key_config.get_api_key()


def test_config_frozen_dataclass() -> None:
    """Covers: PEP-001.2 — PEPConfig is a frozen dataclass."""
    cfg = config.PEPConfig(request_timeout=60.0, max_concurrency=8)
    with pytest.raises(Exception):  # dataclasses.FrozenInstanceError
        cfg.request_timeout = 10.0  # type: ignore[misc]


def test_config_default_values() -> None:
    """Covers: PEP-001.2 — PEPConfig has sensible defaults."""
    cfg = config.PEPConfig()
    assert cfg.request_timeout == 30.0
    assert cfg.max_concurrency == 4
    assert cfg.airflow_pool == "census_pep"


def test_config_curated_dataset_geography_levels() -> None:
    """Covers: PEP-001.2 — curated datasets have correct geography levels."""
    pepprst = config.CONFIG.datasets["pepprst2020"]
    assert "national" in pepprst.geography_levels
    assert "state" in pepprst.geography_levels

    pecp = config.CONFIG.datasets["pecp2020"]
    assert "county" in pecp.geography_levels
    assert "state" in pecp.geography_levels

    gvc = config.CONFIG.datasets["gvc2020"]
    assert "place" in gvc.geography_levels
    assert "state" in gvc.geography_levels


def test_config_curated_dataset_variables() -> None:
    """Covers: PEP-001.2 — curated datasets include population variables."""
    pepprst = config.CONFIG.datasets["pepprst2020"]
    expected_vars = {
        "POPULATION",
        "BIRTHS",
        "DEATHS",
        "NATURAL_INCREASE",
        "DOMESTIC_MIGRATION",
        "INTERNATIONAL_MIGRATION",
        "TOTAL_CHANGE",
    }
    assert expected_vars.issubset(pepprst.variables)


def test_config_freezes_official_current_bulk_products() -> None:
    """Covers: ETL-030 — PEP scope uses official Vintage 2025 bulk products."""
    assert set(config.CONFIG.datasets) == {
        "pep_nst_alldata",
        "pep_county_alldata",
        "pep_subcounty",
    }
    for dataset in config.CONFIG.datasets.values():
        assert dataset.transport == "bulk_csv"
        assert dataset.parser_version == "census-pep-bulk-csv-v1"
        assert dataset.release_page_url.startswith("https://www.census.gov/")


def test_config_has_operational_nonsecret_defaults() -> None:
    """Covers: ETL-030 — PEP runtime defaults are validated without credentials."""
    assert config.CONFIG.postgres_conn_id == "public_data"
    assert config.CONFIG.airflow_pool == "census_api"
    assert config.CONFIG.request_timeout == 60.0
    assert config.CONFIG.max_concurrency == 2


def test_config_separates_current_and_prior_release_vintages() -> None:
    """Covers: ETL-030 — each PEP product retains current and prior releases."""
    release_keys = {
        (release.dataset_code, release.vintage_year)
        for release in config.CONFIG.releases
    }
    assert release_keys == {
        ("pep_nst_alldata", 2024),
        ("pep_nst_alldata", 2025),
        ("pep_county_alldata", 2024),
        ("pep_county_alldata", 2025),
        ("pep_subcounty", 2024),
        ("pep_subcounty", 2025),
    }
    assert all(release.observation_start_year == 2020 for release in config.CONFIG.releases)
    assert all(
        release.observation_end_year == release.vintage_year
        for release in config.CONFIG.releases
    )
