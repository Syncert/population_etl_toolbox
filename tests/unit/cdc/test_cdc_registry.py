"""Unit tests for versioned CDC source registry contracts."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.cdc.registry import (
    ALL_ASSETS,
    CDI_ASSET,
    PLACES_COUNTY_ASSET,
    CdcAsset,
    asset_by_socrata_id,
    enabled_assets,
    get_asset,
)

pytestmark = pytest.mark.unit


def test_registry_enables_both_official_verified_assets() -> None:
    """Covers: EXT-004 — CDC registry contains exact current provider IDs."""
    assert [(asset.asset_id, asset.socrata_id) for asset in enabled_assets()] == [
        ("cdi", "hksd-2xuw"),
        ("places_county", "swc5-untb"),
    ]
    assert all(asset.enabled for asset in ALL_ASSETS)


def test_assets_freeze_complete_non_placeholder_contracts() -> None:
    """Covers: ETL-030, EXT-004 — enabled CDC assets are complete contracts."""
    for asset in enabled_assets():
        assert asset.parser_contract_version
        assert asset.api_path == f"/resource/{asset.socrata_id}.json"
        assert asset.metadata_path == f"/api/views/{asset.socrata_id}"
        assert asset.media_type == "application/json"
        assert asset.expected_columns
        assert asset.stable_order
        assert set(asset.source_key) <= set(asset.select_columns)
        assert set(asset.stable_order) <= set(asset.select_columns)
        assert asset.geography_levels
        assert asset.release_field == "rowsUpdatedAt"
        assert asset.update_cadence
        assert asset.methodology_url.startswith("https://www.cdc.gov/")
        assert "placeholder" not in repr(asset).lower()


def test_products_keep_distinct_method_and_geography_semantics() -> None:
    """Covers: ETL-024 — CDI and PLACES remain semantically distinct."""
    assert CDI_ASSET.geography_levels == ("us", "state")
    assert CDI_ASSET.estimate_method == "provider_published"
    assert PLACES_COUNTY_ASSET.geography_levels == ("county",)
    assert PLACES_COUNTY_ASSET.estimate_method == "model_based_small_area_estimate"
    assert "2020 Census" in PLACES_COUNTY_ASSET.geography_basis
    assert CDI_ASSET.source_key != PLACES_COUNTY_ASSET.source_key


def test_assets_are_immutable_and_identities_are_unique() -> None:
    """Covers: ETL-026 — CDC registry identity and ordering are stable."""
    with pytest.raises(Exception):
        CDI_ASSET.label = "mutated"  # type: ignore[misc]
    assert len({asset.asset_id for asset in ALL_ASSETS}) == len(ALL_ASSETS)
    assert len({asset.socrata_id for asset in ALL_ASSETS}) == len(ALL_ASSETS)


def test_registry_lookups_return_exact_assets() -> None:
    """Covers: ETL-030 — CDC registry lookups do not infer identities."""
    assert get_asset("cdi") is CDI_ASSET
    assert get_asset("places_county") is PLACES_COUNTY_ASSET
    assert asset_by_socrata_id("hksd-2xuw") is CDI_ASSET
    assert asset_by_socrata_id("swc5-untb") is PLACES_COUNTY_ASSET
    assert isinstance(get_asset("cdi"), CdcAsset)


def test_unknown_registry_identities_fail_without_fallback() -> None:
    """Covers: ETL-030 — unknown CDC identities cannot become ingestible."""
    with pytest.raises(KeyError, match="unknown CDC asset"):
        get_asset("does-not-exist")
    assert asset_by_socrata_id("zzzz-9999") is None
