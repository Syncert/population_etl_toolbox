"""Versioned USDA NASS product registry and slice-generation contracts."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.usda_nass.registry import (
    ALL_PRODUCTS,
    QUICK_STATS_FIELDS,
    SUPPORTED_AGG_LEVELS,
    SUPPRESSION_SYMBOLS,
    NassProduct,
    NassSlice,
    NassStatistic,
    enabled_products,
    get_product,
    iter_slices,
    slice_query_parameters,
)

pytestmark = pytest.mark.unit


def test_registry_identity_and_ordering_are_stable() -> None:
    """Covers: ETL-026 — USDA NASS registry identity and ordering are stable."""
    identifiers = [product.product_id for product in ALL_PRODUCTS]
    assert identifiers == [
        "corn_survey_annual",
        "soybeans_survey_annual",
        "wheat_survey_annual",
        "hay_survey_annual",
        "corn_census_county",
    ]
    assert len(identifiers) == len(set(identifiers))
    assert [product.product_id for product in enabled_products()] == identifiers


def test_enabled_products_are_complete_contracts() -> None:
    """Covers: ETL-030, EXT-004 — enabled products are complete contracts."""
    for product in enabled_products():
        assert product.label.strip()
        assert product.source_desc in {"SURVEY", "CENSUS"}
        assert product.sector_desc == "CROPS"
        assert product.group_desc
        assert product.commodity_desc
        assert product.statistics
        assert set(product.agg_level_descs) <= set(SUPPORTED_AGG_LEVELS)
        assert product.freq_descs
        assert product.domain_desc
        assert product.year_start <= product.year_end
        assert product.partition_fields == ("agg_level_desc", "year")
        assert product.parser_contract_version.startswith("quickstats-crop")
        assert product.incremental_field == "load_time"
        assert product.release_expectation
        assert product.methodology_url.startswith("https://www.nass.usda.gov/")
        assert product.expected_units
        assert product.suppression_symbols == SUPPRESSION_SYMBOLS


def test_survey_and_census_products_never_share_a_parser_contract() -> None:
    """Covers: ETL-024 — survey and census products remain distinct."""
    survey = {
        product.parser_contract_version
        for product in ALL_PRODUCTS
        if product.source_desc == "SURVEY"
    }
    census = {
        product.parser_contract_version
        for product in ALL_PRODUCTS
        if product.source_desc == "CENSUS"
    }
    assert survey and census
    assert survey.isdisjoint(census)


def test_statistic_semantics_declare_additive_behavior_honestly() -> None:
    """Covers: ETL-024 — additive behavior is declared, never assumed."""
    for product in ALL_PRODUCTS:
        for statistic in product.statistics:
            assert statistic.expected_units
            assert statistic.value_kind in {"area", "rate", "quantity"}
            assert statistic.calculation_basis
            if statistic.value_kind == "rate":
                assert statistic.additive_behavior == "non_additive"
                assert statistic.additive_behavior_known is True
            else:
                assert statistic.additive_behavior == "not_established"
                assert statistic.additive_behavior_known is False


def test_unknown_identities_cannot_become_ingestible() -> None:
    """Covers: ETL-030 — unknown USDA NASS identities cannot be ingested."""
    with pytest.raises(KeyError, match="unknown USDA NASS product"):
        get_product("barley_survey_annual")


def test_registry_rejects_an_unsupported_aggregate_level() -> None:
    """Covers: ETL-030 — a registry entry cannot select an unmodeled level."""
    with pytest.raises(ValueError, match="unsupported aggregate levels"):
        NassProduct(
            product_id="invalid",
            label="invalid",
            source_desc="SURVEY",
            sector_desc="CROPS",
            group_desc="FIELD CROPS",
            commodity_desc="CORN",
            statistics=(
                NassStatistic("YIELD", ("BU / ACRE",), "rate", "basis", "non_additive"),
            ),
            agg_level_descs=("AGRICULTURAL DISTRICT",),
            freq_descs=("ANNUAL",),
            domain_desc="TOTAL",
            year_start=2024,
            year_end=2024,
            recent_year_window=1,
            partition_fields=("agg_level_desc", "year"),
            parser_contract_version="quickstats-crop-v1",
            incremental_field="load_time",
            release_expectation="x",
            methodology_url="https://www.nass.usda.gov/",
        )


def test_slices_are_deterministic_bounded_and_mode_aware() -> None:
    """Covers: ETL-020 — slices are deterministic, bounded, and mode-aware."""
    product = get_product("corn_survey_annual")
    full = iter_slices(product, mode="full")
    recent = iter_slices(product, mode="recent")

    assert full == iter_slices(product, mode="full")
    assert len(full) == len(product.agg_level_descs) * len(product.years("full"))
    assert len(recent) == len(product.agg_level_descs)
    assert set(recent) < set(full)
    assert [item.slice_key for item in full] == sorted(
        [item.slice_key for item in full],
        key=lambda key: (
            product.agg_level_descs.index(key.split("|")[1]),
            int(key.split("|")[2]),
        ),
    )
    assert len({item.slice_key for item in full}) == len(full)


def test_unknown_slice_mode_is_rejected() -> None:
    """Covers: ETL-020 — an unknown slice mode cannot silently widen scope."""
    with pytest.raises(ValueError, match="unknown slice mode"):
        get_product("corn_survey_annual").years("everything")


def test_slice_parameters_are_registered_and_credential_free() -> None:
    """Covers: ETL-020, ETL-038 — slice parameters never carry a credential."""
    product = get_product("corn_survey_annual")
    item = NassSlice(product.product_id, "COUNTY", product.year_end)
    parameters = slice_query_parameters(product, item)

    assert parameters["source_desc"] == "SURVEY"
    assert parameters["commodity_desc"] == "CORN"
    assert parameters["agg_level_desc"] == "COUNTY"
    assert parameters["year"] == str(product.year_end)
    assert parameters["domain_desc"] == "TOTAL"
    assert parameters["statisticcat_desc"] == list(product.statisticcat_descs)
    assert parameters["format"] == "JSON"
    assert not any(name.lower() == "key" for name in parameters)


def test_slice_parameters_refuse_an_unregistered_partition() -> None:
    """Covers: ETL-020 — an unregistered partition cannot become a request."""
    product = get_product("corn_survey_annual")
    with pytest.raises(ValueError, match="outside the registered range"):
        slice_query_parameters(product, NassSlice(product.product_id, "NATIONAL", 1989))
    with pytest.raises(ValueError, match="is not registered for product"):
        slice_query_parameters(
            product,
            NassSlice(product.product_id, "AGRICULTURAL DISTRICT", product.year_end),
        )
    with pytest.raises(ValueError, match="does not belong"):
        slice_query_parameters(
            product, NassSlice("hay_survey_annual", "NATIONAL", product.year_end)
        )


def test_consumed_field_contract_is_frozen_and_unique() -> None:
    """Covers: EXT-004 — the consumed Quick Stats field contract is frozen."""
    assert len(QUICK_STATS_FIELDS) == len(set(QUICK_STATS_FIELDS))
    for required in (
        "short_desc",
        "domain_desc",
        "domaincat_desc",
        "agg_level_desc",
        "state_fips_code",
        "county_ansi",
        "location_desc",
        "reference_period_desc",
        "week_ending",
        "load_time",
        "Value",
        "CV (%)",
    ):
        assert required in QUICK_STATS_FIELDS
