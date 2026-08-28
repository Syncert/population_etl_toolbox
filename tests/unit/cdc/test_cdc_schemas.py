"""Unit tests for CDI source-shaped normalization helpers."""

from __future__ import annotations

from decimal import Decimal

import pytest

from data_ingestion_toolbox.cdc.registry import CDI_ASSET
from data_ingestion_toolbox.cdc.schemas import (
    CDI_COLUMNS,
    CDI_NATURAL_KEY,
    CDI_NUMERIC_COLUMNS,
    CDI_PRIMARY_VALUE_COLUMN,
    cdi_record_has_ci,
    cdi_record_is_suppressed,
    normalize_cdi_record,
)

pytestmark = pytest.mark.unit


def test_schema_constants_are_derived_from_versioned_registry_contract() -> None:
    """Covers: EXT-004 — CDI parser columns cannot drift from the registry."""
    assert CDI_COLUMNS == CDI_ASSET.select_columns
    assert CDI_NATURAL_KEY == CDI_ASSET.source_key
    assert CDI_NUMERIC_COLUMNS == {
        column.name
        for column in CDI_ASSET.expected_columns
        if column.data_type == "number"
    }
    assert CDI_PRIMARY_VALUE_COLUMN == "datavalue"


def test_numeric_values_become_exact_decimals_and_text_is_trimmed() -> None:
    """Covers: ETL-022 — CDI numeric precision and source text are explicit."""
    normalized = normalize_cdi_record(
        {
            "yearstart": "2020",
            "locationabbr": " TX ",
            "datavalue": "25.300",
            "lowconfidencelimit": "24.1",
            "highconfidencelimit": "26.5",
        }
    )

    assert normalized["yearstart"] == Decimal("2020")
    assert normalized["datavalue"] == Decimal("25.300")
    assert normalized["locationabbr"] == "TX"


@pytest.mark.parametrize("missing", [None, "", "  ", "null", "NA", "N/A", "-", "."])
def test_missing_values_become_none_never_zero(missing: object) -> None:
    """Covers: ETL-022 — CDI missing markers never become numeric zero."""
    for column in CDI_NUMERIC_COLUMNS:
        assert normalize_cdi_record({column: missing})[column] is None
    assert normalize_cdi_record({"locationabbr": missing})["locationabbr"] is None


def test_unparseable_numeric_text_is_preserved_for_quarantine() -> None:
    """Covers: RES-002 — invalid CDI numeric text remains inspectable."""
    normalized = normalize_cdi_record({"datavalue": "abc"})

    assert normalized["datavalue"] == "abc"
    assert normalized["datavalue"] != 0


def test_normalization_is_non_mutating_and_preserves_unknown_fields() -> None:
    """Covers: RES-002 — source records remain intact during normalization."""
    sentinel = object()
    record = {"yearstart": " 2020 ", "new_column": sentinel}
    original = dict(record)

    normalized = normalize_cdi_record(record)

    assert record == original
    assert normalized["new_column"] is sentinel
    assert normalized["yearstart"] == Decimal("2020")


@pytest.mark.parametrize("value", [None, "", "NA"])
def test_missing_primary_value_is_classified_as_suppressed_or_missing(
    value: object,
) -> None:
    """Covers: ETL-025 — absent CDI values receive an explicit non-value state."""
    assert cdi_record_is_suppressed({"datavalue": value}) is True


def test_zero_is_a_real_value_not_suppression() -> None:
    """Covers: ETL-022 — a published CDI zero remains a real numeric value."""
    assert cdi_record_is_suppressed({"datavalue": 0}) is False


def test_confidence_interval_requires_both_limits() -> None:
    """Covers: ETL-025 — CDI uncertainty is present only as a complete pair."""
    assert cdi_record_has_ci({"lowconfidencelimit": "1", "highconfidencelimit": "2"})
    assert not cdi_record_has_ci({"lowconfidencelimit": "1"})
    assert not cdi_record_has_ci({"highconfidencelimit": "2"})
