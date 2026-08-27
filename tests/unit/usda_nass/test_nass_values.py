"""USDA NASS value, CV, and suppression parsing contracts."""

from __future__ import annotations

from decimal import Decimal

import pytest

from data_ingestion_toolbox.usda_nass.registry import (
    SUPPRESSION_SYMBOLS,
    get_product,
)
from data_ingestion_toolbox.usda_nass.silver_nass.values import (
    SYMBOL_STATUS,
    VALUE_STATUS_BELOW_ROUNDING_UNIT,
    VALUE_STATUS_MISSING,
    VALUE_STATUS_QUALITY_FLAGGED,
    VALUE_STATUS_VALID,
    VALUE_STATUS_WITHHELD,
    NassValueError,
    parse_slice_rows,
    parse_source_value,
)

from ._doubles import load_fixture

pytestmark = pytest.mark.unit

PRODUCT = get_product("corn_survey_annual")
BOUNDARY = load_fixture("boundary_records")["records"]


def test_every_registered_symbol_has_an_explicit_state() -> None:
    """Covers: ETL-025 — every registered symbol has an explicit state."""
    assert set(SYMBOL_STATUS) == set(SUPPRESSION_SYMBOLS)
    for symbol, status in SYMBOL_STATUS.items():
        parsed = parse_source_value(symbol)
        assert parsed.status == status
        assert parsed.value is None
        assert parsed.symbol == symbol
        assert parsed.source_text == symbol


def test_thousands_separated_and_decimal_values_parse_exactly() -> None:
    """Covers: ETL-022 — numeric precision and source text are both explicit."""
    grouped = parse_source_value("14,867,000,000")
    assert grouped.value == Decimal("14867000000")
    assert grouped.source_text == "14,867,000,000"
    assert grouped.status == VALUE_STATUS_VALID
    assert grouped.symbol is None

    fractional = parse_source_value("179.3")
    assert fractional.value == Decimal("179.3")
    assert str(fractional.value) == "179.3"


def test_a_published_zero_stays_a_real_numeric_value() -> None:
    """Covers: ETL-022 — a published zero remains a real numeric value."""
    zero = parse_source_value(BOUNDARY["zero_is_a_real_value"]["Value"])
    assert zero.value == Decimal("0")
    assert zero.status == VALUE_STATUS_VALID


def test_a_below_rounding_marker_never_becomes_zero() -> None:
    """Covers: ETL-022 — a below-rounding marker never becomes numeric zero."""
    rounded = parse_source_value("(Z)")
    assert rounded.status == VALUE_STATUS_BELOW_ROUNDING_UNIT
    assert rounded.value is None
    assert rounded.status != VALUE_STATUS_VALID
    assert parse_source_value("(Z)").value != Decimal("0")


def test_an_absent_value_receives_an_explicit_non_value_state() -> None:
    """Covers: ETL-025 — an absent value receives an explicit non-value state."""
    for absent in ("", "   ", None):
        parsed = parse_source_value(absent)
        assert parsed.status == VALUE_STATUS_MISSING
        assert parsed.value is None
        assert parsed.symbol is None


def test_an_unregistered_non_numeric_value_is_refused() -> None:
    """Covers: RES-002 — an unregistered non-numeric value is refused."""
    with pytest.raises(NassValueError, match="unregistered non-numeric"):
        parse_source_value(BOUNDARY["unregistered_value_symbol"]["Value"])
    with pytest.raises(NassValueError, match="non-finite"):
        parse_source_value("NaN")


def test_cv_symbols_are_classified_beside_the_value() -> None:
    """Covers: ETL-025 — CV quality markers are preserved, not interpreted."""
    flagged = parse_source_value(BOUNDARY["high_cv_quality_flag"]["CV (%)"])
    assert flagged.status == VALUE_STATUS_QUALITY_FLAGGED
    assert flagged.symbol == "(H)"
    assert flagged.value is None


def test_reviewed_county_rows_keep_exact_values_and_suppression() -> None:
    """Covers: ETL-004, ETL-022 — county rows keep exact values and symbols."""
    document = load_fixture("corn_survey_annual")
    rows = document["slices"]["COUNTY"]["data"]["data"]
    result = parse_slice_rows(
        rows,
        product=PRODUCT,
        release_watermark="2025-01-10 15:20:33.123000",
        slice_key="corn_survey_annual|COUNTY|2024",
    )

    assert result.input_count == len(rows)
    assert result.quarantined == ()
    assert len(result.observations) == len(rows)

    withheld = [
        item for item in result.observations if item.value_status == VALUE_STATUS_WITHHELD
    ]
    assert withheld, "the reviewed county sample must include a withheld value"
    for item in withheld:
        assert item.value is None
        assert item.value_source == "(D)"
        assert item.suppression_code == "(D)"
        assert item.cv_value is None
        assert item.cv_symbol == "(D)"

    published = [
        item for item in result.observations if item.value_status == VALUE_STATUS_VALID
    ]
    assert published
    for item in published:
        assert item.value is not None
        assert item.value_source.replace(",", "") == str(item.value)
        assert item.cv_status in {VALUE_STATUS_VALID, VALUE_STATUS_MISSING}


def test_source_rows_survive_parsing_unchanged() -> None:
    """Covers: RES-002 — source records remain intact during normalization."""
    rows = load_fixture("corn_survey_annual")["slices"]["STATE"]["data"]["data"]
    result = parse_slice_rows(
        rows,
        product=PRODUCT,
        release_watermark="2025-01-10 15:20:33.123000",
        slice_key="corn_survey_annual|STATE|2024",
    )
    for original, observation in zip(rows, result.observations, strict=True):
        assert observation.source_row == {key: original[key] for key in sorted(original)}
        assert observation.source_row["Value"] == original["Value"]
        assert observation.source_row["CV (%)"] == original["CV (%)"]


@pytest.mark.parametrize(
    ("case", "error_code"),
    [
        ("unregistered_statistic", "unresolvable_identity"),
        ("unregistered_unit", "unresolvable_identity"),
        ("foreign_commodity", "unresolvable_identity"),
        ("county_without_exact_code", "unresolvable_identity"),
        ("unparseable_year", "unresolvable_identity"),
        ("unregistered_value_symbol", "unregistered_value"),
    ],
)
def test_boundary_records_quarantine_instead_of_being_absorbed(
    case: str, error_code: str
) -> None:
    """Covers: RES-002 — boundary records quarantine with an explicit code."""
    result = parse_slice_rows(
        [BOUNDARY[case]],
        product=PRODUCT,
        release_watermark="2025-01-10 15:20:33.123000",
        slice_key="corn_survey_annual|COUNTY|2024",
    )
    assert result.observations == ()
    assert len(result.quarantined) == 1
    assert result.quarantined[0].error_code == error_code
    assert result.quarantined[0].source_row_index == 0
    assert result.quarantined[0].slice_key == "corn_survey_annual|COUNTY|2024"


def test_a_record_missing_a_consumed_field_cannot_reach_parsing() -> None:
    """Covers: RES-002 — missing consumed fields cannot reach parsing."""
    incomplete = load_fixture("boundary_records")["missing_consumed_field_payload"]
    result = parse_slice_rows(
        incomplete["data"],
        product=PRODUCT,
        release_watermark="2025-01-10 15:20:33.123000",
        slice_key="corn_survey_annual|COUNTY|2024",
    )
    assert result.observations == ()
    assert "missing consumed fields" in result.quarantined[0].error_summary
    assert "CV (%)" in result.quarantined[0].error_summary


def test_unsupported_aggregate_levels_are_retained_without_a_geography() -> None:
    """Covers: ETL-024 — unsupported levels never become a county."""
    for case in ("unsupported_aggregate_level", "unsupported_watershed_level"):
        result = parse_slice_rows(
            [BOUNDARY[case]],
            product=PRODUCT,
            release_watermark="2025-01-10 15:20:33.123000",
            slice_key="corn_survey_annual|COUNTY|2024",
        )
        assert len(result.observations) == 1
        observation = result.observations[0]
        assert observation.geography.geo_type == "unsupported"
        assert observation.geography.geo_id is None
        assert observation.geography.county_fips is None
        assert observation.source_row["agg_level_desc"] == BOUNDARY[case][
            "agg_level_desc"
        ]


def test_parsing_reconciles_every_input_row() -> None:
    """Covers: ETL-025 — parsing reconciles every input row exactly once."""
    rows = [
        *load_fixture("corn_survey_annual")["slices"]["COUNTY"]["data"]["data"],
        BOUNDARY["unregistered_statistic"],
        BOUNDARY["unregistered_value_symbol"],
    ]
    result = parse_slice_rows(
        rows,
        product=PRODUCT,
        release_watermark="2025-01-10 15:20:33.123000",
        slice_key="corn_survey_annual|COUNTY|2024",
    )
    assert result.input_count == len(rows)
    assert result.input_count == len(result.observations) + len(result.quarantined)
    assert len(result.quarantined) == 2
