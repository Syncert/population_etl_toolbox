"""Production decimal parsing limits shared by BLS and FRED ingestion."""

from __future__ import annotations

from decimal import Decimal

import pytest

from data_ingestion_toolbox.normalization import NumericParseError, parse_decimal

pytestmark = pytest.mark.unit


def test_decimal_parser_preserves_precision_nulls_signs_and_bounds() -> None:
    """Covers: ETL-022 — production numeric parsing has explicit bounded outcomes."""
    assert parse_decimal("-1234567890.123456789012") == Decimal(
        "-1234567890.123456789012"
    )
    assert parse_decimal(None) is None
    assert parse_decimal(".") is None
    for value in (True, "NaN", "Infinity", "1e31", "0.1234567890123"):
        with pytest.raises(NumericParseError):
            parse_decimal(value)
