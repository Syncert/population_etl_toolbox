"""Covers: ETL-107 — Return A signed fields decode the punched-card way."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.fbi_ucr.return_a.overpunch import (
    OverpunchError,
    decode_signed,
    decode_unsigned,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("00012", 12),
        ("0001{", 10),
        ("0001A", 11),
        ("0001I", 19),
        ("0001}", -10),
        ("0001J", -11),
        ("0001R", -19),
        ("    }", 0),
        ("    J", -1),
        ("00000", 0),
    ],
)
def test_every_overpunch_character_decodes(field: str, value: int) -> None:
    """Covers: ETL-107 — every overpunch character decodes."""
    assert decode_signed(field) == value


def test_a_blank_field_is_absent_never_zero() -> None:
    """Covers: ETL-107 — a blank field is absent never zero."""
    assert decode_signed("     ") is None
    assert decode_unsigned("   ") is None


@pytest.mark.parametrize("field", ["00S12", "0A012", "001-2", "00012S", "12.5"])
def test_a_shifted_or_foreign_field_fails(field: str) -> None:
    """Covers: ETL-107 — a shifted or foreign field fails."""
    with pytest.raises(OverpunchError):
        decode_signed(field)


def test_unsigned_fields_refuse_signs() -> None:
    """Covers: ETL-107 — unsigned fields refuse signs."""
    assert decode_unsigned("0274234") == 274234
    with pytest.raises(OverpunchError):
        decode_unsigned("0001J")
