"""Price series are BLS's own published pairs of item and area.

Covers: ETL-078
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from data_ingestion_toolbox.bls.price_series import (
    is_price_area,
    publication_frequency,
    select_price_series,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "bls" / "price"


def _series_list(name: str) -> list[dict[str, str]]:
    # As `metadata.process_series_data` stores it: the header names stripped.
    with (FIXTURES / name).open(encoding="utf-8", newline="") as handle:
        return [
            {key.strip(): value for key, value in row.items()}
            for row in csv.DictReader(handle, delimiter="\t")
        ]


def test_cpi_selects_unadjusted_monthly_series_for_items_and_price_areas() -> None:
    """Covers: ETL-078 — the item/area pairs BLS publishes, and nothing else."""
    selected = select_price_series("cu", _series_list("cu.series.excerpt"))
    assert selected == [
        "CUUR0000SAF11",
        "CUUR0000SETB01",
        "CUUR0200SAF11",
        "CUUR0200SETB01",
        "CUUR0230SAF11",
        "CUUR0230SETB01",
        "CUURS12ASAF11",
        "CUURS12ASETB01",
        "CUURS35ASAF11",
        "CUURS35ASETB01",
    ]


def test_average_prices_select_only_the_pairs_bls_publishes() -> None:
    """Covers: ETL-078 — eggs are not requested for the West, which BLS does not publish."""
    selected = select_price_series("ap", _series_list("ap.series.excerpt"))
    assert selected == sorted(
        [
            "APU000074714",
            "APU0000708111",
            "APU020074714",
            "APU0200708111",
            "APU023074714",
            "APU040074714",
            "APUS12A74714",
            "APUS35A74714",
        ]
    )
    # Flour is not a configured item; Pittsburgh and the size classes are not places.
    assert not any("701111" in s or "A104" in s or "N100" in s for s in selected)


def test_a_series_bls_stopped_is_not_requested() -> None:
    """Covers: ETL-078 — "current" is read from the list itself, not the clock."""
    rows = _series_list("cu.series.excerpt")
    # The same list with New York's food index ending two years early.
    stopped = [
        {**row, "end_year": "2024"} if row["series_id"].strip() == "CUURS12ASAF11" else row
        for row in rows
    ]
    assert "CUURS12ASAF11" not in select_price_series("cu", stopped)
    assert "CUURS12ASAF11" in select_price_series("cu", rows)


@pytest.mark.parametrize(
    ("area", "price_area", "frequency"),
    [
        ("0000", True, "monthly"),
        ("0300", True, "monthly"),
        ("0370", True, "monthly"),
        ("S12A", True, "monthly"),
        ("S23A", True, "monthly"),
        ("S49A", True, "monthly"),
        ("S35A", True, "bimonthly"),
        ("A104", False, "monthly"),
        ("N100", False, "monthly"),
        ("S000", False, "monthly"),
    ],
)
def test_price_areas_and_their_publication_cadence(
    area: str, price_area: bool, frequency: str
) -> None:
    """Covers: ETL-078 — New York, Chicago and Los Angeles monthly; other metros every other month."""
    assert is_price_area(area) is price_area
    assert publication_frequency(area) == frequency
