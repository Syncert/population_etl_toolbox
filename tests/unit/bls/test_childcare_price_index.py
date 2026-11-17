"""The nation's CPI for day care and preschool is requested as published.

Covers: ETL-125
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.bls.config import CONFIG
from data_ingestion_toolbox.bls.price_series import PRICE_ITEMS

pytestmark = pytest.mark.unit


def test_day_care_and_preschool_cpi_is_curated_once() -> None:
    """Covers: ETL-125 — CUUR0000SEEB03 is one curated cu series, national, unadjusted."""
    curated = CONFIG.curated_by_program["cu"]
    assert curated.count("CUUR0000SEEB03") == 1
    series_id = "CUUR0000SEEB03"
    # CU + U (not seasonally adjusted) + R (monthly) + 0000 (U.S. city average).
    assert series_id[:2] == "CU" and series_id[2] == "U" and series_id[3] == "R"
    assert series_id[4:8] == "0000"
    assert series_id[8:] == "SEEB03"


def test_day_care_is_not_an_area_price_item() -> None:
    """Covers: ETL-125 — no regional or metro childcare index is selected or estimated."""
    assert "SEEB03" not in PRICE_ITEMS["cu"]
    assert all(
        not series.endswith("SEEB03") or series == "CUUR0000SEEB03"
        for series in CONFIG.curated_by_program["cu"]
    )
