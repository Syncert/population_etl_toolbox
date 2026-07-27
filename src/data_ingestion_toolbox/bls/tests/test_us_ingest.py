"""Regression tests for national household labor-statistics routing."""

import pytest

from data_ingestion_toolbox.bls.config import CONFIG
from data_ingestion_toolbox.bls.geography import get_laus_area_codes


def test_laus_national_area_is_rejected() -> None:
    """LAUS has no national area; callers must use CPS/LN."""
    with pytest.raises(ValueError, match=r"use CPS/LN"):
        get_laus_area_codes("us")


def test_authoritative_national_cps_series_remain_curated() -> None:
    """Core national household measures must continue to come from CPS."""
    cps_series = set(CONFIG.curated_by_program["ln"])

    assert {
        "LNS14000000",  # unemployment rate
        "LNS13000000",  # unemployment level
        "LNS12000000",  # employment level
        "LNS11000000",  # civilian labor force
        "LNS11300000",  # labor force participation rate
        "LNS12300000",  # employment-population ratio
    } <= cps_series
