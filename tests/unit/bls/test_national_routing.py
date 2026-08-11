"""Regression tests for national household labor-statistics routing."""

import pytest

from data_ingestion_toolbox.bls.config import CONFIG
from data_ingestion_toolbox.bls.geography import get_laus_area_codes

pytestmark = pytest.mark.unit


def test_laus_national_area_is_rejected() -> None:
    """Covers: ETL-033 — LAUS national area is rejected in favor of CPS."""
    with pytest.raises(ValueError, match=r"use CPS/LN"):
        get_laus_area_codes("us")


def test_authoritative_national_cps_series_remain_curated() -> None:
    """Covers: ETL-033 — authoritative national measures remain in CPS."""
    cps_series = set(CONFIG.curated_by_program["ln"])

    assert {
        "LNS14000000",  # unemployment rate
        "LNS13000000",  # unemployment level
        "LNS12000000",  # employment level
        "LNS11000000",  # civilian labor force
        "LNS11300000",  # labor force participation rate
        "LNS12300000",  # employment-population ratio
    } <= cps_series


def test_recommended_national_series_remain_curated() -> None:
    """Covers: ETL-034 — recommended national BLS series remain curated."""
    expected = {
        "ln": {
            "LNS12300060",
            "LNS11300060",
            "LNS12032194",
            "LNS13008276",
            "LNS14000003",
            "LNS14000006",
            "LNS14000009",
            "LNS14032183",
        },
        "ce": {
            "CES0500000011",
            "CES1000000001",
            "CES2000000001",
            "CES3000000001",
            "CES4000000001",
            "CES5000000001",
            "CES5500000001",
            "CES6000000001",
            "CES6500000001",
            "CES7000000001",
            "CES8000000001",
            "CES9000000001",
        },
        "cu": {
            "CUUR0000SAF1",
            "CUUR0000SA0E",
            "CUUR0000SAH1",
            "CUUR0000SEHA",
            "CUUR0000SEHC",
            "CUUR0000SAM",
        },
        "jt": {
            "JTS000000000000000JOR",
            "JTS000000000000000HIL",
            "JTS000000000000000QUL",
            "JTS000000000000000LDR",
            "JTS000000000000000TSR",
            "JTS000000000000000OSR",
            "JTS000000000000000UOR",
        },
    }

    for program, expected_series in expected.items():
        assert expected_series <= set(CONFIG.curated_by_program[program])
