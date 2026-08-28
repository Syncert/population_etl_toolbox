"""Contracts for exact, minimal Census PEP bulk fixtures."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "census_pep"


def _read_one(name: str) -> tuple[list[str], dict[str, str]]:
    with (FIXTURE_DIR / name).open(encoding="utf-8", newline="") as fixture:
        reader = csv.DictReader(fixture)
        rows = list(reader)
    assert reader.fieldnames is not None
    assert len(rows) == 1
    assert None not in rows[0]
    return reader.fieldnames, rows[0]


def test_current_and_prior_fixtures_preserve_vintage_revision() -> None:
    """Covers: ETL-004 — PEP fixtures retain revised same-year estimates."""
    current_header, current = _read_one("nst_2025.csv")
    prior_header, prior = _read_one("nst_2024.csv")

    assert current["SUMLEV"] == prior["SUMLEV"] == "010"
    assert current["NAME"] == prior["NAME"] == "United States"
    assert current["POPESTIMATE2024"] == "340003797"
    assert prior["POPESTIMATE2024"] == "340110988"
    assert "POPESTIMATE2025" in current_header
    assert "POPESTIMATE2025" not in prior_header
    assert len(current_header) == len(set(current_header))
    assert len(prior_header) == len(set(prior_header))


def test_subcounty_fixture_is_an_incorporated_place() -> None:
    """Covers: ETL-004 — PEP place fixture retains authoritative source codes."""
    header, place = _read_one("subcounty_2025.csv")

    assert header[:10] == [
        "SUMLEV",
        "STATE",
        "COUNTY",
        "PLACE",
        "COUSUB",
        "CONCIT",
        "PRIMGEO_FLAG",
        "FUNCSTAT",
        "NAME",
        "STNAME",
    ]
    assert place["SUMLEV"] == "162"
    assert place["STATE"] == "01"
    assert place["PLACE"] == "00124"
    assert place["NAME"] == "Abbeville city"
    assert place["POPESTIMATE2025"] == "2378"
