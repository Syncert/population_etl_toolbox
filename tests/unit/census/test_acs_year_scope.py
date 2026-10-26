"""Declared ACS year scopes (acs-place-tract-history-backfill, Deliverable 3)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from data_ingestion_toolbox.census_acs.config import CONFIG, AcsConfig
from data_ingestion_toolbox.census_acs.year_scope import YearScope, YearScopeError

pytestmark = pytest.mark.unit

ACS5 = list(range(2009, 2025))
# The 1-year estimates have no standard 2020 release.
ACS1 = [year for year in range(2005, 2025) if year != 2020]


def test_newest_is_the_default_and_names_one_year() -> None:
    """Covers: ETL-055 -- the default scope is the newest published year."""
    assert YearScope.parse("newest").select(ACS5) == {2024}
    assert YearScope.parse("newest:3").select(ACS5) == {2022, 2023, 2024}
    assert CONFIG.place_years("acs5", ACS5) == {2024}
    assert CONFIG.tract_years("acs5", ACS5) == {2024}


def test_a_range_names_its_years_and_only_published_ones() -> None:
    """Covers: ETL-055 -- a declared range is intersected with what is published."""
    assert YearScope.parse("2009-2024").select(ACS5) == set(ACS5)
    assert 2020 not in YearScope.parse("2015-2024").select(ACS1)
    assert YearScope.parse("2019").select(ACS5) == {2019}
    assert YearScope.parse("none").select(ACS5) == set()
    # A range does not move when the Bureau publishes a new year.
    assert YearScope.parse("2009-2024").select([*ACS5, 2025]) == set(ACS5)


def test_full_table_years_follow_their_own_scope() -> None:
    """Covers: ETL-055 -- a place year outside the full-table scope reads the history tables."""
    widened = CONFIG.model_copy(update={"place_full_table_year_scope": "2009-2024"})
    assert widened.place_tables(2009, ACS5) is None
    assert CONFIG.place_tables(2009, ACS5) == CONFIG.place_history_tables


@pytest.mark.parametrize("spec", ["newest:0", "2024-2009", "latest", "19", ""])
def test_an_unreadable_scope_is_refused(spec: str) -> None:
    """Covers: ETL-055 -- a typo never silently becomes another scope."""
    with pytest.raises(YearScopeError):
        YearScope.parse(spec)
    with pytest.raises(ValidationError):
        AcsConfig(place_year_scope=spec)
