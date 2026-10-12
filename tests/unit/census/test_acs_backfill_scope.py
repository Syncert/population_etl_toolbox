"""An ACS backfill run names one year and one grain.

Covers: ETL-083
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs.backfill import (
    BackfillScope,
    BackfillScopeError,
)
from data_ingestion_toolbox.census_acs.config import CONFIG

pytestmark = pytest.mark.unit


def test_a_normal_run_names_no_backfill() -> None:
    """Covers: ETL-083 — the scheduled run is unchanged."""
    assert BackfillScope.from_conf(None) is None
    assert BackfillScope.from_conf({}) is None
    assert BackfillScope.from_conf({"force": True}) is None


def test_a_backfill_names_one_year_and_one_grain() -> None:
    """Covers: ETL-083 — the conf a run is triggered with round-trips."""
    scope = BackfillScope.from_conf(
        {"acs_backfill": {"year": 2019, "geo_level": "tract"}}
    )
    assert scope == BackfillScope(year=2019, geo_level="tract")
    assert BackfillScope.from_conf(scope.as_conf()) == scope


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("2019", "must be an object"),
        ({"year": 2019, "geo_level": "tract", "states": ["10"]}, "unknown keys"),
        ({"year": "2019", "geo_level": "tract"}, "integer year"),
        ({"year": True, "geo_level": "tract"}, "integer year"),
        ({"year": 2004, "geo_level": "tract"}, "2005 or later"),
        ({"year": 2019, "geo_level": "county"}, "must be one of"),
        ({"year": 2019}, "must be one of"),
    ],
)
def test_a_malformed_backfill_refuses_rather_than_running_everything(
    raw: object, message: str
) -> None:
    """Covers: ETL-083 — a typo never becomes the full monthly plan."""
    with pytest.raises(BackfillScopeError, match=message):
        BackfillScope.from_conf({"acs_backfill": raw})


def test_only_the_newest_place_year_carries_the_whole_table_list() -> None:
    """Covers: ETL-083 — an older place year asks for the history tables only."""
    years = [2019, 2020, 2021, 2022, 2023, 2024]
    assert CONFIG.place_full_table_years == 1
    assert CONFIG.place_tables(2024, years) is None
    assert CONFIG.place_tables(2019, years) == CONFIG.place_history_tables
    assert set(CONFIG.place_history_tables) == set(CONFIG.tract_tables)
    assert set(CONFIG.place_history_tables) <= set(CONFIG.curated_tables)
    widened = CONFIG.model_copy(update={"place_full_table_years": 2})
    assert widened.place_tables(2023, years) is None
    assert widened.place_tables(2022, years) == CONFIG.place_history_tables
