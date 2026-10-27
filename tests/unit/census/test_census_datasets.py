"""Where each Census dataset lives on the Data API (decennial-2000-and-1990-history).

Covers: ETL-093
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs.config import CENSUS_DATASETS, census_api_url

pytestmark = pytest.mark.unit


def test_each_dataset_names_its_own_api_path() -> None:
    """Covers: ETL-093 -- the ACS keeps its paths; the 2000 census reads dec/."""
    assert census_api_url(2024, "acs5") == "https://api.census.gov/data/2024/acs/acs5"
    assert census_api_url(2023, "acs1") == "https://api.census.gov/data/2023/acs/acs1"
    assert (
        census_api_url(2000, "dec2000sf1") == "https://api.census.gov/data/2000/dec/sf1"
    )
    assert (
        census_api_url(2000, "dec2000sf3") == "https://api.census.gov/data/2000/dec/sf3"
    )


def test_a_census_publishes_no_margins_and_one_year() -> None:
    """Covers: ETL-093 -- a decennial value has no margin row to request."""
    assert CENSUS_DATASETS["acs5"].margins
    assert not CENSUS_DATASETS["dec2000sf1"].margins
    assert not CENSUS_DATASETS["dec2000sf3"].margins


@pytest.mark.parametrize(
    ("year", "dataset"), [(2010, "dec2000sf1"), (2000, "acs3"), (2000, "sf1")]
)
def test_a_dataset_or_year_the_api_lacks_refuses(year: int, dataset: str) -> None:
    """Covers: ETL-093 -- no URL is guessed for a year or dataset that does not exist."""
    with pytest.raises(ValueError):
        census_api_url(year, dataset)


class _RecordingCursor:
    """Answers the release query, and formats every statement as psycopg2 does."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return None

    def execute(self, sql: str, params: tuple | None = None) -> None:
        # psycopg2 interpolates with `%`: a bare `%` in the SQL text raises
        # here exactly as it does against a server.
        rendered = sql % tuple(repr(value) for value in params) if params else sql
        self.statements.append(rendered)
        self._rows = [("dec2000sf3", 2000)] if "SELECT DISTINCT dataset" in sql else []

    def fetchall(self) -> list[tuple]:
        return self._rows

    def fetchone(self) -> tuple:
        return (0,)


class _RecordingHook:
    def __init__(self) -> None:
        self.cursor = _RecordingCursor()

    def get_conn(self):
        hook = self

        class _Connection:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return None

            def cursor(self):
                return hook.cursor

            def commit(self):
                return None

        return _Connection()


def test_a_census_release_is_described_with_its_own_api_path() -> None:
    """Covers: ETL-093 -- span 0 and a dec/ reference URL, in SQL psycopg2 can format."""
    from data_ingestion_toolbox.census_acs.gold_census.transform import (
        refresh_acs_elements,
    )

    hook = _RecordingHook()
    refresh_acs_elements(hook=hook)
    table_sql = next(
        sql for sql in hook.cursor.statements if "gold_census.dim_acs_table (" in sql
    )
    assert "WHEN 'dec2000sf3' THEN '/dec/sf3'" in table_sql
    assert "LEFT(f.dataset, 3) = 'dec' THEN 0" in table_sql
