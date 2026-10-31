"""The transform's reads of the revision table name a dataset (load performance review).

Covers: ETL-100
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs.silver_census.transform import (
    KNOWN_DATASETS,
    RawSlice,
    _plan_year_slices,
    _year_counts_by_dataset,
)

pytestmark = pytest.mark.unit


class _Hook:
    def __init__(self, answers: dict[str, object]) -> None:
        self.statements: list[tuple[str, tuple]] = []
        self.answers = answers

    def get_conn(self):
        hook = self

        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return None

            def execute(self, sql, params=()):
                hook.statements.append((sql, params))
                self.params = params

            def fetchone(self):
                return (hook.answers.get(self.params[0], 0),)

            def fetchall(self):
                return [(self.params[0], "county", "10")]

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return None

            def cursor(self):
                return _Cursor()

        return _Conn()


def test_year_counts_are_taken_per_dataset_on_the_slice_index() -> None:
    """Covers: ETL-100 -- no count reads the 1.8-billion-row table by year alone."""
    hook = _Hook({"dec2000sf1": 304500, "dec2000sf3": 228375})
    counts = _year_counts_by_dataset(hook, [2000])
    assert counts == {(2000, "dec2000sf1"): 304500, (2000, "dec2000sf3"): 228375}
    assert all("dataset = %s AND year = %s" in sql for sql, _ in hook.statements)
    assert {params[0] for _, params in hook.statements} == set(KNOWN_DATASETS)


def test_a_small_year_is_read_one_dataset_at_a_time() -> None:
    """Covers: ETL-100 -- the 2000 census's 533,000 rows no longer scan 428 GB."""
    hook = _Hook({})
    assert _plan_year_slices(hook, 2000, 532875, ["dec2000sf1", "dec2000sf3"]) == [
        RawSlice(dataset="dec2000sf1"),
        RawSlice(dataset="dec2000sf3"),
    ]
    assert hook.statements == []
    large = _plan_year_slices(hook, 2023, 60_000_000, ["acs5"])
    assert large == [RawSlice(dataset="acs5", geo_level="county", state_fips="10")]
    assert all("dataset = %s AND year = %s" in sql for sql, _ in hook.statements)


def test_each_dataset_slice_has_its_own_resume_key() -> None:
    """Covers: ETL-100 -- a retry cannot skip the second dataset as already done."""
    keys = {RawSlice(dataset="dec2000sf1").key, RawSlice(dataset="dec2000sf3").key}
    assert keys == {"dec2000sf1/*", "dec2000sf3/*"}
    assert RawSlice().key == "*"
