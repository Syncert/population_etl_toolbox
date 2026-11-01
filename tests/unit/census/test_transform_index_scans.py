"""The transform's reads of the revision table name a dataset (load performance review).

Covers: ETL-100
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs.silver_census.transform import (
    KNOWN_DATASETS,
    RawSlice,
    _plan_year_slices,
    _refine_with_ledger,
    _slice_where,
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
                return [(self.params[0], "county", "10", 1000)]

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


def test_an_oversized_state_slice_is_planned_as_power_of_two_buckets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-100 -- acs5 place California (18.7M rows) is four reads, not one."""
    from data_ingestion_toolbox.census_acs.silver_census import transform

    class _Counts(_Hook):
        def get_conn(self):
            conn = super().get_conn()
            rows = self.answers["rows"]

            class _Wrapped:
                def __enter__(self):
                    return self

                def __exit__(self, *_exc):
                    return None

                def cursor(self):
                    cursor = conn.cursor()
                    cursor.fetchall = lambda: rows
                    return cursor

            return _Wrapped()

    hook = _Counts(
        {
            "rows": [
                ("acs5", "place", "06", 18_700_000),
                ("acs5", "state", "06", 9_000_000),
                ("acs5", "tract", "06", 5_000_000),
            ]
        }
    )
    slices = _plan_year_slices(hook, 2024, 190_000_000, ["acs5"])
    assert [s.key for s in slices] == [
        "acs5/place/06/p1of4",
        "acs5/place/06/p2of4",
        "acs5/place/06/p3of4",
        "acs5/place/06/p4of4",
        # A state slice has no sub-geography to split by.
        "acs5/state/06",
        # At the threshold, not over it.
        "acs5/tract/06",
    ]
    monkeypatch.setattr(transform, "_MAX_SLICE_PARTS", 2)
    assert len(_plan_year_slices(hook, 2024, 190_000_000, ["acs5"])) == 4


def test_a_bucket_key_keeps_its_level_where_the_serving_step_reads_it() -> None:
    """Covers: ETL-100 -- `split_part(slice_key, '/', 2)` still names the grain."""
    place = RawSlice(dataset="acs5", geo_level="place", state_fips="06")
    halves = place.split(2)
    assert [half.key for half in halves] == [
        "acs5/place/06/p1of2",
        "acs5/place/06/p2of2",
    ]
    assert {key.split("/")[1] for key in (h.key for h in halves)} == {"place"}
    # A half split again yields exactly that half's rows: buckets b and b+2.
    assert [q.key for q in halves[1].split(4)] == [
        "acs5/place/06/p2of4",
        "acs5/place/06/p4of4",
    ]
    where, params = _slice_where(2024, halves[1])
    assert "hashtext(COALESCE(observation.place_fips_source, ''))" in where
    assert params[-2:] == [2, 1]
    tract = RawSlice(dataset="acs5", geo_level="tract", state_fips="06").split(2)[0]
    assert "observation.county_fips_source" in _slice_where(2024, tract)[0]
    with pytest.raises(ValueError):
        RawSlice(dataset="acs5", geo_level="state", state_fips="06").split(2)


def test_planning_starts_a_slice_at_the_split_an_earlier_timeout_left() -> None:
    """Covers: ETL-100 -- a slice that timed out once is not read whole again."""
    planned = [
        RawSlice(dataset="acs5", geo_level="place", state_fips="06").split(4)[1],
        RawSlice(dataset="acs5", geo_level="place", state_fips="48"),
        RawSlice(dataset="acs5", geo_level="state", state_fips="06"),
    ]
    finished = {
        "acs5/place/06/p2of8": "x",
        "acs5/place/06/p6of8": "x",
        "acs5/place/06/p1of4": "x",
        "acs5/state/06": "x",
    }
    refined = _refine_with_ledger(planned, finished)
    assert [s.key for s in refined] == [
        "acs5/place/06/p2of8",
        "acs5/place/06/p6of8",
        "acs5/place/48",
        "acs5/state/06",
    ]
