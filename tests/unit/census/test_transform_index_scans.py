"""The transform's reads of the revision table name a dataset (load performance review).

Covers: ETL-100
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs.silver_census.transform import (
    KNOWN_DATASETS,
    RawSlice,
    _ledger_ranges,
    _plan_year_slices,
    _ranges_from_counts,
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


def test_an_oversized_state_slice_is_planned_as_code_ranges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-100 -- acs5 place California (18.7M rows) is four index-range reads."""
    from data_ingestion_toolbox.census_acs.silver_census import transform

    monkeypatch.setattr(
        transform,
        "_sub_geo_counts",
        lambda _hook, _year, _slice: [
            (f"{n:05d}", 187_000) for n in range(0, 100000, 1000)
        ],
    )
    monkeypatch.setattr(transform, "_has_place_split_index", lambda _hook: True)

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
        "acs5/place/06/[,25000)",
        "acs5/place/06/[25000,50000)",
        "acs5/place/06/[50000,75000)",
        "acs5/place/06/[75000,)",
        # A state slice has no sub-geography to split by.
        "acs5/state/06",
        # At the threshold, not over it.
        "acs5/tract/06",
    ]
    # A place row that names a county would fall outside every place range
    # (they read county-less rows), so such a slice is read whole.
    monkeypatch.setattr(
        transform,
        "_sub_geo_counts",
        lambda _hook, _year, _slice: [
            (f"{n:05d}", 1000) for n in range(0, 100000, 1000)
        ],
    )
    assert (
        _plan_year_slices(hook, 2024, 190_000_000, ["acs5"])[0].key == "acs5/place/06"
    )
    # Without the place index a place slice stays one read, as before.
    monkeypatch.setattr(transform, "_has_place_split_index", lambda _hook: False)
    unsplit = _plan_year_slices(hook, 2024, 190_000_000, ["acs5"])
    assert unsplit[0].key == "acs5/place/06"
    # Ranges an earlier run recorded are reused, whatever the counts say now.
    finished = {"acs5/place/06/[,40000)": "x", "acs5/place/06/[40000,)": "y"}
    planned = _plan_year_slices(hook, 2024, 190_000_000, ["acs5"], finished)
    assert [s.key for s in planned][:2] == [
        "acs5/place/06/[,40000)",
        "acs5/place/06/[40000,)",
    ]


def test_ranges_cut_on_codes_into_about_equal_rows() -> None:
    """Covers: ETL-100 -- a cell never spans two ranges, and every code is covered."""
    even = [(f"{n:03d}", 10) for n in range(1, 9)]
    assert _ranges_from_counts(even, 2) == [("", "005"), ("005", "")]
    assert _ranges_from_counts(even, 4) == [
        ("", "003"),
        ("003", "005"),
        ("005", "007"),
        ("007", ""),
    ]
    # Los Angeles County holds most of California's tracts: it stays whole, in
    # one range, and the ranges come out fewer rather than splitting it.
    skewed = [(None, 0), ("001", 10), ("037", 70), ("059", 10), ("073", 10)]
    assert _ranges_from_counts(skewed, 4) == [("", "059"), ("059", "073"), ("073", "")]
    # One code, or one part, cannot be cut.
    assert _ranges_from_counts([("037", 99)], 4) == [("", "")]
    assert _ranges_from_counts(even, 1) == [("", "")]


def test_a_range_key_keeps_its_level_and_reads_an_index_range() -> None:
    """Covers: ETL-100 -- `split_part(slice_key, '/', 2)` still names the grain."""
    place = RawSlice(dataset="acs5", geo_level="place", state_fips="06")
    first = place.with_range("", "44000")
    middle = place.with_range("44000", "68000")
    assert first.key == "acs5/place/06/[,44000)"
    assert middle.key.split("/")[1] == "place"
    where, params = _slice_where(2024, middle)
    assert "observation.place_fips_source >= %s" in where
    assert "observation.place_fips_source < %s" in where
    assert params[-2:] == ["44000", "68000"]
    # The first range also takes rows without a code, so none is lost.
    assert "observation.place_fips_source IS NULL" in _slice_where(2024, first)[0]
    tract = RawSlice(dataset="acs5", geo_level="tract", state_fips="06")
    assert (
        "observation.county_fips_source >= %s"
        in _slice_where(2024, tract.with_range("037", ""))[0]
    )
    assert not RawSlice(dataset="acs5", geo_level="state", state_fips="06").splittable


def test_planning_starts_a_slice_at_the_split_an_earlier_timeout_left() -> None:
    """Covers: ETL-100 -- a range halved after a timeout is not read whole again."""
    finished = {
        "acs5/place/06/[,44000)": "x",
        "acs5/place/06/[44000,)": "x",
        # [44000,) timed out and finished as two halves.
        "acs5/place/06/[44000,68000)": "x",
        "acs5/place/06/[68000,)": "x",
        "acs5/place/48/[,50000)": "x",
        "acs5/place/06": "x",
    }
    assert _ledger_ranges(finished, "acs5/place/06") == [
        ("", "44000"),
        ("44000", "68000"),
        ("68000", ""),
    ]
    assert _ledger_ranges(finished, "acs5/tract/06") == []


def test_only_an_operator_stop_counts_as_stopped() -> None:
    """Covers: ETL-100 -- a cancel or terminate stops the run; its own timeout splits."""
    import psycopg2.errors

    from data_ingestion_toolbox.census_acs.silver_census.transform import (
        SliceReadTimeout,
        stopped_by_operator,
    )

    assert stopped_by_operator(
        psycopg2.errors.QueryCanceled("canceling statement due to user request")
    )
    assert stopped_by_operator(
        psycopg2.errors.AdminShutdown(
            "terminating connection due to administrator command"
        )
    )
    timeout = psycopg2.errors.QueryCanceled(
        "canceling statement due to statement timeout"
    )
    assert not stopped_by_operator(timeout)
    assert not stopped_by_operator(RuntimeError("Docker froze"))
    # Found through the cause chain, as a thread pool or wrapper re-raises it.
    try:
        try:
            raise psycopg2.errors.QueryCanceled(
                "canceling statement due to user request"
            )
        except psycopg2.errors.QueryCanceled as exc:
            raise SliceReadTimeout("acs5/place/06") from exc
    except SliceReadTimeout as wrapped:
        assert stopped_by_operator(wrapped)
