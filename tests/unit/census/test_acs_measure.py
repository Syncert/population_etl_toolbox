"""The ACS backfill measurement's arithmetic (backfill plan, Deliverable 1)."""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs.measure import (
    GIB,
    Measurement,
    apportion_bytes,
    project_missing,
    render,
)

pytestmark = pytest.mark.unit


def test_bytes_are_apportioned_by_revision_row_share() -> None:
    """Covers: ETL-085 -- silver splits by row share; gold splits within its year."""
    counts = {
        ("acs5", 2024, "place"): 750,
        ("acs5", 2024, "county"): 250,
        ("acs5", 2023, "place"): 1000,
    }
    grains = {
        (g.dataset, g.year, g.geo_level): g
        for g in apportion_bytes(counts, 2000, {2024: 400, 2023: 100})
    }
    assert grains[("acs5", 2024, "place")].silver_bytes_estimate == 750
    assert grains[("acs5", 2024, "place")].gold_bytes_estimate == 300
    assert grains[("acs5", 2024, "county")].gold_bytes_estimate == 100
    assert grains[("acs5", 2023, "place")].bytes_estimate == 1100


def test_a_missing_year_is_scaled_by_the_variables_its_vintage_publishes() -> None:
    """Covers: ETL-085 -- an older vintage with fewer variables projects smaller."""
    grains = apportion_bytes({("acs5", 2024, "place"): 1}, 3844 * 10)
    variables = {("acs5", 2024): 3844, ("acs5", 2009): 1276, ("acs5", 2015): 2062}
    projections = project_missing(
        grains,
        variables,
        targets=[("acs5", 2009, "place"), ("acs5", 2015, "place")],
        reference={("acs5", "place"): 2024},
        # 2015 already holds a fifth of its variables (the history tables).
        loaded_share={("acs5", 2015, "place"): 0.2},
    )
    by_year = {item.year: item.projected_bytes for item in projections}
    assert by_year[2009] == 12760
    assert by_year[2015] == int(10 * 2062 * 0.8)


def test_a_target_without_a_measured_reference_is_not_guessed() -> None:
    """Covers: ETL-085 -- no measured basis, no projection."""
    assert (
        project_missing(
            [],
            {("acs5", 2015): 2062},
            [("acs5", 2015, "tract")],
            {("acs5", "tract"): 2024},
        )
        == []
    )


def test_the_report_states_free_space_after_the_projection() -> None:
    """Covers: ETL-085 -- the operator reads the share left free, against 20%."""
    measurement = Measurement(
        disk_path="/",
        disk_total_bytes=100 * GIB,
        disk_free_bytes=50 * GIB,
        database_bytes=40 * GIB,
        silver_bytes=30 * GIB,
        grains=apportion_bytes({("acs5", 2024, "place"): 1}, 30 * GIB),
    )
    measurement.projections = project_missing(
        measurement.grains,
        {("acs5", 2024): 10, ("acs5", 2023): 10},
        [("acs5", 2023, "place")],
        {("acs5", "place"): 2024},
    )
    assert measurement.free_after_projection() == pytest.approx(0.20)
    report = render(measurement)
    assert "Projected total: 30 GiB" in report
    assert "free afterwards: 20% of the volume" in report


class _Cursor:
    def __init__(self, answer):
        self._answer = answer
        self._rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return None

    def execute(self, sql, params=None):
        self._rows = self._answer(sql, params)

    def fetchall(self):
        return self._rows


class _Warehouse:
    """Answers the measurement's catalog queries from a small fixed warehouse."""

    closed = False

    def cursor(self):
        return _Cursor(self.answer)

    def close(self):
        self.closed = True

    @staticmethod
    def answer(sql, params):
        if "pg_database_size" in sql:
            return [(600 * GIB,)]
        if "pg_total_relation_size(%s::regclass)" in sql:
            return [(100 * GIB,)]
        if "FROM silver_census.observation_revision" in sql:
            return [("acs5", 2024, "place", 1000)]
        if "pg_inherits" in sql:
            return [("rpt_acs_observations_2024", 50 * GIB)]
        if "FROM raw_census.acs_variables" in sql:
            return [("acs5", 2024, 4000), ("acs5", 2023, 2000)]
        raise AssertionError(f"unexpected query: {sql}")


def test_the_command_measures_the_warehouse_and_projects_the_named_years(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path
) -> None:
    """Covers: ETL-085 -- one run reads the catalog, projects, and writes JSON."""
    from data_ingestion_toolbox.census_acs import measure

    warehouse = _Warehouse()
    monkeypatch.setattr(measure, "_connect", lambda dsn: warehouse)
    monkeypatch.setattr(measure, "disk_usage", lambda path: (6000 * GIB, 4000 * GIB))
    report = tmp_path / "measure.json"

    assert (
        measure.main(
            ["--dsn", "x", "--project", "acs5:place:2023-2024", "--json", str(report)]
        )
        == 0
    )

    out = capsys.readouterr().out
    assert "| acs5 | 2024 | place | 1,000 |" in out
    # 2024 holds 250 GiB (two silver relations and its gold partition); 2023
    # publishes half the variables, so it projects half that.
    assert "| acs5 | 2023 | place | 2,000 | 125.0 |" in out
    assert warehouse.closed
    assert '"database_bytes": 644245094400' in report.read_text(encoding="utf-8")
