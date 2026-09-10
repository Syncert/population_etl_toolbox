"""Static contracts for the Census PEP gold publication.

Covers: PEH-002 — the currently published value for a measure, geography and
year is chosen across every product that publishes it, not within one. These
assertions read the DDL text so a regression fails in the unit tier, before a
warehouse is available to reproduce it in.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
GOLD_SQL = (
    REPOSITORY_ROOT
    / "src"
    / "data_ingestion_toolbox"
    / "census_pep"
    / "gold_pep"
    / "DDL"
    / "gold_pep.sql"
)
TRANSFORM = (
    REPOSITORY_ROOT
    / "src"
    / "data_ingestion_toolbox"
    / "census_pep"
    / "silver_pep"
    / "transform.py"
)


def _view_body(name: str) -> str:
    sql = GOLD_SQL.read_text(encoding="utf-8")
    match = re.search(
        rf"CREATE\s+OR\s+REPLACE\s+VIEW\s+{re.escape(name)}\s+AS(.*?);",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    assert match is not None, f"{name} is not defined in {GOLD_SQL.name}"
    return match.group(1)


def test_latest_is_not_partitioned_by_the_publishing_product() -> None:
    """Covers: PEH-002 — two products publishing one value yield one row.

    While the 2020s were the only decade, ranking inside a dataset was
    harmless. It is not: the state file and the county file both publish
    state rows, so partitioning by dataset answered every state twice.
    """
    body = _view_body("gold_pep.population_estimate_latest")
    partition = re.search(r"PARTITION BY(.*?)ORDER BY", body, re.DOTALL)
    assert partition is not None
    assert "dataset_code" not in partition.group(1)
    for column in ("metric_code", "geo_id", "observation_year"):
        assert column in partition.group(1)


def test_latest_prefers_the_intercensal_series_then_the_newest_vintage() -> None:
    """Covers: PEH-002 — precedence is stated in the order it is applied."""
    body = _view_body("gold_pep.population_estimate_latest")
    order = re.search(r"ORDER BY(.*?)\)\s*AS vintage_rank", body, re.DOTALL)
    assert order is not None
    clause = order.group(1)

    intercensal = clause.index("intercensal")
    vintage = clause.index("pep_vintage DESC")
    grain = clause.index("native_grain")
    assert intercensal < vintage < grain, clause


def test_latest_selects_exactly_one_row_per_geography_measure_and_year() -> None:
    """Covers: PEH-002 — the choice is total, not merely preferred.

    ``DENSE_RANK`` would return every row of a tie; the promise is one row,
    and the final ordering key makes that deterministic rather than left to
    whichever row the planner happened to return last.
    """
    body = _view_body("gold_pep.population_estimate_latest")
    assert "ROW_NUMBER() OVER" in body
    assert "DENSE_RANK" not in body
    assert "WHERE vintage_rank = 1" in body


def test_measure_coverage_is_read_from_the_facts() -> None:
    """Covers: PEH-002 — published coverage follows what actually loaded."""
    body = _view_body("gold_pep.measure_export")
    assert "MIN(fact.estimate_date) AS first_period" in body
    assert "MAX(fact.estimate_date) AS last_period" in body


def test_transform_dates_a_decennial_count_to_april() -> None:
    """Covers: PEH-001 — an April enumeration is not a July estimate.

    This is what keeps a decade's closing count from competing with the next
    decade's opening estimate for the same geography and year.
    """
    transform = TRANSFORM.read_text(encoding="utf-8")
    assert "WHEN source.metric_code = 'CENSUSPOP' THEN 4" in transform
    assert "MAKE_DATE(source.observation_year, 7, 1)" not in transform


def test_release_completeness_reads_the_registry_not_a_product_list() -> None:
    """Covers: PEH-003 — a newly registered product can be complete.

    Completeness named the three 2020s products, so any further product was
    incomplete by construction and its rows never reached the gold views.
    """
    transform = TRANSFORM.read_text(encoding="utf-8")
    assert "dataset.principal_level" in transform
    assert "FROM silver_pep.pep_dataset" in transform
    assert "dataset_code = 'pep_nst_alldata' AND summary_level" not in transform
