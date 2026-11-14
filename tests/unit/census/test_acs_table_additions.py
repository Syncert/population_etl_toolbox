"""A table added to the ACS curated list is asked for alone where years are loaded.

Covers: ETL-114
"""

from __future__ import annotations

import pytest

from data_ingestion_toolbox.census_acs.config import CONFIG, AcsConfig
from data_ingestion_toolbox.census_acs.disk_guard import projected_plan_bytes
from data_ingestion_toolbox.census_acs.table_additions import (
    PENDING_PREFIX,
    added_tables_request,
    earlier_lists,
    pending_hash,
)

pytestmark = pytest.mark.unit


def _digest(tables: list[str]) -> str:
    return "|".join(tables)


def test_property_tax_is_curated_at_every_grain_and_declared_as_added() -> None:
    """Covers: ETL-114 — B25103 is requested for areas, places, metros and tracts."""
    assert "B25103" in CONFIG.curated_tables
    assert "B25103" in CONFIG.tract_tables
    # The rate's denominator is held wherever the tax is.
    assert "B25077" in CONFIG.tract_tables
    assert CONFIG.added_tables[0] == "B25103"
    assert set(CONFIG.added_tables) <= set(CONFIG.curated_tables)


FAMILY_TABLES = ["B13002", "B23008", "B19125"]


def test_the_family_tables_are_curated_but_never_requested_at_tract_grain() -> None:
    """Covers: ETL-114 — births, parents' work and family income for areas, counties, places and metros only."""
    assert CONFIG.added_tables[-3:] == FAMILY_TABLES
    for table in FAMILY_TABLES:
        assert table in CONFIG.curated_tables
        assert table not in CONFIG.tract_tables
    # A tract slice's list gains nothing, so its earlier forms are only B25103's.
    assert earlier_lists(CONFIG.tract_tables, CONFIG.added_tables) == [
        ([t for t in CONFIG.tract_tables if t != "B25103"], ["B25103"])
    ]


def test_a_county_slice_asks_only_for_the_tables_it_lacks() -> None:
    """Covers: ETL-114 — held with or without B25103, a slice asks only for what it lacks."""
    tables = list(CONFIG.curated_tables)
    with_tax = [t for t in tables if t not in FAMILY_TABLES]
    request = added_tables_request(
        _digest(with_tax), "success", tables, CONFIG.added_tables, _digest
    )
    assert request is not None and request.tables == FAMILY_TABLES
    before_tax = [t for t in with_tax if t != "B25103"]
    request = added_tables_request(
        _digest(before_tax), "success", tables, CONFIG.added_tables, _digest
    )
    assert request is not None and request.tables == ["B25103", *FAMILY_TABLES]


def test_an_added_table_must_be_curated_and_named_once() -> None:
    """Covers: ETL-114 — the declaration refuses a table the loader would never ask for."""
    with pytest.raises(ValueError, match="not in curated_tables"):
        AcsConfig(added_tables=["B99999"])
    with pytest.raises(ValueError, match="twice"):
        AcsConfig(added_tables=["B25103", "B25103"])


def test_each_earlier_list_is_the_list_before_an_addition_newest_first() -> None:
    """Covers: ETL-114 — every list a slice may have finished under, smallest request first."""
    tables = ["A", "B", "C", "D"]
    assert earlier_lists(tables, ["C", "D"]) == [
        (["A", "B", "C"], ["D"]),
        (["A", "B"], ["C", "D"]),
    ]
    # An addition outside the level's list (a county-only table at tract
    # grain) leaves no earlier form of its own.
    assert earlier_lists(["A", "B", "D"], ["C", "D"]) == [(["A", "B"], ["D"])]
    assert earlier_lists(["A", "B"], ["C"]) == []


def test_a_slice_finished_before_the_addition_asks_for_the_added_table_only() -> None:
    """Covers: ETL-114 — finished under the earlier list: added tables, pending mark."""
    tables = ["A", "B", "C"]
    for status in ("success", "empty"):
        request = added_tables_request(
            _digest(["A", "B"]), status, tables, ["C"], _digest
        )
        assert request is not None
        assert request.tables == ["C"]
        assert request.ledger_hash == pending_hash(_digest(["A", "B"]))
        assert request.ledger_hash.startswith(PENDING_PREFIX)

    # Two additions since: both asked for together.
    request = added_tables_request(
        _digest(["A"]), "success", tables, ["B", "C"], _digest
    )
    assert request is not None and request.tables == ["B", "C"]


def test_an_outstanding_request_is_asked_again_and_anything_else_in_full() -> None:
    """Covers: ETL-114 — a failed added-tables request retries; partial or unknown slices reload."""
    tables = ["A", "B", "C"]
    pending = pending_hash(_digest(["A", "B"]))
    for status in ("planned", "running", "failed"):
        request = added_tables_request(pending, status, tables, ["C"], _digest)
        assert request is not None and request.tables == ["C"]

    # Failed under the earlier list: its cells may be partial.
    assert (
        added_tables_request(_digest(["A", "B"]), "failed", tables, ["C"], _digest)
        is None
    )
    # Never loaded, or finished under a list no addition explains.
    assert added_tables_request(None, None, tables, ["C"], _digest) is None
    assert (
        added_tables_request("something-else", "success", tables, ["C"], _digest)
        is None
    )
    # A pending mark whose earlier list no longer digests the same.
    assert (
        added_tables_request(pending_hash("stale"), "failed", tables, ["C"], _digest)
        is None
    )


def test_the_disk_guard_projects_only_the_added_variables() -> None:
    """Covers: ETL-114 — a backfill asking for one table projects that table, not the list."""
    per_variable = {"acs5/tract": 52_000.0}
    parents = {("acs5", "tract"): 52}
    whole = {"dataset": "acs5", "geo_level": "tract", "variables_count": 560}
    added = {**whole, "requested_count": 6}
    assert projected_plan_bytes([whole], per_variable, parents) == 560_000
    assert projected_plan_bytes([added], per_variable, parents) == 6_000
