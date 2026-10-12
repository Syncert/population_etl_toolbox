"""Offline contracts for the LEHD LODES adapter.

Covers: ETL-063
"""

from __future__ import annotations

import gzip
import importlib
import sys
from pathlib import Path

import pytest

from data_ingestion_toolbox.census_lodes.client import (
    LodesIntegrityError,
    parse_checksums,
    parse_version,
    verify,
)
from data_ingestion_toolbox.census_lodes.registry import column_available, files_for
from data_ingestion_toolbox.census_lodes.silver_census_lodes.parse import (
    parse_area,
    parse_flows,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "census_lodes"
RAC, WAC, MAIN, AUX = files_for("de", 2023)
RAC_2008, WAC_2008, _main_2008, _aux_2008 = files_for("de", 2008)


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _blocks(name: str, column: str) -> dict[str, int]:
    """The hand sum: every block's figure added to its county by code."""
    lines = gzip.decompress(_fixture(f"{name}.gz")).decode().splitlines()
    header = lines[0].split(",")
    index = header.index(column)
    sums: dict[str, int] = {}
    for line in lines[1:]:
        cells = line.split(",")
        sums[cells[0][:5]] = sums.get(cells[0][:5], 0) + int(cells[index])
    return sums


def test_configuration_imports_without_io_and_holds_no_credential() -> None:
    """Covers: ETL-063 — LODES needs no key, so there is none to leak."""
    for name in list(sys.modules):
        if name.startswith("data_ingestion_toolbox.census_lodes"):
            del sys.modules[name]
    module = importlib.import_module("data_ingestion_toolbox.census_lodes.config")
    assert not {
        field
        for field in module.LodesConfig.model_fields
        if "key" in field or "token" in field
    }


def test_the_registry_names_four_files_a_state_year() -> None:
    """Covers: ETL-063 — residence, workplace, and both origin-destination parts."""
    assert [item.path for item in files_for("de", 2023)] == [
        "/de/rac/de_rac_S000_JT00_2023.csv.gz",
        "/de/wac/de_wac_S000_JT00_2023.csv.gz",
        "/de/od/de_od_main_JT00_2023.csv.gz",
        "/de/od/de_od_aux_JT00_2023.csv.gz",
    ]
    with pytest.raises(KeyError):
        files_for("pr", 2023)


def test_the_vintage_and_checksums_are_read_and_a_mismatch_refused() -> None:
    """Covers: ETL-063 — a file is kept only when its decompressed bytes match the list."""
    assert parse_version(_fixture("version.txt").decode()) == ("20251202_1657", "8.4")
    listed = parse_checksums(_fixture("lodes_de.sha256sum").decode())
    assert (
        verify(_fixture(f"{RAC.name}.gz"), RAC.path, expected=listed[RAC.name])
        == listed[RAC.name]
    )
    with pytest.raises(LodesIntegrityError, match="checksum_mismatch"):
        verify(_fixture(f"{WAC.name}.gz"), WAC.path, expected=listed[RAC.name])
    with pytest.raises(LodesIntegrityError, match="not_gzip"):
        verify(b"<html>", RAC.path, expected=listed[RAC.name])
    with pytest.raises(LodesIntegrityError, match="not_in_checksum_list"):
        verify(_fixture(f"{RAC.name}.gz"), RAC.path, expected=None)
    with pytest.raises(ValueError):
        parse_version("no vintage here")


def test_county_totals_equal_the_hand_summed_blocks() -> None:
    """Covers: ETL-063 — a county is the sum of its blocks, keyed by the code's first five digits."""
    residence = parse_area(_fixture(f"{RAC.name}.gz"), item=RAC)
    workplace = parse_area(_fixture(f"{WAC.name}.gz"), item=WAC)
    assert {
        county: sums["C000"] for county, sums in residence.totals.items()
    } == _blocks(RAC.name, "C000")
    assert {
        county: sums["CNS18"] for county, sums in workplace.totals.items()
    } == _blocks(WAC.name, "CNS18")
    assert residence.quarantined == () and residence.blocks == {
        "10001": 113,
        "10005": 430,
    }


def test_origin_destination_reconciles_to_the_workplace_file() -> None:
    """Covers: ETL-063 — jobs by work county from main and aux equal the workplace total."""
    workplace = parse_area(_fixture(f"{WAC.name}.gz"), item=WAC)
    main = parse_flows(_fixture(f"{MAIN.name}.gz"), item=MAIN)
    aux = parse_flows(_fixture(f"{AUX.name}.gz"), item=AUX)
    for county, sums in workplace.totals.items():
        od = sum(
            jobs for (_home, work), jobs in main.flows.items() if work == county
        ) + sum(jobs for (_home, work), jobs in aux.flows.items() if work == county)
        assert od == sums["C000"]
    assert all(home[:2] != "10" for home, _work in aux.flows)


def test_columns_the_bureau_does_not_publish_are_not_available() -> None:
    """Covers: ETL-063 — pre-2009 demographics and non-JT02 firm columns are not zeros."""
    assert not column_available("CR01", year=2008)
    assert column_available("CR01", year=2009)
    assert not column_available("CFA01", year=2023)
    assert column_available("CFA01", year=2023, job_type="JT02")
    assert not column_available("CFS01", year=2010, job_type="JT02")
    assert column_available("CNS01", year=2002)
    older = parse_area(_fixture(f"{RAC_2008.name}.gz"), item=RAC_2008)
    assert all(
        sums["CR01"] == 0 and sums["CD01"] == 0 for sums in older.totals.values()
    )


def test_a_header_only_file_and_unreadable_rows() -> None:
    """Covers: ETL-063 — no rows is no jobs; a short row or a foreign block is quarantined."""
    header = gzip.decompress(_fixture(f"{RAC.name}.gz")).split(b"\n", 1)[0]
    empty = parse_area(gzip.compress(header + b"\n"), item=RAC)
    assert (empty.totals, empty.row_count, empty.quarantined) == ({}, 0, ())
    lines = gzip.decompress(_fixture(f"{RAC.name}.gz")).split(b"\n")
    lines[1] = b",".join(lines[1].split(b",")[:5])
    lines[2] = b"24" + lines[2][2:]
    lines[3] = lines[3].replace(b",", b",x", 1)
    parsed = parse_area(gzip.compress(b"\n".join(lines)), item=RAC)
    assert sorted(item.error_code for item in parsed.quarantined) == [
        "ragged_row",
        "unreadable_block",
        "unreadable_value",
    ]
    wrong = parse_area(_fixture(f"{WAC.name}.gz"), item=RAC)
    assert [item.error_code for item in wrong.quarantined] == ["unexpected_header"]
