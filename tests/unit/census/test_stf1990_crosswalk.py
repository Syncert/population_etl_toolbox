"""1990 cells summed into the ACS estimates they measure (decennial-2000-and-1990-history).

Covers: ETL-096
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from data_ingestion_toolbox.census_acs.stf1990 import (
    crosswalk_1990,
    measure_values,
    parse_crosswalk_1990,
    requested_cells,
)

pytestmark = pytest.mark.unit


def _cells(dataset: str, value: str = "1") -> dict[str, str]:
    return {cell: value for cell in requested_cells(dataset)}


def test_the_checked_in_1990_crosswalk_names_both_files() -> None:
    """Covers: ETL-096 -- counts from STF1A, income and commute from STF3A."""
    index = crosswalk_1990()
    assert set(index) == {"dec1990stf1", "dec1990stf3"}
    assert index["dec1990stf1"]["B01003_001E"].cells == ("P0010001",)
    poverty = index["dec1990stf3"]["B17001_002E"]
    assert len(poverty.cells) == 12 and poverty.dollars_of == 1989
    assert poverty.source_header.startswith("P1170013+P1170014")


def test_a_measure_published_in_parts_is_their_sum() -> None:
    """Covers: ETL-096 -- Vermont's 1990 workers, from the thirteen commute cells."""
    cells = _cells("dec1990stf3", "0")
    cells.update({f"P04900{n:02d}": "1" for n in range(1, 14)})
    cells["P0490001"] = "199808"
    cells["P0490002"] = "35731"
    cells.update({"P080A001": "29792", "H043A001": "446", "H061A001": "95600"})
    values = {v.acs_variable: v for v in measure_values("dec1990stf3", cells)}
    assert values["B08301_003E"].value == Decimal(199808)
    assert values["B08301_002E"].value == Decimal(235539)
    assert values["B08301_001E"].value == Decimal(199808 + 35731 + 11)
    assert values["B19013_001E"].value_status == "valid"


def test_a_non_number_spoils_the_sum_and_a_zero_median_is_a_sentinel() -> None:
    """Covers: ETL-096 -- never a partial sum; a median of 0 was not computed."""
    cells = _cells("dec1990stf3", "5")
    cells["P1170013"] = "x"
    cells["P080A001"] = "000000000"
    values = {v.acs_variable: v for v in measure_values("dec1990stf3", cells)}
    assert (values["B17001_002E"].value, values["B17001_002E"].value_status) == (
        None,
        "invalid",
    )
    assert (values["B19013_001E"].value, values["B19013_001E"].value_status) == (
        None,
        "sentinel",
    )
    # A count of zero is a zero.
    cells["P1170013"] = "5"
    cells["P0490001"] = "0"
    values = {v.acs_variable: v for v in measure_values("dec1990stf3", cells)}
    assert values["B08301_003E"].value == Decimal(0)


@pytest.mark.parametrize(
    ("datasets", "message"),
    [
        ({"dec2000sf1": {}}, "not a 1990 file"),
        (
            {"dec1990stf1": {"B99999_001E": {"cells": ["P0010001"], "label": "x"}}},
            "curated",
        ),
        ({"dec1990stf1": {"B01003_001E": {"cells": [], "label": "x"}}}, "no cell"),
        (
            {
                "dec1990stf3": {
                    "B19013_001E": {"cells": ["A", "B"], "label": "x", "median": True}
                }
            },
            "median",
        ),
    ],
)
def test_a_malformed_1990_crosswalk_refuses(datasets: dict, message: str) -> None:
    """Covers: ETL-096 -- a 2000 file, an uncurated target, no cells, a summed median."""
    with pytest.raises(ValueError, match=message):
        parse_crosswalk_1990({"datasets": datasets})
