"""P0 Census request and response normalization contracts."""

from __future__ import annotations

import uuid

import polars as pl
import pytest

from data_ingestion_toolbox.census_acs.ingest import (
    CensusNoContent,
    CensusPayloadError,
    build_geo_params,
    rows_to_polars,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("level", "state_fips", "expected"),
    [
        ("us", None, {"for": "us:1"}),
        ("state", None, {"for": "state:*"}),
        ("county", "06", {"for": "county:*", "in": "state:06"}),
    ],
)
def test_census_geography_parameters_are_exact(
    level: str, state_fips: str | None, expected: dict[str, str]
) -> None:
    """Covers: ETL-001 — supported levels produce exact request parameters."""
    assert build_geo_params(level, state_fips) == expected


@pytest.mark.parametrize(("level", "state_fips"), [("county", None), ("tract", "06")])
def test_census_geography_parameters_reject_invalid_scope(
    level: str, state_fips: str | None
) -> None:
    """Covers: ETL-001 — invalid request scopes are rejected."""
    with pytest.raises(ValueError):
        build_geo_params(level, state_fips)


def _convert(raw: list[list[str]]) -> pl.DataFrame:
    return rows_to_polars(
        raw=raw,
        dataset="acs5",
        year=2024,
        geo_level="county",
        state_fips="55",
        load_batch_id=uuid.UUID(int=0),
    )


def test_census_response_conversion_preserves_contract(source_fixture) -> None:
    """Covers: ETL-004 — a reviewed payload becomes exact observations."""
    frame = _convert(source_fixture("census", "representative.json"))
    assert frame.height == 6
    assert set(frame["variable_name"]) == {
        "B01003_001E",
        "B01003_001M",
        "B19013_001E",
    }
    assert set(frame["measure_type"]) == {"E", "M"}
    assert set(frame["geo_id"]) == {
        "state:55|county:001",
        "state:55|county:003",
    }
    assert frame.filter(pl.col("measure_type") == "M").height == 2


@pytest.mark.parametrize("raw", [[], [["B01003_001E", "state"]]])
def test_census_empty_response_is_typed(raw: list[list[str]]) -> None:
    """Covers: ETL-005 — empty or header-only data is typed no-content."""
    with pytest.raises(CensusNoContent):
        _convert(raw)


def test_census_malformed_row_has_deterministic_error() -> None:
    """Covers: ETL-005, RES-002 — shifted rows fail deterministically."""
    with pytest.raises(CensusPayloadError, match="row length"):
        _convert([["B01003_001E", "state", "county"], ["1", "55"]])


def test_census_duplicate_and_missing_geography_headers_are_rejected() -> None:
    """Covers: ETL-005, RES-002 — malformed Census headers are rejected."""
    with pytest.raises(CensusPayloadError, match="duplicate headers"):
        _convert(
            [
                ["B01003_001E", "state", "county", "county"],
                ["1", "55", "001", "001"],
            ]
        )
    with pytest.raises(CensusPayloadError, match="missing geography columns"):
        _convert([["B01003_001E", "state"], ["1", "55"]])


def test_census_sentinels_become_null_but_negative_values_survive(
    source_fixture,
) -> None:
    """Covers: ETL-006 — sentinels are null and valid negatives remain."""
    frame = _convert(source_fixture("census", "representative.json"))
    sentinel = frame.filter(
        (pl.col("variable_name") == "B19013_001E") & (pl.col("county_fips") == "001")
    )
    blank = frame.filter(
        (pl.col("variable_name") == "B01003_001M") & (pl.col("county_fips") == "003")
    )
    negative = frame.filter(
        (pl.col("variable_name") == "B01003_001E") & (pl.col("county_fips") == "003")
    )
    assert sentinel["value"][0] is None
    assert blank["value"][0] is None
    assert negative["value"][0] == -12.5
