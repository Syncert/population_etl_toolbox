"""Reviewed CDC registry, metadata, and observation fixture contracts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_ingestion_toolbox.cdc.registry import CDI_ASSET, PLACES_COUNTY_ASSET

pytestmark = pytest.mark.unit
FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "cdc"


def _json(name: str) -> object:
    return json.loads((FIXTURE_ROOT / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize(
    "asset,fixture_name",
    [
        (CDI_ASSET, "cdi_metadata.json"),
        (PLACES_COUNTY_ASSET, "places_county_metadata.json"),
    ],
)
def test_metadata_fixture_matches_registered_identity_and_column_types(
    asset, fixture_name: str
) -> None:
    """Covers: EXT-004 — CDC metadata snapshots match registered contracts."""
    metadata = _json(fixture_name)

    assert metadata["id"] == asset.socrata_id
    assert metadata["name"] == asset.label
    assert metadata["rowsUpdatedAt"] > 0
    assert tuple(map(tuple, metadata["columns"])) == tuple(
        (column.name, column.data_type) for column in asset.expected_columns
    )


@pytest.mark.parametrize(
    "asset,fixture_name",
    [
        (CDI_ASSET, "cdi_observations.json"),
        (PLACES_COUNTY_ASSET, "places_county_observations.json"),
    ],
)
def test_observation_fixtures_use_only_registered_provider_fields(
    asset, fixture_name: str
) -> None:
    """Covers: RES-002 — CDC fixtures cannot silently expand parser schema."""
    rows = _json(fixture_name)
    allowed = set(asset.select_columns)

    assert rows
    assert all(set(row) <= allowed for row in rows)
    natural_keys = [tuple(row.get(field) for field in asset.source_key) for row in rows]
    assert len(natural_keys) == len(set(natural_keys))


def test_cdi_fixture_covers_required_geography_strata_uncertainty_and_missing() -> None:
    """Covers: ETL-024, RES-002 — CDI fixture preserves reviewed edge states."""
    rows = _json("cdi_observations.json")

    assert {row["locationabbr"] for row in rows} == {"US", "AL"}
    assert {row["stratificationcategory1"] for row in rows} == {
        "Overall",
        "Age",
        "Grade",
    }
    assert sum("lowconfidencelimit" in row for row in rows) == 2
    assert sum("datavalue" not in row for row in rows) == 1
    assert rows[-1]["datavaluefootnote"] == "No data available"


def test_places_fixture_preserves_modeled_values_population_and_suppression() -> None:
    """Covers: ETL-022, RES-002 — PLACES modeled and suppressed states are exact."""
    rows = _json("places_county_observations.json")

    assert {row["datavaluetypeid"] for row in rows} == {"AgeAdjPrv", "CrdPrv"}
    assert rows[0]["locationid"] == "01001"
    assert rows[0]["totalpop18plus"] == "46253"
    assert any(row["locationid"] == "59" for row in rows)
    assert rows[-1]["locationid"] == "48301"
    assert "data_value" not in rows[-1]
    assert rows[-1]["data_value_footnote_symbol"] == "*"


def test_expected_reconciliation_counts_match_reviewed_fixture_rows() -> None:
    """Covers: ETL-025 — CDC expected outcomes reconcile every fixture row."""
    expected = _json("expected_contracts.json")

    for asset_id, fixture_name in (
        ("cdi", "cdi_observations.json"),
        ("places_county", "places_county_observations.json"),
    ):
        contract = expected[asset_id]
        assert contract["fixture_rows"] == len(_json(fixture_name))
        assert contract["fixture_rows"] == contract["publishable_rows"] + sum(
            contract.get(key, 0) for key in ("missing_rows", "suppressed_rows")
        )
