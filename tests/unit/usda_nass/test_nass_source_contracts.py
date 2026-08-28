"""Agreement between the USDA NASS registry, fixtures, and expected outcomes."""

from __future__ import annotations

import json
from collections import Counter

import pytest

from data_ingestion_toolbox.usda_nass.registry import (
    ALL_PRODUCTS,
    QUICK_STATS_FIELDS,
    SUPPRESSION_SYMBOLS,
    get_product,
)
from data_ingestion_toolbox.usda_nass.silver_nass.values import (
    SYMBOL_STATUS,
    parse_slice_rows,
)

from ._doubles import FIXTURE_DIR, load_fixture

pytestmark = pytest.mark.unit

EXPECTED = load_fixture("expected_contracts")
PRODUCT_FIXTURES = {
    name: entry
    for name, entry in EXPECTED["products"].items()
    if name != "corn_survey_annual_revised"
}


def test_every_registered_product_has_a_reviewed_fixture() -> None:
    """Covers: EXT-004 — every registered product has a reviewed fixture."""
    registered = {product.product_id for product in ALL_PRODUCTS}
    assert registered == set(PRODUCT_FIXTURES)
    for product_id in registered:
        assert (FIXTURE_DIR / f"{product_id}.json").is_file()


def test_source_notes_document_every_registered_symbol_and_fixture() -> None:
    """Covers: EXT-004 — the source notes document the registered contract."""
    notes = (FIXTURE_DIR / "SOURCE_NOTES.md").read_text(encoding="utf-8")
    for symbol in SUPPRESSION_SYMBOLS:
        assert f"`{symbol}`" in notes, symbol
    for path in sorted(FIXTURE_DIR.glob("*.json")):
        assert f"`{path.name}`" in notes, path.name
    assert "/api/api_GET" in notes
    assert "/api/get_counts" in notes
    assert "50,000" in notes


def test_expected_symbol_table_matches_the_implementation() -> None:
    """Covers: EXT-004 — the reviewed symbol table matches the parser."""
    assert EXPECTED["symbol_status"] == SYMBOL_STATUS
    assert set(EXPECTED["symbol_status"]) == set(SUPPRESSION_SYMBOLS)


def test_fixtures_cannot_silently_expand_the_parser_schema() -> None:
    """Covers: RES-002 — fixtures cannot silently expand the parser schema."""
    expected_fields = set(QUICK_STATS_FIELDS)
    for product_id in PRODUCT_FIXTURES:
        document = load_fixture(product_id)
        for level, envelope in document["slices"].items():
            for row in envelope["data"]["data"]:
                assert set(row) == expected_fields, f"{product_id}/{level}"


def test_fixture_counts_agree_with_their_own_preflight_payloads() -> None:
    """Covers: EXT-004 — each fixture slice agrees with its preflight count."""
    for product_id, entry in EXPECTED["products"].items():
        document = load_fixture(product_id)
        for level, expectation in entry["slices"].items():
            envelope = document["slices"][level]
            assert int(envelope["count"]["count"]) == expectation["provider_count"]
            assert len(envelope["data"]["data"]) == expectation["row_count"]


def test_expected_outcomes_reconcile_every_reviewed_fixture_row() -> None:
    """Covers: ETL-025 — expected outcomes reconcile every fixture row."""
    for product_id, entry in EXPECTED["products"].items():
        product = get_product(
            "corn_survey_annual"
            if product_id == "corn_survey_annual_revised"
            else product_id
        )
        for level, expectation in entry["slices"].items():
            rows = load_fixture(product_id)["slices"][level]["data"]["data"]
            result = parse_slice_rows(
                rows,
                product=product,
                release_watermark=expectation["load_times"][-1],
                slice_key=f"{product.product_id}|{level}|{entry['sample_year']}",
            )
            assert result.quarantined == (), f"{product_id}/{level}"
            assert result.input_count == expectation["row_count"]

            observations = result.observations
            assert (
                dict(
                    sorted(Counter(item.value_status for item in observations).items())
                )
                == expectation["value_status_counts"]
            )
            assert (
                dict(sorted(Counter(item.cv_status for item in observations).items()))
                == expectation["cv_status_counts"]
            )
            assert {item.geography.geo_type for item in observations} == {
                expectation["geo_type"]
            }
            assert (
                sorted({item.geography.geo_id for item in observations})
                == (expectation["geo_ids"])
            )
            assert (
                sorted({item.statistic.unit_desc for item in observations})
                == expectation["units"]
            )
            assert (
                sorted({item.statistic.short_desc for item in observations})
                == expectation["short_descs"]
            )
            assert (
                sorted({item.commodity.class_desc for item in observations})
                == expectation["class_descs"]
            )
            assert (
                sorted(
                    {
                        f"{item.domain.domain_desc}|{item.domain.domaincat_desc}"
                        for item in observations
                    }
                )
                == expectation["domains"]
            )


def test_incompatible_units_never_share_an_unlabeled_metric() -> None:
    """Covers: ETL-024 — acres, bushels, and tons never share one metric."""
    identities: dict[str, set[str]] = {}
    for product_id in PRODUCT_FIXTURES:
        product = get_product(product_id)
        for level, envelope in load_fixture(product_id)["slices"].items():
            result = parse_slice_rows(
                envelope["data"]["data"],
                product=product,
                release_watermark="2025-01-10 15:20:33.123000",
                slice_key=f"{product_id}|{level}|2024",
            )
            for observation in result.observations:
                identities.setdefault(observation.statistic.statistic_sk, set()).add(
                    observation.statistic.unit_desc
                )
    assert identities
    assert all(len(units) == 1 for units in identities.values())
    assert {"ACRES", "BU", "BU / ACRE", "TONS", "TONS / ACRE"} <= {
        unit for units in identities.values() for unit in units
    }


def test_no_fixture_contains_a_credential_shaped_field() -> None:
    """Covers: ETL-038 — no reviewed fixture carries a credential."""
    for path in sorted(FIXTURE_DIR.glob("*.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        rendered = json.dumps(document).lower()
        for forbidden in ('"key"', "api_key", "apikey", "authorization", "token"):
            assert forbidden not in rendered, f"{path.name}: {forbidden}"
