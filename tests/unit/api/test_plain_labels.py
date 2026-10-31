"""Plain-language metric labels and definitions (A-2).

Covers: API-170
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from apps.api.schemas.analysis import (
    ComparisonResponse,
    DistributionBinsResponse,
    MatrixMetricSummary,
)
from apps.api.schemas.catalog import MetricCatalog
from apps.api.schemas.labels import label_fields
from apps.api.schemas.observations import NeutralObservation
from data_ingestion_toolbox.semantics.definitions import (
    DefinitionError,
    parse_registry,
    plain_label,
    provider_label,
    registry,
)

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
PLACE_CHAPTERS = ROOT / "apps" / "web" / "lib" / "placeChapters.ts"
GROCERIES_AND_GAS = ROOT / "apps" / "web" / "lib" / "groceriesAndGas.ts"

#: Census division -> its region, the two codes BLS builds an area's CPI id from.
_DIVISION_REGION = {1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 3, 8: 4, 9: 4}


def _groceries_and_gas_codes() -> set[str]:
    """Every code the place page's grocery-and-gas card can ask for.

    The card builds BLS ids from the containing region's and division's
    codes; each template is expanded over every region and division.
    """
    text = GROCERIES_AND_GAS.read_text(encoding="utf-8")
    codes: set[str] = set()
    for template in re.findall(r'metricCode: [`"]([^`"]+)[`"]', text):
        if "${" not in template:
            codes.add(template)
        elif "${division}" in template:
            codes |= {
                template.replace("${region}", str(r)).replace("${division}", str(d))
                for d, r in _DIVISION_REGION.items()
            }
        else:
            codes |= {template.replace("${region}", str(r)) for r in range(1, 5)}
    assert codes, "the card's metric templates were not found"
    return codes


def _place_page_codes() -> set[str]:
    """Every metric code the place pages ask for, read from their module."""
    text = PLACE_CHAPTERS.read_text(encoding="utf-8")
    codes = set(re.findall(r'"((?:[A-Z_]+):[^"\s]+)"', text))
    for variable in re.findall(r'acs\("([BC]\d{5}_\d{3})"\)', text):
        codes |= {f"CENSUS_ACS:acs5:{variable}", f"CENSUS_ACS:acs1:{variable}"}
    for measure in re.findall(r'saipe\("(\w+)"\)', text):
        codes.add(f"CENSUS_SAIPE_SAHIE:saipe:{measure}")
    for measure in re.findall(r'sahie\("(\w+)"\)', text):
        codes.add(f"CENSUS_SAIPE_SAHIE:sahie:{measure}")
    for table, line in re.findall(r'bea\("(\w+)", "(\w+)"\)', text):
        codes.add(f"BEA:{table}:{line}")
    for measure, industry, ownership in re.findall(
        r'qcew\("(\w+)", "([\w-]+)", "(\w+)"\)', text
    ):
        codes.add(f"BLS_QCEW:{measure}:{industry}:{ownership}")
    for measure, structure, frequency in re.findall(
        r'bps\("(\w+)", "(\w+)", "(\w+)"\)', text
    ):
        codes.add(f"CENSUS_BPS:{measure}:{structure}:{frequency}")

    def sector_codes(name: str) -> list[str]:
        block = text[text.index(name) :]
        block = block[: block.index("];")]
        return re.findall(r'\["([\d-]+)", "[^"]+"\]', block)

    codes |= {f"BEA:CAINC5N:{line}" for line in sector_codes("BEA_EARNINGS_SECTORS")}
    codes |= {
        f"BLS_QCEW:employment:{code}:5" for code in sector_codes("INDUSTRY_MIX_SECTORS")
    }
    # Prefixes the regexes above catch but that are not codes.
    return {code for code in codes if not code.endswith(":")}


def test_every_metric_a_place_page_shows_has_a_definition() -> None:
    """Covers: API-170 -- the place pages' measures are all defined."""
    defined = set(registry())
    undefined = sorted(
        code
        for code in _place_page_codes() | _groceries_and_gas_codes()
        if code not in defined and not code.startswith("USDA_NASS:")
    )
    assert undefined == []


def test_a_defined_metric_is_served_its_label_and_status() -> None:
    """Covers: API-170 -- the label says whether it has been reviewed."""
    metric = MetricCatalog(
        metric_code="CENSUS_ACS:acs5:B19083_001",
        metric_display_name="Estimate!!Gini Index",
    )
    assert metric.metric_label == "Gini index of income inequality"
    assert metric.metric_label_status == "draft"
    assert metric.metric_display_name == "Estimate!!Gini Index"
    assert metric.definition is not None
    assert metric.definition.universe == "households"
    assert metric.definition.citations


def test_an_undefined_acs_metric_shows_the_bureaus_label_set_as_words() -> None:
    """Covers: API-170 -- no `Estimate!!`, and nothing the Bureau did not write."""
    row = NeutralObservation(
        source_code="CENSUS_ACS",
        metric_code="CENSUS_ACS:acs5:B01001_003",
        metric_display_name="Estimate!!Total:!!Male:!!Under 5 years",
    )
    assert row.metric_label == "Total: Male: Under 5 years"
    assert row.metric_label_status == "provider"
    assert provider_label("BLS:LAU:X", "Labor force") == "Labor force"
    assert plain_label("NOPE:x", None) == (None, "provider")


def test_the_registry_refuses_a_broken_entry() -> None:
    """Covers: API-170 -- a code defined twice or a missing field is an error."""
    entry = {
        "metric_id": "A",
        "metric_codes": ["X:1"],
        "name": "n",
        "definition": "d",
        "intended_use": "u",
        "limitations": [],
        "status": "draft",
        "owner": "o",
        "reviewer": "r",
        "version": 1,
        "effective_date": "2026-10-26",
        "last_reviewed_date": "2026-10-26",
        "citations": ["https://example.org"],
    }
    assert set(parse_registry({"entries": [entry]})) == {"X:1"}
    with pytest.raises(DefinitionError, match="defined twice"):
        parse_registry({"entries": [entry, {**entry, "metric_id": "B"}]})
    with pytest.raises(DefinitionError, match="lacks"):
        parse_registry(
            {"entries": [{k: v for k, v in entry.items() if k != "citations"}]}
        )
    with pytest.raises(DefinitionError, match="status"):
        parse_registry({"entries": [{**entry, "status": "final"}]})


def test_every_response_naming_a_metric_by_code_carries_its_label() -> None:
    """Covers: API-170 -- comparison sides, matrix measures and bins are labelled."""
    gini = {"metric_display_name": "Estimate!!Gini Index"}
    under5 = {"metric_display_name": "Estimate!!Total:!!Male:!!Under 5 years"}
    response = ComparisonResponse(
        metric_code_a="CENSUS_ACS:acs5:B19083_001",
        metric_code_b="CENSUS_ACS:acs5:B01001_003",
        **label_fields("CENSUS_ACS:acs5:B19083_001", gini, "_a"),
        **label_fields("CENSUS_ACS:acs5:B01001_003", under5, "_b"),
        total=0,
        limit=10,
        offset=0,
        items=[],
    )
    assert response.metric_label_a == "Gini index of income inequality"
    assert response.metric_label_status_a == "draft"
    assert response.metric_label_b == "Total: Male: Under 5 years"
    assert response.metric_label_status_b == "provider"
    assert label_fields("NOPE:x", None) == {
        "metric_label": None,
        "metric_label_status": None,
    }

    summary = MatrixMetricSummary(metric_code="CENSUS_ACS:acs5:B19083_001", **gini)
    assert summary.metric_label == "Gini index of income inequality"
    bins = DistributionBinsResponse(
        metric_code="EIA:EPMR",
        metric_display_name="Weekly U.S. Regular All Formulations Retail Gasoline Prices",
        total=0,
        bin_count=0,
        items=[],
    )
    assert bins.metric_label == "Regular gasoline, average retail price"


def test_reviewed_only_serves_the_provider_label_for_a_draft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: API-170 -- with the switch on, a draft is never shown as the label."""
    monkeypatch.setenv("METRIC_LABELS_REVIEWED_ONLY", "1")
    metric = MetricCatalog(
        metric_code="CENSUS_ACS:acs5:B19083_001",
        metric_display_name="Estimate!!Gini Index",
    )
    assert metric.metric_label == "Gini Index"
    assert metric.metric_label_status == "provider"
    # The draft definition is still served, saying it is a draft.
    assert metric.definition is not None and metric.definition.status == "draft"
