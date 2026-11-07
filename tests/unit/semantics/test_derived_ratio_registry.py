"""The per-capita and share pairing registry keeps its contract (ADR-0009).

Covers: ETL-104
"""

from __future__ import annotations

import json

import pytest

from data_ingestion_toolbox.semantics.definitions import registry as definitions
from data_ingestion_toolbox.semantics.derived_ratios import (
    REGISTRY_PATH,
    SERVE_DRAFTS_ENV,
    RatioRegistryError,
    load_registry,
    parse_registry,
)

pytestmark = pytest.mark.unit

SCHEMA = REGISTRY_PATH.parent / "derived_ratio.schema.json"


def _entry(**overrides: object) -> dict:
    base = {
        "ratio_id": "poverty_share",
        "kind": "share",
        "scale": 100,
        "unit": "percent",
        "label": "Share of people below the poverty level",
        "numerator": "CENSUS_ACS:{dataset}:B17001_002",
        "denominator": "CENSUS_ACS:{dataset}:B17001_001",
        "datasets": ["acs5", "acs1"],
        "denominator_label": "people whose poverty status is determined",
        "universe": "people whose poverty status is determined",
        "numerator_is_subset": True,
        "status": "approved",
        "owner": "data-eng",
        "reviewer": "Nick",
        "version": 1,
        "effective_date": "2026-10-31",
        "last_reviewed_date": "2026-10-31",
        "rationale": "B17001_002 is a line of the table B17001_001 totals.",
        "limitations": [],
        "citations": ["https://data.census.gov/table/ACSDT5Y2023.B17001"],
    }
    base.update(overrides)
    return {key: value for key, value in base.items() if value is not None}


def _parse(*entries: dict) -> dict:
    return parse_registry({"entries": list(entries)})


def test_an_acs_entry_expands_to_one_pairing_per_dataset() -> None:
    """Covers: ETL-104 — a 5-year count is only ever divided by a 5-year total."""
    pairings = _parse(_entry())
    assert sorted(pairings) == ["poverty_share:acs1", "poverty_share:acs5"]
    five = pairings["poverty_share:acs5"]
    assert five.numerator == "CENSUS_ACS:acs5:B17001_002"
    assert five.denominator == "CENSUS_ACS:acs5:B17001_001"
    assert five.dataset == "acs5"
    assert five.derived is True


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {
                "numerator": "CENSUS_ACS:acs5:B17001_002",
                "denominator": "CENSUS_PEP:POPESTIMATE",
                "datasets": None,
            },
            "same program",
        ),
        (
            {
                "numerator": "CENSUS_ACS:acs5:B17001_002",
                "denominator": "CENSUS_ACS:acs1:B17001_001",
                "datasets": None,
            },
            "same program",
        ),
        ({"numerator_is_subset": False}, "subset"),
        ({"scale": 1000}, "percent"),
        ({"scale": 7}, "scale"),
        ({"datasets": None}, "placeholder"),
        ({"denominator": None}, "denominator"),
        ({"kind": "index"}, "unknown kind"),
        ({"reviewer": "pending review"}, "without a reviewer"),
        ({"citations": []}, "cites nothing"),
    ],
)
def test_the_registry_refuses_a_broken_pairing(overrides: dict, message: str) -> None:
    """Covers: ETL-104 — nothing is paired across programs, periods or universes."""
    with pytest.raises(RatioRegistryError, match=message):
        _parse(_entry(**overrides))


def test_a_rate_the_provider_publishes_is_never_recomputed() -> None:
    """Covers: ETL-104 — ADR-0009 rule 5: provider rates take precedence."""
    provider = _entry(
        ratio_id="violent_crime_rate",
        kind="provider_rate",
        scale=100000,
        unit="per 100,000 people",
        numerator="FBI_UCR:summarized_violent_crime:V:offense:absolute_total",
        provider_rate="FBI_UCR:summarized_violent_crime:V:offense:rate",
        denominator=None,
        datasets=None,
        numerator_is_subset=None,
    )
    pairings = _parse(provider)
    assert pairings["violent_crime_rate"].derived is False

    recomputed = _entry(
        ratio_id="violent_per_1000",
        kind="per_capita",
        scale=1000,
        unit="per 1,000",
        numerator="FBI_UCR:summarized_violent_crime:V:offense:absolute_total",
        denominator="FBI_UCR:summarized_violent_crime:V:population",
        datasets=None,
        numerator_is_subset=False,
    )
    with pytest.raises(RatioRegistryError, match="rule 5"):
        _parse(provider, recomputed)


def test_a_provider_rate_names_no_denominator() -> None:
    """Covers: ETL-104 — a provider rate is the provider's, with no denominator of ours."""
    with pytest.raises(RatioRegistryError, match="provider_rate"):
        _parse(
            _entry(
                kind="provider_rate",
                provider_rate="CENSUS_PEP:RBIRTH",
                numerator="CENSUS_PEP:BIRTHS",
                denominator="CENSUS_PEP:POPESTIMATE",
                datasets=None,
            )
        )


def test_only_approved_pairings_are_served_unless_drafts_are_opted_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Covers: ETL-104 — a draft is a proposal, not a served view."""
    draft = _parse(_entry(status="draft"))["poverty_share:acs5"]
    approved = _parse(_entry())["poverty_share:acs5"]
    monkeypatch.delenv(SERVE_DRAFTS_ENV, raising=False)
    assert approved.is_served() and approved.refusal() is None
    assert not draft.is_served()
    assert "draft" in draft.refusal()
    monkeypatch.setenv(SERVE_DRAFTS_ENV, "1")
    assert draft.is_served()
    deprecated = _parse(_entry(status="deprecated"))["poverty_share:acs5"]
    assert not deprecated.is_served()


def test_the_shipped_registry_loads_and_matches_its_schema_fields() -> None:
    """Covers: ETL-104 — every entry carries the schema's required fields and no others."""
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    item = schema["properties"]["entries"]["items"]
    allowed = set(item["properties"])
    document = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    for entry in document["entries"]:
        assert set(item["required"]) <= set(entry), entry["ratio_id"]
        assert set(entry) <= allowed, entry["ratio_id"]
    pairings = load_registry()
    assert len(pairings) >= 60


def test_every_numerator_is_a_metric_the_place_pages_define() -> None:
    """Covers: ETL-104 — a pairing divides a measure readers already see, by its code."""
    defined = set(definitions())
    for pairing in load_registry().values():
        if pairing.kind == "agency_rate":
            # A county agency rate (ADR-0012) stands on the safety card in
            # place of the FBI rate the county does not have.
            assert pairing.reads_beside in defined, pairing.ratio_id
        elif pairing.derived:
            assert pairing.numerator in defined, pairing.ratio_id
        else:
            # The count or the provider's rate is on a page already.
            assert {pairing.numerator, pairing.provider_rate} & defined, (
                pairing.ratio_id
            )


def test_a_ratio_divides_one_measure_by_another_it_is_not_part_of() -> None:
    """Covers: ETL-114 — property tax over home value is a reviewed ratio, never a share."""
    shipped = load_registry()
    for dataset in ("acs5", "acs1"):
        pairing = shipped[f"property_tax_to_home_value:{dataset}"]
        assert pairing.kind == "ratio" and pairing.derived
        assert pairing.numerator == f"CENSUS_ACS:{dataset}:B25103_001"
        assert pairing.denominator == f"CENSUS_ACS:{dataset}:B25077_001"
        assert pairing.scale == 100 and not pairing.numerator_is_subset
        assert pairing.status == "approved"

    ratio = _entry(
        ratio_id="property_tax_to_home_value",
        kind="ratio",
        numerator="CENSUS_ACS:{dataset}:B25103_001",
        denominator="CENSUS_ACS:{dataset}:B25077_001",
        numerator_is_subset=None,
    )
    assert _parse(ratio)["property_tax_to_home_value:acs5"].kind == "ratio"
    with pytest.raises(RatioRegistryError, match="not a part of"):
        _parse({**ratio, "numerator_is_subset": True})
    # Same program and dataset, as for every pairing (ADR-0009 rule 2).
    with pytest.raises(RatioRegistryError, match="same program"):
        _parse({**ratio, "denominator": "CENSUS_PEP:POP", "datasets": ["acs5"]})


def test_a_time_cost_is_minutes_of_a_qcew_wage_for_each_bls_price_area() -> None:
    """Covers: ETL-104 — minutes of work, the ADR-0009 amendment of 2026-11-06."""
    shipped = load_registry()
    for area in ("0000", "0100", "0200", "0300", "0400"):
        pairing = shipped[f"minutes_of_work_electricity:{area}"]
        assert pairing.kind == "time_cost" and pairing.derived
        assert pairing.numerator == f"BLS:APU{area}72610"
        assert pairing.denominator == "BLS_QCEW:annual_avg_weekly_wage:10:0"
        assert (pairing.scale, pairing.hours_per_week, pairing.area) == (60, 40, area)
        assert set(pairing.wage_geo_levels) == {"COUNTY", "STATE", "NATIONAL"}
        assert pairing.status == "approved" and pairing.dataset is None

    cost = _entry(
        ratio_id="minutes_of_work_eggs",
        kind="time_cost",
        scale=60,
        unit="minutes of work",
        numerator="BLS:APU{area}708111",
        denominator="BLS_QCEW:annual_avg_weekly_wage:10:0",
        datasets=None,
        areas=["0000", "0200"],
        hours_per_week=40,
        wage_geo_levels=["COUNTY", "STATE", "NATIONAL"],
        numerator_is_subset=None,
    )
    # Two programs are the point of a time cost, so rule 2 does not refuse it.
    assert sorted(_parse(cost)) == [
        "minutes_of_work_eggs:0000",
        "minutes_of_work_eggs:0200",
    ]
    for broken in (
        {"scale": 100},
        {"hours_per_week": None},
        {"wage_geo_levels": ["METRO"]},
        {"denominator": "CENSUS_ACS:acs5:B19013_001"},
        {"areas": None},
    ):
        with pytest.raises(RatioRegistryError):
            _parse({**cost, **broken})
    # Only a time cost names a working week.
    with pytest.raises(RatioRegistryError, match="hours_per_week"):
        _parse(_entry(hours_per_week=40))
