"""Summarized-offense measure identity, missing reports, and coverage."""

from __future__ import annotations

import copy
from decimal import Decimal

import pytest

from data_ingestion_toolbox.fbi_ucr.registry import (
    SUMMARIZED_VIOLENT_CRIME,
    FbiSubject,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.offenses import (
    FbiSubjectLabelError,
    parse_summarized_observations,
    rate_is_recomputable,
    subject_label,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.participation import (
    parse_participation,
    period_bounds,
)

pytestmark = pytest.mark.unit

PRODUCT = SUMMARIZED_VIOLENT_CRIME
RELEASE = "2026-08-15"
NATIONAL = FbiSubject("national", "US")
STATE = FbiSubject("state", "WI")
COUNTY_SHERIFF = FbiSubject("agency", "WI0130000")
TRIBAL = FbiSubject("agency", "WI0400100")
CAMPUS = FbiSubject("agency", "WI0050700")

#: Two measure forms x two counted entities x six months.
SLICE_INPUTS = 24


def _parse(document, subject, label):
    return parse_summarized_observations(
        document,
        product=PRODUCT,
        release_key=RELEASE,
        subject=subject,
        label=label,
        slice_key=subject.slice_key,
    )


def _index(result) -> dict[tuple[str, str, str], object]:
    return {
        (item.measure_form, item.counted_entity_basis, item.period): item
        for item in result.observations
    }


def test_national_slice_reconciles_every_input_to_one_outcome(fbi_payload) -> None:
    """Covers: ETL-025 — inputs equal observations plus quarantined rows."""
    result = _parse(fbi_payload("summarized_national_V"), NATIONAL, "United States")

    assert result.input_count == SLICE_INPUTS
    assert len(result.observations) + len(result.quarantined) == SLICE_INPUTS
    assert not result.quarantined


def test_absolute_totals_and_rates_are_distinct_measures(fbi_payload) -> None:
    """Covers: ETL-023 — a total is never derived from or mixed with a rate."""
    observations = _index(
        _parse(fbi_payload("summarized_national_V"), NATIONAL, "United States")
    )

    total = observations[("absolute_total", "offense", "01-2023")]
    rate = observations[("rate", "offense", "01-2023")]

    assert total.measure_id == "V:offense:absolute_total"
    assert rate.measure_id == "V:offense:rate"
    assert total.value == Decimal("102094")
    assert total.unit == "count"
    assert rate.value == Decimal("31.59")
    assert rate.unit == "per_100000_population"
    assert total.population_denominator is None
    assert rate.population_denominator == Decimal("338357687")


def test_offense_and_clearance_series_never_share_a_measure(fbi_payload) -> None:
    """Covers: ETL-023 — cleared offenses are a different counted entity."""
    observations = _index(
        _parse(fbi_payload("summarized_national_V"), NATIONAL, "United States")
    )

    offense = observations[("absolute_total", "offense", "01-2023")]
    clearance = observations[("absolute_total", "clearance", "01-2023")]

    assert offense.counted_entity_basis == "offense"
    assert clearance.counted_entity_basis == "clearance"
    assert offense.measure_id != clearance.measure_id
    assert offense.value != clearance.value


def test_state_slice_ignores_the_national_comparison_series(fbi_payload) -> None:
    """Covers: ETL-024 — a subject never absorbs another subject's series."""
    document = fbi_payload("summarized_state_WI_V")
    result = _parse(document, STATE, "Wisconsin")

    assert len(result.observations) == SLICE_INPUTS
    assert {item.subject_code for item in result.observations} == {"WI"}
    assert {item.subject_label for item in result.observations} == {"Wisconsin"}
    national_values = {
        Decimal(str(value))
        for value in document["offenses"]["rates"]["United States Offenses"].values()
    }
    published = {
        item.value
        for item in result.observations
        if item.measure_form == "rate" and item.counted_entity_basis == "offense"
    }
    assert not (published & national_values)


def test_agency_slice_publishes_only_the_agency_absolute_totals(
    fbi_payload, agency_names
) -> None:
    """Covers: ETL-024 — agency totals stay at ORI grain."""
    label = subject_label(COUNTY_SHERIFF, agency_names=agency_names)
    result = _parse(fbi_payload("summarized_agency_WI0130000_V"), COUNTY_SHERIFF, label)

    assert label == "Dane County Sheriff's Office"
    assert {item.source_geo_level for item in result.observations} == {
        "fbi_agency:WI0130000"
    }
    assert len(result.observations) == SLICE_INPUTS


def test_agency_series_cannot_be_identified_without_its_reference_slice() -> None:
    """Covers: ETL-024 — an agency label comes only from the reference slice."""
    with pytest.raises(FbiSubjectLabelError, match="WI0130000"):
        subject_label(COUNTY_SHERIFF, agency_names={})


def test_a_month_without_a_report_is_not_zero(fbi_payload, agency_names) -> None:
    """Covers: ETL-006 — an omitted month is not reported, never zero."""
    label = subject_label(TRIBAL, agency_names=agency_names)
    result = _parse(fbi_payload("summarized_agency_WI0400100_V"), TRIBAL, label)
    observations = _index(result)

    missing = observations[("absolute_total", "offense", "03-2023")]
    reported = observations[("absolute_total", "offense", "01-2023")]

    assert missing.value_status == "not_reported"
    assert missing.value is None
    assert missing.value_source is None
    assert reported.value_status == "reported"
    assert reported.value == Decimal("3")


def test_a_published_zero_stays_a_published_zero(fbi_payload, agency_names) -> None:
    """Covers: ETL-006 — a reported zero is a value, not a missing report."""
    label = subject_label(CAMPUS, agency_names=agency_names)
    observations = _index(
        _parse(fbi_payload("summarized_agency_WI0050700_V"), CAMPUS, label)
    )

    zero = observations[("absolute_total", "offense", "01-2023")]

    assert zero.value == Decimal("0")
    assert zero.value_status == "reported"
    assert zero.value_source == "0"


def test_absent_subject_series_quarantines_that_measure(fbi_payload) -> None:
    """Covers: RES-002 — a renamed series never silently loses rows."""
    document = copy.deepcopy(fbi_payload("summarized_national_V"))
    del document["offenses"]["actuals"]["United States Offenses"]

    result = _parse(document, NATIONAL, "United States")

    assert len(result.quarantined) == len(PRODUCT.expected_periods)
    assert {item.error_code for item in result.quarantined} == {"subject_series_absent"}
    assert result.input_count == len(result.observations) + len(result.quarantined)


def test_absent_measure_container_quarantines_every_input(fbi_payload) -> None:
    """Covers: RES-002 — a dropped container is a schema violation."""
    document = copy.deepcopy(fbi_payload("summarized_national_V"))
    del document["offenses"]["rates"]

    result = _parse(document, NATIONAL, "United States")

    assert {item.error_code for item in result.quarantined} == {
        "missing_measure_container"
    }
    assert len(result.quarantined) == SLICE_INPUTS // 2


@pytest.mark.parametrize(
    ("value", "error_code"),
    [("not-a-number", "invalid_numeric_value"), (-5, "negative_measure_value")],
)
def test_unusable_measure_values_are_quarantined(
    value: object, error_code: str, fbi_payload
) -> None:
    """Covers: ETL-022 — a non-numeric or negative count never publishes."""
    document = copy.deepcopy(fbi_payload("summarized_national_V"))
    document["offenses"]["actuals"]["United States Offenses"]["01-2023"] = value

    result = _parse(document, NATIONAL, "United States")

    assert [item.error_code for item in result.quarantined] == [error_code]
    assert result.input_count == len(result.observations) + len(result.quarantined)


def test_non_object_payload_quarantines_the_whole_slice(fbi_payload) -> None:
    """Covers: RES-002 — a list-shaped payload never parses partially."""
    result = _parse(["unexpected"], NATIONAL, "United States")

    assert not result.observations
    assert len(result.quarantined) == SLICE_INPUTS


def test_period_bounds_cover_each_month_including_february() -> None:
    """Covers: ETL-013 — month bounds are inclusive and leap-year correct."""
    assert period_bounds("01-2023")[1].day == 31
    assert period_bounds("02-2023")[1].day == 28
    assert period_bounds("02-2024")[1].day == 29
    assert period_bounds("12-2023")[1].isoformat() == "2023-12-31"


def test_participation_is_published_for_every_registered_month(
    fbi_payload,
) -> None:
    """Covers: ETL-025 — coverage reconciles one row per registered month."""
    result = parse_participation(
        fbi_payload("summarized_state_WI_V"),
        product=PRODUCT,
        release_key=RELEASE,
        subject=STATE,
        subject_label="Wisconsin",
        slice_key=STATE.slice_key,
    )

    assert result.input_count == len(PRODUCT.expected_periods)
    assert len(result.participation) == len(PRODUCT.expected_periods)
    row = {item.period: item for item in result.participation}["01-2023"]
    assert row.population == Decimal("5910955")
    assert row.participated_population == Decimal("5831652")
    assert row.coverage_percent == Decimal("98.66")
    assert row.coverage_basis == "provider_population_coverage_percent"
    assert row.participation_status == "partial_participation"


def test_agency_participation_records_absence_of_a_coverage_percent(
    fbi_payload, agency_names
) -> None:
    """Covers: ETL-006 — an absent coverage percentage is never invented."""
    label = subject_label(COUNTY_SHERIFF, agency_names=agency_names)
    result = parse_participation(
        fbi_payload("summarized_agency_WI0130000_V"),
        product=PRODUCT,
        release_key=RELEASE,
        subject=COUNTY_SHERIFF,
        subject_label=label,
        slice_key=COUNTY_SHERIFF.slice_key,
    )

    row = result.participation[0]

    assert row.coverage_percent is None
    assert row.coverage_basis == "provider_population_only"
    assert row.participation_status == "full_participation"


def test_a_non_reporting_month_is_visible_in_participation(
    fbi_payload, agency_names
) -> None:
    """Covers: ETL-006 — no report is recorded as no participation."""
    label = subject_label(TRIBAL, agency_names=agency_names)
    result = parse_participation(
        fbi_payload("summarized_agency_WI0400100_V"),
        product=PRODUCT,
        release_key=RELEASE,
        subject=TRIBAL,
        subject_label=label,
        slice_key=TRIBAL.slice_key,
    )
    rows = {item.period: item for item in result.participation}

    assert rows["03-2023"].participation_status == "no_participation"
    assert rows["03-2023"].participated_population == Decimal("0")
    assert rows["01-2023"].participation_status == "full_participation"


@pytest.mark.parametrize(
    ("section", "value", "error_code"),
    [
        ("population", "not-a-number", "invalid_participation_value"),
        ("participated_population", -1, "negative_population"),
    ],
)
def test_unusable_participation_values_are_quarantined(
    section: str, value: object, error_code: str, fbi_payload
) -> None:
    """Covers: ETL-022 — invalid coverage never becomes a usable denominator."""
    document = copy.deepcopy(fbi_payload("summarized_state_WI_V"))
    document["populations"][section]["Wisconsin"]["01-2023"] = value

    result = parse_participation(
        document,
        product=PRODUCT,
        release_key=RELEASE,
        subject=STATE,
        subject_label="Wisconsin",
        slice_key=STATE.slice_key,
    )

    assert [item.error_code for item in result.quarantined] == [error_code]


def test_out_of_range_coverage_percentage_is_quarantined(fbi_payload) -> None:
    """Covers: ETL-029 — a coverage percentage outside 0..100 is rejected."""
    document = copy.deepcopy(fbi_payload("summarized_state_WI_V"))
    document["tooltips"]["Percent of Population Coverage"]["Wisconsin"]["01-2023"] = 150

    result = parse_participation(
        document,
        product=PRODUCT,
        release_key=RELEASE,
        subject=STATE,
        subject_label="Wisconsin",
        slice_key=STATE.slice_key,
    )

    assert [item.error_code for item in result.quarantined] == ["coverage_out_of_range"]


def test_participation_above_covered_population_is_quarantined(fbi_payload) -> None:
    """Covers: ETL-029 — more participation than population is impossible."""
    document = copy.deepcopy(fbi_payload("summarized_state_WI_V"))
    document["populations"]["participated_population"]["Wisconsin"]["01-2023"] = (
        10_000_000
    )

    result = parse_participation(
        document,
        product=PRODUCT,
        release_key=RELEASE,
        subject=STATE,
        subject_label="Wisconsin",
        slice_key=STATE.slice_key,
    )

    assert [item.error_code for item in result.quarantined] == [
        "participation_exceeds_population"
    ]


def test_rate_recomputation_requires_a_compatible_basis() -> None:
    """Covers: ETL-029 — a rate is not recomputable without both inputs."""
    assert rate_is_recomputable(Decimal("10"), Decimal("100000"))
    assert not rate_is_recomputable(None, Decimal("100000"))
    assert not rate_is_recomputable(Decimal("10"), None)
    assert not rate_is_recomputable(Decimal("10"), Decimal("0"))
