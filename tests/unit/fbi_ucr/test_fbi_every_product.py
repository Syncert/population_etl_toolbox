"""Every registered summarized offense replays as its own isolated product."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from uuid import uuid4

import pytest

from data_ingestion_toolbox.fbi_ucr.registry import (
    ALL_PRODUCTS,
    COUNTED_ENTITY_BASES,
    MEASURE_FORMS,
    FbiSubject,
    FbiUcrProduct,
    agency_directory_endpoint,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.replay import (
    CapturedSlice,
    replay_slices,
)

from .conftest import fixture_scoped, load_bytes, load_payload, observation_fixture

pytestmark = pytest.mark.unit

FIXTURE_PRODUCTS = tuple(fixture_scoped(product) for product in ALL_PRODUCTS)

RELEASE = "2026-09-15"
PRODUCT_IDS = {"ids": lambda item: item.product_id}


def _slice(endpoint: str, payload: bytes) -> CapturedSlice:
    return CapturedSlice(
        uuid4(), endpoint, payload, hashlib.sha256(payload).hexdigest()
    )


def _slices(
    product: FbiUcrProduct, payloads: dict[str, bytes] | None = None
) -> dict[str, CapturedSlice]:
    slices = {
        agency_directory_endpoint(state): _slice(
            agency_directory_endpoint(state), load_bytes(f"agency_directory_{state}")
        )
        for state in product.reference_states
    }
    for subject in product.subjects:
        endpoint = product.observation_endpoint(subject)
        payload = (payloads or {}).get(endpoint) or load_bytes(
            observation_fixture(product, subject)
        )
        slices[endpoint] = _slice(endpoint, payload)
    return slices


def _published_measures(product: FbiUcrProduct) -> set[str]:
    return {
        product.measure_id(basis, measure_form)
        for measure_form, _unit in MEASURE_FORMS.values()
        for basis in COUNTED_ENTITY_BASES.values()
    }


@pytest.mark.parametrize("product", FIXTURE_PRODUCTS, **PRODUCT_IDS)
def test_each_product_emits_exactly_the_measures_its_provider_publishes(
    product: FbiUcrProduct,
) -> None:
    """Covers: ETL-052 — each offense emits its own four published measures."""
    result = replay_slices(product, _slices(product), release_key=RELEASE)

    # Every captured offense publishes actuals and rates for offenses and
    # clearances at every grain (SOURCE_NOTES.md), so all four are present.
    assert {item.measure_id for item in result.observations} == (
        _published_measures(product)
    )
    assert all(
        item.measure_id.startswith(f"{product.offense_code}:")
        for item in result.observations
    )
    assert {item.product_id for item in result.observations} == {product.product_id}
    assert {item.offense_code for item in result.observations} == {product.offense_code}
    assert {item.product_id for item in result.participation} <= {product.product_id}


@pytest.mark.parametrize("product", FIXTURE_PRODUCTS, **PRODUCT_IDS)
def test_each_product_publishes_its_own_provider_values(
    product: FbiUcrProduct,
) -> None:
    """Covers: ETL-052 — a product's values come from its own payload only."""
    national = FbiSubject("national", "US")
    document = load_payload(observation_fixture(product, national))
    expected = document["offenses"]["actuals"]["United States Offenses"]["01-2023"]

    result = replay_slices(product, _slices(product), release_key=RELEASE)
    [observation] = [
        item
        for item in result.observations
        if item.subject_type == "national"
        and item.period == "01-2023"
        and item.measure_id == product.measure_id("offense", "absolute_total")
    ]

    assert observation.value == Decimal(str(expected))


def test_no_two_products_share_a_record_or_a_value_series() -> None:
    """Covers: ETL-052 — one product's series never lands in another's."""
    record_ids: dict[str, str] = {}
    national_series: dict[str, tuple] = {}
    for product in FIXTURE_PRODUCTS:
        result = replay_slices(product, _slices(product), release_key=RELEASE)
        for item in result.observations:
            assert record_ids.setdefault(item.source_record_id, product.product_id) == (
                product.product_id
            )
        national_series[product.product_id] = tuple(
            item.value
            for item in result.observations
            if item.subject_type == "national"
            and item.measure_id == product.measure_id("offense", "absolute_total")
        )

    assert len(set(national_series.values())) == len(FIXTURE_PRODUCTS)


@pytest.mark.parametrize("product", FIXTURE_PRODUCTS, **PRODUCT_IDS)
def test_a_series_the_provider_omits_registers_no_measure_and_no_zero(
    product: FbiUcrProduct,
) -> None:
    """Covers: ETL-052 — an absent series is never published or zero-filled."""
    payloads: dict[str, bytes] = {}
    for subject in product.subjects:
        document = json.loads(load_bytes(observation_fixture(product, subject)))
        rates = document["offenses"]["rates"]
        for label in list(rates):
            if label.endswith(" Clearances"):
                del rates[label]
        payloads[product.observation_endpoint(subject)] = json.dumps(document).encode()

    result = replay_slices(product, _slices(product, payloads), release_key=RELEASE)
    omitted = product.measure_id("clearance", "rate")

    assert omitted not in {item.measure_id for item in result.observations}
    assert {item.measure_id for item in result.observations} == (
        _published_measures(product) - {omitted}
    )
    absent = [
        item
        for item in result.quarantined
        if item.error_code == "subject_series_absent"
    ]
    assert len(absent) == len(product.subjects) * len(product.expected_periods)


@pytest.mark.parametrize("product", FIXTURE_PRODUCTS, **PRODUCT_IDS)
def test_states_beyond_wisconsin_resolve_by_their_own_contract(
    product: FbiUcrProduct,
) -> None:
    """Covers: ETL-052 — state subjects resolve without Wisconsin's evidence.

    Pennsylvania reports every month with partial participation; the Virgin
    Islands territory published no month in the window, so every one of its
    observations is ``not_reported`` with a null value -- never a zero.
    """
    result = replay_slices(product, _slices(product), release_key=RELEASE)
    state_rows = [item for item in result.observations if item.subject_type == "state"]

    assert {(item.subject_code, item.subject_label) for item in state_rows} == {
        ("PA", "Pennsylvania"),
        ("VI", "U.S. Virgin Islands"),
        ("WI", "Wisconsin"),
    }
    pennsylvania = [
        item
        for item in state_rows
        if item.subject_code == "PA"
        and item.period.endswith("-2023")
        and item.period <= "06-2023"
        and item.measure_id == product.measure_id("offense", "absolute_total")
    ]
    assert len(pennsylvania) == 6
    assert all(item.value_status == "reported" for item in pennsylvania)
    islands = [item for item in state_rows if item.subject_code == "VI"]
    assert islands
    assert all(item.value is None for item in islands)
    assert {item.value_status for item in islands} == {"not_reported"}
    coverage = {
        item.subject_code: item
        for item in result.participation
        if item.subject_type == "state" and item.period == "01-2023"
    }
    assert set(coverage) == {"PA", "VI", "WI"}
