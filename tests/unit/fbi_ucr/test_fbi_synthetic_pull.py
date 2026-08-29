"""The FBI live-pull path, executed against synthetic provider responses.

The orchestrated DAG suite stubs the FBI provider at the ``fetch_*`` function
boundary, and the client unit tests drive the transport with minimal payloads.
Neither executes the exact path the scheduled external tier calls: the real
``fetch_summarized_observations``/``fetch_agency_directory`` transport followed
by the contract assertions in ``tests/external/test_fbi_source_contracts.py``.

These tests close that gap without a credential or a network. A scripted
httpx-shaped client serves the reviewed fixture bytes exactly as the Crime
Data Explorer served them, the real client code fetches through it, and every
live assertion the external module makes is applied to the result. What
remains for the first credentialed ``external-contract`` run is only the
question these tests cannot answer: whether the provider still matches the
fixtures.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from data_ingestion_toolbox.fbi_ucr.client import (
    fetch_agency_directory,
    fetch_summarized_observations,
    observation_parameters,
)
from data_ingestion_toolbox.fbi_ucr.config import API_KEY_PARAMETER, FbiUcrConfig
from data_ingestion_toolbox.fbi_ucr.metadata import parse_release
from data_ingestion_toolbox.fbi_ucr.registry import (
    COUNTED_ENTITY_BASES,
    MEASURE_FORMS,
    NATIONAL_SUBJECT_LABEL,
    ORI_PATTERN,
    FbiSubject,
    FbiUcrProduct,
    agency_directory_endpoint,
    enabled_products,
)
from data_ingestion_toolbox.fbi_ucr.silver_fbi.agency import parse_agency_directory
from data_ingestion_toolbox.fbi_ucr.silver_fbi.offenses import (
    parse_summarized_observations,
)

from ._doubles import API_KEY, ScriptedCdeClient, cde_response

pytestmark = pytest.mark.unit

FIXTURE_DIR = pathlib.Path(__file__).resolve().parents[2] / "fixtures" / "fbi_ucr"


def _fixture_bytes(name: str) -> bytes:
    """Serve the reviewed fixture exactly as captured, not re-serialized."""
    return (FIXTURE_DIR / f"{name}.json").read_bytes()


def _config() -> FbiUcrConfig:
    return FbiUcrConfig(cde_api_key=API_KEY, min_spacing_seconds=0.0)


def _period_ordinal(period: str) -> int:
    month, year = period.split("-")
    return int(year) * 12 + int(month) - 1


@pytest.mark.parametrize(
    "product", enabled_products(), ids=lambda product: product.product_id
)
def test_synthetic_national_pull_satisfies_the_live_contract_assertions(
    product: FbiUcrProduct,
) -> None:
    """Covers: EXT-013 — the live-pull assertions execute against fixture bytes."""
    client = ScriptedCdeClient(
        [cde_response(200, raw=_fixture_bytes("summarized_national_V"))]
    )
    subject = FbiSubject("national", "US")

    response = fetch_summarized_observations(
        product, subject, config=_config(), client=client
    )

    # The transport carried the credential; nothing durable did.
    assert client.calls == 1
    outgoing = client.requests[0][1]["params"]
    assert client.requests[0][1]["headers"]["X-Api-Key"] == API_KEY
    assert API_KEY_PARAMETER not in outgoing
    assert API_KEY_PARAMETER not in response.request_parameters
    assert response.request_parameters == observation_parameters(product)
    assert response.http_status == 200

    # The exact assertions the external module makes on a live response.
    release = parse_release(response.raw_bytes)
    assert release.release_key == release.refresh_date.isoformat()
    assert _period_ordinal(release.max_data_period) >= _period_ordinal(
        product.period_end
    )
    document = json.loads(response.raw_bytes)
    assert {"actuals", "rates"} <= set(document["offenses"])


@pytest.mark.parametrize(
    "product", enabled_products(), ids=lambda product: product.product_id
)
def test_synthetic_national_pull_parses_to_observations(
    product: FbiUcrProduct,
) -> None:
    """Covers: EXT-013 — fixture bytes fetched through the real transport parse."""
    client = ScriptedCdeClient(
        [cde_response(200, raw=_fixture_bytes("summarized_national_V"))]
    )
    subject = FbiSubject("national", "US")

    response = fetch_summarized_observations(
        product, subject, config=_config(), client=client
    )
    release = parse_release(response.raw_bytes)
    result = parse_summarized_observations(
        json.loads(response.raw_bytes),
        product=product,
        release_key=release.release_key,
        subject=subject,
        label=NATIONAL_SUBJECT_LABEL,
        slice_key="synthetic:national",
    )

    assert result.observations
    assert not result.quarantined
    valid_measures = {
        product.measure_id(basis, form)
        for basis in COUNTED_ENTITY_BASES.values()
        for form, _unit in MEASURE_FORMS.values()
    }
    assert {record.measure_id for record in result.observations} <= valid_measures
    assert {record.period for record in result.observations} <= set(
        product.expected_periods
    )


@pytest.mark.parametrize(
    "product", enabled_products(), ids=lambda product: product.product_id
)
def test_synthetic_agency_pull_publishes_every_registered_ori(
    product: FbiUcrProduct,
) -> None:
    """Covers: EXT-013 — the directory pull and ORI-coverage assertions execute."""
    for state_code in product.reference_states:
        client = ScriptedCdeClient(
            [cde_response(200, raw=_fixture_bytes(f"agency_directory_{state_code}"))]
        )

        response = fetch_agency_directory(state_code, config=_config(), client=client)

        assert response.endpoint == agency_directory_endpoint(state_code)
        assert API_KEY_PARAMETER not in response.request_parameters

        parsed = parse_agency_directory(
            json.loads(response.raw_bytes),
            state_code=state_code,
            slice_key=f"synthetic:{state_code}",
        )
        assert parsed.agencies
        assert not parsed.quarantined
        published = {record.ori for record in parsed.agencies}
        assert all(ORI_PATTERN.fullmatch(ori) for ori in published)
        registered = {ori for ori in product.agency_scope if ori.startswith(state_code)}
        assert registered <= published
