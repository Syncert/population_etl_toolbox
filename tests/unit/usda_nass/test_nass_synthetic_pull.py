"""The USDA NASS live-pull path, executed against synthetic provider responses.

The orchestrated DAG suite stubs Quick Stats at the ``fetch_*`` function
boundary, and the client unit tests drive the transport with minimal payloads.
Neither executes the exact path the scheduled external tier calls: the real
``fetch_slice_count``/``fetch_param_values``/``fetch_slice_records`` transport
followed by the contract assertions in
``tests/external/test_nass_source_contracts.py``.

These tests close that gap without a credential or a network. The provider is
a scripted httpx-shaped client serving the documented Quick Stats envelopes —
``{"count": "…"}``, ``{"<param>": […]}``, ``{"data": […]}`` — built from the
reviewed fixture rows and the registry's own selections, and every live
assertion the external module makes is applied to the result. What remains for
the first credentialed ``external-contract`` run is only the question these
tests cannot answer: whether the provider still matches the registry.
"""

from __future__ import annotations

import json

import pytest

from data_ingestion_toolbox.usda_nass.client import (
    count_parameters,
    fetch_param_values,
    fetch_slice_count,
    fetch_slice_records,
)
from data_ingestion_toolbox.usda_nass.config import QUICK_STATS_MAX_RECORDS
from data_ingestion_toolbox.usda_nass.registry import (
    NassProduct,
    NassSlice,
    enabled_products,
)
from data_ingestion_toolbox.usda_nass.silver_nass.values import parse_slice_rows

from ._doubles import RecordingClient, deterministic_config, json_response, load_fixture

pytestmark = pytest.mark.unit

#: Same list the external module checks against the provider's live domain.
DOMAIN_PARAMETERS: tuple[str, ...] = (
    "source_desc",
    "sector_desc",
    "group_desc",
    "commodity_desc",
    "statisticcat_desc",
    "domain_desc",
    "freq_desc",
    "agg_level_desc",
)

#: Plausible values the registry does not select, so the subset assertion is
#: proven against a domain larger than the selections rather than an equal one.
DOMAIN_DECOYS: dict[str, tuple[str, ...]] = {
    "source_desc": ("CENSUS", "SURVEY"),
    "sector_desc": ("ANIMALS & PRODUCTS", "ECONOMICS"),
    "group_desc": ("VEGETABLES", "FRUIT & TREE NUTS"),
    "commodity_desc": ("RICE", "BARLEY", "OATS"),
    "statisticcat_desc": ("SALES", "INVENTORY"),
    "domain_desc": ("IRRIGATION STATUS", "ECONOMIC CLASS"),
    "freq_desc": ("MONTHLY", "POINT IN TIME"),
    "agg_level_desc": ("ZIP CODE", "WATERSHED"),
}


def _widest_slice(product: NassProduct) -> NassSlice:
    """Mirror the external module's choice of the most record-heavy partition."""
    for level in ("COUNTY", "STATE", "NATIONAL"):
        if level in product.agg_level_descs:
            return NassSlice(product.product_id, level, product.year_end)
    raise AssertionError(f"{product.product_id} registers no supported aggregate level")


def _fixture_rows(product: NassProduct, item: NassSlice) -> list[dict[str, object]]:
    """Return the reviewed rows for one slice, stamped with the requested year.

    This is the same shaping the orchestrated stub applies: the reviewed sample
    covers one year per product, so the requested year is stamped on and every
    other field stays exactly as reviewed.
    """
    document = load_fixture(product.product_id)
    level = document["slices"][item.agg_level_desc]
    return [{**row, "year": str(item.year)} for row in level["data"]["data"]]


def _registered_selections(parameter: str) -> set[str]:
    selections: set[str] = set()
    for product in enabled_products():
        if parameter == "statisticcat_desc":
            selections.update(product.statisticcat_descs)
        elif parameter == "agg_level_desc":
            selections.update(product.agg_level_descs)
        elif parameter == "freq_desc":
            selections.update(product.freq_descs)
        else:
            selections.add(getattr(product, parameter))
    return selections


@pytest.mark.parametrize(
    "product", enabled_products(), ids=lambda product: product.product_id
)
def test_synthetic_count_preflight_satisfies_the_live_contract_assertions(
    product: NassProduct,
) -> None:
    """Covers: EXT-014 — the ceiling-preflight assertions execute end to end."""
    config = deterministic_config()
    item = _widest_slice(product)
    rows = _fixture_rows(product, item)
    client = RecordingClient([json_response({"count": str(len(rows))})])

    response = fetch_slice_count(product, item, config=config, client=client)

    # The transport carried the credential; nothing durable did.
    assert client.calls[0]["params"]["key"] == config.usda_nass_api_key
    assert "key" not in response.request_parameters
    assert response.request_parameters == count_parameters(product, item)

    # The exact assertions the external module makes on a live preflight.
    assert response.http_status == 200
    assert response.count > 0
    assert response.count <= config.slice_record_limit <= QUICK_STATS_MAX_RECORDS


@pytest.mark.parametrize("parameter", DOMAIN_PARAMETERS)
def test_synthetic_domain_pull_confirms_every_registered_selection(
    parameter: str,
) -> None:
    """Covers: EXT-014 — the domain-membership assertion executes end to end."""
    registered = _registered_selections(parameter)
    domain = sorted(registered | set(DOMAIN_DECOYS[parameter]))
    client = RecordingClient([json_response({parameter: domain})])

    response = fetch_param_values(
        parameter, config=deterministic_config(), client=client
    )

    assert response.http_status == 200
    published = set(response.values)
    assert registered <= published
    # Where a decoy exists outside the selections, prove the subset check ran
    # against a strictly larger domain. source_desc has no such value: SURVEY
    # and CENSUS are both registered and are the provider's whole domain.
    extras = set(DOMAIN_DECOYS[parameter]) - registered
    if extras:
        assert extras <= published - registered


@pytest.mark.parametrize(
    "product", enabled_products(), ids=lambda product: product.product_id
)
def test_synthetic_record_pull_parses_to_observations(
    product: NassProduct,
) -> None:
    """Covers: EXT-014 — a fixture-backed api_GET pull reaches typed observations."""
    config = deterministic_config()
    item = _widest_slice(product)
    rows = _fixture_rows(product, item)
    client = RecordingClient([json_response({"data": rows})])

    response = fetch_slice_records(product, item, config=config, client=client)
    assert response.row_count == len(rows)

    served = json.loads(response.raw_bytes)["data"]
    result = parse_slice_rows(
        served,
        product=product,
        release_watermark="synthetic-release",
        slice_key=item.slice_key,
    )

    assert result.observations
    assert len(result.observations) + len(result.quarantined) == len(rows)
    assert not result.quarantined, (
        f"reviewed {product.product_id} rows no longer parse cleanly: "
        f"{[record.error_code for record in result.quarantined][:3]}"
    )
    registered_statistics = set(product.statisticcat_descs)
    assert {
        record.statistic.statisticcat_desc for record in result.observations
    } <= registered_statistics
