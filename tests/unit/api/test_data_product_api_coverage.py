"""Every source-scoped API route has one authoritative end-to-end owner.

The warehouse half of this contract is in
``tests/unit/shared/test_data_product_e2e_coverage.py``; the routes live here
because only the API environment installs FastAPI.
"""

from __future__ import annotations

import pytest

from apps.api.main import create_app
from apps.api.versioning import legacy_path_for
from data_ingestion_toolbox.config import Settings
from tests.support.product_coverage import PRODUCTS, SHARED_API_PREFIXES

pytestmark = [pytest.mark.unit, pytest.mark.api]


def _application_routes() -> set[str]:
    """Read the served contract from OpenAPI rather than the router objects.

    FastAPI includes routers lazily, so ``app.routes`` still holds unexpanded
    ``_IncludedRouter`` entries with no path; the generated schema is the
    surface a consumer actually sees.

    Versioned paths collapse to their unversioned form first. A product owns a
    *resource*, and ``/api/v1/cdc/observations`` is the same resource as
    ``/api/cdc/observations`` with the same end-to-end owner -- requiring a
    separate claim per version would make the registry a list of mount points
    rather than of data products. The collapse hides nothing: a route served only
    under a version still normalizes into ``/api/...`` and still needs an owner.
    """
    paths = create_app(Settings()).openapi()["paths"]
    return {legacy_path_for(path) for path in paths if path.startswith("/api/")}


def test_every_registered_api_route_is_served_by_the_application() -> None:
    """Covers: E2E-008 — a removed route cannot leave the registry claiming it."""
    served = _application_routes()
    for product in PRODUCTS:
        missing = sorted(set(product.api_routes) - served)
        assert not missing, (
            f"{product.product_id} claims routes the application does not "
            f"serve: {missing}"
        )


def test_every_source_scoped_api_route_is_claimed_by_a_product() -> None:
    """Covers: E2E-008 — a new source router cannot ship without E2E evidence."""
    claimed = {route for product in PRODUCTS for route in product.api_routes}
    unclaimed = sorted(
        route
        for route in _application_routes()
        if not route.startswith(SHARED_API_PREFIXES) and route not in claimed
    )
    assert not unclaimed, (
        "these source-scoped API routes have no registered end-to-end owner: "
        f"{unclaimed}"
    )
