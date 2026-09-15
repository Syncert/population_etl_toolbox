"""API unit tests: every public analytical read is a cacheable read.

Covers: API-076 — cache eligibility is derived from the routers the
        application mounts, and every served path is classified.

API-063 asserts the authenticated resources stay *out* of the cacheable set.
Nothing asserted the public ones were *in* it, and the hand-written prefix
list reached 8 of the 21 public analytical GETs: the neutral
``/api/v1/observations`` resource missed because the prefix carried a
trailing slash it does not, and every source-scoped route missed because it
begins with a source segment no prefix named. Those are the routes the
consumer guide tells clients to prefer and the routes ``apps/web`` reads.
"""

from __future__ import annotations

import pytest

from apps.api.main import PRIVATE_ROUTERS, PUBLIC_CACHE_TARGETS, app
from apps.api.versioning import UNVERSIONED_PATHS, VERSIONED_ROOT

pytestmark = [pytest.mark.unit, pytest.mark.api]

#: Served paths that are deliberately never cached and are not private user
#: storage: a probe answer must describe now, not the last five minutes.
#:
#: ``/health/content`` (API-137) is the one entry here that reads the
#: warehouse, and it is the entry most worth stating: a monitor asking which
#: sources currently publish a measure must not be answered a picture of the
#: last five minutes, which is precisely the interval an empty warehouse
#: would stay invisible for.
UNCACHED_BY_DESIGN = (
    frozenset({f"{VERSIONED_ROOT}/health", f"{VERSIONED_ROOT}/health/content"})
    | UNVERSIONED_PATHS
)


def _private_paths() -> set[str]:
    return {
        f"{VERSIONED_ROOT}{route.path}"
        for router in PRIVATE_ROUTERS
        for route in router.routes
    }


def _served_get_paths() -> set[str]:
    return {
        path
        for path, item in app.openapi()["paths"].items()
        if "get" in {method.lower() for method in item}
    }


def test_every_public_analytical_read_is_cacheable() -> None:
    """Covers: API-076 — the cached set is the public analytical surface."""
    expected = _served_get_paths() - _private_paths() - UNCACHED_BY_DESIGN
    uncovered = sorted(
        path for path in expected if not PUBLIC_CACHE_TARGETS.covers(path)
    )
    assert uncovered == [], (
        "public analytical reads that answer without x-cache or Cache-Control: "
        f"{uncovered}"
    )


def test_the_neutral_resource_and_every_source_route_are_covered() -> None:
    """Covers: API-076 — the exact paths the prefix list used to miss."""
    for path in (
        f"{VERSIONED_ROOT}/observations",
        f"{VERSIONED_ROOT}/observations/releases",
        f"{VERSIONED_ROOT}/bls/observations/latest",
        f"{VERSIONED_ROOT}/census/observations/timeseries",
        f"{VERSIONED_ROOT}/fred/observations/latest",
        f"{VERSIONED_ROOT}/pep/observations/timeseries",
        f"{VERSIONED_ROOT}/cdc/observations",
        f"{VERSIONED_ROOT}/usda-nass/observations",
        f"{VERSIONED_ROOT}/usda-nass/source-notes",
        f"{VERSIONED_ROOT}/catalog/metrics/CENSUS_ACS:acs5:B01003_001",
    ):
        assert PUBLIC_CACHE_TARGETS.covers(path), path


def test_private_and_probe_paths_are_never_cacheable() -> None:
    """Covers: API-076, API-063 — widening the cache did not widen it onto these."""
    for path in sorted(_private_paths() | UNCACHED_BY_DESIGN):
        assert not PUBLIC_CACHE_TARGETS.covers(path), path
    # A concrete id, not only the template.
    assert not PUBLIC_CACHE_TARGETS.covers(f"{VERSIONED_ROOT}/evidence-packets/17")
    assert not PUBLIC_CACHE_TARGETS.covers(
        f"{VERSIONED_ROOT}/analysis-configurations/17"
    )


def test_a_sibling_path_sharing_a_prefix_is_not_swept_in() -> None:
    """Covers: API-076 — coverage is by path, not by string prefix.

    ``/observations`` as a bare string prefix would also match a resource
    named ``/observations-private``; the parameterised templates are the only
    place a prefix is used, and it ends at the separator.
    """
    assert not PUBLIC_CACHE_TARGETS.covers(f"{VERSIONED_ROOT}/observations-private")
    assert not PUBLIC_CACHE_TARGETS.covers(f"{VERSIONED_ROOT}/catalog/metricsX")
    assert not PUBLIC_CACHE_TARGETS.covers("/api/v2/observations")
