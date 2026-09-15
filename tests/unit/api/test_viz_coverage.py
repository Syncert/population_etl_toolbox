"""Every web visualization, every source, and a reviewed answer for each cell.

The agreement sweeps this repository already runs ask whether the API serves
what the catalog publishes. This asks the question one level up, the one a
reader's blank screen actually answers: of the presentations the web app
offers, which ones can each source be drawn in, and where a source cannot be,
is that a reviewed policy or a capability that quietly went away.

Nothing here needs a warehouse. The capability payload is the one
``/catalog/capabilities`` serves -- built by the same function, from the same
served OpenAPI document -- so a verdict reached here is a verdict about what a
browser would discover.
"""

from __future__ import annotations

import json

import pytest

from apps.api.main import app
from apps.api.registry import OBSERVATION_DISPATCH, SOURCE_DISCOVERY
from apps.api.services.catalog_service import list_source_capabilities
from tests.support.regenerate_viz_coverage import (
    SNAPSHOT_PATH,
    application_query_parameters,
    build_snapshot,
)
from tests.support.source_grains import ADVERTISED_GEO_GRAINS
from tests.support.viz_coverage import (
    ANALYSIS_POLICY,
    ANALYSIS_SURFACES,
    REDUCTION_POLICY,
    REDUCTION_SURFACES,
    REVIEWED_DECLINES,
    SPATIAL_POLICY,
    SPATIAL_SURFACES,
    VIZ_SURFACES,
    coverage_matrix,
    describe,
    drawable_tile_grains,
    published_decline_reason,
    surfaces_by_id,
)

pytestmark = [pytest.mark.unit, pytest.mark.api]


@pytest.fixture(scope="module")
def openapi_paths() -> dict:
    return app.openapi()["paths"]


@pytest.fixture(scope="module")
def capabilities(openapi_paths: dict) -> list[dict]:
    return [item.model_dump() for item in list_source_capabilities(openapi_paths).items]


@pytest.fixture(scope="module")
def matrix(openapi_paths: dict, capabilities: list[dict]) -> list:
    return coverage_matrix(
        capabilities,
        application_parameters=application_query_parameters(openapi_paths),
    )


def test_every_surface_names_routes_the_application_actually_serves(
    openapi_paths: dict,
) -> None:
    """Covers: API-138 — a renamed route cannot leave a visualization claiming it."""
    served = {
        path[len("/api/v1") :] if path.startswith("/api/v1") else path
        for path, item in openapi_paths.items()
        if (item or {}).get("get") is not None
    }
    unserved = sorted(
        f"{surface.surface_id} -> {path}"
        for surface in VIZ_SURFACES
        for path in surface.all_paths
        if path not in served
    )
    assert not unserved, (
        "these visualizations name routes the application does not serve, so "
        "the surface is unreachable and the matrix below it is fiction: "
        f"{unserved}"
    )


def test_every_surface_sends_only_parameters_its_routes_declare(
    openapi_paths: dict,
) -> None:
    """Covers: API-138 — a dropped parameter fails here, not in a browser."""
    declared = application_query_parameters(openapi_paths)
    undeclared = sorted(
        f"{surface.surface_id} sends {name} to {path}, which accepts "
        f"{declared.get(path, [])}"
        for surface in VIZ_SURFACES
        for path, names in surface.required_parameters.items()
        for name in names
        if name not in set(declared.get(path, ()))
    )
    assert not undeclared, (
        "these visualizations send query parameters the served contract does "
        f"not declare: {undeclared}"
    )


def test_every_registered_source_has_a_verdict_in_every_surface(matrix) -> None:
    """Covers: API-138 — a new source cannot land without a verdict per surface."""
    expected = {
        (surface.surface_id, source_code)
        for surface in VIZ_SURFACES
        for source_code in SOURCE_DISCOVERY
    }
    actual = {(verdict.surface_id, verdict.source_code) for verdict in matrix}
    assert actual == expected, (
        "the coverage matrix and the registered sources disagree; missing "
        f"{sorted(expected - actual)}, unexpected {sorted(actual - expected)}"
    )


def test_a_declining_cell_is_one_the_registry_reviewed(matrix) -> None:
    """Covers: API-138 — coverage shrinks only on purpose, and grows visibly."""
    computed = {
        (verdict.surface_id, verdict.source_code): verdict.reason
        for verdict in matrix
        if not verdict.served
    }
    reviewed = REVIEWED_DECLINES

    unreviewed = sorted(
        f"{surface_id}/{source_code}: {reason}"
        for (surface_id, source_code), reason in computed.items()
        if (surface_id, source_code) not in reviewed
    )
    assert not unreviewed, (
        "these source/visualization pairs stopped being servable and no "
        "reviewed policy says they should have; every one of them is a screen "
        "a reader can reach and find empty:\n" + "\n".join(unreviewed) + "\n\n"
        f"current coverage:\n{describe(matrix)}"
    )

    stale = sorted(
        f"{surface_id}/{source_code}"
        for (surface_id, source_code) in reviewed
        if (surface_id, source_code) not in computed
    )
    assert not stale, (
        "these pairs are recorded as declined but the API now serves them; a "
        "stale refusal hides the next real one and keeps a working "
        f"presentation off the screen: {stale}"
    )


def test_an_analysis_decline_is_the_registrys_own_refusal() -> None:
    """Covers: API-138 — the reviewed table cannot state a policy the API does not hold.

    The table is written out so that flipping ``analysis_ready`` fails loudly
    instead of moving the cell and its expectation together. That only helps
    if the table is also checked against the thing it claims to record, in
    both directions: every source the table declines must be one the dispatch
    registry declines, and every source the registry declines must appear for
    every analysis surface.
    """
    registry_declines = {
        source_code
        for source_code, dispatch in OBSERVATION_DISPATCH.items()
        if not dispatch.analysis_ready
    }
    assert registry_declines, (
        "no source is stratified, so the analysis policy grades nothing; if "
        "that is real, the reviewed table and these surfaces need rereading"
    )
    for surface_id in sorted(ANALYSIS_SURFACES | REDUCTION_SURFACES):
        policy_word = (
            ANALYSIS_POLICY if surface_id in ANALYSIS_SURFACES else REDUCTION_POLICY
        )
        tabled = {
            source_code
            for (recorded_surface, source_code), policy in REVIEWED_DECLINES.items()
            if recorded_surface == surface_id and policy == policy_word
        }
        assert tabled == registry_declines, (
            f"{surface_id}: the reviewed table declines {sorted(tabled)} and "
            f"the dispatch registry declines {sorted(registry_declines)}; an "
            "aligned analysis is offered or refused per source, not per screen"
        )

    unexplained = sorted(
        f"{surface_id}/{source_code}"
        for (surface_id, source_code), policy in REVIEWED_DECLINES.items()
        if policy in (ANALYSIS_POLICY, REDUCTION_POLICY)
        and not published_decline_reason(surface_id, source_code)
    )
    assert not unexplained, (
        "an analysis refusal is served by `/comparison/preflight`, "
        "`/distribution/bins`, a stored analysis document and the "
        "per-geography reduction; a cell the registry cannot explain leaves a "
        f"reader with a blank panel and no sentence: {unexplained}"
    )


def test_a_spatial_decline_is_a_fact_about_the_sources_own_grains() -> None:
    """Covers: API-138 — a source with no mappable grain gets no map, not an empty one."""
    drawable = set(drawable_tile_grains())
    assert drawable, "the web declares no drawable grain; every map would decline"
    assert set(ADVERTISED_GEO_GRAINS) == set(OBSERVATION_DISPATCH), (
        "the reviewed grain ranges and the dispatch registry name different "
        f"sources: {sorted(set(ADVERTISED_GEO_GRAINS) ^ set(OBSERVATION_DISPATCH))}"
    )
    for surface_id in SPATIAL_SURFACES:
        for source_code, grains in ADVERTISED_GEO_GRAINS.items():
            mappable = bool(grains & drawable)
            recorded = REVIEWED_DECLINES.get((surface_id, source_code))
            assert mappable != (recorded == SPATIAL_POLICY), (
                f"{surface_id}/{source_code}: publishes {sorted(grains)}, the "
                f"boundary draws {sorted(drawable)}, and the reviewed table "
                f"{'declines' if recorded else 'serves'} it"
            )
            if recorded == SPATIAL_POLICY:
                reason = published_decline_reason(surface_id, source_code) or ""
                assert source_code in reason and "spatial presentation" in reason, (
                    f"{surface_id}/{source_code}: the decline has no reason a "
                    f"reader could be shown, only {reason!r}"
                )


def test_every_surface_serves_at_least_one_source(matrix) -> None:
    """Covers: API-138 — a presentation nothing can be drawn in is dead screen."""
    served_by_surface: dict[str, list[str]] = {
        surface.surface_id: [] for surface in VIZ_SURFACES
    }
    for verdict in matrix:
        if verdict.served:
            served_by_surface[verdict.surface_id].append(verdict.source_code)
    empty = sorted(
        surface_id for surface_id, sources in served_by_surface.items() if not sources
    )
    assert not empty, (
        "the web app offers these presentations and no source can be drawn in "
        f"any of them: {empty}"
    )


def test_the_unrestricted_surfaces_reach_every_registered_source(matrix) -> None:
    """Covers: API-138 — the stated goal, asserted rather than assumed.

    Every presentation that no reviewed policy restricts must answer for all
    seven sources. That is the whole coverage claim -- the explorer's map
    where the boundary can draw one, its trend, table, export, quality panel
    and as-released reading, the workbench's multi-series chart, and the
    composed profile, for BLS, CDC, Census ACS, Census PEP, FBI UCR, FRED and
    USDA NASS alike -- and stating it as a bound is what makes losing one cell
    a failure instead of a smaller number nobody reads.

    The three restrictions are named sets rather than exceptions granted case
    by case: the aligned analysis routes, the per-geography reductions, and a
    spatial presentation. A surface in none of them has no way to serve six of
    seven sources and still pass.
    """
    universal = [
        surface
        for surface in VIZ_SURFACES
        if surface.surface_id
        not in ANALYSIS_SURFACES | REDUCTION_SURFACES | SPATIAL_SURFACES
    ]
    assert universal, "no surface is claimed universal; the bound would be vacuous"
    gaps = sorted(
        f"{verdict.surface_id}/{verdict.source_code}: {verdict.reason}"
        for verdict in matrix
        if not verdict.served
        and verdict.surface_id in {surface.surface_id for surface in universal}
    )
    assert not gaps, (
        "these presentations are served for some sources and not others, with "
        "no aligned-analysis, reduction or spatial policy behind the gap:\n"
        + "\n".join(gaps)
    )


def test_the_reviewed_snapshot_matches_the_served_contract() -> None:
    """Covers: API-138 — the matrix the frontend reads is the matrix the API serves.

    The snapshot is the only thing both languages can share. If it drifts from
    what the API declares, the frontend tier grades itself against a contract
    nothing serves -- which is the defect WEB-043 closed one layer down.
    """
    recorded = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    current = build_snapshot()
    assert recorded == current, (
        "tests/fixtures/api/viz_coverage.json is stale. Regenerate it with "
        "`python -m tests.support.regenerate_viz_coverage` and read the diff: "
        "a source leaving a surface's served list is a reader's screen going "
        "blank, not a snapshot to refresh."
    )


def test_the_surface_registry_is_well_formed() -> None:
    """Covers: API-138 — the matrix's own shape is checked, not assumed."""
    indexed = surfaces_by_id()
    assert len(indexed) == len(VIZ_SURFACES)
    # The floor is well under the current count so ordinary edits do not trip
    # it, and far enough above zero that an emptied registry cannot report the
    # same green as a complete one.
    assert len(VIZ_SURFACES) >= 10
    pages = {surface.page for surface in VIZ_SURFACES}
    assert {"/explore", "/compare", "/workbench", "/profiles"} <= pages
