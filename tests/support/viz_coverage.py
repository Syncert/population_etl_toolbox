"""Which web visualization each source can actually be drawn in, and why not.

Every existing coverage sweep in this repository runs along one axis: does the
warehouse publish what the catalog advertises, and does the API serve what the
catalog publishes. `tests/integration/api/test_catalog_serving_agreement.py`
walks it exhaustively, `/health/content` reports it per source, and the
live-stack smoke tier replays it against a deployment.

None of them asks the question a reader asks. A source can publish current
measures, answer `/observations` with rows, satisfy every agreement sweep and
report `serving` -- and still have nothing to draw in the explorer's map, the
workbench's heatmap, the comparison scatter or the distribution histogram,
because the presentation each of those needs rests on a *capability* rather
than on rows: a route that must be declared, a parameter that must be
accepted, a grain the tile boundary must be able to draw. When one of those
goes away the screen goes blank and nothing in the stack reports an error,
which is the same failure mode `/health/content` was built for, one level up.

So this module declares the other axis. Each `VizSurface` below is one
presentation a reader can select, named with the page it lives on and the
module that builds its request, and carrying the exact API capabilities that
presentation needs. Evaluating a surface against a source's own
`/catalog/capabilities` entry -- the entry the web client itself discovers
from, built by `apps.api.services.catalog_service.list_source_capabilities` --
answers "can this source be drawn here" without a warehouse, a browser, or a
deployment.

Three properties make the answer worth trusting:

- **Nothing is inferred.** A surface's requirements are the paths and
  parameters its own web module sends. They are checked against the served
  OpenAPI document, so a route renamed or a parameter dropped fails here
  rather than in a browser.
- **A decline is reviewed, not discovered.** `REVIEWED_DECLINES` names every
  source/surface pair the API deliberately does not serve, with the published
  reason. A cell that starts declining without an entry fails; an entry for a
  cell that has started being served fails too, so coverage can only be lost
  deliberately.
- **The grain question is asked before the rows are.** A source publishing
  only `NATIONAL` has no spatial presentation whatever it serves, and the
  drawable grains are read from the web's own `DRAWABLE_TILE_GRAINS`
  declaration rather than restated here.
"""

from __future__ import annotations

import re

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from apps.api.registry import OBSERVATION_DISPATCH
from tests.support.source_grains import ADVERTISED_GEO_GRAINS

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

#: The versioned root every served path carries. A capability entry publishes
#: absolute paths; a surface declares them version-relative, the way the
#: registry's own `neutral_paths` do.
VERSIONED_ROOT = "/api/v1"

#: The web module that declares which geography grains the tile boundary can
#: draw. Read rather than restated: a grain added there without a serving
#: source behind it, or removed while a surface still claims to draw at it,
#: is exactly the disagreement this matrix exists to name.
TILE_GRAINS_MODULE = REPOSITORY_ROOT / "apps/web/lib/tileGrains.ts"


class VizCoverageError(ValueError):
    """Raised when a declaration cannot describe a real surface or source."""


def drawable_tile_grains() -> tuple[str, ...]:
    """The grains `apps/web/lib/tileGrains.ts` declares the boundary can draw.

    Parsed from the declaration rather than copied. The file states in its own
    comment that three places used to answer this question and did not agree;
    a fourth copy here would restart that.
    """
    source = TILE_GRAINS_MODULE.read_text(encoding="utf-8")
    body = re.search(
        r"DRAWABLE_TILE_GRAINS:\s*readonly\s+DrawableGrain\[\]\s*=\s*\[(.*?)\];",
        source,
        re.DOTALL,
    )
    if body is None:
        raise VizCoverageError(
            f"{TILE_GRAINS_MODULE} declares no DRAWABLE_TILE_GRAINS array; the "
            "map surface has no reviewed grain list to check against"
        )
    grains = tuple(re.findall(r"grain:\s*\"([A-Z_]+)\"", body.group(1)))
    if not grains:
        raise VizCoverageError(
            f"{TILE_GRAINS_MODULE} declares DRAWABLE_TILE_GRAINS with no grain in "
            "it; every map surface would decline for every source"
        )
    return grains


@dataclass(frozen=True, slots=True)
class VizSurface:
    """One presentation a reader can select, and what the API must declare for it.

    ``capability_paths`` is a list of alternatives, each an *all-of* group:
    the surface is reachable when any one group is declared in full. That is
    how the explorer's own access shapes are spelled -- the neutral
    ``/observations`` resource is preferred, and a source publishing only its
    own ``latest``/``timeseries`` pair is still reachable -- so a surface
    states both rather than pretending one is the contract.
    """

    #: ``<page>.<presentation>``; the matrix key.
    surface_id: str
    #: The route the presentation lives on, as the web app mounts it.
    page: str
    #: What a reader sees. Used in failure messages, so it reads as the thing
    #: that broke rather than as an identifier.
    title: str
    #: Repository-relative web modules that build this surface's requests.
    web_modules: tuple[str, ...]
    #: Version-relative paths that must be served by the application at all.
    #: Checked once, against OpenAPI: they are not per-source declarations.
    application_paths: tuple[str, ...] = ()
    #: Alternative all-of groups of version-relative paths the source's own
    #: capability entry must declare. Empty means the surface needs no
    #: per-source route -- the catalog answers it for every source alike.
    capability_paths: tuple[tuple[str, ...], ...] = ()
    #: ``path -> query parameters`` the surface sends. Required of every
    #: declared path the surface actually uses, and of every application path.
    required_parameters: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    #: Neutral ``observation_filters`` the source must declare.
    required_filters: tuple[str, ...] = ()
    #: True when the presentation is spatial, so a source must publish at
    #: least one grain the tile boundary can draw.
    requires_drawable_grain: bool = False
    #: True when the presentation renders the source's own dimensions, so the
    #: capability entry must declare at least one.
    requires_published_dimensions: bool = False
    #: True when the presentation sends one of the aligned per-geography
    #: reductions (``newest_per_geography``, ``newest_release_per_period``).
    #:
    #: A separate question from whether the route declares the parameter,
    #: because a route declares one parameter set for every source while
    #: ``reduction_refusal`` answers per source. The capability entry's
    #: ``publishes_aligned_reduction`` is the declaration of the second.
    requires_aligned_reduction: bool = False

    def __post_init__(self) -> None:
        if "." not in self.surface_id:
            raise VizCoverageError(
                f"{self.surface_id}: a surface id is '<page>.<presentation>'"
            )
        if not self.web_modules:
            raise VizCoverageError(
                f"{self.surface_id}: a surface must name the web module that "
                "builds its requests, so a failure points at the code to read"
            )
        for module in self.web_modules:
            if not (REPOSITORY_ROOT / module).exists():
                raise VizCoverageError(
                    f"{self.surface_id}: names web module '{module}', which does "
                    "not exist"
                )
        declared = set(self.application_paths)
        for group in self.capability_paths:
            if not group:
                raise VizCoverageError(
                    f"{self.surface_id}: an empty alternative would make every "
                    "source reachable without declaring anything"
                )
            declared.update(group)
        unknown = sorted(set(self.required_parameters) - declared)
        if unknown:
            raise VizCoverageError(
                f"{self.surface_id}: requires parameters on paths it does not "
                f"declare: {unknown}"
            )

    @property
    def all_paths(self) -> tuple[str, ...]:
        """Every path this surface can reach, application and per-source."""
        paths = list(self.application_paths)
        for group in self.capability_paths:
            paths.extend(group)
        return tuple(dict.fromkeys(paths))


@dataclass(frozen=True, slots=True)
class VizVerdict:
    """Whether one source can be drawn in one surface, and why not."""

    surface_id: str
    source_code: str
    served: bool
    #: Empty when served. Otherwise the missing declaration, in the API's own
    #: terms -- a path, a parameter, a filter, or the grain fact.
    reason: str

    def as_row(self) -> dict[str, Any]:
        return {
            "surface_id": self.surface_id,
            "source_code": self.source_code,
            "served": self.served,
            "reason": self.reason,
        }


# ---------------------------------------------------------------------------
# The reviewed surface registry
# ---------------------------------------------------------------------------

_EXPLORER = "apps/web/components/SourceExplorerPage.tsx"
_WORKBENCH = "apps/web/components/WorkbenchPage.tsx"
_COMPARISON = "apps/web/components/ComparisonWorkspace.tsx"
_PROFILES = "apps/web/components/ProfileProduct.tsx"
_QUALITY = "apps/web/components/DataQualityExplorer.tsx"
_ACCESS = "apps/web/lib/observationAccess.ts"
_VIEW_MODES = "apps/web/lib/viewModes.ts"
_WORKBENCH_LIB = "apps/web/lib/workbench.ts"

#: The neutral observation resource and the source-scoped pair, as the two
#: access shapes `buildLatestObservationRequest` chooses between. Spelled once
#: because six surfaces read observations and all of them choose the same way.
_NEUTRAL = ("/observations",)


def _source_scoped(segment: str, suffix: str) -> tuple[str, ...]:
    return (f"/{segment}{suffix}",)


VIZ_SURFACES: tuple[VizSurface, ...] = (
    VizSurface(
        surface_id="explorer.map",
        page="/explore",
        title="the explorer's choropleth map",
        web_modules=(_EXPLORER, _VIEW_MODES, "apps/web/components/ChoroplethMap.tsx"),
        capability_paths=(_NEUTRAL,),
        required_parameters={"/observations": ("metric_code", "geo_level", "limit")},
        required_filters=("geo_level",),
        requires_drawable_grain=True,
    ),
    VizSurface(
        surface_id="explorer.trend",
        page="/explore",
        title="the explorer's trend line",
        web_modules=(_EXPLORER, _ACCESS, "apps/web/components/LineChart.tsx"),
        capability_paths=(_NEUTRAL,),
        required_parameters={"/observations": ("metric_code", "geo_id", "limit")},
        required_filters=("geo_id",),
    ),
    VizSurface(
        surface_id="explorer.table",
        page="/explore",
        title="the explorer's observation table",
        web_modules=(_EXPLORER, _ACCESS),
        capability_paths=(_NEUTRAL,),
        required_parameters={"/observations": ("metric_code", "limit", "offset")},
    ),
    VizSurface(
        surface_id="explorer.export",
        page="/explore",
        title="the explorer's CSV export",
        web_modules=(_EXPLORER, "apps/web/lib/observationExport.ts"),
        capability_paths=(_NEUTRAL,),
        required_parameters={"/observations": ("metric_code", "limit", "offset")},
        # The export writes one column per declared dimension, so a source
        # declaring none exports a table with the source's own semantics
        # stripped out of it (WEB-061).
        requires_published_dimensions=True,
    ),
    VizSurface(
        surface_id="explorer.quality",
        page="/explore, /quality",
        title="the measure's freshness and provenance panel",
        web_modules=(
            _QUALITY,
            "apps/web/lib/catalog.ts",
            "apps/web/lib/dataQuality.ts",
        ),
        # Read from the catalog for every source alike; no per-source route is
        # declared for it, and none is needed.
        application_paths=("/catalog/freshness", "/catalog/metrics/{metric_code}"),
        required_parameters={"/catalog/freshness": ()},
    ),
    VizSurface(
        surface_id="explorer.distribution",
        page="/explore",
        title="the explorer's distribution histogram and legend bins",
        web_modules=(_EXPLORER, "apps/web/components/ChoroplethLegend.tsx"),
        capability_paths=(("/distribution/bins",),),
        required_parameters={
            "/distribution/bins": ("metric_code", "geo_level", "bin_count")
        },
    ),
    VizSurface(
        surface_id="explorer.as_released",
        page="/explore",
        title="the explorer's as-released reading and release pin",
        web_modules=(_EXPLORER, _ACCESS),
        capability_paths=(("/observations", "/observations/releases"),),
        required_parameters={
            "/observations": ("metric_code", "scope", "release"),
            "/observations/releases": ("metric_code", "limit", "offset"),
        },
    ),
    VizSurface(
        surface_id="explorer.settled_history",
        page="/explore",
        title="the explorer's settled trend",
        web_modules=(_EXPLORER, _ACCESS),
        capability_paths=(_NEUTRAL,),
        required_parameters={
            "/observations": ("metric_code", "scope", "newest_release_per_period")
        },
        required_filters=("geo_id",),
        requires_aligned_reduction=True,
    ),
    VizSurface(
        surface_id="comparison.workspace",
        page="/compare",
        title="the comparison workspace's scatter, table, map and export",
        web_modules=(_COMPARISON, "apps/web/lib/comparison.ts"),
        capability_paths=(("/comparison", "/comparison/preflight"),),
        required_parameters={
            "/comparison": ("metric_code_a", "metric_code_b", "geo_level"),
            "/comparison/preflight": ("metric_code_a", "metric_code_b"),
        },
    ),
    VizSurface(
        surface_id="workbench.series",
        page="/workbench",
        title="the workbench's multi-series line and bar chart",
        web_modules=(_WORKBENCH, _WORKBENCH_LIB, _ACCESS),
        capability_paths=(_NEUTRAL,),
        required_parameters={"/observations": ("metric_code", "geo_id", "limit")},
        required_filters=("geo_id",),
    ),
    VizSurface(
        surface_id="workbench.cross_section",
        page="/workbench",
        title="the workbench's cross-sectional scatter",
        web_modules=(_WORKBENCH, _WORKBENCH_LIB, _ACCESS),
        capability_paths=(_NEUTRAL,),
        required_parameters={
            "/observations": ("metric_code", "geo_level", "newest_per_geography")
        },
        required_filters=("geo_level",),
        requires_aligned_reduction=True,
    ),
    VizSurface(
        surface_id="workbench.heatmap",
        page="/workbench",
        title="the workbench's geography-by-period heatmap",
        web_modules=(_WORKBENCH, _WORKBENCH_LIB, _ACCESS),
        capability_paths=(_NEUTRAL,),
        required_parameters={
            "/observations": (
                "metric_code",
                "geo_level",
                "scope",
                "newest_release_per_period",
            )
        },
        required_filters=("geo_level",),
        requires_aligned_reduction=True,
    ),
    VizSurface(
        surface_id="workbench.correlation",
        page="/workbench",
        title="the workbench's correlation panel",
        web_modules=(
            _WORKBENCH,
            _WORKBENCH_LIB,
            "apps/web/components/CorrelationPanel.tsx",
        ),
        capability_paths=(("/comparison/correlation", "/comparison/preflight"),),
        required_parameters={
            "/comparison/correlation": ("metric_code_a", "metric_code_b", "geo_level"),
            "/comparison/preflight": ("metric_code_a", "metric_code_b"),
        },
    ),
    VizSurface(
        surface_id="workbench.matrix",
        page="/workbench",
        title="the workbench's correlation matrix",
        web_modules=(
            _WORKBENCH,
            _WORKBENCH_LIB,
            "apps/web/components/CorrelationMatrixChart.tsx",
        ),
        capability_paths=(("/comparison/matrix",),),
        required_parameters={"/comparison/matrix": ("metric_codes", "geo_level")},
    ),
    VizSurface(
        surface_id="profiles.product",
        page="/profiles",
        title="the composed geography profile",
        web_modules=(_PROFILES, "apps/web/lib/productTemplates.ts"),
        application_paths=("/catalog/metrics/{metric_code}",),
        capability_paths=(_NEUTRAL,),
        required_parameters={"/observations": ("metric_code", "geo_id", "limit")},
        required_filters=("geo_id",),
    ),
)


def surfaces_by_id() -> dict[str, VizSurface]:
    indexed: dict[str, VizSurface] = {}
    for surface in VIZ_SURFACES:
        if surface.surface_id in indexed:
            raise VizCoverageError(
                f"{surface.surface_id} is declared twice; a surface needs one row"
            )
        indexed[surface.surface_id] = surface
    return indexed


# ---------------------------------------------------------------------------
# The reviewed declines
# ---------------------------------------------------------------------------

#: The two policies a decline can rest on. Short labels, because the evidence
#: for each is read from the API's own declarations below rather than restated
#: in prose here.
ANALYSIS_POLICY = "aligned-analysis routes decline this source"
REDUCTION_POLICY = "the source publishes no aligned per-geography reduction"
SPATIAL_POLICY = "the source publishes no grain the tile boundary can draw"

#: The presentations the aligned-analysis policy governs: each one is answered
#: by a route that reduces both sides to one value per geography, which is the
#: reduction ``analysis_ready`` decides.
ANALYSIS_SURFACES: frozenset[str] = frozenset(
    {
        "explorer.distribution",
        "comparison.workspace",
        "workbench.correlation",
        "workbench.matrix",
    }
)

#: The presentations that send one of the aligned per-geography reductions.
#: Governed by the same ``analysis_ready`` declaration as the analysis routes
#: -- ``reduction_refusal`` reads it -- but a separate set, because these
#: screens are not analysis routes and a reader reaches them from the explorer
#: and the workbench rather than from the comparison workspace.
REDUCTION_SURFACES: frozenset[str] = frozenset(
    {
        "explorer.settled_history",
        "workbench.cross_section",
        "workbench.heatmap",
    }
)

#: The presentations that put a value on a polygon.
SPATIAL_SURFACES: frozenset[str] = frozenset({"explorer.map"})

#: Every source/surface pair the API deliberately does not serve.
#:
#: Written out rather than derived. A derived table agrees with whatever the
#: registry currently says, so flipping ``analysis_ready`` on a source would
#: move the cell and the expectation together and report green -- which is the
#: shape of the silent skip DB-043 closed in the catalog sweeps. Written out,
#: the same flip fails naming the source and the four screens it empties.
#:
#: A cell absent here must be served, and a cell present here must not be.
#: Both directions are asserted, so coverage cannot quietly shrink and a stale
#: entry cannot mask a presentation that has started working.
#:
#: Each entry's policy is bound to published evidence by its own test:
#:
#: - **Aligned analysis.** CDC, FBI UCR and USDA NASS publish rows carrying
#:   strata, domains, participation bases or agency grains, so
#:   ``analysis_ready`` is false for them and the four analysis surfaces have
#:   nothing to draw. The sentence a reader is shown comes from the dispatch
#:   entry's own ``analysis_refusal()`` -- four served surfaces already read
#:   that one sentence against each other, and a fifth copy here is how they
#:   drift.
#: - **The aligned per-geography reduction.** ``newest_per_geography`` and
#:   ``newest_release_per_period`` are declared by ``/observations`` for every
#:   source, because a route declares one parameter set. Whether a *source*
#:   reduces to one value per geography is the question
#:   ``reduction_refusal`` answers, and it answers it from the same
#:   ``analysis_ready`` flag: the three stratified sources get a 422 naming
#:   the strata a reduction would collapse. Three screens send one of those
#:   parameters, so three screens decline those three sources -- and until
#:   the capability entry published the fact (API-139) the client could not
#:   tell, so it sent the request and drew nothing.
#: - **Spatial presentation.** ``gold_fred.fact_fred_observation`` writes
#:   ``'NATIONAL'`` as a literal: FRED is national by construction, and the
#:   boundary draws no national polygon. The map declines it before any
#:   observation is read, rather than drawing a map it cannot colour.
REVIEWED_DECLINES: dict[tuple[str, str], str] = {
    ("comparison.workspace", "CDC"): ANALYSIS_POLICY,
    ("comparison.workspace", "FBI_UCR"): ANALYSIS_POLICY,
    ("comparison.workspace", "USDA_NASS"): ANALYSIS_POLICY,
    ("explorer.distribution", "CDC"): ANALYSIS_POLICY,
    ("explorer.distribution", "FBI_UCR"): ANALYSIS_POLICY,
    ("explorer.distribution", "USDA_NASS"): ANALYSIS_POLICY,
    ("explorer.map", "FRED"): SPATIAL_POLICY,
    ("explorer.settled_history", "CDC"): REDUCTION_POLICY,
    ("explorer.settled_history", "FBI_UCR"): REDUCTION_POLICY,
    ("explorer.settled_history", "USDA_NASS"): REDUCTION_POLICY,
    ("workbench.cross_section", "CDC"): REDUCTION_POLICY,
    ("workbench.cross_section", "FBI_UCR"): REDUCTION_POLICY,
    ("workbench.cross_section", "USDA_NASS"): REDUCTION_POLICY,
    ("workbench.heatmap", "CDC"): REDUCTION_POLICY,
    ("workbench.heatmap", "FBI_UCR"): REDUCTION_POLICY,
    ("workbench.heatmap", "USDA_NASS"): REDUCTION_POLICY,
    ("workbench.correlation", "CDC"): ANALYSIS_POLICY,
    ("workbench.correlation", "FBI_UCR"): ANALYSIS_POLICY,
    ("workbench.correlation", "USDA_NASS"): ANALYSIS_POLICY,
    ("workbench.matrix", "CDC"): ANALYSIS_POLICY,
    ("workbench.matrix", "FBI_UCR"): ANALYSIS_POLICY,
    ("workbench.matrix", "USDA_NASS"): ANALYSIS_POLICY,
}


def published_decline_reason(surface_id: str, source_code: str) -> str | None:
    """The reason the API itself publishes for a reviewed decline, or ``None``.

    An analysis refusal is the dispatch entry's own sentence; a spatial one is
    the source's grain range against the boundary's. Reading both from the
    declarations that decide them is what keeps this table a record of policy
    rather than a second opinion about it.
    """
    policy = REVIEWED_DECLINES.get((surface_id, source_code))
    if policy is None:
        return None
    if policy in (ANALYSIS_POLICY, REDUCTION_POLICY):
        dispatch = OBSERVATION_DISPATCH.get(source_code)
        return dispatch.analysis_refusal() if dispatch is not None else None
    grains = ADVERTISED_GEO_GRAINS.get(source_code, frozenset())
    drawable = sorted(drawable_tile_grains())
    spelled = ", ".join(sorted(grains)) or "no grain at all"
    return (
        f"source '{source_code}' publishes {spelled}, and the tile boundary "
        f"draws only {', '.join(drawable)}: it has no spatial presentation"
    )


# ---------------------------------------------------------------------------
# Evaluation against a capability entry
# ---------------------------------------------------------------------------


def _relative(path: str) -> str:
    return path[len(VERSIONED_ROOT) :] if path.startswith(VERSIONED_ROOT) else path


def _declared_routes(capability: Mapping[str, Any]) -> dict[str, frozenset[str]]:
    """The source's declared routes, version-relative, with their parameters."""
    routes: dict[str, frozenset[str]] = {}
    for route in capability.get("observation_routes") or []:
        routes[_relative(str(route["path"]))] = frozenset(route.get("parameters") or [])
    return routes


def _missing_parameters(
    surface: VizSurface, path: str, accepted: frozenset[str]
) -> list[str]:
    required = surface.required_parameters.get(path, ())
    return sorted(name for name in required if name not in accepted)


def evaluate_surface(
    surface: VizSurface,
    capability: Mapping[str, Any],
    *,
    application_parameters: Mapping[str, Sequence[str]] | None = None,
) -> VizVerdict:
    """Whether one source can be drawn in one surface, from its own declarations.

    ``capability`` is a `/catalog/capabilities` item exactly as the API serves
    it and the web client reads it -- the same object, not a summary of one --
    so a verdict here is a verdict about what a browser would find.

    ``application_parameters`` maps version-relative application paths to the
    query parameters the served document declares for them. Passed in rather
    than read here because building it means building the OpenAPI document,
    which is the caller's environment, not this module's.
    """
    source_code = str(capability.get("source_code") or "")
    routes = _declared_routes(capability)
    application = {
        path: frozenset(names) for path, names in (application_parameters or {}).items()
    }

    def declined(reason: str) -> VizVerdict:
        return VizVerdict(surface.surface_id, source_code, False, reason)

    for path in surface.application_paths:
        if path not in application:
            return declined(
                f"the application serves no {path}, which {surface.title} reads"
            )
        missing = _missing_parameters(surface, path, application[path])
        if missing:
            return declined(
                f"{path} does not accept {', '.join(missing)}, which "
                f"{surface.title} sends"
            )

    if surface.capability_paths:
        attempts: list[str] = []
        chosen: tuple[str, ...] | None = None
        for group in surface.capability_paths:
            absent = [path for path in group if path not in routes]
            if absent:
                attempts.append(
                    f"{' + '.join(group)} (not declared: {', '.join(absent)})"
                )
                continue
            chosen = group
            break
        if chosen is None:
            return declined(
                f"source '{source_code}' declares no route group that answers "
                f"{surface.title}: tried {'; '.join(attempts)}"
            )
        for path in chosen:
            missing = _missing_parameters(surface, path, routes[path])
            if missing:
                return declined(
                    f"{path} is declared for '{source_code}' but does not accept "
                    f"{', '.join(missing)}, which {surface.title} sends"
                )

    declared_filters = frozenset(capability.get("observation_filters") or [])
    missing_filters = sorted(
        name for name in surface.required_filters if name not in declared_filters
    )
    if missing_filters:
        return declined(
            f"source '{source_code}' declares no {', '.join(missing_filters)} "
            f"observation filter, which {surface.title} sends"
        )

    if surface.requires_published_dimensions and not (
        capability.get("observation_dimensions") or []
    ):
        return declined(
            f"source '{source_code}' declares no observation dimension, so "
            f"{surface.title} would drop the source's own semantics"
        )

    if surface.requires_aligned_reduction and not capability.get(
        "publishes_aligned_reduction"
    ):
        return declined(
            f"source '{source_code}' does not publish an aligned per-geography "
            f"reduction, which {surface.title} asks for; the resource refuses "
            "the request rather than collapsing the source's strata"
        )

    if surface.requires_drawable_grain:
        drawable = set(drawable_tile_grains())
        grains = ADVERTISED_GEO_GRAINS.get(source_code)
        if grains is None:
            return declined(
                f"source '{source_code}' has no reviewed grain range in "
                "tests/support/source_grains, so whether it can be drawn is "
                "unknown rather than false"
            )
        if not (grains & drawable):
            spelled = ", ".join(sorted(grains)) or "no grain at all"
            return declined(
                f"source '{source_code}' publishes {spelled}, and the tile "
                f"boundary draws only {', '.join(sorted(drawable))}: it has no "
                "spatial presentation"
            )

    return VizVerdict(surface.surface_id, source_code, True, "")


def coverage_matrix(
    capabilities: Sequence[Mapping[str, Any]],
    *,
    application_parameters: Mapping[str, Sequence[str]] | None = None,
) -> list[VizVerdict]:
    """Every surface against every capability entry, in a stable order."""
    verdicts: list[VizVerdict] = []
    for surface in VIZ_SURFACES:
        for capability in sorted(
            capabilities, key=lambda entry: str(entry.get("source_code") or "")
        ):
            verdicts.append(
                evaluate_surface(
                    surface,
                    capability,
                    application_parameters=application_parameters,
                )
            )
    return verdicts


def render_matrix(verdicts: Sequence[VizVerdict]) -> dict[str, Any]:
    """The matrix as a reviewable document, ordered for a readable diff."""
    served: dict[str, list[str]] = {}
    declined: dict[str, dict[str, str]] = {}
    for verdict in verdicts:
        if verdict.served:
            served.setdefault(verdict.surface_id, []).append(verdict.source_code)
        else:
            declined.setdefault(verdict.surface_id, {})[verdict.source_code] = (
                verdict.reason
            )
    return {
        "surfaces": [
            {
                "surface_id": surface.surface_id,
                "page": surface.page,
                "title": surface.title,
                "web_modules": list(surface.web_modules),
                "application_paths": list(surface.application_paths),
                "capability_paths": [list(group) for group in surface.capability_paths],
                "required_parameters": {
                    path: list(names)
                    for path, names in sorted(surface.required_parameters.items())
                },
                "required_filters": list(surface.required_filters),
                "requires_drawable_grain": surface.requires_drawable_grain,
                "requires_published_dimensions": surface.requires_published_dimensions,
                "served_sources": sorted(served.get(surface.surface_id, ())),
                "declined_sources": dict(
                    sorted(declined.get(surface.surface_id, {}).items())
                ),
            }
            for surface in VIZ_SURFACES
        ],
        "drawable_tile_grains": list(drawable_tile_grains()),
        # The reviewed grain range per source, carried so the frontend tier can
        # pick a grain a source actually publishes instead of guessing one.
        # A test that asked every source for COUNTY would fail FBI UCR for a
        # reason about the test.
        "advertised_geo_grains": {
            source_code: sorted(grains)
            for source_code, grains in sorted(ADVERTISED_GEO_GRAINS.items())
        },
    }


def describe(verdicts: Sequence[VizVerdict]) -> str:
    """A readable per-surface coverage report, for a failure message or a log."""
    by_surface: dict[str, list[VizVerdict]] = {}
    for verdict in verdicts:
        by_surface.setdefault(verdict.surface_id, []).append(verdict)
    lines: list[str] = []
    for surface in VIZ_SURFACES:
        rows = by_surface.get(surface.surface_id, [])
        served = [row.source_code for row in rows if row.served]
        declined = [row for row in rows if not row.served]
        lines.append(
            f"{surface.surface_id}: {len(served)}/{len(rows)} sources "
            f"({', '.join(served) or 'none'})"
        )
        for row in declined:
            lines.append(f"    - {row.source_code}: {row.reason}")
    return "\n".join(lines)
