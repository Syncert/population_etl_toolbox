// Capability-derived explorer sources: which sources the explorer can
// drive, and how it reaches each one, decided by the routes and filters
// `/api/v1/catalog/capabilities` declares rather than a closed client-side
// enumeration.
//
// Two access shapes reach observations, both declared by the API:
//
// - `neutral` — the registry-dispatched `/observations` resource, which
//   answers for every completed source. Its accepted filters are the
//   capability's own `observation_filters`; a filter the source does not
//   declare is rejected with a 422 rather than silently ignored, so nothing
//   here may send one it did not read from the contract.
// - `source-scoped` — the source's own `latest` + `timeseries` route pair.
//   Its accepted filters are the parameters those routes declare.
//
// The neutral shape is preferred wherever it is declared, and the
// source-scoped pair is the fallback for a source that publishes no neutral
// route. That order was first a correctness rule: the source-scoped pair is
// the original MVP surface over the legacy cross-source union views, which
// keyed Census ACS observations on that era's metric identity
// (`ACS:acs5:B01003_001`) while the catalog this client reads its metric codes
// from publishes the glossary identity (`CENSUS_ACS:acs5:B01003_001`), so a
// catalog code sent to the legacy pair matched nothing and returned an empty
// page indistinguishable from a geography with no published values. ARC-005
// removed that disagreement in the warehouse — the ACS serving relations now
// carry the catalog's own code — so both shapes answer the same code today.
// The preference stands on its own terms: the neutral resource resolves a
// metric through the published glossary and the reviewed dispatch registry,
// and API_CONSUMER_GUIDE states the legacy routes retire and that new work
// should use `/observations`.
//
// A source that declares neither shape is not explorable and is left out;
// membership is never a source-code list.
//
// The as-released surface is read from the same declarations. Every release
// question lives on the neutral resource — `scope=as_released` over
// `/observations`, with the identities `/observations/releases` publishes —
// so a source-scoped source reaches it there too, and only when its own
// capability entry declares those routes and parameters.

import { API_BASE } from "./api/client";
import type { SourceCapability } from "./api/types";

/** How the explorer reaches one source's observations. */
export type ObservationAccessShape = "source-scoped" | "neutral";

export interface ExplorerSource {
  /**
   * Stable key for tabs and URL state. The API's own route segment when the
   * source publishes one, otherwise its published `source_code` — a
   * segment-less source (FBI UCR) still needs a shareable identity, and its
   * glossary code is the published one.
   */
  key: string;
  /** Route segment, `null` for a source served only by the neutral resource. */
  segment: string | null;
  sourceCode: string;
  /** The capability's published display name. */
  title: string;
  /** Short tab label derived from the published identity, never invented. */
  tabLabel: string;
  accessShape: ObservationAccessShape;
  /** Every filter the API declares this source accepts on an observation read. */
  requestFilters: string[];
  /**
   * Declared filters outside the shared geography/period vocabulary — the
   * source's own dimensional filters (stratum, adjustment, subject, domain).
   * Derived by subtraction, so a filter this client has never heard of still
   * becomes a control instead of being dropped.
   */
  dimensionFilters: string[];
  /**
   * The field names a row's `dimensions` object carries for this source, as
   * `/catalog/capabilities` declares them (`observation_dimensions`).
   *
   * A different list from `dimensionFilters`, and the reason this exists:
   * the table and the export took their dimension columns from the
   * filterable names, so four of seven sources showed no dimension at all
   * and CDC showed two of fourteen -- `footnote_text`, which is how CDC
   * qualifies an estimate, among the twelve missing (WEB-061). Read from
   * the declaration rather than from a loaded row, so a declared dimension
   * a page happens not to publish is still shown, empty.
   */
  publishedDimensions: string[];
  /** True when `/distribution/bins` is declared for this source. */
  servesDistribution: boolean;
  /**
   * True when the aligned comparison routes are declared for this source.
   * A source that declares them is not thereby comparable with any other —
   * `/comparison/preflight` decides each pair — but a source that declares
   * none is one the analysis routes have already declined, which is worth
   * saying before a reader picks it.
   */
  servesComparison: boolean;
  latestParameters: string[];
  timeseriesParameters: string[];
  /**
   * Filters a request to the neutral `/observations` resource may carry —
   * the capability's own `observation_filters` plus the universal parameter
   * set. Populated for every source the neutral resource answers for,
   * including the source-scoped ones, because the as-released surface lives
   * only on that resource.
   */
  neutralFilters: string[];
  /** The neutral shape's own dimension filters, by the same subtraction. */
  neutralDimensionFilters: string[];
  /** True when `/observations/releases` is declared: releases are listable. */
  servesReleases: boolean;
  /**
   * True when the neutral resource declares `scope` for this source, so
   * `scope=as_released` is a request the API accepts rather than one this
   * client invented.
   */
  supportsAsReleased: boolean;
  /** True when the neutral resource declares `release`, so one can be pinned. */
  supportsReleasePin: boolean;
  /**
   * True when the neutral resource declares `newest_per_geography`, so the
   * map can ask for one row per geography instead of paging a source's
   * whole latest publication and reducing it here.
   */
  supportsNewestPerGeography: boolean;
  /** True when `/observations` declares `newest_release_per_period` (API-081). */
  supportsSettledHistory: boolean;
  /**
   * True when this source's rows can arrive with `value: null` and a
   * published `value_status` saying why — the capability's own
   * `publishes_value_status` (API-127).
   *
   * False is the fact a chart needs: the serving relations then carry only
   * published numbers, so a period the source published *without* one is
   * absent from the series rather than present and marked, and a gap in a
   * line is that period rather than an interval the measure moved across.
   */
  publishesValueStatus: boolean;
}

const LATEST_SUFFIX = "/observations/latest";
const TIMESERIES_SUFFIX = "/observations/timeseries";

/** The registry-dispatched provider-neutral observation resource. */
export const NEUTRAL_OBSERVATIONS_PATH = "/observations";
/** The release listing that says what `release=` accepts for a metric. */
export const RELEASES_PATH = "/observations/releases";
const DISTRIBUTION_PATH = "/distribution/bins";
const COMPARISON_PREFLIGHT_PATH = "/comparison/preflight";

/**
 * Parameters the neutral resource accepts for every source regardless of
 * its declared filters (API_CONSUMER_GUIDE: the universal parameter set).
 * They are request mechanics, not filter controls.
 */
export const UNIVERSAL_OBSERVATION_PARAMETERS = Object.freeze([
  "metric_code",
  "scope",
  "release",
  "limit",
  "offset",
] as const);

/**
 * The shared geography and period filter vocabulary. These already have
 * first-class explorer controls; anything else a source declares is one of
 * its own dimensions and gets a generated control.
 */
export const SHARED_OBSERVATION_FILTERS = Object.freeze([
  "geo_id",
  "geo_level",
  "state_fips",
  "county_fips",
  "year_from",
  "year_to",
] as const);

const NON_DIMENSION_FILTERS = new Set<string>([
  ...UNIVERSAL_OBSERVATION_PARAMETERS,
  ...SHARED_OBSERVATION_FILTERS,
]);

/**
 * Offline fallback for the mounted default source only, used when
 * capability discovery is unavailable so the explorer degrades to its
 * previous single-source behavior instead of going blank. This is a
 * labeled fallback (the sources status pill reports discovery failure),
 * not a source enumeration.
 */
export const FALLBACK_EXPLORER_SOURCES: ExplorerSource[] = [
  {
    key: "census",
    segment: "census",
    sourceCode: "CENSUS_ACS",
    title: "Census American Community Survey",
    tabLabel: "CENSUS",
    accessShape: "source-scoped",
    requestFilters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
    dimensionFilters: [],
    publishedDimensions: [],
    servesDistribution: true,
    servesComparison: false,
    latestParameters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
    timeseriesParameters: [
      "end_date",
      "geo_id",
      "limit",
      "metric_code",
      "offset",
      "start_date",
    ],
    // The offline fallback claims no neutral surface: with discovery
    // unavailable nothing has declared one, and an as-released control the
    // API never declared would be this client inventing a contract.
    neutralFilters: [],
    neutralDimensionFilters: [],
    servesReleases: false,
    supportsAsReleased: false,
    supportsReleasePin: false,
    supportsNewestPerGeography: false,
    supportsSettledHistory: false,
    // Claiming a published value state with discovery unavailable would be
    // this client inventing a contract; claiming none is the conservative
    // reading, and the note it produces is true of the fallback source.
    publishesValueStatus: false,
  },
];

function routePath(capability: SourceCapability, suffix: string): string | null {
  const segment = capability.route_segment;
  return segment ? `${API_BASE}/${segment}${suffix}` : null;
}

function dimensionFiltersOf(requestFilters: string[]): string[] {
  return requestFilters.filter((filter) => !NON_DIMENSION_FILTERS.has(filter));
}

export function buildExplorerSources(
  capabilities: SourceCapability[] | null | undefined,
): ExplorerSource[] {
  const sources: ExplorerSource[] = [];

  for (const capability of capabilities || []) {
    if (!capability?.source_code) {
      continue;
    }

    const routes = capability.observation_routes || [];
    const declaredPaths = new Set(routes.map((route) => route.path));
    const latest = routes.find((route) => route.path === routePath(capability, LATEST_SUFFIX));
    const timeseries = routes.find(
      (route) => route.path === routePath(capability, TIMESERIES_SUFFIX),
    );
    const sourceScoped = Boolean(latest && timeseries);
    const neutralRoute = routes.find(
      (route) => route.path === `${API_BASE}${NEUTRAL_OBSERVATIONS_PATH}`,
    );
    const neutral = Boolean(neutralRoute);

    if (!sourceScoped && !neutral) {
      continue;
    }

    const segment = capability.route_segment || null;
    const key = segment || capability.source_code;
    const latestParameters = latest?.parameters || [];
    const timeseriesParameters = timeseries?.parameters || [];
    // Source-scoped requests are bounded by the parameters their own routes
    // declare; neutral requests by the capability's declared filters plus
    // the universal parameter set the resource always accepts.
    const neutralFilters = neutral
      ? [
          ...new Set([
            ...(capability.observation_filters || []),
            ...UNIVERSAL_OBSERVATION_PARAMETERS,
          ]),
        ].sort()
      : [];
    // The neutral resource is preferred wherever it is declared.
    //
    // Both shapes are real, but they read different relations. The
    // source-scoped `latest`/`timeseries` pair is the original MVP surface
    // over the legacy cross-source union views, which keyed Census ACS
    // observations on that era's metric identity — `ACS:acs5:B01003_001` —
    // while the catalog this client draws its metric codes from publishes the
    // glossary identity, `CENSUS_ACS:acs5:B01003_001`. Sending a catalog code
    // to the legacy pair matched nothing and answered an empty page that was
    // indistinguishable from a geography with no published values. ARC-005
    // ended the disagreement at its source, so both shapes now answer the
    // catalog's own code.
    //
    // The preference remains. The neutral resource resolves the metric
    // through the published glossary and the reviewed dispatch registry, and
    // API_CONSUMER_GUIDE says the legacy routes retire and new work should
    // use `/observations`; this is new work.
    const usesNeutral = neutral;
    const requestFilters = usesNeutral ? neutralFilters : [...latestParameters];
    // `scope` and `release` are read from the neutral route's own declared
    // parameters. A source whose route declares neither cannot answer an
    // as-released question, and offering the control anyway would be this
    // client asserting a contract the API did not publish.
    const neutralParameters = neutralRoute?.parameters || [];

    sources.push({
      key,
      segment,
      sourceCode: capability.source_code,
      title: capability.display_name || capability.source_code,
      tabLabel: key.toUpperCase(),
      accessShape: usesNeutral ? "neutral" : "source-scoped",
      requestFilters,
      dimensionFilters: dimensionFiltersOf(requestFilters),
      publishedDimensions: [...(capability.observation_dimensions || [])],
      servesDistribution: declaredPaths.has(`${API_BASE}${DISTRIBUTION_PATH}`),
      servesComparison: declaredPaths.has(`${API_BASE}${COMPARISON_PREFLIGHT_PATH}`),
      latestParameters,
      timeseriesParameters,
      neutralFilters,
      neutralDimensionFilters: dimensionFiltersOf(neutralFilters),
      servesReleases: declaredPaths.has(`${API_BASE}${RELEASES_PATH}`),
      supportsAsReleased: neutralParameters.includes("scope"),
      supportsReleasePin: neutralParameters.includes("release"),
      supportsNewestPerGeography: neutralParameters.includes(
        "newest_per_geography",
      ),
      supportsSettledHistory: neutralParameters.includes(
        "newest_release_per_period",
      ),
      publishesValueStatus: Boolean(capability.publishes_value_status),
    });
  }

  return sources;
}

/** Resolve a URL/tab key against discovery, case-insensitively. */
export function findExplorerSource(
  sources: ExplorerSource[],
  key: string | null | undefined,
): ExplorerSource | null {
  if (!key) {
    return null;
  }
  const wanted = key.toLowerCase();
  return (
    sources.find((source) => source.key.toLowerCase() === wanted) ||
    // Either published identity resolves. The tab key is the source's route
    // segment, which is what the explorer's own links carry; a link built
    // from a metric row carries `source_code`, because that is the only
    // source identity a metric publishes. Both come from the API, so the
    // resolver accepts both rather than making every caller learn that
    // `CENSUS_ACS` is reached at `census` (WEB-072).
    sources.find((source) => source.sourceCode.toLowerCase() === wanted) ||
    null
  );
}

/** Whether the API declares this source accepts a filter on its reads. */
export function sourceSupportsParameter(
  source: ExplorerSource | null | undefined,
  parameter: string,
): boolean {
  return Boolean(source && source.requestFilters.includes(parameter));
}
