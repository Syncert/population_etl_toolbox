// Capability-derived explorer sources: which sources the explorer can
// drive, and how it reaches each one, decided by the routes and filters
// `/api/v1/catalog/capabilities` declares rather than a closed client-side
// enumeration.
//
// Two access shapes reach observations, both declared by the API:
//
// - `source-scoped` — the source declares its own `latest` + `timeseries`
//   route pair (Census ACS, BLS, FRED, Census PEP). Its accepted filters
//   are the parameters those routes declare.
// - `neutral` — the source is served by the registry-dispatched
//   `/observations` resource (CDC, FBI UCR, USDA NASS, and every other
//   source too). Its accepted filters are the capability's own
//   `observation_filters`; a filter the source does not declare is
//   rejected with a 422 rather than silently ignored, so nothing here may
//   send one it did not read from the contract.
//
// A source that declares neither shape is not explorable and is left out;
// membership is never a source-code list.

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
  /** True when `/distribution/bins` is declared for this source. */
  servesDistribution: boolean;
  latestParameters: string[];
  timeseriesParameters: string[];
}

const LATEST_SUFFIX = "/observations/latest";
const TIMESERIES_SUFFIX = "/observations/timeseries";

/** The registry-dispatched provider-neutral observation resource. */
export const NEUTRAL_OBSERVATIONS_PATH = "/observations";
const DISTRIBUTION_PATH = "/distribution/bins";

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
    servesDistribution: true,
    latestParameters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
    timeseriesParameters: ["end_date", "geo_id", "limit", "metric_code", "start_date"],
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
    const neutral = declaredPaths.has(`${API_BASE}${NEUTRAL_OBSERVATIONS_PATH}`);

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
    const requestFilters = sourceScoped
      ? [...latestParameters]
      : [
          ...new Set([
            ...(capability.observation_filters || []),
            ...UNIVERSAL_OBSERVATION_PARAMETERS,
          ]),
        ].sort();

    sources.push({
      key,
      segment,
      sourceCode: capability.source_code,
      title: capability.display_name || capability.source_code,
      tabLabel: key.toUpperCase(),
      accessShape: sourceScoped ? "source-scoped" : "neutral",
      requestFilters,
      dimensionFilters: dimensionFiltersOf(requestFilters),
      servesDistribution: declaredPaths.has(`${API_BASE}${DISTRIBUTION_PATH}`),
      latestParameters,
      timeseriesParameters,
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
  return sources.find((source) => source.key.toLowerCase() === wanted) || null;
}

/** Whether the API declares this source accepts a filter on its reads. */
export function sourceSupportsParameter(
  source: ExplorerSource | null | undefined,
  parameter: string,
): boolean {
  return Boolean(source && source.requestFilters.includes(parameter));
}
