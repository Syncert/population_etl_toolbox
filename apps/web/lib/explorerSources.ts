// Capability-derived explorer sources: which sources the explorer can
// drive, decided by the routes `/api/v1/catalog/capabilities` declares for
// each source rather than a closed client-side enumeration.
//
// The explorer's workflow needs a source-scoped latest + timeseries route
// pair; a source whose declared routes carry that pair is explorable, and a
// dispatch-only source (served through the neutral `/observations`
// resource) is honestly excluded until the explorer understands that shape.

import { API_BASE } from "./api/client";
import type { SourceCapability } from "./api/types";

export interface ExplorerSource {
  /** Stable key for tabs and URL state — the API's own route segment. */
  key: string;
  segment: string;
  sourceCode: string;
  /** The capability's published display name. */
  title: string;
  /** Short tab label derived from the route segment, never invented. */
  tabLabel: string;
  latestParameters: string[];
  timeseriesParameters: string[];
}

const LATEST_SUFFIX = "/observations/latest";
const TIMESERIES_SUFFIX = "/observations/timeseries";

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
    latestParameters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
    timeseriesParameters: ["end_date", "geo_id", "limit", "metric_code", "start_date"],
  },
];

export function buildExplorerSources(
  capabilities: SourceCapability[] | null | undefined,
): ExplorerSource[] {
  const sources: ExplorerSource[] = [];

  for (const capability of capabilities || []) {
    const segment = capability.route_segment;
    if (!segment) {
      continue;
    }

    const routes = capability.observation_routes || [];
    const latest = routes.find(
      (route) => route.path === `${API_BASE}/${segment}${LATEST_SUFFIX}`,
    );
    const timeseries = routes.find(
      (route) => route.path === `${API_BASE}/${segment}${TIMESERIES_SUFFIX}`,
    );
    if (!latest || !timeseries) {
      continue;
    }

    sources.push({
      key: segment,
      segment,
      sourceCode: capability.source_code,
      title: capability.display_name || capability.source_code,
      tabLabel: segment.toUpperCase(),
      latestParameters: latest.parameters || [],
      timeseriesParameters: timeseries.parameters || [],
    });
  }

  return sources;
}

export function findExplorerSource(
  sources: ExplorerSource[],
  segment: string | null | undefined,
): ExplorerSource | null {
  if (!segment) {
    return null;
  }
  return sources.find((source) => source.segment === segment) || null;
}

/** Whether the source's declared latest route accepts a query parameter. */
export function sourceSupportsParameter(
  source: ExplorerSource | null | undefined,
  parameter: string,
): boolean {
  return Boolean(source && source.latestParameters.includes(parameter));
}
