// How the explorer reads observations for one capability-discovered source.
//
// Two declared access shapes (see ./explorerSources) are turned into request
// descriptions here, and the provider-neutral envelope is mapped onto the
// row shape the explorer view models read. Two rules bound everything in
// this module:
//
// 1. A filter reaches a request only when the source's capability entry
//    declares it. The neutral resource rejects an undeclared filter with a
//    422 precisely so it is never silently ignored, and a filter dropped
//    here would silently widen the answer instead.
// 2. Nothing is invented, aggregated, or collapsed. A stratified source
//    publishes several series per geography; this module reports that fact
//    so the caller can decline to map or chart them as one, rather than
//    keeping whichever row happened to arrive last.
//
// Scope is part of the same discipline. `scope=latest` is the source's own
// latest publication; `scope=as_released` reads every published release and
// is offered only where the capability entry declares `/observations/releases`
// and the neutral `scope` parameter. An unpinned as-released answer carries
// one series per release, which is reported as a stratification rather than
// collapsed to whichever release sorted last.

import type { QueryParams } from "./api/client";
import type { ExplorerSource } from "./explorerSources";
import { NEUTRAL_OBSERVATIONS_PATH, RELEASES_PATH } from "./explorerSources";
import type { ObservationRow } from "./explorerViewModel";

export interface ObservationRequest {
  resource: string;
  params: QueryParams;
}

/** `scope=latest` reads the source's own declared latest semantics. */
export const SCOPE_LATEST = "latest";
/**
 * `scope=as_released` reads every published release, each row carrying its
 * release identity. Pinning one with `release=` reproduces the analysis as
 * that release published it.
 */
export const SCOPE_AS_RELEASED = "as_released";

export type ObservationScope = typeof SCOPE_LATEST | typeof SCOPE_AS_RELEASED;

interface ScopedQuery {
  /** Defaults to `latest`; `as_released` only where the source declares it. */
  scope?: ObservationScope;
  /**
   * A release identity from `/observations/releases`. Sent only alongside
   * `scope=as_released` — the API answers `release` without it with a 422,
   * because "the latest publication, but an older one" is a contradiction.
   */
  release?: string;
}

export interface LatestObservationQuery extends ScopedQuery {
  metricCode: string;
  geoLevel?: string;
  stateFips?: string;
  limit?: string | number;
  /** Selected values for the source's own declared dimension filters. */
  dimensions?: Record<string, string>;
}

export interface HistoryObservationQuery extends ScopedQuery {
  metricCode: string;
  geoId: string;
  limit?: string | number;
  dimensions?: Record<string, string>;
}

export interface ReleaseListQuery {
  metricCode: string;
  limit?: string | number;
}

function declaredOnly(
  source: ExplorerSource,
  candidates: QueryParams,
  allowed: string[],
): QueryParams {
  const params: QueryParams = {};
  for (const [name, value] of Object.entries(candidates)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    if (allowed.includes(name)) {
      params[name] = value;
    }
  }
  return params;
}

function dimensionParams(
  names: string[],
  dimensions: Record<string, string> | undefined,
): QueryParams {
  const params: QueryParams = {};
  for (const name of names) {
    const value = dimensions?.[name];
    if (value) {
      params[name] = value;
    }
  }
  return params;
}

/**
 * Whether this source can answer an as-released question at all: the release
 * listing and the neutral `scope` parameter must both be declared for it.
 */
export function servesAsReleased(source: ExplorerSource | null | undefined): boolean {
  return Boolean(source?.servesReleases && source.supportsAsReleased);
}

/** The dimension controls that apply under one scope. */
export function scopedDimensionFilters(
  source: ExplorerSource | null | undefined,
  scope: ObservationScope,
): string[] {
  if (!source) {
    return [];
  }
  // An as-released read always goes through the neutral resource, so the
  // filters it accepts are the neutral ones even for a source-scoped source.
  return scope === SCOPE_AS_RELEASED && servesAsReleased(source)
    ? source.neutralDimensionFilters
    : source.dimensionFilters;
}

/**
 * The `/observations/releases` request for one metric, or `null` when the
 * source's capability entry does not declare that route. A null answer is
 * the honest "this source publishes no release listing here"; nothing here
 * may guess a release identity.
 */
export function buildReleaseListRequest(
  source: ExplorerSource | null | undefined,
  query: ReleaseListQuery,
): ObservationRequest | null {
  if (!source?.servesReleases) {
    return null;
  }
  return {
    resource: RELEASES_PATH,
    params: { metric_code: query.metricCode, limit: query.limit },
  };
}

/**
 * The scope parameters a neutral request carries. `release` travels only
 * with `scope=as_released`, and only when the source declares that it can
 * be pinned.
 */
function scopeParams(source: ExplorerSource, query: ScopedQuery): QueryParams {
  if (query.scope !== SCOPE_AS_RELEASED || !servesAsReleased(source)) {
    return { scope: SCOPE_LATEST };
  }
  return {
    scope: SCOPE_AS_RELEASED,
    release: source.supportsReleasePin ? query.release : undefined,
  };
}

/** True when this query asks the neutral resource for an as-released read. */
function asReleased(source: ExplorerSource, query: ScopedQuery): boolean {
  return query.scope === SCOPE_AS_RELEASED && servesAsReleased(source);
}

/** The row field carrying a row's own release identity. */
export const RELEASE_DIMENSION = "release";

/**
 * The axes along which a loaded answer can carry more than one series per
 * geography. Under `scope=as_released` the release is one of them: every
 * published release answers, so a choropleth join or a single-line chart
 * would keep whichever release arrived last unless one is pinned. Adding it
 * here lets the caller decline for the same reason, and name the release
 * control as the filter that resolves it.
 */
export function stratificationDimensions(
  dimensionFilters: string[] | null | undefined,
  scope: ObservationScope,
): string[] {
  const names = dimensionFilters || [];
  return scope === SCOPE_AS_RELEASED ? [...names, RELEASE_DIMENSION] : [...names];
}

/**
 * The cross-geography "latest published values" request for one metric.
 *
 * The source-scoped shape keeps its own route and parameter discipline; the
 * neutral shape asks `/observations` with `scope=latest` and only the
 * filters the capability declares.
 */
export function buildLatestObservationRequest(
  source: ExplorerSource,
  query: LatestObservationQuery,
): ObservationRequest {
  const shared: QueryParams = {
    geo_level: query.geoLevel,
    state_fips: query.stateFips,
  };
  const released = asReleased(source, query);

  if (source.accessShape === "source-scoped" && !released) {
    return {
      resource: `/${source.segment}/observations/latest`,
      params: {
        metric_code: query.metricCode,
        limit: query.limit,
        ...declaredOnly(source, shared, source.latestParameters),
      },
    };
  }

  // Both the neutral shape and every as-released read answer here; the
  // filters a source-scoped source may carry across are its declared
  // neutral ones, not the parameters of the route it left behind.
  const allowed = released ? source.neutralFilters : source.requestFilters;
  return {
    resource: NEUTRAL_OBSERVATIONS_PATH,
    params: {
      metric_code: query.metricCode,
      ...scopeParams(source, query),
      limit: query.limit,
      ...declaredOnly(source, shared, allowed),
      ...dimensionParams(scopedDimensionFilters(source, query.scope || SCOPE_LATEST), query.dimensions),
    },
  };
}

/**
 * The single-geography history request.
 *
 * The neutral resource has no separate timeseries route: `scope=latest` over
 * one `geo_id` is the source's currently published series for that
 * geography, which is what the panel labels it.
 */
export function buildHistoryObservationRequest(
  source: ExplorerSource,
  query: HistoryObservationQuery,
): ObservationRequest {
  const released = asReleased(source, query);

  if (source.accessShape === "source-scoped" && !released) {
    return {
      resource: `/${source.segment}/observations/timeseries`,
      params: {
        metric_code: query.metricCode,
        geo_id: query.geoId,
        limit: query.limit,
      },
    };
  }

  const allowed = released ? source.neutralFilters : source.requestFilters;
  return {
    resource: NEUTRAL_OBSERVATIONS_PATH,
    params: {
      metric_code: query.metricCode,
      ...scopeParams(source, query),
      limit: query.limit,
      ...declaredOnly(source, { geo_id: query.geoId }, allowed),
      ...dimensionParams(scopedDimensionFilters(source, query.scope || SCOPE_LATEST), query.dimensions),
    },
  };
}

function firstText(...values: unknown[]): string | null {
  for (const value of values) {
    if (typeof value === "string" && value !== "") {
      return value;
    }
  }
  return null;
}

/**
 * The period a neutral row covers, spelled exactly as published: a single
 * date when the bounds agree, an explicit range when they do not. CDC
 * publishes multi-year periods, and rendering only one bound would state a
 * narrower period than the source did.
 */
export function observationPeriodLabel(row: ObservationRow | null | undefined): string {
  const start = firstText(row?.period_start);
  const end = firstText(row?.period_end);
  if (start && end) {
    return start === end ? start : `${start} – ${end}`;
  }
  return (
    firstText(start, end, row?.period, row?.observation_date) || ""
  );
}

/**
 * Map the provider-neutral envelope onto the row shape the explorer view
 * models read, without overwriting anything the row already published and
 * without touching `value` (text, or `null` when nothing was published).
 * Source-scoped rows already carry this shape and pass through unchanged.
 */
export function normalizeObservationRows(
  source: ExplorerSource | null | undefined,
  items: ObservationRow[] | null | undefined,
): ObservationRow[] {
  const rows = Array.isArray(items) ? items : [];
  if (!source || source.accessShape !== "neutral") {
    return rows;
  }

  return rows.map((row) => {
    const uncertainty = (row.uncertainty || {}) as Record<string, unknown>;
    const period = observationPeriodLabel(row);
    const normalized: ObservationRow = { ...row };

    if (normalized.period === undefined && period) {
      normalized.period = period;
    }
    // The chart and table read a single ordering date; the published end of
    // the period is the point the observation is current as of.
    if (normalized.observation_date === undefined) {
      normalized.observation_date = firstText(row.period_end, row.period_start);
    }
    if (normalized.units === undefined && row.unit !== undefined) {
      normalized.units = row.unit;
    }
    if (normalized.source === undefined && row.source_code !== undefined) {
      normalized.source = row.source_code;
    }
    if (normalized.margin_of_error === undefined && uncertainty.margin_of_error !== undefined) {
      normalized.margin_of_error = uncertainty.margin_of_error;
    }
    if (
      normalized.margin_of_error_pct === undefined &&
      uncertainty.margin_of_error_pct !== undefined
    ) {
      normalized.margin_of_error_pct = uncertainty.margin_of_error_pct;
    }

    return normalized;
  });
}

/** A declared dimension's published value on one row, or `""` when absent. */
export function observationDimensionValue(
  row: ObservationRow | null | undefined,
  name: string,
): string {
  const dimensions = (row?.dimensions || {}) as Record<string, unknown>;
  const value = dimensions[name] !== undefined ? dimensions[name] : row?.[name];
  return value === undefined || value === null ? "" : String(value);
}

/**
 * The distinct published values of one declared dimension across the loaded
 * rows, sorted for deterministic rendering. These are provider-published
 * values read back from the answer, never a client-authored option list.
 */
export function observationDimensionOptions(
  rows: ObservationRow[] | null | undefined,
  name: string,
): string[] {
  const values = new Set<string>();
  for (const row of rows || []) {
    const value = observationDimensionValue(row, name);
    if (value) {
      values.add(value);
    }
  }
  return [...values].sort();
}

export interface ObservationStratification {
  /** Distinct declared-dimension signatures present in the loaded rows. */
  seriesCount: number;
  /** True when a geography carries more than one series in this answer. */
  stratified: boolean;
  /** The declared dimensions that actually vary, so the caller can name them. */
  varyingDimensions: string[];
}

/**
 * Whether the loaded rows resolve to one value per geography.
 *
 * A stratified source (CDC strata and adjustment statuses, FBI UCR subject
 * types, USDA NASS domains) returns several rows per geography, and both the
 * choropleth join and a single-line chart would keep whichever arrived last.
 * The caller uses this to decline rather than collapse, and to name the
 * declared filters that would narrow the selection.
 */
export function describeStratification(
  rows: ObservationRow[] | null | undefined,
  dimensionFilters: string[] | null | undefined,
): ObservationStratification {
  const names = (dimensionFilters || []).filter(Boolean);
  const items = Array.isArray(rows) ? rows : [];
  if (names.length === 0 || items.length === 0) {
    return { seriesCount: items.length === 0 ? 0 : 1, stratified: false, varyingDimensions: [] };
  }

  const signatures = new Set<string>();
  const valuesByName = new Map<string, Set<string>>(names.map((name) => [name, new Set()]));
  for (const row of items) {
    const signature: string[] = [];
    for (const name of names) {
      const value = observationDimensionValue(row, name);
      signature.push(`${name}=${value}`);
      valuesByName.get(name)!.add(value);
    }
    signatures.add(signature.join("|"));
  }

  return {
    seriesCount: signatures.size,
    stratified: signatures.size > 1,
    varyingDimensions: names.filter((name) => (valuesByName.get(name)?.size || 0) > 1),
  };
}
