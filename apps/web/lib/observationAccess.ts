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

import type { QueryParams } from "./api/client";
import type { ExplorerSource } from "./explorerSources";
import { NEUTRAL_OBSERVATIONS_PATH } from "./explorerSources";
import type { ObservationRow } from "./explorerViewModel";

export interface ObservationRequest {
  resource: string;
  params: QueryParams;
}

export interface LatestObservationQuery {
  metricCode: string;
  geoLevel?: string;
  stateFips?: string;
  limit?: string | number;
  /** Selected values for the source's own declared dimension filters. */
  dimensions?: Record<string, string>;
}

export interface HistoryObservationQuery {
  metricCode: string;
  geoId: string;
  limit?: string | number;
  dimensions?: Record<string, string>;
}

/** `scope=latest` reads the source's own declared latest semantics. */
export const SCOPE_LATEST = "latest";

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
  source: ExplorerSource,
  dimensions: Record<string, string> | undefined,
): QueryParams {
  const params: QueryParams = {};
  for (const name of source.dimensionFilters) {
    const value = dimensions?.[name];
    if (value) {
      params[name] = value;
    }
  }
  return params;
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

  if (source.accessShape === "source-scoped") {
    return {
      resource: `/${source.segment}/observations/latest`,
      params: {
        metric_code: query.metricCode,
        limit: query.limit,
        ...declaredOnly(source, shared, source.latestParameters),
      },
    };
  }

  return {
    resource: NEUTRAL_OBSERVATIONS_PATH,
    params: {
      metric_code: query.metricCode,
      scope: SCOPE_LATEST,
      limit: query.limit,
      ...declaredOnly(source, shared, source.requestFilters),
      ...dimensionParams(source, query.dimensions),
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
  if (source.accessShape === "source-scoped") {
    return {
      resource: `/${source.segment}/observations/timeseries`,
      params: {
        metric_code: query.metricCode,
        geo_id: query.geoId,
        limit: query.limit,
      },
    };
  }

  return {
    resource: NEUTRAL_OBSERVATIONS_PATH,
    params: {
      metric_code: query.metricCode,
      scope: SCOPE_LATEST,
      limit: query.limit,
      ...declaredOnly(source, { geo_id: query.geoId }, source.requestFilters),
      ...dimensionParams(source, query.dimensions),
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
