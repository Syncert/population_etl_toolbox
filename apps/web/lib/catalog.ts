// Catalog view-model helpers shared by catalog surfaces.
//
// The catalog's filters are exactly the ones `/api/v1/catalog/metrics`
// declares (`q`, `source_code`, `active_only`, `limit`, `offset`). Filtering
// a page of results client-side would report a total the API never
// published, so anything the resource cannot filter on is not offered as a
// filter here — it is shown as published provenance instead.

import type { MetricSummary, SourceSummary } from "./api/types";

export interface SourceFilterOption {
  value: string;
  label: string;
}

// Builds the source filter from the API's published source list so the
// catalog never carries a closed client-side source enumeration.
export function sourceFilterOptions(
  sourceItems: SourceSummary[] | null | undefined,
): SourceFilterOption[] {
  return [
    { value: "", label: "All sources" },
    ...(Array.isArray(sourceItems) ? sourceItems : []).map((item) => ({
      value: item.source_code,
      label: item.source_name || item.source_code,
    })),
  ];
}

/** One page of catalog results; the resource caps `limit` at 1000. */
export const CATALOG_PAGE_SIZE = 50;

export interface CatalogState {
  query: string;
  sourceCode: string;
  /** `false` includes retired metrics, which the API filters server-side. */
  activeOnly: boolean;
  /** Zero-based page index over the API's own limit/offset paging. */
  page: number;
}

export const DEFAULT_CATALOG_STATE: CatalogState = Object.freeze({
  query: "",
  sourceCode: "",
  activeOnly: true,
  page: 0,
});

const SOURCE_CODE_PATTERN = /^[A-Za-z][A-Za-z0-9_-]{0,49}$/;
const MAX_QUERY_LENGTH = 200;

export function parseCatalogState(search: string | null | undefined): CatalogState {
  const params = new URLSearchParams(search || "");
  const query = (params.get("q") || "").slice(0, MAX_QUERY_LENGTH);
  const sourceCode = params.get("source") || "";
  const page = Number.parseInt(params.get("page") || "", 10);

  return {
    query,
    sourceCode: SOURCE_CODE_PATTERN.test(sourceCode) ? sourceCode : "",
    activeOnly: params.get("include_retired") !== "1",
    page: Number.isInteger(page) && page > 0 ? page : 0,
  };
}

/** Serializes only non-default values so shared catalog links stay stable. */
export function serializeCatalogState(state: CatalogState): string {
  const params = new URLSearchParams();
  if (state.query) {
    params.set("q", state.query);
  }
  if (state.sourceCode && SOURCE_CODE_PATTERN.test(state.sourceCode)) {
    params.set("source", state.sourceCode);
  }
  if (!state.activeOnly) {
    params.set("include_retired", "1");
  }
  if (state.page > 0) {
    params.set("page", String(state.page));
  }
  return params.toString();
}

/** The exact request parameters for one catalog page. */
export function catalogRequestParams(
  state: CatalogState,
  pageSize: number = CATALOG_PAGE_SIZE,
): Record<string, string> {
  const params: Record<string, string> = {
    limit: String(pageSize),
    offset: String(Math.max(0, state.page) * pageSize),
  };
  if (state.activeOnly) {
    params.active_only = "true";
  }
  const query = state.query.trim();
  if (query) {
    params.q = query;
  }
  if (state.sourceCode) {
    params.source_code = state.sourceCode;
  }
  return params;
}

export interface CatalogPageModel {
  /** The API's own published total for this filter, or `null` when absent. */
  total: number | null;
  shown: number;
  pageIndex: number;
  pageCount: number;
  /** 1-based inclusive display range, `null` when the page is empty. */
  firstRow: number | null;
  lastRow: number | null;
  hasPrevious: boolean;
  hasNext: boolean;
}

/**
 * Deterministic paging over the API's `{items, total, limit, offset}`
 * envelope. `hasNext` is decided by the published total when there is one
 * and by a full page otherwise — never by guessing that a short page is the
 * last one, and never by a client-side count standing in for the total.
 *
 * The displayed range is anchored to the offset the response published, not
 * to the requested page. Between a page click and its answer those two
 * disagree, and using the requested one would label the previous page's rows
 * with the new page's range — stale values presented as current.
 */
export function catalogPageModel(
  payload: { items?: unknown[]; total?: number; offset?: number } | null | undefined,
  state: CatalogState,
  pageSize: number = CATALOG_PAGE_SIZE,
): CatalogPageModel {
  const shown = Array.isArray(payload?.items) ? payload.items.length : 0;
  const total =
    typeof payload?.total === "number" && Number.isFinite(payload.total)
      ? payload.total
      : null;
  const offset =
    typeof payload?.offset === "number" && Number.isFinite(payload.offset) && payload.offset >= 0
      ? payload.offset
      : Math.max(0, state.page) * pageSize;
  const pageIndex = Math.floor(offset / pageSize);
  const pageCount = total === null ? 0 : Math.max(1, Math.ceil(total / pageSize));

  return {
    total,
    shown,
    pageIndex,
    pageCount,
    firstRow: shown > 0 ? offset + 1 : null,
    lastRow: shown > 0 ? offset + shown : null,
    hasPrevious: pageIndex > 0,
    hasNext: total === null ? shown === pageSize : offset + shown < total,
  };
}

export interface ProvenanceEntry {
  label: string;
  value: string;
}

/**
 * The publication provenance the glossary actually published for one metric,
 * in a fixed order. A field the publisher did not publish is omitted rather
 * than shown as a placeholder — "Pending" beside an absent harvest time
 * states something the source did not.
 */
export function metricProvenance(metric: MetricSummary | null | undefined): ProvenanceEntry[] {
  if (!metric) {
    return [];
  }

  const lineage = (metric.physical_lineage || {}) as Record<string, unknown>;
  const lineageRelation =
    lineage.schema && lineage.relation ? `${lineage.schema}.${lineage.relation}` : null;

  const candidates: [string, unknown][] = [
    ["Units", metric.units],
    ["Measure kind", metric.measure_kind],
    ["Aggregation", metric.aggregation_characteristic],
    ["Geographies", joinGrains(metric.valid_geo_grains)],
    ["Time grain", joinGrains(metric.valid_time_grains)],
    ["Source object", metric.source_object_type],
    ["Publisher contract", metric.publisher_contract_version],
    ["Source watermark", metric.source_watermark],
    ["Published", metric.publication_time],
    ["Harvested", metric.harvested_at],
    ["Serving relation", lineageRelation],
  ];

  return candidates
    .filter(([, value]) => value !== null && value !== undefined && value !== "")
    .map(([label, value]) => ({ label, value: String(value) }));
}

function joinGrains(grains: unknown): string | null {
  if (!Array.isArray(grains) || grains.length === 0) {
    return null;
  }
  return grains.map((grain) => String(grain)).join(", ");
}

export interface MetricQualityState {
  /** Request-state vocabulary value, for the shared status pill. */
  state: string;
  label: string;
}

/**
 * The metric's published freshness, mapped onto the shared request-state
 * vocabulary. A metric whose publisher published no freshness state reads
 * as unknown — never as healthy, and never as a failure.
 */
export function metricQualityState(
  metric: MetricSummary | null | undefined,
): MetricQualityState {
  const freshness = metric?.freshness_state;
  if (typeof freshness !== "string" || freshness === "") {
    return { state: "idle", label: "freshness not published" };
  }
  const normalized = freshness.toLowerCase();
  if (normalized === "fresh" || normalized === "current") {
    return { state: "ok", label: freshness };
  }
  return { state: "warn", label: freshness };
}
