// Response shapes for the documented /api/v1 contract.
//
// These follow docs/reference/API_CONSUMER_GUIDE.md. They are deliberately
// permissive where the API publishes source-specific fields under the
// source's own names (`dimensions`), and deliberately strict where the
// guide makes a guarantee a consumer must not violate — above all that
// `value` is text and is `null` whenever the source published no usable
// number, so nothing here may type it as a number.

/** A paged collection response: `{items, total, limit, offset}`. */
export interface CollectionResponse<T> {
  items: T[];
  total?: number;
  limit?: number;
  offset?: number;
}

export interface SourceSummary {
  source_code: string;
  source_name?: string | null;
  [key: string]: unknown;
}

export interface MetricSummary {
  metric_code: string;
  metric_display_name?: string | null;
  source_code?: string | null;
  source_object_type?: string | null;
  valid_geo_grains?: string[] | null;
  valid_time_grains?: string[] | null;
  harvested_at?: string | null;
  [key: string]: unknown;
}

export interface GeographySummary {
  geo_id: string;
  geo_level?: string | null;
  state_fips?: string | null;
  county_fips?: string | null;
  state_name?: string | null;
  county_name?: string | null;
  latitude?: number | string | null;
  longitude?: number | string | null;
  [key: string]: unknown;
}

/**
 * Per-source capability entry from `/catalog/capabilities` — the route map.
 * `observation_filters` is the contract for per-source filtering: a filter a
 * source does not declare is rejected with a 422, never silently ignored.
 */
export interface SourceCapability {
  source_code: string;
  route_segment?: string | null;
  neutral_routes_supported?: boolean | null;
  datasets?: string[] | null;
  observation_filters?: string[] | null;
  routes?: unknown;
  [key: string]: unknown;
}

/**
 * One observation row. `value` is text to preserve provider precision, and
 * is `null` when the source published no usable number — `value_status`
 * then says why in the source's own vocabulary. Nothing is ever zero.
 */
export interface Observation {
  metric_code?: string | null;
  geo_id?: string | null;
  geo_level?: string | null;
  value: string | null;
  value_status?: string | null;
  units?: string | null;
  unit?: string | null;
  period?: string | null;
  observation_date?: string | null;
  release?: string | null;
  as_of?: string | null;
  source_record_id?: string | null;
  capture_id?: string | null;
  dimensions?: Record<string, unknown> | null;
  uncertainty?: Record<string, unknown> | null;
  coverage?: Record<string, unknown> | null;
  [key: string]: unknown;
}

export interface DistributionBin {
  bin_index: number;
  count: number;
  [key: string]: unknown;
}

/** API-derived equal-width bins, labelled `derived: true`. */
export interface DistributionResponse {
  items: DistributionBin[];
  total?: number;
  bin_count?: number;
  min_value?: number;
  max_value?: number;
  derived?: boolean;
  source_code?: string | null;
  units?: string | null;
  [key: string]: unknown;
}

export interface ComparisonRule {
  rule?: string;
  status?: "pass" | "fail" | "unknown" | string;
  reason?: string | null;
  [key: string]: unknown;
}

/** The compatibility verdict; an incompatible pair is a 200 explanation. */
export interface ComparisonPreflight {
  comparable?: boolean;
  derivations?: string[];
  rules?: ComparisonRule[];
  caveats?: string[];
  [key: string]: unknown;
}

export interface HealthResponse {
  status?: string;
  [key: string]: unknown;
}
