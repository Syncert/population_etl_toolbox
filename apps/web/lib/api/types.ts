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

/** One versioned route that answers for a source, with its query parameters. */
export interface ObservationRouteCapability {
  path: string;
  parameters: string[];
}

/**
 * Per-source capability entry from `/catalog/capabilities` — the route map.
 * `observation_routes` lists the exact versioned routes that serve the
 * source (never inferred by prefix), and `observation_filters` is the
 * contract for per-source filtering: a filter a source does not declare is
 * rejected with a 422, never silently ignored.
 */
export interface SourceCapability {
  source_code: string;
  display_name: string;
  route_segment?: string | null;
  served_by_neutral_routes: boolean;
  datasets?: string[] | null;
  observation_filters?: string[] | null;
  observation_routes?: ObservationRouteCapability[] | null;
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

/**
 * One published release holding a metric's observations, from
 * `/observations/releases`. `release` is the source's own release identity
 * (a CDC/NASS release watermark, an FBI release key, a Census vintage, a
 * BLS/FRED as-of date) and is what `release=` accepts alongside
 * `scope=as_released`.
 */
export interface MetricRelease {
  release: string;
  as_of?: string | null;
  observation_count?: number | null;
  [key: string]: unknown;
}

/** `/observations/releases`: a metric's published releases, newest first. */
export interface MetricReleaseListResponse extends CollectionResponse<MetricRelease> {
  metric_code?: string;
  source_code?: string;
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

/**
 * One declared compatibility rule's three-valued verdict.
 *
 * `unknown` is not incompatibility: where a source publishes nothing to
 * check (Census ACS publishes no units), the comparison is still served and
 * the unverified rule travels as a caveat. Only `fail` blocks a pair.
 */
export interface ComparisonRule {
  rule: string;
  status: "pass" | "fail" | "unknown" | string;
  reason: string;
  [key: string]: unknown;
}

/**
 * The compatibility verdict from `/comparison/preflight`. An incompatible
 * pair is a 200 explanation, not an error; only an unknown metric code is a
 * 404. `/comparison` enforces exactly this verdict, so a client that
 * preflights first can trust it.
 */
export interface ComparisonPreflight {
  metric_code_a?: string;
  metric_code_b?: string;
  source_code_a?: string | null;
  source_code_b?: string | null;
  comparable?: boolean;
  /** The fields `/comparison` would compute, each explicitly API-derived. */
  derivations?: string[];
  rules?: ComparisonRule[];
  caveats?: string[];
  [key: string]: unknown;
}

/**
 * One geography's paired inputs and their API-derived combinations.
 *
 * `value_a`/`value_b` are the provider-published inputs — each side's newest
 * value for that geography — and `period_a`/`period_b` carry the period each
 * input actually describes, so a differing as-of context stays visible
 * instead of being implied away. `difference` and `ratio` are API-derived
 * and named in the response's `derivations`. A `null` value means that side
 * published nothing for the geography; it is never zero.
 */
export interface ComparisonRow {
  geo_id?: string | null;
  geo_level?: string | null;
  state_fips?: string | null;
  county_fips?: string | null;
  state_name?: string | null;
  county_name?: string | null;
  metric_code_a?: string | null;
  metric_code_b?: string | null;
  period_a?: string | null;
  period_b?: string | null;
  value_a?: number | null;
  value_b?: number | null;
  difference?: number | null;
  ratio?: number | null;
  [key: string]: unknown;
}

/** An aligned comparison, served only for pairs the policy accepts. */
export interface ComparisonResponse {
  metric_code_a?: string;
  metric_code_b?: string;
  source_code_a?: string | null;
  source_code_b?: string | null;
  units_a?: string | null;
  units_b?: string | null;
  derivations?: string[];
  caveats?: string[];
  total?: number;
  limit?: number;
  offset?: number;
  items: ComparisonRow[];
  [key: string]: unknown;
}

export interface HealthResponse {
  status?: string;
  [key: string]: unknown;
}

// --- Saved analysis configurations (ADR-0003) ---

/** The resources a saved configuration may describe. */
export type ConfigurationKind = "observations" | "comparison" | "distribution";

/**
 * One saved analysis intent, validated at write time against the same
 * contracts the live routes enforce — so a stored configuration can never
 * encode a request the API would refuse. It is deliberately not a copy of
 * observation data: it is replayed against live publications, so a saved
 * analysis follows the warehouse instead of freezing a snapshot of it.
 * `visualization` is opaque user content the API stores verbatim.
 */
export interface AnalysisDocument {
  kind: ConfigurationKind;
  metric_code?: string | null;
  metric_code_a?: string | null;
  metric_code_b?: string | null;
  scope?: "latest" | "as_released";
  release?: string | null;
  filters?: Record<string, unknown>;
  bin_count?: number | null;
  visualization?: Record<string, unknown>;
}

/**
 * Whether a stored document still matches live capabilities. Reported on
 * read rather than repaired: a stale configuration is returned unmodified,
 * because rewriting it would substitute the API's guess for the user's
 * intent.
 */
export interface ConfigurationValidation {
  valid: boolean;
  reason?: string | null;
}

export interface SavedAnalysisSummary {
  configuration_id: number;
  name: string;
  kind?: string | null;
  version: number;
  created_at: string;
  updated_at: string;
  [key: string]: unknown;
}

export interface SavedAnalysisListResponse {
  total: number;
  limit?: number;
  offset?: number;
  items: SavedAnalysisSummary[];
}

export interface SavedAnalysisConfiguration {
  configuration_id: number;
  name: string;
  version: number;
  document: AnalysisDocument;
  validation: ConfigurationValidation;
  created_at: string;
  updated_at: string;
}
