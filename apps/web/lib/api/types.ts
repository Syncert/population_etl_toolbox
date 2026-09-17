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
  /**
   * The place a row is, where the row is a place. Published by
   * `/catalog/geographies` on every row and undeclared here until WEB-064,
   * which is why the picker had no name to show for Census PEP's own grain
   * and offered states instead.
   */
  place_fips?: string | null;
  place_name?: string | null;
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
/**
 * Per-source publication state, exactly as `/catalog/freshness` rolls it up.
 *
 * Declared here with the rest of the transport's shapes rather than beside
 * the view model that reads it: it is what a route answers, and the walker
 * over this file grades it against the reviewed snapshot's `SourceFreshness`.
 */
export interface SourceFreshness {
  source_code: string;
  metric_count: number;
  current_count: number;
  stale_count: number;
  retired_count: number;
  latest_publication_time?: string | null;
  latest_harvested_at?: string | null;
  [key: string]: unknown;
}

export interface SourceCapability {
  source_code: string;
  display_name: string;
  route_segment?: string | null;
  served_by_neutral_routes: boolean;
  datasets?: string[] | null;
  observation_filters?: string[] | null;
  /**
   * Field names a neutral row's `dimensions` object carries for this source
   * (API-109). Declared here because an undeclared field is an invisible
   * one — WEB-057's lesson — even where an index signature would let it
   * through.
   */
  observation_dimensions?: string[] | null;
  /**
   * Whether a row of this source can arrive with `value: null` and a
   * published `value_status` (API-127). Declared here for WEB-057's reason,
   * and read for a concrete one: where it is false the serving relations
   * carry only published numbers, so a period published without one is
   * absent from the series rather than present and marked.
   */
  publishes_value_status?: boolean | null;
  /**
   * Whether a read of this source may ask for the aligned per-geography
   * reduction — `newest_per_geography`, `newest_release_per_period`
   * (API-139). Declared here for WEB-057's reason and read for a concrete
   * one: the route declares both parameters for every source, and the
   * resource refuses them for the sources whose rows carry strata, so a
   * client reading only the route's parameters sends a request it is told,
   * in a 422, that it should never have sent.
   */
  publishes_aligned_reduction?: boolean | null;
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

export interface DistributionBin {
  bin_index: number;
  count: number;
  /**
   * The bin's own bounds, as `/distribution/bins` publishes them — required
   * fields of the served `DistributionBin`, and undeclared here until
   * WEB-057, which is why every reader of this interface rebuilt them from
   * the response's `min_value`/`max_value` instead.
   */
  lower_bound: number;
  upper_bound: number;
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
  /** The one period every binned row came from, or null when they differ. */
  period?: string | null;
  /** True when the bins were built from more than one period (WEB-054). */
  periods_differ?: boolean;
  /** What the analysis could not carry, in the caller's terms (WEB-055). */
  caveats?: string[];
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
  /** Rows this request can page: the geographies both sides published. */
  total?: number;
  /**
   * How many geographies each side published under the same filters, before
   * the inner join (API-087). Absent on a deployment serving an older
   * contract, which is not the same as zero.
   */
  geographies_a?: number;
  geographies_b?: number;
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
export type ConfigurationKind =
  | "observations"
  | "comparison"
  | "distribution"
  | "workbench";

/**
 * One saved analysis intent, validated at write time against the same
 * contracts the live routes enforce — so a stored configuration can never
 * encode a request the API would refuse. It is deliberately not a copy of
 * observation data: it is replayed against live publications, so a saved
 * analysis follows the warehouse instead of freezing a snapshot of it.
 * `visualization` is opaque user content the API stores verbatim.
 */
/** One series of a stored workbench: exactly an observations request. */
export interface SeriesDocument {
  metric_code: string;
  scope?: "latest" | "as_released";
  release?: string | null;
  newest_per_geography?: boolean;
  newest_release_per_period?: boolean;
  filters?: Record<string, unknown>;
}

/** How a stored workbench was drawn. `options` is opaque to the API. */
export interface PresentationDocument {
  type: "line" | "bar" | "scatter" | "ranking" | "correlation" | "heatmap";
  options?: Record<string, unknown>;
}

/**
 * The shared grain a cross-sectional presentation was read at. Absent on a
 * longitudinal composition, which has no shared grain.
 */
export interface AlignmentDocument {
  geo_level: string;
  state_fips?: string | null;
  year?: number | null;
}

export interface AnalysisDocument {
  kind: ConfigurationKind;
  metric_code?: string | null;
  metric_code_a?: string | null;
  metric_code_b?: string | null;
  scope?: "latest" | "as_released";
  release?: string | null;
  /**
   * The reduction the view was read with (API-082). Each belongs to one
   * scope, and the API refuses the other pairing, so a document carries at
   * most one.
   */
  newest_per_geography?: boolean;
  newest_release_per_period?: boolean;
  filters?: Record<string, unknown>;
  bin_count?: number | null;
  /**
   * A workbench's series, one to eight. Each is validated by the API as an
   * observations request in its own right, so nothing a series carries can
   * be a request the observations route would refuse.
   */
  series?: SeriesDocument[] | null;
  presentation?: PresentationDocument | null;
  alignment?: AlignmentDocument | null;
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

// --- Evidence packets (ADR-0004) ---

export type PacketBlockType =
  | "text"
  | "analysis"
  | "table"
  | "map"
  | "source-note"
  | "methodology"
  | "caveat";

/** The envelope as the API stores it: snake_case, every field present. */
export interface ApiReproducibilityEnvelope {
  metric_codes: string[];
  source_codes: string[];
  geo_id: string;
  geo_level: string;
  scope: "latest" | "as_released";
  release: string;
  /**
   * The reduction the block's query was viewed with. Cross-checked against
   * the block's document by the API, like `scope` and `release`: a map of one
   * value per geography whose stored query replays the whole publication is a
   * different set of rows than the packet argued from (API-120/WEB-071).
   */
  newest_per_geography: boolean;
  newest_release_per_period: boolean;
  period: string;
  units: string;
  transformation: string;
  api_query: string;
  caveats: string[];
}

export interface ApiPacketBlock {
  block_id: string;
  type: PacketBlockType;
  title: string;
  content: string;
  envelope?: ApiReproducibilityEnvelope | null;
  document?: AnalysisDocument | null;
  source_configuration_id?: number | null;
}

/**
 * A packet as the API stores it. `schema_version` is the document's own
 * shape version; the row's `version` is the concurrency counter.
 */
export interface EvidencePacketDocument {
  schema_version: 1;
  title: string;
  purpose: string;
  blocks: ApiPacketBlock[];
}

/**
 * One block's read-time state. `missing` names envelope fields the composer
 * never filled (incomplete); `reason` carries the live contract's verdict
 * (stale) or the incomplete explanation. Different problems, different fixes.
 */
export interface BlockValidation {
  block_id: string;
  valid: boolean;
  reason?: string | null;
  missing: string[];
}

export interface PacketValidation {
  valid: boolean;
  reason?: string | null;
  blocks: BlockValidation[];
}

/** List row: identity, lifecycle, and size — never a verdict. */
export interface EvidencePacketSummary {
  packet_id: number;
  name: string;
  version: number;
  block_count: number;
  analytical_block_count: number;
  created_at: string;
  updated_at: string;
}

export interface EvidencePacketListResponse {
  total: number;
  limit?: number;
  offset?: number;
  items: EvidencePacketSummary[];
}

export interface EvidencePacketRecord {
  packet_id: number;
  name: string;
  version: number;
  document: EvidencePacketDocument;
  validation: PacketValidation;
  created_at: string;
  updated_at: string;
}

/**
 * The API-derived coefficients over one pair, as `/comparison/correlation`
 * and each `/comparison/matrix` cell serve them.
 *
 * Every field is optional because a deployment may serve an older contract,
 * and an absent coefficient is not the same as a coefficient of zero — which
 * is exactly the distinction the route exists to keep. Read
 * `pearson_r != null` before formatting it.
 */
export interface CorrelationStatistic {
  n?: number;
  contemporaneous_pairs?: number;
  pearson_r?: number | null;
  spearman_rho?: number | null;
  periods_differ?: boolean;
  derivations?: string[];
  [key: string]: unknown;
}

/** `GET /comparison/correlation`: the statistic, its coverage, its caveats. */
export interface ComparisonCorrelation extends CorrelationStatistic {
  metric_code_a?: string;
  metric_code_b?: string;
  source_code_a?: string | null;
  source_code_b?: string | null;
  units_a?: string | null;
  units_b?: string | null;
  derived?: boolean;
  geo_level?: string | null;
  state_fips?: string | null;
  year?: number | null;
  geographies_a?: number;
  geographies_b?: number;
  period_a?: string | null;
  period_b?: string | null;
  caveats?: string[];
}

/** One measure as `/comparison/matrix` read it. */
export interface MatrixMetricSummary {
  metric_code?: string;
  source_code?: string | null;
  units?: string | null;
  valid_time_grains?: string[];
  valid_geo_grains?: string[];
  geographies?: number;
  period?: string | null;
  periods_differ?: boolean;
  [key: string]: unknown;
}

/** One unordered pair of the matrix: served, or declined with its rules. */
export interface MatrixPair {
  metric_code_a?: string;
  metric_code_b?: string;
  comparable?: boolean;
  rules?: ComparisonRule[];
  caveats?: string[];
  /** `null` for a declined cell — never a zeroed statistic. */
  statistic?: CorrelationStatistic | null;
  [key: string]: unknown;
}

/** One measure's published value for one geography, or the absence of one. */
export interface MatrixCell {
  metric_code?: string;
  value?: number | null;
  period?: string | null;
  release?: string | null;
}

/** One geography, with one cell per requested measure. */
export interface MatrixRow {
  geo_id?: string | null;
  geo_level?: string | null;
  state_fips?: string | null;
  county_fips?: string | null;
  state_name?: string | null;
  county_name?: string | null;
  values?: MatrixCell[];
  [key: string]: unknown;
}

/** `GET /comparison/matrix`: two to eight measures aligned on geography. */
export interface ComparisonMatrix {
  derived?: boolean;
  geo_level?: string | null;
  state_fips?: string | null;
  year?: number | null;
  metrics?: MatrixMetricSummary[];
  pairs?: MatrixPair[];
  caveats?: string[];
  total?: number;
  limit?: number;
  offset?: number;
  items?: MatrixRow[];
  [key: string]: unknown;
}
