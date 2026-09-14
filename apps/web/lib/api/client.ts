// Versioned API client for the documented /api/v1 contract.
//
// This is the single transport boundary for browser data access. Routes,
// parameters, pagination, and error behavior follow
// docs/reference/API_CONSUMER_GUIDE.md; nothing here may query warehouse
// state or invent client-side substitutes for API behavior.

import type {
  CollectionResponse,
  ComparisonCorrelation,
  ComparisonMatrix,
  ComparisonPreflight,
  ComparisonResponse,
  ComparisonRow,
  DistributionResponse,
  GeographySummary,
  HealthResponse,
  AnalysisDocument,
  MetricReleaseListResponse,
  MetricSummary,
  Observation,
  EvidencePacketDocument,
  EvidencePacketListResponse,
  EvidencePacketRecord,
  SavedAnalysisConfiguration,
  SavedAnalysisListResponse,
  SourceCapability,
  SourceSummary,
} from "./types";

export const API_BASE = "/api/v1";

/** Classified failure kinds, from the guide's error table. */
export type ApiErrorKind =
  | "unauthorized"
  | "forbidden"
  | "not-found"
  | "conflict"
  | "invalid"
  | "rate-limited"
  | "unavailable"
  | "error";

const ERROR_KIND_BY_STATUS: Record<number, ApiErrorKind> = {
  401: "unauthorized",
  403: "forbidden",
  404: "not-found",
  409: "conflict",
  422: "invalid",
  429: "rate-limited",
  503: "unavailable",
};

/** Query parameters; empty, null, and undefined values are omitted. */
export type QueryParams = Record<string, string | number | boolean | null | undefined>;

export interface RequestOptions {
  params?: QueryParams;
  signal?: AbortSignal;
  /** Injectable transport, for deterministic tests. */
  fetchImpl?: typeof fetch;
  /**
   * Operator-provisioned bearer token for the user-scoped routes. It is
   * sent only as an `Authorization` header — never as a query parameter,
   * because a URL travels into history, referrers, and server logs.
   */
  token?: string | null;
  /** HTTP method; defaults to GET. */
  method?: "GET" | "POST" | "PUT" | "DELETE";
  /** JSON request body, for the write routes. */
  body?: unknown;
}

export interface PageOptions extends RequestOptions {
  pageSize?: number;
  maxPages?: number;
}

interface ApiErrorInit {
  status: number;
  detail?: string | null;
  path: string;
  retryAfter?: number | null;
}

export class ApiError extends Error {
  readonly status: number;
  readonly detail: string | null;
  readonly path: string;
  readonly retryAfter: number | null;
  readonly kind: ApiErrorKind;

  constructor({ status, detail, path, retryAfter = null }: ApiErrorInit) {
    super(detail || `API request failed with status ${status}`);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail || null;
    this.path = path;
    this.retryAfter = retryAfter;
    this.kind = ERROR_KIND_BY_STATUS[status] || (status >= 500 ? "unavailable" : "error");
  }
}

// Status-first message for UI state pills: the HTTP status stays visible
// and the API's own `detail` travels with it when present.
//
// Where the API published a `Retry-After`, the interval travels too. Its own
// detail for a limited request reads "rate limit exceeded; retry after the
// indicated interval" -- a sentence that points at a number the client held
// on the error object and dropped on the way to the screen (WEB-040). The
// reader decides whether to retry; this only tells them when they could.
export function apiErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    const status = `status ${error.status}${error.detail ? `: ${error.detail}` : ""}`;
    const retryAfter = Number(error.retryAfter);
    return Number.isFinite(retryAfter) && retryAfter > 0
      ? `${status} (retry in ${retryAfter}s)`
      : status;
  }
  if (error instanceof Error && error.message) {
    return error.message;
  }
  return "request failed";
}

function buildQuery(params: QueryParams = {}): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    query.set(key, String(value));
  }
  const text = query.toString();
  return text ? `?${text}` : "";
}

export function buildApiPath(resource: string, params: QueryParams = {}): string {
  const path = resource.startsWith("/") ? resource : `/${resource}`;
  return `${API_BASE}${path}${buildQuery(params)}`;
}

/** How many field refusals a message carries before it counts the rest. */
const VALIDATION_DETAIL_LIMIT = 3;

/**
 * A refusal the API made before the endpoint ran, as a readable sentence.
 *
 * `422` answers two bodies (see the consumer guide's Errors section): a
 * string for a refusal the API decided, and `HTTPValidationError` -- an
 * array of `{loc, msg, type}` -- for a request refused against the declared
 * parameter and body schemas. This renders the second, because a reader told
 * only "status 422" on the one class of error the API can explain has been
 * handed the explanation and shown the number.
 *
 * `loc` is the path to what was refused, so it is what names the parameter.
 * The entry's `input` is deliberately not rendered: it is the caller's own
 * submitted value, of unbounded size, and it says nothing the `loc` and the
 * message do not. The count is bounded for the same reason.
 */
function describeValidationDetail(entries: unknown[]): string | null {
  const described: string[] = [];
  for (const entry of entries) {
    if (!entry || typeof entry !== "object") {
      continue;
    }
    const { loc, msg } = entry as { loc?: unknown; msg?: unknown };
    if (typeof msg !== "string" || !msg) {
      continue;
    }
    const where = Array.isArray(loc)
      ? loc
          .filter((part) => typeof part === "string" || typeof part === "number")
          .join(".")
      : "";
    described.push(where ? `${where}: ${msg}` : msg);
  }
  if (described.length === 0) {
    return null;
  }
  const shown = described.slice(0, VALIDATION_DETAIL_LIMIT);
  const remaining = described.length - shown.length;
  return remaining > 0
    ? `${shown.join("; ")} (and ${remaining} more)`
    : shown.join("; ");
}

async function decodeErrorDetail(response: Response): Promise<string | null> {
  try {
    const payload: unknown = await response.json();
    const detail = (payload as { detail?: unknown } | null)?.detail;
    if (typeof detail === "string") {
      return detail;
    }
    // A body carrying no usable entry falls through to null, so the caller
    // still shows the status line rather than an empty sentence.
    return Array.isArray(detail) ? describeValidationDetail(detail) : null;
  } catch {
    return null;
  }
}

export async function apiFetch<T>(
  resource: string,
  { params = {}, signal, fetchImpl, token, method = "GET", body }: RequestOptions = {},
): Promise<T> {
  const path = buildApiPath(resource, params);
  const doFetch = fetchImpl || fetch;
  const headers: Record<string, string> = {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  const response = await doFetch(path, {
    cache: "no-store",
    signal,
    method,
    headers,
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });

  if (!response.ok) {
    const retryAfterHeader = response.headers?.get?.("retry-after");
    throw new ApiError({
      status: response.status,
      detail: await decodeErrorDetail(response),
      // The path is kept for diagnostics; it never carries a token, because
      // the token travels only in the Authorization header.
      path,
      retryAfter: retryAfterHeader ? Number(retryAfterHeader) || null : null,
    });
  }

  // 204 No Content: a successful delete has no body to decode.
  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

/** Every page of a `{items, total}` collection, and whether that was all of it. */
export interface CollectionPages<T> {
  items: T[];
  /** The resource's reported total, or `null` when it published none. */
  total: number | null;
  /**
   * False when paging stopped at `maxPages` before reaching the reported
   * total: the items are a prefix of the answer, not the answer.
   */
  complete: boolean;
}

// Deterministic limit/offset paging over `{items, total}` collection
// responses. Bounded so a contract regression cannot loop forever, and
// honest about it: a caller that hits the bound is told the answer is a
// prefix rather than handed a truncated list as if it were whole.
export async function fetchCollectionPages<T>(
  resource: string,
  { params = {}, pageSize = 1000, maxPages = 50, signal, fetchImpl, token }: PageOptions = {},
): Promise<CollectionPages<T>> {
  const items: T[] = [];
  let offset = 0;
  let total: number | null = null;
  let pages = 0;

  do {
    const payload = await apiFetch<CollectionResponse<T>>(resource, {
      params: { ...params, limit: String(pageSize), offset: String(offset) },
      signal,
      fetchImpl,
      // Carried on every page, and only as an `Authorization` header: an
      // account's own library is a paged collection like any other
      // (WEB-044).
      token,
    });
    const pageItems = Array.isArray(payload.items) ? payload.items : [];
    total =
      typeof payload.total === "number" && Number.isFinite(payload.total)
        ? payload.total
        : null;
    items.push(...pageItems);
    offset += pageItems.length;
    pages += 1;

    if (pageItems.length === 0) {
      return { items, total, complete: true };
    }
    if (pages >= maxPages) {
      return { items, total, complete: total !== null && items.length >= total };
    }
  } while (total === null || items.length < total);

  return { items, total, complete: true };
}

/** A read the page bound cut short, so the list it returned is a prefix. */
export class IncompleteCollectionError extends Error {
  readonly resource: string;
  readonly received: number;
  readonly total: number | null;

  constructor(resource: string, received: number, total: number | null) {
    super(
      `${resource} answered ${received}${total === null ? "" : ` of ${total}`}` +
        " records within the page bound; that is a prefix, not the whole list",
    );
    this.name = "IncompleteCollectionError";
    this.resource = resource;
    this.received = received;
    this.total = total;
  }
}

/**
 * Every record of a collection, or a refusal.
 *
 * `fetchCollectionPages` computes `complete` because "a caller that hits the
 * bound is told the answer is a prefix rather than handed a truncated list as
 * if it were whole" -- and this wrapper used to drop it, which is every
 * caller in the application. A prefix rendered as a measure list or a county
 * picker is a list a person searches and does not find themselves in, told
 * nothing; the data-quality screen went further and stated its length as the
 * number published (WEB-056).
 *
 * Callers already have a failure path for a failed read, so raising is what
 * makes the bound visible. `fetchCollectionPages` is still there for a caller
 * that wants the prefix and the flag.
 */
export async function fetchAllPages<T>(
  resource: string,
  options: PageOptions = {},
): Promise<T[]> {
  const { items, total, complete } = await fetchCollectionPages<T>(resource, options);
  if (!complete) {
    throw new IncompleteCollectionError(resource, items.length, total);
  }
  return items;
}

// --- Discovery ---

export function getSources(options?: RequestOptions): Promise<SourceSummary[]> {
  return apiFetch<SourceSummary[]>("/catalog/sources", options);
}

export function searchMetrics(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<CollectionResponse<MetricSummary>> {
  return apiFetch<CollectionResponse<MetricSummary>>("/catalog/metrics", { ...options, params });
}

export function fetchAllMetrics(
  params: QueryParams,
  options: PageOptions = {},
): Promise<MetricSummary[]> {
  return fetchAllPages<MetricSummary>("/catalog/metrics", { ...options, params });
}

export function getMetric(
  metricCode: string,
  options: RequestOptions = {},
): Promise<MetricSummary> {
  return apiFetch<MetricSummary>(`/catalog/metrics/${encodeURIComponent(metricCode)}`, options);
}

export function getGeographies(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<CollectionResponse<GeographySummary>> {
  return apiFetch<CollectionResponse<GeographySummary>>("/catalog/geographies", {
    ...options,
    params,
  });
}

export function fetchAllGeographies(
  params: QueryParams,
  options: PageOptions = {},
): Promise<GeographySummary[]> {
  return fetchAllPages<GeographySummary>("/catalog/geographies", { ...options, params });
}

// The capability resource answers with the standard `{total, items}`
// collection envelope (CapabilityListResponse), not a bare array.
export function getCapabilities(
  options?: RequestOptions,
): Promise<CollectionResponse<SourceCapability>> {
  return apiFetch<CollectionResponse<SourceCapability>>("/catalog/capabilities", options);
}

export function getFreshness(options?: RequestOptions): Promise<unknown> {
  return apiFetch<unknown>("/catalog/freshness", options);
}

// --- Observations ---

export function getObservations(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<CollectionResponse<Observation>> {
  return apiFetch<CollectionResponse<Observation>>("/observations", { ...options, params });
}

// The release identities `scope=as_released` accepts for one metric,
// newest first. This is the only way to learn what `release=` accepts; a
// client must not invent or infer a release identity.
export function getObservationReleases(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<MetricReleaseListResponse> {
  return apiFetch<MetricReleaseListResponse>("/observations/releases", {
    ...options,
    params,
  });
}

// Legacy MVP shapes (Census ACS, BLS, FRED only); retained consumers should
// migrate to getObservations.
export function getLatestObservations(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<CollectionResponse<Observation>> {
  return apiFetch<CollectionResponse<Observation>>("/observations/latest", { ...options, params });
}

export function getTimeseries(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<CollectionResponse<Observation>> {
  return apiFetch<CollectionResponse<Observation>>("/observations/timeseries", {
    ...options,
    params,
  });
}

// Source-scoped exploration routes, e.g. sourceSegment "census" | "bls" |
// "fred" | "pep". The segment must come from capability discovery, not a
// client-side enumeration.
export function getSourceLatestObservations(
  sourceSegment: string,
  params: QueryParams,
  options: RequestOptions = {},
): Promise<CollectionResponse<Observation>> {
  return apiFetch<CollectionResponse<Observation>>(`/${sourceSegment}/observations/latest`, {
    ...options,
    params,
  });
}

export function getSourceTimeseries(
  sourceSegment: string,
  params: QueryParams,
  options: RequestOptions = {},
): Promise<CollectionResponse<Observation>> {
  return apiFetch<CollectionResponse<Observation>>(`/${sourceSegment}/observations/timeseries`, {
    ...options,
    params,
  });
}

// --- Analysis ---

export function getDistributionBins(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<DistributionResponse> {
  return apiFetch<DistributionResponse>("/distribution/bins", { ...options, params });
}

export function getComparisonPreflight(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<ComparisonPreflight> {
  return apiFetch<ComparisonPreflight>("/comparison/preflight", { ...options, params });
}

// Preflight before you compare: `/comparison` enforces exactly the verdict
// `/comparison/preflight` publishes, and answers an incompatible pair with a
// 422 naming the failed rules.
export function getComparison(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<ComparisonResponse> {
  return apiFetch<ComparisonResponse>("/comparison", { ...options, params });
}

/**
 * The API-derived correlation over a comparable pair (API-130).
 *
 * Takes the parameters `/comparison` takes and no paging: the statistic is
 * over the whole join, so there is no page to ask for. An incomparable pair
 * answers 422 with its failed rules, exactly as `/comparison` does, which is
 * why a caller asks `/comparison/preflight` first.
 */
export function getComparisonCorrelation(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<ComparisonCorrelation> {
  return apiFetch<ComparisonCorrelation>("/comparison/correlation", {
    ...options,
    params,
  });
}

/**
 * Two to eight measures aligned on geography, with a verdict per pair
 * (API-132).
 *
 * A pair the policy declines is a cell in `pairs`, not an error; a measure
 * whose source the analysis routes decline refuses the whole request. `items`
 * pages the union of the geographies the measures published, so this is the
 * one comparison-family read whose rows are not an intersection.
 */
export function getComparisonMatrix(
  params: QueryParams,
  options: RequestOptions = {},
): Promise<ComparisonMatrix> {
  return apiFetch<ComparisonMatrix>("/comparison/matrix", { ...options, params });
}

/** A paged comparison: one envelope, every aligned row it could reach. */
export interface ComparisonPages {
  /** The first page's response, with every page's rows in `items`. */
  payload: ComparisonResponse | null;
  items: ComparisonRow[];
  total: number | null;
  /** False when a page bound stopped the read before the reported total. */
  complete: boolean;
}

/**
 * Every aligned geography `/comparison` will serve for one selection.
 *
 * The route caps `limit` at 1000 and a national county comparison aligns
 * 3,144 geographies, so a single request held the first thousand rows
 * ordered by `geo_id` -- Alabama through part of Illinois -- and the scatter
 * plot, the choropleth, and the export were drawn from them (WEB-039). That
 * is not a sample of the United States; it is a systematically biased subset
 * no reader could identify from the chart.
 *
 * The envelope -- units, derivations, caveats, the metric and source
 * identities -- describes the pair rather than the page, so it is taken from
 * the first response and kept. Only rows accumulate.
 */
export async function fetchComparisonPages(
  params: QueryParams,
  { pageSize = 1000, maxPages = 8, signal, fetchImpl }: PageOptions = {},
): Promise<ComparisonPages> {
  let payload: ComparisonResponse | null = null;
  const items: ComparisonRow[] = [];
  let total: number | null = null;
  let offset = 0;
  let pages = 0;

  do {
    const page = await apiFetch<ComparisonResponse>("/comparison", {
      params: { ...params, limit: String(pageSize), offset: String(offset) },
      signal,
      fetchImpl,
    });
    const pageItems = Array.isArray(page.items) ? page.items : [];
    if (payload === null) {
      payload = page;
    }
    total =
      typeof page.total === "number" && Number.isFinite(page.total) ? page.total : null;
    items.push(...pageItems);
    offset += pageItems.length;
    pages += 1;

    if (pageItems.length === 0) {
      break;
    }
    if (pages >= maxPages) {
      return {
        payload: payload === null ? null : { ...payload, items },
        items,
        total,
        // A resource that published no total states no shortfall, and
        // inventing one would assert a count the API did not publish.
        complete: total === null || items.length >= total,
      };
    }
  } while (total === null || items.length < total);

  return {
    payload: payload === null ? null : { ...payload, items },
    items,
    total,
    complete: true,
  };
}

// --- Health ---

export function getHealth(options?: RequestOptions): Promise<HealthResponse> {
  return apiFetch<HealthResponse>("/health", options);
}

// --- Saved analysis configurations (ADR-0003) ---
//
// Every route here is user-scoped and requires a bearer token. The API
// answers `private, no-store`, and these paths sit outside the cacheable
// public prefixes, so user content has no path into a shared cache. Nothing
// here may place a configuration's content or its owner's token into a URL.

export function listSavedAnalyses(
  token: string,
  params: QueryParams = {},
  options: RequestOptions = {},
): Promise<SavedAnalysisListResponse> {
  return apiFetch<SavedAnalysisListResponse>("/analysis-configurations", {
    ...options,
    params,
    token,
  });
}

export function getSavedAnalysis(
  token: string,
  configurationId: number,
  options: RequestOptions = {},
): Promise<SavedAnalysisConfiguration> {
  return apiFetch<SavedAnalysisConfiguration>(
    `/analysis-configurations/${configurationId}`,
    { ...options, token },
  );
}

export function createSavedAnalysis(
  token: string,
  payload: { name: string; document: AnalysisDocument },
  options: RequestOptions = {},
): Promise<SavedAnalysisConfiguration> {
  return apiFetch<SavedAnalysisConfiguration>("/analysis-configurations", {
    ...options,
    token,
    method: "POST",
    body: payload,
  });
}

// An update states the version it read; a mismatch is a 409 naming the
// current version, which the caller resolves rather than overwriting.
export function updateSavedAnalysis(
  token: string,
  configurationId: number,
  payload: { name: string; document: AnalysisDocument; expected_version: number },
  options: RequestOptions = {},
): Promise<SavedAnalysisConfiguration> {
  return apiFetch<SavedAnalysisConfiguration>(
    `/analysis-configurations/${configurationId}`,
    { ...options, token, method: "PUT", body: payload },
  );
}

export function deleteSavedAnalysis(
  token: string,
  configurationId: number,
  options: RequestOptions = {},
): Promise<void> {
  return apiFetch<void>(`/analysis-configurations/${configurationId}`, {
    ...options,
    token,
    method: "DELETE",
  });
}

// --- Evidence packets (ADR-0004) ---
//
// The same discipline as the configuration routes: user-scoped, bearer token
// only as a header, `private, no-store`, outside the cacheable prefixes.
// Nothing here may place a packet's content, id, or owner's token in a URL.

export function listEvidencePackets(
  token: string,
  params: QueryParams = {},
  options: RequestOptions = {},
): Promise<EvidencePacketListResponse> {
  return apiFetch<EvidencePacketListResponse>("/evidence-packets", {
    ...options,
    params,
    token,
  });
}

export function getEvidencePacket(
  token: string,
  packetId: number,
  options: RequestOptions = {},
): Promise<EvidencePacketRecord> {
  return apiFetch<EvidencePacketRecord>(`/evidence-packets/${packetId}`, {
    ...options,
    token,
  });
}

export function createEvidencePacket(
  token: string,
  payload: { name: string; document: EvidencePacketDocument },
  options: RequestOptions = {},
): Promise<EvidencePacketRecord> {
  return apiFetch<EvidencePacketRecord>("/evidence-packets", {
    ...options,
    token,
    method: "POST",
    body: payload,
  });
}

export function updateEvidencePacket(
  token: string,
  packetId: number,
  payload: { name: string; document: EvidencePacketDocument; expected_version: number },
  options: RequestOptions = {},
): Promise<EvidencePacketRecord> {
  return apiFetch<EvidencePacketRecord>(`/evidence-packets/${packetId}`, {
    ...options,
    token,
    method: "PUT",
    body: payload,
  });
}

export function deleteEvidencePacket(
  token: string,
  packetId: number,
  options: RequestOptions = {},
): Promise<void> {
  return apiFetch<void>(`/evidence-packets/${packetId}`, {
    ...options,
    token,
    method: "DELETE",
  });
}
