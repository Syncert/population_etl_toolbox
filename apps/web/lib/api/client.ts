// Versioned API client for the documented /api/v1 contract.
//
// This is the single transport boundary for browser data access. Routes,
// parameters, pagination, and error behavior follow
// docs/reference/API_CONSUMER_GUIDE.md; nothing here may query warehouse
// state or invent client-side substitutes for API behavior.

import type {
  CollectionResponse,
  ComparisonPreflight,
  ComparisonResponse,
  DistributionResponse,
  GeographySummary,
  HealthResponse,
  AnalysisDocument,
  MetricReleaseListResponse,
  MetricSummary,
  Observation,
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
export function apiErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return `status ${error.status}${error.detail ? `: ${error.detail}` : ""}`;
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

async function decodeErrorDetail(response: Response): Promise<string | null> {
  try {
    const payload: unknown = await response.json();
    const detail = (payload as { detail?: unknown } | null)?.detail;
    return typeof detail === "string" ? detail : null;
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

// Deterministic limit/offset paging over `{items, total}` collection
// responses. Bounded so a contract regression cannot loop forever.
export async function fetchAllPages<T>(
  resource: string,
  { params = {}, pageSize = 1000, maxPages = 50, signal, fetchImpl }: PageOptions = {},
): Promise<T[]> {
  const items: T[] = [];
  let offset = 0;
  let total: number | null = null;
  let pages = 0;

  do {
    const payload = await apiFetch<CollectionResponse<T>>(resource, {
      params: { ...params, limit: String(pageSize), offset: String(offset) },
      signal,
      fetchImpl,
    });
    const pageItems = Array.isArray(payload.items) ? payload.items : [];
    total =
      typeof payload.total === "number" && Number.isFinite(payload.total)
        ? payload.total
        : null;
    items.push(...pageItems);
    offset += pageItems.length;
    pages += 1;

    if (pageItems.length === 0 || pages >= maxPages) {
      break;
    }
  } while (total === null || items.length < total);

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
