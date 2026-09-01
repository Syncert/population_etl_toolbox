// Versioned API client for the documented /api/v1 contract.
//
// This is the single transport boundary for browser data access. Routes,
// parameters, pagination, and error behavior follow
// docs/reference/API_CONSUMER_GUIDE.md; nothing here may query warehouse
// state or invent client-side substitutes for API behavior.

export const API_BASE = "/api/v1";

const ERROR_KIND_BY_STATUS = {
  401: "unauthorized",
  403: "forbidden",
  404: "not-found",
  409: "conflict",
  422: "invalid",
  429: "rate-limited",
  503: "unavailable",
};

export class ApiError extends Error {
  constructor({ status, detail, path, retryAfter = null }) {
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
export function apiErrorMessage(error) {
  if (error instanceof ApiError) {
    return `status ${error.status}${error.detail ? `: ${error.detail}` : ""}`;
  }
  return error?.message || "request failed";
}

function buildQuery(params = {}) {
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

export function buildApiPath(resource, params = {}) {
  const path = resource.startsWith("/") ? resource : `/${resource}`;
  return `${API_BASE}${path}${buildQuery(params)}`;
}

async function decodeErrorDetail(response) {
  try {
    const payload = await response.json();
    return typeof payload?.detail === "string" ? payload.detail : null;
  } catch {
    return null;
  }
}

export async function apiFetch(resource, { params = {}, signal, fetchImpl } = {}) {
  const path = buildApiPath(resource, params);
  const doFetch = fetchImpl || fetch;
  const response = await doFetch(path, { cache: "no-store", signal });

  if (!response.ok) {
    const retryAfterHeader = response.headers?.get?.("retry-after");
    throw new ApiError({
      status: response.status,
      detail: await decodeErrorDetail(response),
      path,
      retryAfter: retryAfterHeader ? Number(retryAfterHeader) || null : null,
    });
  }

  return response.json();
}

// Deterministic limit/offset paging over `{items, total}` collection
// responses. Bounded so a contract regression cannot loop forever.
export async function fetchAllPages(
  resource,
  { params = {}, pageSize = 1000, maxPages = 50, signal, fetchImpl } = {},
) {
  const items = [];
  let offset = 0;
  let total = null;
  let pages = 0;

  do {
    const payload = await apiFetch(resource, {
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

export function getSources(options) {
  return apiFetch("/catalog/sources", options);
}

export function searchMetrics(params, options = {}) {
  return apiFetch("/catalog/metrics", { ...options, params });
}

export function fetchAllMetrics(params, options = {}) {
  return fetchAllPages("/catalog/metrics", { ...options, params });
}

export function getMetric(metricCode, options = {}) {
  return apiFetch(`/catalog/metrics/${encodeURIComponent(metricCode)}`, options);
}

export function getGeographies(params, options = {}) {
  return apiFetch("/catalog/geographies", { ...options, params });
}

export function fetchAllGeographies(params, options = {}) {
  return fetchAllPages("/catalog/geographies", { ...options, params });
}

export function getCapabilities(options) {
  return apiFetch("/catalog/capabilities", options);
}

export function getFreshness(options) {
  return apiFetch("/catalog/freshness", options);
}

// --- Observations ---

export function getObservations(params, options = {}) {
  return apiFetch("/observations", { ...options, params });
}

export function getObservationReleases(params, options = {}) {
  return apiFetch("/observations/releases", { ...options, params });
}

// Legacy MVP shapes (Census ACS, BLS, FRED only); retained consumers should
// migrate to getObservations.
export function getLatestObservations(params, options = {}) {
  return apiFetch("/observations/latest", { ...options, params });
}

export function getTimeseries(params, options = {}) {
  return apiFetch("/observations/timeseries", { ...options, params });
}

// Source-scoped exploration routes, e.g. sourceSegment "census" | "bls" |
// "fred" | "pep". The segment must come from capability discovery, not a
// client-side enumeration.
export function getSourceLatestObservations(sourceSegment, params, options = {}) {
  return apiFetch(`/${sourceSegment}/observations/latest`, { ...options, params });
}

export function getSourceTimeseries(sourceSegment, params, options = {}) {
  return apiFetch(`/${sourceSegment}/observations/timeseries`, { ...options, params });
}

// --- Analysis ---

export function getDistributionBins(params, options = {}) {
  return apiFetch("/distribution/bins", { ...options, params });
}

export function getComparisonPreflight(params, options = {}) {
  return apiFetch("/comparison/preflight", { ...options, params });
}

export function getComparison(params, options = {}) {
  return apiFetch("/comparison", { ...options, params });
}

// --- Health ---

export function getHealth(options) {
  return apiFetch("/health", options);
}
