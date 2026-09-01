import { describe, expect, test } from "vitest";

// Covers: WEB-009 — versioned API client URL construction, pagination,
// error decoding/classification, cancellation passthrough, and
// stale-response protection.

import {
  ApiError,
  apiFetch,
  buildApiPath,
  fetchAllPages,
  getDistributionBins,
  getSourceLatestObservations,
  searchMetrics,
} from "../../../apps/web/lib/api/client";
import {
  REQUEST_STATES,
  createRequestTracker,
} from "../../../apps/web/lib/api/requestState";

function jsonResponse(payload, { status = 200, headers = {} } = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name) => headers[name.toLowerCase()] || null },
    json: async () => payload,
  };
}

function recordingFetch(responses) {
  const calls = [];
  const queue = [...responses];
  const fetchImpl = async (path, init) => {
    calls.push({ path, init });
    if (queue.length === 0) {
      throw new Error("unexpected extra request");
    }
    return queue.shift();
  };
  return { calls, fetchImpl };
}

describe("versioned API client", () => {
  test("builds /api/v1 paths and omits empty parameters", () => {
    expect(
      buildApiPath("/catalog/metrics", {
        source_code: "CENSUS_ACS",
        q: "",
        state_fips: null,
        active_only: "true",
      }),
    ).toBe("/api/v1/catalog/metrics?source_code=CENSUS_ACS&active_only=true");
    expect(buildApiPath("/catalog/sources")).toBe("/api/v1/catalog/sources");
  });

  test("requests with no-store and returns decoded payloads", async () => {
    const { calls, fetchImpl } = recordingFetch([
      jsonResponse({ items: [{ metric_code: "X" }], total: 1 }),
    ]);
    const payload = await searchMetrics({ q: "population" }, { fetchImpl });
    expect(payload.items).toHaveLength(1);
    expect(calls[0].path).toBe("/api/v1/catalog/metrics?q=population");
    expect(calls[0].init.cache).toBe("no-store");
  });

  test("decodes the stable error envelope and classifies statuses", async () => {
    const { fetchImpl } = recordingFetch([
      jsonResponse({ detail: "metric_code not found" }, { status: 404 }),
    ]);
    const error = await apiFetch("/catalog/metrics/NOPE", { fetchImpl }).catch(
      (caught) => caught,
    );
    expect(error).toBeInstanceOf(ApiError);
    expect(error.status).toBe(404);
    expect(error.detail).toBe("metric_code not found");
    expect(error.kind).toBe("not-found");

    const { fetchImpl: limitedFetch } = recordingFetch([
      jsonResponse({ detail: "rate limited" }, { status: 429, headers: { "retry-after": "7" } }),
    ]);
    const limited = await apiFetch("/observations", { fetchImpl: limitedFetch }).catch(
      (caught) => caught,
    );
    expect(limited.kind).toBe("rate-limited");
    expect(limited.retryAfter).toBe(7);

    const { fetchImpl: downFetch } = recordingFetch([
      jsonResponse({ detail: "service unavailable" }, { status: 503 }),
    ]);
    const down = await apiFetch("/health", { fetchImpl: downFetch }).catch(
      (caught) => caught,
    );
    expect(down.kind).toBe("unavailable");
  });

  test("pages deterministically until the reported total is reached", async () => {
    const { calls, fetchImpl } = recordingFetch([
      jsonResponse({ items: [{ id: 1 }, { id: 2 }], total: 3 }),
      jsonResponse({ items: [{ id: 3 }], total: 3 }),
    ]);
    const items = await fetchAllPages("/catalog/metrics", {
      params: { source_code: "BLS" },
      pageSize: 2,
      fetchImpl,
    });
    expect(items.map((item) => item.id)).toEqual([1, 2, 3]);
    expect(calls[0].path).toContain("limit=2");
    expect(calls[0].path).toContain("offset=0");
    expect(calls[1].path).toContain("offset=2");
  });

  test("stops paging on an empty page and on the page bound", async () => {
    const emptyPage = recordingFetch([jsonResponse({ items: [], total: null })]);
    await expect(
      fetchAllPages("/catalog/geographies", { fetchImpl: emptyPage.fetchImpl }),
    ).resolves.toEqual([]);

    const endless = recordingFetch([
      jsonResponse({ items: [{ id: 1 }], total: null }),
      jsonResponse({ items: [{ id: 2 }], total: null }),
      jsonResponse({ items: [], total: null }),
    ]);
    const items = await fetchAllPages("/catalog/metrics", {
      pageSize: 1,
      maxPages: 2,
      fetchImpl: endless.fetchImpl,
    });
    expect(items).toHaveLength(2);
    expect(endless.calls).toHaveLength(2);
  });

  test("passes abort signals through to the transport", async () => {
    const controller = new AbortController();
    const { calls, fetchImpl } = recordingFetch([jsonResponse([])]);
    await apiFetch("/catalog/sources", { signal: controller.signal, fetchImpl });
    expect(calls[0].init.signal).toBe(controller.signal);
  });

  test("constructs source-scoped and analysis routes from the contract", async () => {
    const latest = recordingFetch([jsonResponse({ items: [] })]);
    await getSourceLatestObservations(
      "census",
      { metric_code: "ACS:acs5:B01003_001", geo_level: "COUNTY" },
      { fetchImpl: latest.fetchImpl },
    );
    expect(latest.calls[0].path).toBe(
      "/api/v1/census/observations/latest?metric_code=ACS%3Aacs5%3AB01003_001&geo_level=COUNTY",
    );

    const bins = recordingFetch([jsonResponse({ items: [] })]);
    await getDistributionBins(
      { metric_code: "M", bin_count: 5 },
      { fetchImpl: bins.fetchImpl },
    );
    expect(bins.calls[0].path).toBe(
      "/api/v1/distribution/bins?metric_code=M&bin_count=5",
    );
  });
});

describe("request lifecycle state", () => {
  test("exposes one shared state vocabulary", () => {
    expect(REQUEST_STATES.loading).toBe("loading");
    expect(Object.isFrozen(REQUEST_STATES)).toBe(true);
  });

  test("suppresses stale completions after a newer request begins", () => {
    const tracker = createRequestTracker();
    const first = tracker.begin();
    const second = tracker.begin();
    expect(first.isCurrent()).toBe(false);
    expect(second.isCurrent()).toBe(true);

    tracker.invalidate();
    expect(second.isCurrent()).toBe(false);
  });
});
