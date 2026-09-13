import { describe, expect, test } from "vitest";

// Covers: WEB-009 — versioned API client URL construction, pagination,
// error decoding/classification, cancellation passthrough, and
// stale-response protection.

import {
  ApiError,
  apiErrorMessage,
  apiFetch,
  buildApiPath,
  fetchAllPages,
  fetchCollectionPages,
  fetchComparisonPages,
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

  test("renders status-first user-facing error messages", () => {
    expect(
      apiErrorMessage(
        new ApiError({ status: 503, detail: "fallback unavailable", path: "/api/v1/x" }),
      ),
    ).toBe("status 503: fallback unavailable");
    expect(
      apiErrorMessage(new ApiError({ status: 404, detail: null, path: "/api/v1/x" })),
    ).toBe("status 404");
    expect(apiErrorMessage(new Error("network down"))).toBe("network down");
    expect(apiErrorMessage(undefined)).toBe("request failed");
  });

  // Covers: WEB-040 — the API publishes how long to wait and the client
  // captured it, then dropped it. The detail it renders beside the status
  // says "retry after the indicated interval" and indicated nothing.
  test("a rate-limited message says how long to wait", () => {
    expect(
      apiErrorMessage(
        new ApiError({
          status: 429,
          detail: "rate limit exceeded; retry after the indicated interval",
          path: "/api/v1/observations",
          retryAfter: 12,
        }),
      ),
    ).toBe(
      "status 429: rate limit exceeded; retry after the indicated interval " +
        "(retry in 12s)",
    );
  });

  test("an error the API published no interval for is unchanged", () => {
    for (const retryAfter of [null, 0, undefined, Number.NaN]) {
      expect(
        apiErrorMessage(
          new ApiError({ status: 503, detail: "unavailable", path: "/x", retryAfter }),
        ),
      ).toBe("status 503: unavailable");
    }
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
      { metric_code: "CENSUS_ACS:acs5:B01003_001", geo_level: "COUNTY" },
      { fetchImpl: latest.fetchImpl },
    );
    expect(latest.calls[0].path).toBe(
      "/api/v1/census/observations/latest?metric_code=CENSUS_ACS%3Aacs5%3AB01003_001&geo_level=COUNTY",
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

  test("reports the total and whether the page bound cut the answer short", async () => {
    const whole = recordingFetch([
      jsonResponse({ items: [{ id: 1 }, { id: 2 }], total: 3 }),
      jsonResponse({ items: [{ id: 3 }], total: 3 }),
    ]);
    await expect(
      fetchCollectionPages("/observations", { pageSize: 2, fetchImpl: whole.fetchImpl }),
    ).resolves.toEqual({ items: [{ id: 1 }, { id: 2 }, { id: 3 }], total: 3, complete: true });

    const cut = recordingFetch([
      jsonResponse({ items: [{ id: 1 }, { id: 2 }], total: 5 }),
      jsonResponse({ items: [{ id: 3 }, { id: 4 }], total: 5 }),
      jsonResponse({ items: [{ id: 5 }], total: 5 }),
    ]);
    const prefix = await fetchCollectionPages("/observations", {
      pageSize: 2,
      maxPages: 2,
      fetchImpl: cut.fetchImpl,
    });
    expect(prefix.items.map((item) => item.id)).toEqual([1, 2, 3, 4]);
    expect(prefix.total).toBe(5);
    expect(prefix.complete).toBe(false);
    expect(cut.calls).toHaveLength(2);

    const unreported = recordingFetch([jsonResponse({ items: [{ id: 1 }] }), jsonResponse({ items: [] })]);
    await expect(
      fetchCollectionPages("/observations", { fetchImpl: unreported.fetchImpl }),
    ).resolves.toEqual({ items: [{ id: 1 }], total: null, complete: true });
  });
});

// Covers: WEB-039 — the comparison is paged. `/comparison` caps `limit` at
// 1000 and a national county comparison aligns 3,144 geographies, so a
// single request held the first thousand rows ordered by geo_id -- Alabama
// through part of Illinois -- and the scatter, the map, and the export were
// drawn from them.
describe("comparison paging", () => {
  const envelope = (items, total) => ({
    metric_code_a: "A",
    metric_code_b: "B",
    units_a: "people",
    derivations: ["difference", "ratio"],
    caveats: ["units unverified"],
    total,
    items,
  });

  test("reads every aligned geography the API reports", async () => {
    const { calls, fetchImpl } = recordingFetch([
      jsonResponse(envelope([{ geo_id: "1" }, { geo_id: "2" }], 3)),
      jsonResponse(envelope([{ geo_id: "3" }], 3)),
    ]);
    const pages = await fetchComparisonPages(
      { metric_code_a: "A", metric_code_b: "B" },
      { pageSize: 2, fetchImpl },
    );

    expect(pages.items.map((row) => row.geo_id)).toEqual(["1", "2", "3"]);
    expect(pages.total).toBe(3);
    expect(pages.complete).toBe(true);
    expect(calls[0].path).toContain("offset=0");
    expect(calls[1].path).toContain("offset=2");
  });

  test("the envelope comes from the first page and is preserved", async () => {
    // Units, derivations, and caveats describe the pair, not the page.
    const { fetchImpl } = recordingFetch([
      jsonResponse(envelope([{ geo_id: "1" }], 2)),
      jsonResponse({ items: [{ geo_id: "2" }], total: 2 }),
    ]);
    const pages = await fetchComparisonPages(
      { metric_code_a: "A", metric_code_b: "B" },
      { pageSize: 1, fetchImpl },
    );
    expect(pages.payload?.units_a).toBe("people");
    expect(pages.payload?.caveats).toEqual(["units unverified"]);
    expect(pages.payload?.items.map((row) => row.geo_id)).toEqual(["1", "2"]);
  });

  test("a bound-limited read is reported as incomplete", async () => {
    const { fetchImpl } = recordingFetch([
      jsonResponse(envelope([{ geo_id: "1" }], 9999)),
      jsonResponse(envelope([{ geo_id: "2" }], 9999)),
    ]);
    const pages = await fetchComparisonPages(
      { metric_code_a: "A", metric_code_b: "B" },
      { pageSize: 1, maxPages: 2, fetchImpl },
    );
    expect(pages.items).toHaveLength(2);
    expect(pages.total).toBe(9999);
    expect(pages.complete).toBe(false);
  });

  test("an empty page ends the read, and no total is not a shortfall", async () => {
    const { fetchImpl } = recordingFetch([
      jsonResponse(envelope([{ geo_id: "1" }], null)),
      jsonResponse(envelope([], null)),
    ]);
    const pages = await fetchComparisonPages(
      { metric_code_a: "A", metric_code_b: "B" },
      { pageSize: 1, fetchImpl },
    );
    expect(pages.items).toHaveLength(1);
    expect(pages.total).toBe(null);
    expect(pages.complete).toBe(true);
  });
});

// Covers: WEB-044 — an account's library is paged, and the token travels
// with every page. Three screens read the library with one request at the
// route's maximum and reported a partial answer in green; the entries past
// it could not be opened, edited, or added to a packet.
describe("authenticated collection paging", () => {
  test("the bearer token travels on every page, never in the query", async () => {
    const { calls, fetchImpl } = recordingFetch([
      jsonResponse({ items: [{ id: 1 }], total: 2 }),
      jsonResponse({ items: [{ id: 2 }], total: 2 }),
    ]);
    const pages = await fetchCollectionPages("/evidence-packets", {
      pageSize: 1,
      token: "secret-token",
      fetchImpl,
    });

    expect(pages.items).toHaveLength(2);
    expect(pages.complete).toBe(true);
    expect(calls).toHaveLength(2);
    for (const call of calls) {
      expect(call.init.headers.Authorization).toBe("Bearer secret-token");
      // A token in a query string travels into history, referrers, and logs.
      expect(call.path).not.toContain("secret-token");
    }
  });

  test("an unauthenticated collection is unchanged", async () => {
    const { calls, fetchImpl } = recordingFetch([
      jsonResponse({ items: [{ id: 1 }], total: 1 }),
    ]);
    await fetchCollectionPages("/catalog/metrics", { pageSize: 1, fetchImpl });
    expect(calls[0].init.headers.Authorization).toBeUndefined();
  });
});
