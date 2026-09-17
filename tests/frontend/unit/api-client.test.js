import { describe, expect, test } from "vitest";

// Covers: WEB-009 — versioned API client URL construction, cache mode by
// request kind, pagination, in-flight de-duplication of catalog reads,
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
  requestCacheMode,
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

  test("a public read is sent with the browser's own cache rules", async () => {
    const { calls, fetchImpl } = recordingFetch([
      jsonResponse({ items: [{ metric_code: "X" }], total: 1 }),
    ]);
    const payload = await searchMetrics({ q: "population" }, { fetchImpl });
    expect(payload.items).toHaveLength(1);
    expect(calls[0].path).toBe("/api/v1/catalog/metrics?q=population");
    // `default`, not `no-store`: the API answers public analytical reads with
    // `Cache-Control: public, max-age=<ttl>`, and `no-store` here threw that
    // away. The TTL stays the API's to decide; this client only stops
    // refusing it.
    expect(calls[0].init.cache).toBe("default");
  });

  test("the cache mode follows the kind of request, not the caller", () => {
    expect(requestCacheMode({})).toBe("default");
    expect(requestCacheMode({ method: "GET" })).toBe("default");
    expect(requestCacheMode({ method: "get" })).toBe("default");
    // Somebody's own library, served `private, no-store`: never in a cache
    // the next reader of this browser can reach.
    expect(requestCacheMode({ method: "GET", token: "secret" })).toBe("no-store");
    // A write has nothing to reuse, with or without a token.
    expect(requestCacheMode({ method: "POST", body: { name: "x" } })).toBe("no-store");
    expect(requestCacheMode({ method: "DELETE" })).toBe("no-store");
    expect(requestCacheMode({ method: "GET", body: { q: "x" } })).toBe("no-store");
  });

  test("a token-bearing read is still sent with no-store", async () => {
    const { calls, fetchImpl } = recordingFetch([jsonResponse({ items: [], total: 0 })]);
    await apiFetch("/analysis-configurations", { token: "secret", fetchImpl });
    expect(calls[0].init.cache).toBe("no-store");
    expect(calls[0].path).not.toContain("secret");
  });

  test("two catalog reads in flight at once are one request", async () => {
    const { calls, fetchImpl } = recordingFetch([
      jsonResponse({ items: [{ geo_id: "01" }], total: 1 }),
    ]);
    // Both start before either settles -- the explorer and the workbench
    // mounting in one navigation. One request answers both.
    const [left, right] = await Promise.all([
      fetchAllPages("/catalog/geographies", { params: { geo_level: "STATE" }, fetchImpl }),
      fetchAllPages("/catalog/geographies", { params: { geo_level: "STATE" }, fetchImpl }),
    ]);
    expect(calls).toHaveLength(1);
    expect(left).toEqual([{ geo_id: "01" }]);
    expect(right).toBe(left);
  });

  test("the sharing is in flight only, and never across different reads", async () => {
    const { calls, fetchImpl } = recordingFetch([
      jsonResponse({ items: [{ geo_id: "01" }], total: 1 }),
      jsonResponse({ items: [{ geo_id: "01" }], total: 1 }),
      jsonResponse({ items: [{ geo_id: "06001" }], total: 1 }),
    ]);
    // Settled, then asked again: a second request, because this holds
    // promises rather than answers.
    await fetchAllPages("/catalog/geographies", { params: { geo_level: "STATE" }, fetchImpl });
    await fetchAllPages("/catalog/geographies", { params: { geo_level: "STATE" }, fetchImpl });
    expect(calls).toHaveLength(2);

    // A different query is a different read even while the first is open.
    await Promise.all([
      fetchAllPages("/catalog/geographies", { params: { geo_level: "COUNTY" }, fetchImpl }),
    ]);
    expect(calls).toHaveLength(3);
    expect(calls[2].path).toContain("geo_level=COUNTY");
  });

  test("a cancellable or token-bearing read is never shared", async () => {
    const { calls, fetchImpl } = recordingFetch([
      jsonResponse({ items: [], total: 0 }),
      jsonResponse({ items: [], total: 0 }),
      jsonResponse({ items: [], total: 0 }),
      jsonResponse({ items: [], total: 0 }),
    ]);
    // One caller's abort must not reject another caller's promise, and a
    // token-bearing read is somebody's own.
    const controller = new AbortController();
    await Promise.all([
      fetchAllPages("/catalog/metrics", { fetchImpl, signal: controller.signal }),
      fetchAllPages("/catalog/metrics", { fetchImpl, signal: controller.signal }),
    ]);
    expect(calls).toHaveLength(2);

    await Promise.all([
      fetchAllPages("/catalog/metrics", { fetchImpl, token: "t" }),
      fetchAllPages("/catalog/metrics", { fetchImpl, token: "t" }),
    ]);
    expect(calls).toHaveLength(4);
  });

  test("only the catalog is shared", async () => {
    const { calls, fetchImpl } = recordingFetch([
      jsonResponse({ items: [], total: 0 }),
      jsonResponse({ items: [], total: 0 }),
    ]);
    // Two screens asking for observations are not asking the same question,
    // and the answer is not a catalog every screen holds the whole of.
    await Promise.all([
      fetchAllPages("/observations", { params: { metric_code: "M" }, fetchImpl }),
      fetchAllPages("/observations", { params: { metric_code: "M" }, fetchImpl }),
    ]);
    expect(calls).toHaveLength(2);
  });

  test("two transports are never handed each other's answer", async () => {
    const first = recordingFetch([jsonResponse({ items: [{ geo_id: "01" }], total: 1 })]);
    const second = recordingFetch([jsonResponse({ items: [{ geo_id: "02" }], total: 1 })]);
    const [left, right] = await Promise.all([
      fetchAllPages("/catalog/geographies", { fetchImpl: first.fetchImpl }),
      fetchAllPages("/catalog/geographies", { fetchImpl: second.fetchImpl }),
    ]);
    expect(first.calls).toHaveLength(1);
    expect(second.calls).toHaveLength(1);
    expect(left).toEqual([{ geo_id: "01" }]);
    expect(right).toEqual([{ geo_id: "02" }]);
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

  test("a validation refusal keeps the explanation it was handed", async () => {
    // Covers: WEB-063 — `422` answers two bodies. The API's own refusals are
    // a string; a request refused against the declared parameter and body
    // schemas answers HTTPValidationError, an array of {loc, msg, type}.
    // Only a string survived `decodeErrorDetail`, so the one status the
    // guide calls "a request the API can explain" reached the reader as a
    // bare number.
    const { fetchImpl } = recordingFetch([
      jsonResponse(
        {
          detail: [
            {
              type: "less_than_equal",
              loc: ["query", "limit"],
              msg: "Input should be less than or equal to 1000",
              input: "5000",
              ctx: { le: 1000 },
            },
          ],
        },
        { status: 422 },
      ),
    ]);
    const error = await apiFetch("/catalog/metrics", { fetchImpl }).catch(
      (caught) => caught,
    );
    expect(error.status).toBe(422);
    expect(error.detail).toBe(
      "query.limit: Input should be less than or equal to 1000",
    );
    // The submitted value is not echoed back to the screen: it is the
    // caller's own, of unbounded size, and says nothing the message does not.
    expect(error.detail).not.toContain("5000");
    expect(apiErrorMessage(error)).toBe(
      "status 422: query.limit: Input should be less than or equal to 1000",
    );
  });

  test("several refused fields are counted rather than listed without end", async () => {
    const entries = Array.from({ length: 5 }, (unused, index) => ({
      loc: ["body", "blocks", index, "type"],
      msg: "Input should be a valid block type",
      type: "enum",
    }));
    const { fetchImpl } = recordingFetch([
      jsonResponse({ detail: entries }, { status: 422 }),
    ]);
    const error = await apiFetch("/evidence-packets", { fetchImpl }).catch(
      (caught) => caught,
    );
    expect(error.detail).toBe(
      "body.blocks.0.type: Input should be a valid block type; " +
        "body.blocks.1.type: Input should be a valid block type; " +
        "body.blocks.2.type: Input should be a valid block type (and 2 more)",
    );
  });

  test("a refusal with nothing readable still shows its status", async () => {
    // An empty array, and entries carrying no message, are not sentences.
    // Reporting the status alone is what this did for every array before
    // WEB-063; it stays the answer where there is nothing to add.
    for (const detail of [[], [{ loc: ["query", "limit"] }], [null, 7, "x"]]) {
      const { fetchImpl } = recordingFetch([jsonResponse({ detail }, { status: 422 })]);
      const error = await apiFetch("/catalog/metrics", { fetchImpl }).catch(
        (caught) => caught,
      );
      expect(error.detail).toBeNull();
      expect(apiErrorMessage(error)).toBe("status 422");
    }
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
    // The bound still stops the read at two requests. What it no longer does
    // is hand those two back as the whole list: with no published total the
    // client cannot know whether more exist, so it says so rather than
    // guessing (WEB-056).
    await expect(
      fetchAllPages("/catalog/metrics", {
        pageSize: 1,
        maxPages: 2,
        fetchImpl: endless.fetchImpl,
      }),
    ).rejects.toThrow(/that is a prefix, not the whole list/);
    expect(endless.calls).toHaveLength(2);
  });

  test("passes abort signals through to the transport", async () => {
    const controller = new AbortController();
    const { calls, fetchImpl } = recordingFetch([jsonResponse([])]);
    await apiFetch("/catalog/sources", { signal: controller.signal, fetchImpl });
    expect(calls[0].init.signal).toBe(controller.signal);
  });

  test("constructs source-scoped and analysis routes from the contract", async () => {
    // The source-scoped routes are addressed through `observationAccess.ts`,
    // which picks the access shape a source declares and hands back the
    // resource and params; `apiFetch` is what sends it. The four wrappers
    // that addressed those routes a second way had no caller and are gone.
    const latest = recordingFetch([jsonResponse({ items: [] })]);
    await apiFetch(
      "/census/observations/latest",
      {
        params: { metric_code: "CENSUS_ACS:acs5:B01003_001", geo_level: "COUNTY" },
        fetchImpl: latest.fetchImpl,
      },
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

// Covers: WEB-056 — a bounded read is never handed back as the whole list.
//
// `fetchCollectionPages` computes `complete` so that "a caller that hits the
// bound is told the answer is a prefix rather than handed a truncated list as
// if it were whole". The convenience wrapper beside it dropped that, and the
// wrapper is what every caller in the application uses.
describe("fetchAllPages", () => {
  test("returns every record when the read completed", async () => {
    const whole = recordingFetch([
      jsonResponse({ items: [{ id: 1 }, { id: 2 }], total: 3 }),
      jsonResponse({ items: [{ id: 3 }], total: 3 }),
    ]);
    await expect(
      fetchAllPages("/catalog/metrics", { pageSize: 2, fetchImpl: whole.fetchImpl }),
    ).resolves.toEqual([{ id: 1 }, { id: 2 }, { id: 3 }]);
  });

  test("refuses to hand back a prefix, naming what it got and what there is", async () => {
    const cut = recordingFetch([
      jsonResponse({ items: [{ id: 1 }, { id: 2 }], total: 5 }),
      jsonResponse({ items: [{ id: 3 }, { id: 4 }], total: 5 }),
      jsonResponse({ items: [{ id: 5 }], total: 5 }),
    ]);
    await expect(
      fetchAllPages("/catalog/geographies", {
        pageSize: 2,
        maxPages: 2,
        fetchImpl: cut.fetchImpl,
      }),
    ).rejects.toThrow(/\/catalog\/geographies answered 4 of 5 records/);
    // Bounded as before: it stops at the bound rather than reading on.
    expect(cut.calls).toHaveLength(2);
  });

  test("a collection that publishes no total still completes", async () => {
    // `complete` is true when a page came back empty, whatever the total says,
    // so an API that reports none is not treated as an endless one.
    const unreported = recordingFetch([
      jsonResponse({ items: [{ id: 1 }] }),
      jsonResponse({ items: [] }),
    ]);
    await expect(
      fetchAllPages("/catalog/metrics", { fetchImpl: unreported.fetchImpl }),
    ).resolves.toEqual([{ id: 1 }]);
  });
});
