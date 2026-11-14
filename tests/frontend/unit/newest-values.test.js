// Covers: WEB-149 — the newest-values batch splits at the API's bounds and
// keys each item by its metric and place; only sources whose single read is
// the reduced, geo_id-filtered read are batched.
import { describe, expect, test } from "vitest";

import {
  NEWEST_BATCH_MOST_CODES,
  NEWEST_BATCH_MOST_GEOGRAPHIES,
  batchesNewestValues,
  getNewestValuesBatch,
  newestKey,
} from "../../../apps/web/lib/newestValues";

function answering() {
  const calls = [];
  const fetchImpl = async (path) => {
    calls.push(path);
    const url = new URL(path, "http://test");
    const codes = url.searchParams.get("metric_codes").split(",");
    const places = url.searchParams.get("geo_ids").split(",");
    const items = codes.flatMap((metric_code) =>
      places.map((geo_id) => ({ metric_code, geo_id, status: 200, detail: null, observations: { items: [] } })),
    );
    return { ok: true, status: 200, headers: new Headers(), json: async () => ({ items }) };
  };
  return { calls, fetchImpl };
}

describe("newest-values batch", () => {
  test("splits at the API's bounds and keys every pair", async () => {
    const { calls, fetchImpl } = answering();
    const codes = Array.from({ length: NEWEST_BATCH_MOST_CODES + 1 }, (_, n) => `M:${n}`);
    const places = Array.from({ length: NEWEST_BATCH_MOST_GEOGRAPHIES + 1 }, (_, n) => `state:${n}`);
    const found = await getNewestValuesBatch([...codes, "M:0"], places, { fetchImpl });
    expect(calls).toHaveLength(4);
    for (const path of calls) {
      const url = new URL(path, "http://test");
      expect(url.pathname).toBe("/api/v1/observations/newest/batch");
      expect(url.searchParams.get("metric_codes").split(",").length).toBeLessThanOrEqual(NEWEST_BATCH_MOST_CODES);
      expect(url.searchParams.get("geo_ids").split(",").length).toBeLessThanOrEqual(NEWEST_BATCH_MOST_GEOGRAPHIES);
    }
    expect(found.size).toBe(codes.length * places.length);
    expect(found.get(newestKey("M:40", "state:8"))?.status).toBe(200);
  });

  test("batches only the reduced read filtered by geo_id", () => {
    expect(batchesNewestValues({ supportsNewestPerGeography: true, neutralFilters: ["geo_id"] })).toBe(true);
    expect(batchesNewestValues({ supportsNewestPerGeography: false, neutralFilters: ["geo_id"] })).toBe(false);
    expect(batchesNewestValues({ supportsNewestPerGeography: true, neutralFilters: ["geo_level"] })).toBe(false);
    expect(batchesNewestValues(null)).toBe(false);
  });
});
