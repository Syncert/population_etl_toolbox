// Covers: WEB-153 — the settled-history batch splits at the API's bounds and
// keys each item by its metric and place; only sources whose history read is
// the settled as-released read filtered by geo_id are batched.
import { describe, expect, test } from "vitest";

import {
  HISTORY_BATCH_MOST_CODES,
  HISTORY_BATCH_MOST_GEOGRAPHIES,
  batchesHistory,
  getHistoriesBatch,
  historyKey,
} from "../../../apps/web/lib/historyBatch";

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

describe("settled-history batch", () => {
  test("splits at the bounds the API sets and keys every pair", async () => {
    const { calls, fetchImpl } = answering();
    const codes = Array.from({ length: HISTORY_BATCH_MOST_CODES + 1 }, (_, n) => `M:${n}`);
    const places = Array.from({ length: HISTORY_BATCH_MOST_GEOGRAPHIES + 1 }, (_, n) => `state:${n}`);
    const found = await getHistoriesBatch([...codes, "M:0"], places, { fetchImpl });
    expect(calls).toHaveLength(4);
    for (const path of calls) {
      const url = new URL(path, "http://test");
      expect(url.pathname).toBe("/api/v1/observations/history/batch");
      expect(url.searchParams.get("metric_codes").split(",").length).toBeLessThanOrEqual(HISTORY_BATCH_MOST_CODES);
      expect(url.searchParams.get("geo_ids").split(",").length).toBeLessThanOrEqual(HISTORY_BATCH_MOST_GEOGRAPHIES);
    }
    expect(found.size).toBe(codes.length * places.length);
    expect(found.get(historyKey("M:12", "state:8"))?.status).toBe(200);
  });

  test("batches only the settled read filtered by geo_id", () => {
    const settled = { supportsSettledHistory: true, supportsAsReleased: true, neutralFilters: ["geo_id"] };
    expect(batchesHistory(settled)).toBe(true);
    expect(batchesHistory({ ...settled, supportsSettledHistory: false })).toBe(false);
    expect(batchesHistory({ ...settled, supportsAsReleased: false })).toBe(false);
    expect(batchesHistory({ ...settled, neutralFilters: ["geo_level"] })).toBe(false);
    expect(batchesHistory(null)).toBe(false);
  });
});
