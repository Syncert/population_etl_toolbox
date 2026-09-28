import { expect, test } from "vitest";

import {
  makeSweepReport,
  parseSweepSelection,
  selectSweepMetrics,
} from "../support/mapSweep.js";

test("a bounded shard selects a stable contiguous slice of the active catalog", () => {
  const codes = Array.from({ length: 10 }, (_, index) => `metric-${index}`);
  const shard = parseSweepSelection({ MAP_SWEEP_OFFSET: "3", MAP_SWEEP_LIMIT: "4" });
  expect(shard).toEqual({ mode: "shard", offset: 3, limit: 4, budget: 40 });
  expect(selectSweepMetrics(codes, shard)).toEqual(codes.slice(3, 7));
  expect(selectSweepMetrics(codes, { ...shard, offset: 10 })).toEqual([]);
  expect(selectSweepMetrics(codes, parseSweepSelection({ MAP_SWEEP_ALL: "1" }))).toEqual(codes);
});

test("the ordinary sample remains an even deterministic spread", () => {
  const codes = Array.from({ length: 10 }, (_, index) => `metric-${index}`);
  expect(selectSweepMetrics(codes, parseSweepSelection({ MAP_SWEEP_METRICS: "4" })))
    .toEqual(["metric-0", "metric-3", "metric-6", "metric-9"]);
  expect(selectSweepMetrics(codes, parseSweepSelection({ MAP_SWEEP_METRICS: "1" })))
    .toEqual(["metric-0"]);
});

test("a scheduled shard refuses missing, invalid, or conflicting bounds", () => {
  expect(() => parseSweepSelection({ MAP_SWEEP_OFFSET: "2" })).toThrow(/MAP_SWEEP_LIMIT/);
  expect(() => parseSweepSelection({ MAP_SWEEP_LIMIT: "0" })).toThrow(/MAP_SWEEP_LIMIT/);
  expect(() => parseSweepSelection({ MAP_SWEEP_OFFSET: "-1", MAP_SWEEP_LIMIT: "2" }))
    .toThrow(/MAP_SWEEP_OFFSET/);
  expect(() => parseSweepSelection({ MAP_SWEEP_ALL: "1", MAP_SWEEP_LIMIT: "2" }))
    .toThrow(/MAP_SWEEP_ALL/);
});

test("the report preserves every metric and grain verdict with catalog coverage", () => {
  const selection = { mode: "shard", offset: 0, limit: 2, budget: 40 };
  const sources = [{ source: "ACS", catalog_total: 3, selected_metric_codes: ["A", "B"] }];
  const results = [
    { source: "ACS", metric: "A", grain: "COUNTY", verdict: "coloured", rows: 5, coloured: 5, problem: null },
    { source: "ACS", metric: "B", grain: "STATE", verdict: "fail", rows: 1, coloured: 0, problem: "no join" },
  ];
  expect(makeSweepReport({ selection, sources, results, complete: true })).toEqual({
    schema_version: 1,
    selection,
    complete: true,
    sources,
    results,
    summary: { coloured: 1, narrowed: 0, empty: 0, fail: 1 },
  });
});
