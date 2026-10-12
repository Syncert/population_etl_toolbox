import { describe, expect, it } from "vitest";

import { rankSentence, standouts } from "../../../apps/web/lib/distinctive";

// Covers: WEB-131 — the highest and lowest are separate lists of single
// measures, worded with their gaps; nothing is combined.

const measure = (code, rank, extra = {}) => ({ metric_code: code, value: 1, siblings_with_value: 10, siblings_withheld: 0, siblings_missing: 0, siblings_below: Math.round(rank * 10), siblings_tied: 0, percentile_rank: rank, caveats: [], request: "", ...extra });

describe("what stands out", () => {
  it("splits ranked measures into highest and lowest without overlap", () => {
    const response = { ranked: [measure("a", 0.9), measure("b", 0.1), measure("c", 0.5), measure("d", 0.8), measure("e", 0.2), measure("f", 0.3), measure("g", 0.7)] };
    const { highest, lowest, rankedCount } = standouts(response);
    expect(highest.map((item) => item.metric_code)).toEqual(["a", "d", "g"]);
    expect(lowest.map((item) => item.metric_code)).toEqual(["b", "e", "f"]);
    expect(rankedCount).toBe(7);
    const few = standouts({ ranked: [measure("a", 0.9), measure("b", 0.1)] });
    expect(few.highest.map((item) => item.metric_code)).toEqual(["a", "b"]);
    expect(few.lowest).toEqual([]);
  });

  it("names withheld and missing siblings rather than counting them as zero", () => {
    expect(rankSentence(measure("a", 0.4, { siblings_below: 4, siblings_withheld: 1, siblings_missing: 2, siblings_tied: 1 }), "Wisconsin counties"))
      .toBe("Higher than 4 of 10 Wisconsin counties with a published value, equal to 1 (1 withheld a value; 2 published none)");
    expect(rankSentence(measure("a", 0.4, { siblings_below: 4 }), "states")).toBe("Higher than 4 of 10 states with a published value");
  });
});
