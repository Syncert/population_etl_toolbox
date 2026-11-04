// Covers: WEB-163 — depth rows say what they share once, keep their own
// margin as a share, mark low-reliability estimates, and industry tables draw
// sorted bars with sectors that have no value gathered by reason.

import { describe, expect, it } from "vitest";

import {
  depthShared,
  gapReason,
  industryBars,
  marginShare,
  needsCare,
  sharedLine,
} from "../../../apps/web/lib/placeDepth";

const acs = (value, margin, pct) => ({
  value,
  margin_of_error: margin,
  margin_of_error_pct: pct ?? null,
  period_start: "2020-01-01",
  period_end: "2024-12-31",
});

describe("depth rows", () => {
  it("states the source and period most rows share, once", () => {
    const shared = depthShared([
      { metricCode: "CENSUS_ACS:acs5:B19301_001", row: acs(51478, 888) },
      { metricCode: "CENSUS_ACS:acs5:B17001_002", row: acs(68337, 3359) },
      { metricCode: "CENSUS_SAIPE_SAHIE:saipe:SAEPOVALL", row: { value: 60000, margin_of_error: 2000, period_start: "2024-01-01", period_end: "2024-12-31" } },
      { metricCode: "CENSUS_ACS:acs5:B01001_001", row: null },
    ]);
    expect(shared).toEqual({ basis: "Survey estimate (American Community Survey)", period: "2020–2024", margins: true });
    expect(sharedLine(shared)).toBe(
      "Survey estimate (American Community Survey) · 2020–2024 · ± is the 90% margin of error, as a share of the value",
    );
  });

  it("writes the margin as a share, the served one first, never a null as zero", () => {
    expect(marginShare(acs(51478, 888, 1.73))).toBe("±1.7%");
    expect(marginShare(acs(3232, 355))).toBe("±11%");
    expect(marginShare(acs(3232, null))).toBe("");
    expect(marginShare(acs(0, 12))).toBe("±12");
  });

  it("marks an estimate whose coefficient of variation is above 30 percent", () => {
    // CV = 355 / 1.645 / 3232 = 6.7 percent.
    expect(needsCare(acs(3232, 355))).toBe(false);
    // CV = 900 / 1.645 / 1200 = 45.6 percent.
    expect(needsCare(acs(1200, 900))).toBe(true);
    expect(needsCare(acs(1200, null))).toBe(false);
  });
});

describe("industry bars", () => {
  it("sorts bars largest first and groups sectors without a value by reason", () => {
    const { bars, max, gaps } = industryBars([
      { id: "farm", label: "Farm", value: 259254, message: "" },
      { id: "mining", label: "Mining", value: null, message: "Withheld by the publisher to protect confidential employer data" },
      { id: "manufacturing", label: "Manufacturing", value: 3736613, message: "" },
      { id: "utilities", label: "Utilities", value: null, message: "Withheld by the publisher to protect confidential employer data" },
      { id: "construction", label: "Construction", value: 2573697, message: "" },
    ]);
    expect(bars.map((bar) => bar.id)).toEqual(["manufacturing", "construction", "farm"]);
    expect(max).toBe(3736613);
    expect(gaps).toEqual([
      { reason: "Withheld by the publisher to protect confidential employer data", entries: [expect.objectContaining({ id: "mining" }), expect.objectContaining({ id: "utilities" })] },
    ]);
  });

  it("names a withheld figure in words and keeps other statuses plain", () => {
    expect(gapReason({ value: null, value_status: "withheld" }, "x")).toBe("Withheld by the publisher to protect confidential employer data");
    expect(gapReason({ value: null, value_status: "not_available" }, "x")).toBe("Published without a value");
    expect(gapReason(null, "Not published for this place")).toBe("Not published for this place");
  });
});
