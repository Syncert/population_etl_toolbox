import { describe, expect, it } from "vitest";

import { barPosition, compareMeasure, grainMismatch, pairPath } from "../../../apps/web/lib/placeComparison";

// Covers: WEB-129 — two places compared only as far as preflight allows,
// in one shared period, with the verdict carried through unchanged.

const a = { geoId: "state:55|county:025", name: "Dane County, Wisconsin", level: "COUNTY" };
const b = { geoId: "state:55|county:105", name: "Rock County, Wisconsin", level: "COUNTY" };
const row = (value, year = 2024) => ({ value, period_start: `${year}-01-01`, period_end: `${year}-12-31` });
const pass = { comparable: true, rules: [{ rule: "units", status: "pass", reason: "both publish dollars" }], caveats: [] };
const inputs = (overrides = {}) => ({ measureId: "m", label: "Median income", metricCode: "M", preflight: pass, a: row("80000"), b: row("65000"), parents: [{ name: "Wisconsin", row: row("72000") }, { name: "United States", row: row("78000", 2023) }], ...overrides });

describe("comparing one measure for two places", () => {
  it("carries the preflight verdict through unchanged", () => {
    const { row: compared } = compareMeasure(inputs(), a, b);
    expect(compared.preflight).toBe(pass);
    expect(compared.period).toBe("2024-01-01 – 2024-12-31");
    expect(compared.ticks).toEqual([{ name: "Wisconsin", value: 72000 }]);
  });

  it("turns a preflight refusal into a Not comparable row with its reasons", () => {
    const refusal = { comparable: false, rules: [
      { rule: "source_analysis_ready", status: "fail", reason: "FBI UCR subjects are not canonical geographies" },
      { rule: "source_analysis_ready", status: "fail", reason: "FBI UCR subjects are not canonical geographies" },
      { rule: "units", status: "pass", reason: "same" },
    ] };
    const { row: compared, refused } = compareMeasure(inputs({ preflight: refusal }), a, b);
    expect(compared).toBeNull();
    expect(refused.preflight).toBe(refusal);
    expect(refused.reasons).toEqual(["FBI UCR subjects are not canonical geographies"]);
  });

  it("refuses a measure one place did not publish, or published for another period", () => {
    expect(compareMeasure(inputs({ b: null }), a, b).refused.reasons).toEqual(["No published value for Rock County, Wisconsin."]);
    expect(compareMeasure(inputs({ b: row(null) }), a, b).refused.reasons[0]).toMatch(/No published value/);
    expect(compareMeasure(inputs({ b: row("65000", 2023) }), a, b).refused.reasons[0]).toMatch(/newest published period is 2024-01-01 – 2024-12-31 and Rock County, Wisconsin's is 2023/);
    expect(compareMeasure(inputs({ preflight: null, preflightError: "503" }), a, b).refused.reasons[0]).toMatch(/could not be read: 503/);
  });

  it("places bars and ticks on one shared scale from zero", () => {
    const { row: compared } = compareMeasure(inputs(), a, b);
    expect(barPosition(80000, compared)).toBe(100);
    expect(barPosition(0, compared)).toBe(0);
    expect(barPosition(40000, compared)).toBe(50);
  });

  it("addresses pairs and refuses mixed grains", () => {
    expect(pairPath("/us/wisconsin/dane-county", "/us/wisconsin/rock-county")).toBe("/us/wisconsin/dane-county/vs/us/wisconsin/rock-county");
    expect(grainMismatch(a, b)).toBeNull();
    expect(grainMismatch(a, { geoId: "state:27", name: "Minnesota", level: "STATE" })).toMatch(/Places are compared at the same grain/);
  });
});
