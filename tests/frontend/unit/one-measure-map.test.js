import { describe, expect, it } from "vitest";

import { assertOneMeasure, buildMeasureMapView, equalWidthBins, rankedPage } from "../../../apps/web/lib/oneMeasureMap";

// Covers: WEB-128 — one measure, one period, every county: no second
// measure, no painted or printed zero, and legend counts that add up.

const county = (fips, name) => ({ geo_id: `state:55|county:${fips}`, geo_level: "COUNTY", state_fips: "55", county_fips: fips, county_name: name, state_name: "Wisconsin" });
const counties = [county("001", "Adams County"), county("003", "Ashland County"), county("005", "Barron County"), county("025", "Dane County"), county("105", "Rock County")];
const row = (fips, value, extra = {}) => ({ metric_code: "M", geo_id: `state:55|county:${fips}`, value, period_start: "2024-01-01", period_end: "2024-12-31", ...extra });

describe("the one-measure view model", () => {
  it("refuses a second metric code", () => {
    expect(() => assertOneMeasure("M", [row("001", "1"), { ...row("003", "2"), metric_code: "OTHER" }])).toThrow("exactly one measure");
    expect(() => assertOneMeasure("", [])).toThrow("exactly one measure");
    expect(() => buildMeasureMapView("M", [{ ...row("001", "1"), metric_code: "OTHER" }], counties)).toThrow();
  });

  it("ranks published values highest first and keeps every gap at the end", () => {
    const view = buildMeasureMapView("M", [row("001", "10"), row("003", "30"), row("025", null, { value_status: "suppressed" }), row("105", "20")], counties);
    expect(view.ranked.map((entry) => entry.county.county_name)).toEqual(["Ashland County", "Rock County", "Adams County", "Barron County", "Dane County"]);
    expect(view.ranked.slice(3).map((entry) => [entry.value, entry.gap])).toEqual([[null, "missing"], [null, "withheld: suppressed"]]);
    expect(view.withValue).toBe(3);
    expect(view.withoutValue).toBe(2);
    expect(view.period).toBe("2024-01-01 – 2024-12-31");
  });

  it("never turns a withheld or missing value into a zero", () => {
    const view = buildMeasureMapView("M", [row("001", "0"), row("003", null), row("005", "")], counties);
    expect(view.ranked[0]).toMatchObject({ value: 0, gap: "" });
    expect(view.ranked.filter((entry) => entry.value === 0)).toHaveLength(1);
    expect(view.withoutValue).toBe(4);
  });

  it("bins with counts that add up to the counties with a value", () => {
    const view = buildMeasureMapView("M", ["10", "20", "30", "40", "50"].map((value, index) => row(counties[index].county_fips, value)), counties);
    expect(view.bins.map((bin) => bin.count)).toEqual([1, 1, 1, 1, 1]);
    expect(view.bins.reduce((total, bin) => total + bin.count, 0) + view.withoutValue).toBe(counties.length);
    expect(view.distribution.items).toHaveLength(5);
    expect(view.distribution.items[4]).toMatchObject({ bin_index: 5, upper_bound: 50 });
    expect(equalWidthBins([7, 7])).toEqual([{ binIndex: 1, lowerBound: 7, upperBound: 7, count: 2 }]);
    expect(equalWidthBins([])).toEqual([]);
  });

  it("pages the table and says when rows disagree on the period", () => {
    const view = buildMeasureMapView("M", [row("001", "1"), row("003", "2", { period_start: "2023-01-01", period_end: "2023-12-31" })], counties);
    expect(view.period).toBe("several periods");
    expect(rankedPage(view, 1, 2).map((entry) => entry.county.county_fips)).toEqual(["005", "025"]);
  });
});
