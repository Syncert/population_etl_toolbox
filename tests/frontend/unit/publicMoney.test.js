import { describe, expect, it } from "vitest";

import {
  changeScale,
  gapText,
  incomeRows,
  lineGeometry,
  linePath,
  lineSeries,
  nationGapSentence,
  paymentRows,
  percentText,
  pointsText,
  propertyRow,
  spreadLabels,
  stripMax,
  tenureYears,
  windowText,
} from "../../../apps/web/lib/publicMoney";
import { governorTermsAnswer, publicMoneyAnswer } from "../support/publicMoneyScenarios.js";

// Covers: WEB-168 — taxes and public money arrange what the API served: every
// level on one scale per group, a missing value said and never drawn as zero,
// lines broken at missing years, and no measure combined with another.

const DANE = "state:55|county:025";

describe("taxes and public money view model", () => {
  it("puts the total first, then each program, every level on the same row", () => {
    const rows = paymentRows(publicMoneyAnswer(DANE));
    expect(rows.map((row) => row.id)).toEqual(["total", "social_security", "medicare", "medicaid", "income_support", "unemployment", "other"]);
    expect(rows[0].emphasis).toBe(true);
    expect(rows[0].values.map((entry) => [entry.role, entry.value])).toEqual([["this place", 13.1], ["state", 18.4], ["nation", 17.9]]);
    // The nation's unpublished program is null, not zero.
    expect(rows.find((row) => row.id === "unemployment").values[2].value).toBeNull();
    expect(stripMax(rows)).toBe(20);
    expect(nationGapSentence(rows[0], "Dane County", "payments are")).toBe("Dane County: payments are 4.8 points below the nation.");
  });

  it("reads income sources and property tax for each level", () => {
    const answer = publicMoneyAnswer(DANE);
    expect(incomeRows(answer).map((row) => row.values[0].value)).toEqual([67, 20, 13]);
    expect(propertyRow(answer).values.map((entry) => entry.value)).toEqual([1.62, 1.58, 0.98]);
    expect(propertyRow({ ...answer, property_tax: { ...answer.property_tax, values: [] } })).toBeNull();
    expect(paymentRows({ ...answer, payments: [] })).toEqual([]);
  });

  it("draws lines from zero and breaks them where a year is missing", () => {
    const answer = publicMoneyAnswer(DANE);
    const burden = lineSeries(answer.tax_burden, answer.levels);
    // Census of Governments years only for the county: two points, five years apart.
    const county = burden.find((series) => series.role === "this place");
    expect(county.points.map((point) => point.year)).toEqual([2017, 2022]);
    const geometry = lineGeometry(burden, 560, 180);
    expect(geometry.values[0]).toBe(0);
    expect(linePath(county.points, geometry).match(/M/g)).toHaveLength(2);
    const stateOnly = lineSeries(answer.tax_burden, answer.levels, (id) => !id.includes("|county:"));
    expect(stateOnly.map((series) => series.role)).toEqual(["state", "nation"]);
    expect(linePath(stateOnly[0].points, geometry).match(/M/g)).toHaveLength(1);
  });

  it("words values and gaps plainly", () => {
    expect(percentText("13.1")).toBe("13.1%");
    expect(percentText(null)).toBe("Not published");
    expect(pointsText("-0.900")).toBe("−0.9");
    expect(pointsText("0.1")).toBe("+0.1");
    expect(pointsText("0.04")).toBe("0.0");
    expect(gapText("start_year_not_published")).toBe("Not published for the first year compared");
    expect(gapText("tenure_shorter_than_a_full_year")).toBe("No full year in office");
  });

  it("states each governor's years compared and puts every change on one scale", () => {
    const answer = governorTermsAnswer("state:55");
    const [evers, walker, doyle] = answer.items;
    expect(tenureYears(evers)).toBe("2018–2024 so far");
    expect(tenureYears(walker)).toBe("2010–2018");
    expect(windowText(doyle.fiscal[0], false)).toBe("Not published for the first year compared");
    expect(changeScale(answer.items)).toBeCloseTo(0.9);
  });
});

describe("end labels", () => {
  it("push close labels apart and leave distant ones where their lines end", () => {
    expect(spreadLabels([100, 102], 13)).toEqual([100, 113]);
    expect(spreadLabels([102, 100], 13)).toEqual([113, 100]);
    expect(spreadLabels([20, 100], 13)).toEqual([20, 100]);
  });
});
