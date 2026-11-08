import { describe, expect, it } from "vitest";

import {
  CHILDCARE_CODES,
  catalogedCodes,
  childcareMetricCode,
  childcareRatioId,
  childcareTable,
  footnoteReasons,
  formatWeekly,
  imputationReasons,
  workFormula,
  workGap,
  workText,
} from "../../../apps/web/lib/childcare";

// Covers: WEB-171 — county-childcare-prices: a county's eight childcare
// prices are read as published for one study year, an imputed price is
// marked with the reasons its code gives, and a blank price is missing,
// never zero and never an older year's price.

const row = (metric, year, value, imputation_code = "00000") => ({
  metric_code: childcareMetricCode(...metric),
  geo_id: "state:55|county:025",
  period_start: `${year}-01-01`,
  value,
  dimensions: { imputed: imputation_code.includes("1") ? "true" : "false", imputation_code },
});

describe("a county's childcare table", () => {
  const newest = {
    [childcareMetricCode("center", "infant")]: row(["center", "infant"], 2022, "293"),
    [childcareMetricCode("center", "toddler")]: row(["center", "toddler"], 2022, "258.75", "00001"),
    [childcareMetricCode("home", "infant")]: row(["home", "infant"], 2022, "220"),
    [childcareMetricCode("home", "school_age")]: row(["home", "school_age"], 2022, null),
    // Newest row older than the table's year: not published for 2022.
    [childcareMetricCode("home", "toddler")]: row(["home", "toddler"], 2021, "199"),
  };
  const table = childcareTable(newest);
  const cell = (care, age) => table.cells.find((item) => item.care === care && item.age === age);

  it("names one study year, the newest any price was published for", () => {
    expect(table.year).toBe(2022);
    expect(table.cells).toHaveLength(8);
    expect(table.max).toBe(293);
  });

  it("keeps each price as published and marks an imputed one", () => {
    expect(cell("center", "infant")).toMatchObject({ value: 293, imputed: false, code: "00000" });
    expect(cell("center", "toddler")).toMatchObject({ value: 258.75, imputed: true, code: "00001" });
    expect(formatWeekly(293)).toBe("$293 a week");
    expect(formatWeekly(258.75)).toBe("$258.75 a week");
  });

  it("reads a blank price, an older year and an unread price as missing, never zero", () => {
    expect(cell("home", "school_age").value).toBeNull();
    expect(cell("home", "toddler")).toMatchObject({ value: null, year: null });
    expect(cell("center", "preschool").value).toBeNull();
    expect(childcareTable({}).year).toBeNull();
  });

  it("gives each imputation position's reason once, in the code's order", () => {
    expect(imputationReasons("10001")).toEqual([
      "taken from another age group",
      "filled in for a year between the state's surveys",
    ]);
    expect(imputationReasons("0001")).toEqual([]);
    expect(footnoteReasons(table.cells)).toEqual(["filled in for a year between the state's surveys"]);
  });
});

// Covers: WEB-171 — county-childcare-prices: hours of work divide each price
// by the county's own hourly wage, read from the reviewed pairing, and the
// arithmetic is stated with the wage, its year and source; a price not
// computed shows no hours, never zero.
describe("hours of work for a childcare price", () => {
  const row = { value: "586.00", wage_geo_name: "Dane County", wage_year: "same_year", denominator: { value: "1200", period_start: "2022-01-01" } };

  it("names the reviewed pairing for each price", () => {
    expect(childcareRatioId("home", "school_age")).toBe("minutes_of_work_childcare_home_school_age");
  });

  it("is hours and minutes of work, or nothing", () => {
    expect(workText(row)).toBe("9 hours 46 minutes of work");
    expect(workText({ ...row, value: "45" })).toBe("45 minutes of work");
    expect(workText({ ...row, value: null, unavailable_reason: "denominator_missing" })).toBe("");
    expect(workText(null)).toBe("");
  });

  it("states the wage, its year, the hourly wage and the division", () => {
    expect(workFormula(row, 293, "center-based infant care", "Dane County")).toBe(
      "How the hours are worked out: Dane County's average wage in 2022 was $1,200 a week (Bureau of Labor Statistics, QCEW, all jobs located in the county), or $30.00 an hour over a 40-hour week. $293 a week for center-based infant care ÷ $30.00 an hour = 9.8 hours of work. Each price is divided the same way.",
    );
    expect(workFormula({ ...row, value: null }, 293, "x", "Dane County")).toBe("");
  });

  it("never divides by another year's wage, and says the wage is not published yet", () => {
    const later = { ...row, wage_year: "newest_published_year", denominator: { value: "1300", period_start: "2025-01-01" } };
    expect(workText(later)).toBe("");
    expect(workFormula(later, 293, "x", "Dane County")).toBe("");
    expect(workGap(later, 2022, "Dane County")).toBe("Hours of work: Dane County's wage for 2022 is not published yet.");
    expect(workGap({ value: null, unavailable_reason: "denominator_missing" }, 2022, "Dane County")).toBe(
      "Hours of work: no wage is loaded for Dane County yet.",
    );
    expect(workGap(row, 2022, "Dane County")).toBe("");
    expect(workFormula({ ...row, value: null }, 293, "x", "Dane County")).toBe("");
  });
});

// Covers: WEB-171 — county-childcare-prices: a price the catalog does not
// hold is never asked for, so a deployment without the load reads nothing.
describe("which childcare prices are read", () => {
  it("reads only the catalogued prices, none while the catalog is pending", () => {
    expect(CHILDCARE_CODES).toHaveLength(8);
    expect(CHILDCARE_CODES[0]).toBe("DOL_CHILDCARE:center_infant");
    expect(catalogedCodes(null)).toBeNull();
    expect(catalogedCodes(new Map())).toEqual([]);
    expect(catalogedCodes(new Map([["DOL_CHILDCARE:home_infant", {}], ["BLS:X", {}]]))).toEqual(["DOL_CHILDCARE:home_infant"]);
    expect(catalogedCodes(undefined)).toEqual(CHILDCARE_CODES);
  });
});
