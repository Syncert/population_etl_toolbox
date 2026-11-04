// Covers: WEB-164 — a headline card's sparkline draws the place's own values
// over the record's last ten years and says how they moved in ADR-0011 verbs.

import { describe, expect, it } from "vitest";

import { forbiddenWordsIn } from "../../../apps/web/lib/checkTheRecord";
import { changeLine, sparkline } from "../../../apps/web/lib/sparkline";

const yearly = (year, value, margin = null) => ({ period_start: `${year}-01-01`, period_end: `${year}-12-31`, value, margin_of_error: margin });
const acs5 = (end, value, margin) => ({ period_start: `${end - 4}-01-01`, period_end: `${end}-12-31`, value, margin_of_error: margin });
const monthly = (year, month, value) => {
  const mm = String(month).padStart(2, "0");
  return { period_start: `${year}-${mm}-01`, period_end: `${year}-${mm}-28`, value, margin_of_error: null };
};

describe("sparkline", () => {
  it("draws the last ten years of published values, newest at the right edge", () => {
    const rows = [2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024].map((year, index) => yearly(year, 100 + index));
    const spark = sparkline(rows, 240, 40);
    expect(spark.points).toHaveLength(6);
    expect(spark.first).toBe("2014");
    expect(spark.last).toBe("2024");
    expect(spark.points.at(-1).x).toBe(240);
    expect(spark.points[0].x).toBe(0);
    // Higher values sit higher: smaller y.
    expect(spark.points.at(-1).y).toBeLessThan(spark.points[0].y);
  });

  it("draws nothing with fewer than three published periods", () => {
    expect(sparkline([yearly(2023, 1), yearly(2024, 2), yearly(2022, null)], 240, 40)).toBeNull();
  });
});

describe("changeLine", () => {
  it("compares the same calendar month for a monthly series", () => {
    const rows = [monthly(2025, 8, 3.1), monthly(2025, 9, 2.9), monthly(2026, 8, 2.6), monthly(2026, 9, 2.5)];
    expect(changeLine(rows)).toBe("Fell from 2.9 in September 2025.");
  });

  it("compares a five-year estimate with one that shares none of its years, beyond the margins", () => {
    const rows = [acs5(2019, 70000, 900), acs5(2020, 72000, 900), acs5(2024, 89975, 1284)];
    expect(changeLine(rows)).toBe("Rose from 70,000 in 2015–2019.");
    const close = [acs5(2019, 89000, 1200), acs5(2022, 89500, 1200), acs5(2024, 89975, 1284)];
    const line = changeLine(close);
    expect(line).toBe("Not distinguishable from 89,000 in 2015–2019 at the published margins of error.");
    expect(forbiddenWordsIn(line)).toEqual([]);
  });

  it("says nothing when only one of the two margins is published", () => {
    expect(changeLine([acs5(2019, 1, null), acs5(2021, 2, 1), acs5(2024, 3, 1)])).toBe("");
  });
});
