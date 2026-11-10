import { describe, expect, it } from "vitest";

import { FAMILY_CODES } from "../../../apps/web/lib/familyCodes";
import {
  FAMILY_GROUPS,
  FAMILY_ROW_CODES,
  FAMILY_ROWS,
  headlineLevel,
  hoursFormula,
  hoursGap,
  hoursText,
} from "../../../apps/web/lib/familyCollection";

// Covers: WEB-173 — starting-a-family-collection: eight published measures in
// four groups, each standing alone; a county-only figure on a city page is
// the county's, named; hours of work state their arithmetic and are never
// zero; no row judges a value good or bad.

const chain = [
  { level: "PLACE", geoId: "state:55|place:48000", name: "Madison city" },
  { level: "COUNTY", geoId: "state:55|county:025", name: "Dane County" },
  { level: "STATE", geoId: "state:55", name: "Wisconsin" },
  { level: "NATIONAL", geoId: "us:1", name: "United States" },
];

const minutes = (value, wage, year, wageYear = "same_year", price = "293", priceYear = year) => ({
  value,
  wage_year: wageYear,
  wage_geo_name: "Dane County",
  numerator: { value: price, period_start: `${priceYear}-01-01` },
  denominator: { value: wage, period_start: `${year}-01-01` },
});

describe("the Starting a family rows", () => {
  it("are eight, in the four groups, each a metric or a reviewed pairing", () => {
    expect(FAMILY_ROWS).toHaveLength(8);
    expect(FAMILY_GROUPS.map((group) => group.key)).toEqual(["care", "home", "money", "surroundings"]);
    for (const row of FAMILY_ROWS) {
      expect(Boolean(row.metricCode) !== Boolean(row.ratioId)).toBe(true);
      expect(FAMILY_GROUPS.some((group) => group.key === row.group)).toBe(true);
    }
    // The page's catalog read names exactly the rows' metrics.
    expect([...FAMILY_CODES].sort()).toEqual([...FAMILY_ROW_CODES].sort());
    expect(FAMILY_CODES).toContain("DOL_CHILDCARE:center_infant");
    expect(FAMILY_CODES).toContain("HUD_FMR_IL:fmr_3br");
    expect(FAMILY_ROWS.find((row) => row.id === "rent-3br").hoursRatioId).toBe("minutes_of_work_rent_3br");
  });

  it("never call a value good, bad, easier or harder", () => {
    const words = FAMILY_ROWS.flatMap((row) => [row.label, row.note || "", row.gap || ""]).join(" ").toLowerCase();
    for (const judgement of ["easier", "harder", "better", "worse", "favourable", "favorable", "blocker", "score"]) {
      expect(words).not.toContain(judgement);
    }
  });

  it("show a county-only figure on a city page as the county's", () => {
    const care = FAMILY_ROWS.find((row) => row.id === "infant-care");
    expect(headlineLevel(care, chain).name).toBe("Dane County");
    const homes = FAMILY_ROWS.find((row) => row.id === "home-value");
    expect(headlineLevel(homes, chain).name).toBe("Madison city");
    // A state page has no county: the row says where it is published instead.
    expect(headlineLevel(care, chain.slice(2))).toBeNull();
  });
});

describe("hours of work", () => {
  const care = FAMILY_ROWS.find((row) => row.id === "infant-care");
  const rent = FAMILY_ROWS.find((row) => row.id === "rent-3br");

  it("are hours and minutes with their arithmetic, never zero", () => {
    // $293 at $1,347 a week over 40 hours: 522 minutes.
    const row = minutes("522.00", "1347", 2022);
    expect(hoursText(row, true)).toBe("8 hours 42 minutes of work");
    expect(hoursFormula(row, care, "Dane County")).toBe(
      "$293 a week ÷ (Dane County's average wage in 2022, $1,347 a week ÷ 40 hours = $33.68 an hour) = 8.7 hours for a week of infant care at a center.",
    );
    expect(hoursText({ value: null, unavailable_reason: "suppressed_input" })).toBe("");
    expect(hoursText(null)).toBe("");
  });

  it("childcare takes only its own year's wage; the rent says when the wage is the newest", () => {
    const newer = minutes("3840.00", "1250", 2025, "newest_published_year", "2000", 2026);
    expect(hoursText(newer, care.hoursSameYearOnly)).toBe("");
    expect(hoursGap(newer, care, "Dane County")).toBe("Hours of work: Dane County's wage for 2026 is not published.");
    expect(hoursText(newer, rent.hoursSameYearOnly)).toBe("64 hours of work");
    expect(hoursFormula(newer, rent, "Dane County")).toContain("Dane County's average wage in 2025, the newest year QCEW has published");
    expect(hoursGap({ unavailable_reason: "denominator_missing" }, rent, "Dane County")).toBe(
      "Hours of work: no wage is loaded for Dane County yet.",
    );
  });
});
