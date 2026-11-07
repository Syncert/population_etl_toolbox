// Covers: WEB-157 — the age map's model (web-age-and-sex-structure, API-191):
// every indicator the API serves is a page and nothing else is; the answer
// becomes the one-measure map's rows with a withheld value kept null with its
// reason; the switcher finds the indicators by their words; a place page
// links its own grain and state.
import { describe, expect, test } from "vitest";

import {
  AGE_MAP_INDICATORS,
  ageMapIndicator,
  ageMapKey,
  ageMapMarginText,
  ageMapPath,
  ageMapPathForPlace,
  ageMapPeriodText,
  ageMapRows,
  ageMapValueText,
  searchAgeMapIndicators,
} from "../../../apps/web/lib/ageMap";
import { buildMeasureMapView } from "../../../apps/web/lib/oneMeasureMap";

const SERVED = [
  "under_18", "age_18_64", "age_65_plus", "age_85_plus", "child_dependency_ratio", "old_age_dependency_ratio",
  "total_dependency_ratio", "sex_ratio", "median_age", "median_age_men", "median_age_women",
];

function answer(items, unit = "percent") {
  return {
    indicator: "age_65_plus", label: "65 and over (share of everyone)", unit, dataset: "acs5", geo_level: "COUNTY",
    release: "2023", period_start: "2019-01-01", period_end: "2023-12-31", scheme: { scheme_id: "s", version: 1, status: "approved" }, total: items.length, items,
  };
}

describe("the indicators", () => {
  test("are exactly the API's", () => {
    expect(AGE_MAP_INDICATORS.map((item) => item.id).sort()).toEqual([...SERVED].sort());
    expect(ageMapIndicator("median_age")?.unit).toBe("years");
    expect(ageMapIndicator("B01001_011")).toBeNull();
  });

  test("the switcher finds them by their words, and only from three letters", () => {
    expect(searchAgeMapIndicators("median age").map((item) => item.id)).toEqual(["median_age", "median_age_men", "median_age_women"]);
    expect(searchAgeMapIndicators("elderly").map((item) => item.id)).toContain("age_65_plus");
    expect(searchAgeMapIndicators("ag")).toEqual([]);
    expect(searchAgeMapIndicators("household income")).toEqual([]);
  });
});

describe("addresses", () => {
  test("carry the grain and, for a city or tract, the state", () => {
    expect(ageMapPath("median_age")).toBe("/map/age/median_age");
    expect(ageMapPath("median_age", "METRO", "55")).toBe("/map/age/median_age?grain=METRO");
    expect(ageMapPath("sex_ratio", "PLACE", "55")).toBe("/map/age/sex_ratio?grain=PLACE&state=55");
    expect(ageMapPathForPlace("median_age", "state:55|place:48000", "PLACE")).toBe("/map/age/median_age?grain=PLACE&state=55");
    expect(ageMapPathForPlace("median_age", "state:55|county:025", "COUNTY")).toBe("/map/age/median_age");
  });
});

describe("the answer as map rows", () => {
  test("a withheld value stays null with its reason, and never counts as a value", () => {
    const rows = ageMapRows(answer([
      { geo_id: "state:55|county:025", value: "14.90", moe: "0.40", unavailable_reason: null },
      { geo_id: "state:55|county:078", value: null, moe: null, unavailable_reason: "suppressed_input" },
    ]));
    expect(rows[0]).toMatchObject({ metric_code: ageMapKey("age_65_plus"), value: "14.90", margin_of_error: "0.40", value_status: null });
    expect(rows[1].value).toBeNull();
    expect(rows[1].value_status).toBe("the Census Bureau withheld one of the published lines this group is made of");
    const view = buildMeasureMapView(ageMapKey("age_65_plus"), rows, [
      { geo_id: "state:55|county:025" }, { geo_id: "state:55|county:078" }, { geo_id: "state:55|county:003" },
    ]);
    expect(view.withValue).toBe(1);
    expect(view.withoutValue).toBe(2);
  });

  test("values, margins and periods read as a reader says them", () => {
    expect(ageMapValueText(17.3, "percent")).toBe("17.3%");
    expect(ageMapValueText(38.7, "years")).toBe("38.7 years");
    expect(ageMapValueText(96.2, "ratio")).toBe("96.2");
    expect(ageMapValueText(null, "years")).toBe("not available");
    expect(ageMapMarginText("0.02", "percent")).toBe("± 0.02");
    expect(ageMapMarginText("0.4", "years")).toBe("± 0.4");
    expect(ageMapPeriodText(answer([]))).toBe("2019–2023");
  });
});
