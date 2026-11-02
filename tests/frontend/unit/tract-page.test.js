import { describe, expect, it } from "vitest";

import { tractRouteTitle } from "../../../apps/web/lib/routeTitles";
import {
  TRACT_CHAPTERS,
  TRACT_SEGMENT,
  acsPeriod,
  boundaryHeading,
  pointBasis,
  resolveTractSegment,
  tractCodeFromSegment,
  tractMetricCodes,
  tractName,
  tractSegment,
} from "../../../apps/web/lib/tractPage";

const TRACTS = [
  { geo_id: "state:48|county:453|tract:001831", geo_level: "TRACT", state_fips: "48", county_fips: "453" },
  { geo_id: "state:48|county:453|tract:001100", geo_level: "TRACT", state_fips: "48", county_fips: "453" },
];

// Covers: WEB-155 — a tract has a page under its county's address, named by
// its number, and every earlier number derived across the 2020 redraw says so.
describe("a tract's address", () => {
  it("is its number, under its county", () => {
    expect(tractSegment("state:48|county:453|tract:001831")).toBe("tract-18-31");
    expect(tractSegment("state:48|county:453|tract:001100")).toBe("tract-11");
    expect(tractName("state:48|county:453|tract:001831")).toBe("Census Tract 18.31");
    expect(TRACT_SEGMENT.test("tract-18-31")).toBe(true);
    expect(TRACT_SEGMENT.test("tract-001831")).toBe(true);
    expect(TRACT_SEGMENT.test("tract-18.31")).toBe(false);
    expect(TRACT_SEGMENT.test("travis-county")).toBe(false);
    expect(tractCodeFromSegment("tract-18-31")).toBe("001831");
    expect(tractCodeFromSegment("tract-11")).toBe("001100");
    expect(tractCodeFromSegment("tract-001100")).toBe("001100");
  });

  it("resolves against the county's own tracts and names the canonical form", () => {
    expect(resolveTractSegment("tract-18-31", TRACTS)).toEqual({ tract: TRACTS[0], canonical: null });
    expect(resolveTractSegment("tract-001831", TRACTS)).toEqual({ tract: TRACTS[0], canonical: "tract-18-31" });
    expect(resolveTractSegment("tract-18-05", TRACTS)).toEqual({ tract: null, canonical: null });
  });

  it("titles the route from the address alone", () => {
    expect(tractRouteTitle("texas", "travis-county", "tract-18-31")).toBe("Census Tract 18.31, Travis County, Texas");
    expect(tractRouteTitle("texas", "travis-county", "tract-001100")).toBe("Census Tract 11, Travis County, Texas");
  });
});

describe("the tract page's measures and history", () => {
  it("reads only tract-grain ACS 5-year measures, few enough for one batch and one history read", () => {
    const codes = tractMetricCodes();
    expect(codes.every((code) => code.startsWith("CENSUS_ACS:acs5:"))).toBe(true);
    expect(codes.length).toBeLessThanOrEqual(24);
    expect(TRACT_CHAPTERS.map((chapter) => chapter.id)).toEqual(["people", "income", "housing", "work", "internet"]);
    // Medians and per-person incomes are never carried across the redraw.
    const medians = TRACT_CHAPTERS.flatMap((chapter) => chapter.measures).filter((measure) => /median|per person/i.test(measure.label));
    expect(medians.every((measure) => measure.count === false)).toBe(true);
  });

  it("says how each point was counted, and names a derived one as derived", () => {
    const base = { vintage_year: 2015, observation_date: "2015-01-01", value: 3800, margin_of_error: null, weight_basis: "share of land area" };
    expect(pointBasis({ ...base, status: "apportioned", derived: true, source_geo_ids: ["state:48|county:453|tract:001805"] })).toBe(
      "Derived from Census Tract 18.05, by share of land area",
    );
    expect(pointBasis({ ...base, status: "published", derived: false, source_geo_ids: [] })).toBe("Published for this tract");
    expect(pointBasis({ ...base, status: "unchanged", derived: false, source_geo_ids: [] })).toMatch(/same boundary/);
    expect(pointBasis({ ...base, value: null, status: "incomplete", derived: true, source_geo_ids: [] })).toMatch(/^Not available/);
    expect(acsPeriod(2020)).toBe("2016–2020");
    expect(boundaryHeading("split")).toBe("This tract was split off in 2020");
    expect(boundaryHeading("unchanged")).toBe("Same boundary since 2010");
  });
});
