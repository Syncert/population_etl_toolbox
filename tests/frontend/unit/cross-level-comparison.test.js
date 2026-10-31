import { describe, expect, it } from "vitest";

import {
  containmentSentence,
  differenceText,
  isCrossLevel,
  noteSentences,
  refusalSentence,
  sideBar,
  sideValueText,
} from "../../../apps/web/lib/crossLevelComparison";
import { COMPARE_GEO_ID, compareHref } from "../../../apps/web/lib/placeComparison";

// Covers: WEB-146 — the API's cross-level answer worded for a reader: no
// codes or "side a", a refused total says why and what is shown instead, a
// nested or overlapping pair says no difference is worked out, a missing
// value is never zero, and a pair with a metro is addressed by identity.

const city = { geoId: "state:48|place:05000", name: "Austin city, Texas", level: "PLACE" };
const county = { geoId: "state:48|county:453", name: "Travis County, Texas", level: "COUNTY" };
const state = { geoId: "state:48", name: "Texas", level: "STATE" };
const metro = { geoId: "cbsa:12420", name: "Austin-Round Rock-San Marcos, TX", level: "METRO" };
const nation = { geoId: "us:1", name: "United States", level: "NATIONAL" };

const side = (overrides = {}) => ({ geo_id: "x", metric_code: "M", published: true, value: "100", period_start: "2019-01-01", period_end: "2023-12-31", margin_of_error: "4", ...overrides });
const answer = (overrides = {}) => ({
  measure_kind: "median", measure_kind_status: "draft", cross_level: true, comparable: true, refusal: null,
  side_a: side(), side_b: side({ value: "80" }), containment: { relationship: "none_recorded" },
  difference: null, intervals_overlap: false, derived: false, notes: [], caveats: [], ...overrides,
});

describe("the cross-level answer in a reader's words", () => {
  it("refuses a total across levels by size, and says what is shown instead", () => {
    const refused = answer({ comparable: false, refusal: { reason: "counts_differ_in_size", detail: "CENSUS_ACS:acs5:B17001_002 is a count", alternatives: [{ ratio_id: "poverty_share:acs5", label: "Share", unit: "percent", href: "" }] } });
    const text = refusalSentence(refused, county, state);
    expect(text).toContain("Travis County, Texas is a county and Texas is a state, so they differ in size by their nature");
    expect(text).toContain("shown as a share or per resident instead");
    expect(text).not.toMatch(/B17001|side a|count,/);
    const none = refusalSentence({ ...refused, refusal: { ...refused.refusal, alternatives: [] } }, county, state);
    expect(none).toContain("left out");
  });

  it("names every other refusal without codes", () => {
    for (const reason of ["programs_differ", "universes_differ", "units_differ", "different_measures"]) {
      const text = refusalSentence(answer({ comparable: false, refusal: { reason, detail: "CODE_A and CODE_B", alternatives: [] } }), city, county);
      expect(text).toMatch(/not set side by side/);
      expect(text).not.toMatch(/CODE_/);
    }
  });

  it("says a nested or overlapping pair gets no difference", () => {
    expect(containmentSentence(answer({ containment: { relationship: "part_of", smaller_side: "a", share_of_smaller_inside_larger: 1 } }), metro, nation)).toBe(
      "Austin-Round Rock-San Marcos, TX lies inside United States, so United States's figure already includes Austin-Round Rock-San Marcos, TX. They are shown side by side; no difference is worked out between them.",
    );
    expect(containmentSentence(answer({ containment: { relationship: "contains", smaller_side: "b", share_of_smaller_inside_larger: 1 } }), state, county)).toMatch(/^Travis County, Texas lies inside Texas/);
    expect(containmentSentence(answer({ containment: { relationship: "intersects", smaller_side: "a", share_of_smaller_inside_larger: 0.962 } }), city, county)).toMatch(
      /^96% of Austin city, Texas's area lies inside Travis County, Texas, so the two figures partly describe the same people/,
    );
    expect(containmentSentence(answer(), county, state)).toBe("");
  });

  it("words the period, overlap and size notes, and drops the ones said elsewhere", () => {
    const notes = noteSentences(answer({
      side_b: side({ period_start: "2018-01-01", period_end: "2022-12-31" }),
      notes: [
        { reason: "periods_differ", detail: "side a ..." },
        { reason: "intervals_overlap", detail: "x" },
        { reason: "not_published", detail: "side b has no published value" },
        { reason: "contained", detail: "side a lies inside" },
      ],
    }), county, state);
    expect(notes).toEqual([
      "No single period covers both: Travis County, Texas's newest figure is for 2019-01-01 – 2023-12-31 and Texas's is for 2018-01-01 – 2022-12-31. No difference is worked out across different periods.",
      "The two margins of error overlap, so these places may not truly differ on this measure.",
    ]);
  });

  it("shows a missing value as not published, never zero", () => {
    expect(sideValueText(side({ published: false, value: null }), "dollars")).toBe("Not published for this place");
    expect(sideValueText(side({ value: null, value_status: "withheld" }), "dollars")).toBe("Withheld by the publisher");
    expect(sideValueText(side({ value: "71234.5" }), "dollars")).toBe("71,234.5 dollars");
    const bars = answer({ side_b: side({ published: false, value: null }) });
    expect(sideBar(100, bars)).toBe(100);
  });

  it("prints the API's difference only when it computed one", () => {
    expect(differenceText(answer(), county, state, "dollars")).toBe("");
    expect(differenceText(answer({ difference: "-8.5" }), county, state, "percent")).toBe("Travis County, Texas minus Texas: -8.5 percent");
  });
});

describe("addressing a comparison", () => {
  it("uses the place-page form when both sides have pages, identities otherwise", () => {
    expect(compareHref({ geoId: county.geoId, path: "/us/texas/travis-county" }, { geoId: "us:1", path: "/us" })).toBe("/us/texas/travis-county/vs/us");
    expect(compareHref({ geoId: "us:1", path: "/us" }, { geoId: state.geoId, path: "/us/texas" })).toBe("/us/vs/us/texas");
    expect(compareHref({ geoId: metro.geoId, path: null }, { geoId: "us:1", path: "/us" })).toBe("/compare/places?a=cbsa%3A12420&b=us%3A1");
  });

  it("accepts only the identities a comparison side can be", () => {
    for (const id of ["us:1", "state:48", "state:48|county:453", "state:48|place:05000", "cbsa:12420"]) expect(COMPARE_GEO_ID.test(id)).toBe(true);
    for (const id of ["division:3", "state:48|county:453|tract:000100", "zcta:78701", "cbsa:1242", ""]) expect(COMPARE_GEO_ID.test(id)).toBe(false);
    expect(isCrossLevel(city, county)).toBe(true);
    expect(isCrossLevel(county, { ...county, geoId: "state:48|county:491" })).toBe(false);
  });
});
