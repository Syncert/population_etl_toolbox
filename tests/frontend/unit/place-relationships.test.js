import { describe, expect, it } from "vitest";

import { groupNearby, isEmpty, relatedPath, shareText } from "../../../apps/web/lib/placeRelationships";

// Covers: WEB-130 — the Nearby section's groups, links and overlap wording,
// all from the relationship rows' own identities.

const states = [
  { geo_id: "state:55", state_fips: "55", state_name: "Wisconsin" },
  { geo_id: "state:27", state_fips: "27", state_name: "Minnesota" },
];
const wisconsin = [
  { geo_id: "state:55|county:025", state_fips: "55", county_fips: "025", county_name: "Dane County" },
  { geo_id: "state:55|county:105", state_fips: "55", county_fips: "105", county_name: "Rock County" },
];
const row = (relationship, geo_id, geo_level, geo_name, state_fips, extra = {}) => ({ relationship, geo_id, geo_level, geo_name, state_fips, geography_vintage: 2025, evidence_source: "x", ...extra });

describe("grouping related geographies", () => {
  const groups = groupNearby({ geo_id: "state:55|county:025", total: 6, items: [
    row("adjacent", "state:55|county:105", "COUNTY", "Rock County", "55"),
    row("adjacent", "state:27|county:053", "COUNTY", "Hennepin County", "27"),
    row("intersects", "state:55|place:1", "PLACE", "Small town", "55", { overlap_weight: 1 }),
    row("intersects", "state:55|place:2", "PLACE", "Crossing city", "55", { overlap_weight: 0.6 }),
    row("part_of", "us:1", "NATIONAL", "us:1", null),
    row("part_of", "state:55", "STATE", "Wisconsin", "55"),
  ] }, states, wisconsin);

  it("puts places within, counties around, and containers above", () => {
    expect(groups.within.map((entry) => entry.name)).toEqual(["Small town, Wisconsin", "Crossing city, Wisconsin"]);
    expect(groups.neighbours.map((entry) => [entry.name, entry.href])).toEqual([
      ["Hennepin County, Minnesota", "/us/minnesota/27053"],
      ["Rock County, Wisconsin", "/us/wisconsin/rock-county"],
    ]);
    expect(groups.partOf.map((entry) => [entry.name, entry.href])).toEqual([["Wisconsin", "/us/wisconsin"], ["United States", "/us"]]);
  });

  it("says when a place extends into another county", () => {
    expect(groups.within[0].crossesCounty).toBe(false);
    expect(shareText(groups.within[0])).toBe("all of it lies in this county");
    expect(shareText(groups.within[1])).toBe("60% of it lies in this county; the rest is in another county");
  });

  it("reads an empty answer as empty and encodes the identity in the path", () => {
    expect(isEmpty(groupNearby({ geo_id: "x", total: 0, items: [] }, states, []))).toBe(true);
    expect(isEmpty(groupNearby(null, states, []))).toBe(true);
    expect(relatedPath("state:55|county:025")).toBe("/catalog/geographies/state%3A55%7Ccounty%3A025/related");
  });
});
