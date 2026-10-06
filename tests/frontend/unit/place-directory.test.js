import { describe, expect, it } from "vitest";

import {
  SITE_RULES,
  buildPlaceDirectory,
  featurePlaceHref,
  nearestCounty,
  searchPlaces,
  sourceCards,
} from "../../../apps/web/lib/placeDirectory";

// Covers: WEB-127 — the home search, map and location rules over catalog
// rows, and the public data page's cards and rules.

const nation = { geo_id: "us:1", geo_level: "NATIONAL" };
const states = [
  { geo_id: "state:55", geo_level: "STATE", state_fips: "55", state_name: "Wisconsin" },
  { geo_id: "state:27", geo_level: "STATE", state_fips: "27", state_name: "Minnesota" },
];
const counties = [
  { geo_id: "state:55|county:025", geo_level: "COUNTY", state_fips: "55", county_fips: "025", county_name: "Dane County", geo_latitude: 43.07, geo_longitude: -89.42 },
  { geo_id: "state:55|county:105", geo_level: "COUNTY", state_fips: "55", county_fips: "105", county_name: "Rock County", geo_latitude: 42.67, geo_longitude: -89.07 },
  { geo_id: "state:27|county:053", geo_level: "COUNTY", state_fips: "27", county_fips: "053", county_name: "Hennepin County", geo_latitude: "45.0", geo_longitude: "-93.47" },
];
const entries = buildPlaceDirectory(nation, states, counties);

describe("the place directory", () => {
  it("addresses every place page the catalog publishes", () => {
    expect(entries.map((entry) => entry.href)).toEqual([
      "/us", "/us/wisconsin", "/us/minnesota",
      "/us/wisconsin/dane-county", "/us/wisconsin/rock-county", "/us/minnesota/hennepin-county",
    ]);
  });

  it("matches every typed word and ranks names that start with it first", () => {
    expect(searchPlaces(entries, "dane").map((entry) => entry.name)).toEqual(["Dane County, Wisconsin"]);
    expect(searchPlaces(entries, "county wis").map((entry) => entry.name)).toEqual(["Dane County, Wisconsin", "Rock County, Wisconsin"]);
    expect(searchPlaces(entries, "wisconsin")[0].level).toBe("STATE");
    expect(searchPlaces(entries, "   ")).toEqual([]);
    expect(searchPlaces(entries, "atlantis")).toEqual([]);
  });

  it("matches a coordinate to the nearest county center in the catalog", () => {
    expect(nearestCounty(43.1, -89.4, counties).geo_id).toBe("state:55|county:025");
    expect(nearestCounty(44.98, -93.27, counties).geo_id).toBe("state:27|county:053");
    expect(nearestCounty(43, -89, [{ geo_id: "x" }])).toBeNull();
  });

  it("opens the page a clicked boundary addresses, through the tile join key", () => {
    expect(featurePlaceHref({ geoid: "55025" }, "geoid", counties, entries)).toBe("/us/wisconsin/dane-county");
    expect(featurePlaceHref({ geo_id: "state:55|county:105" }, "geo_id", counties, entries)).toBe("/us/wisconsin/rock-county");
    expect(featurePlaceHref({ geoid: "99999" }, "geoid", counties, entries)).toBeNull();
    expect(featurePlaceHref(null, "geoid", counties, entries)).toBeNull();
  });
});

describe("the public data page", () => {
  it("states the five rules", () => {
    expect(SITE_RULES.map((rule) => rule.rule)).toEqual([
      "Suppressed is not zero.",
      "No composite score.",
      "Association is not causation.",
      "Every number names its period and source.",
      "Revisions are shown, not overwritten.",
    ]);
  });

  it("reports a source the freshness resource omits as not reported, never fresh", () => {
    const cards = sourceCards(
      [{ source_code: "FRED", source_name: "FRED" }, { source_code: "BLS", source_name: "Bureau of Labor Statistics" }],
      [{ source_code: "BLS", metric_count: 10, current_count: 9, stale_count: 1, retired_count: 0, latest_harvested_at: "2026-10-01T00:00:00Z", latest_publication_time: "2026-09-10T00:00:00Z", geo_grains: ["COUNTY", "STATE"] }],
    );
    expect(cards.map((card) => card.sourceCode)).toEqual(["BLS", "FRED"]);
    expect(cards[0]).toMatchObject({ reported: true, grains: ["COUNTY", "STATE"], staleCount: 1 });
    expect(cards[1]).toMatchObject({ reported: false, lastRefresh: null, grains: [], metricCount: null });
  });
});
