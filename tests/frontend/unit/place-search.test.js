import { describe, expect, it } from "vitest";

import {
  foundPlaces,
  locateLanding,
  nationMatch,
  roundCoordinate,
  safePagePath,
  searchable,
} from "../../../apps/web/lib/placeSearch";

// Covers: WEB-127 — the home search over the API's place search and point
// lookup (D-1): "Austin" offers Austin city, Travis County and the Austin
// metro, location lands on the city, and nothing the API answers can send a
// reader off the site.

const pop = (population, year = 2023) => ({ population, population_year: year, population_metric_code: "CENSUS_ACS:acs5:B01003_001" });
const AUSTIN = [
  { geo_id: "state:48|place:05000", geo_level: "PLACE", geo_name: "Austin city", short_name: "Austin", match: "exact", state_fips: "48", state_name: "Texas", county_geo_id: "state:48|county:453", county_name: "Travis County", page_path: "/us/texas/austin-city", ...pop(967862) },
  { geo_id: "state:48|county:453", geo_level: "COUNTY", geo_name: "Travis County", short_name: "Travis", match: "contains_match", matched_via: { geo_id: "state:48|place:05000", geo_name: "Austin city" }, state_fips: "48", state_name: "Texas", page_path: "/us/texas/travis-county", ...pop(1290188) },
  { geo_id: "cbsa:12420", geo_level: "METRO", geo_name: "Austin-Round Rock-San Marcos, TX Metro Area", short_name: "Austin-Round Rock-San Marcos", match: "contains_match", matched_via: { geo_id: "state:48|place:05000", geo_name: "Austin city" }, state_fips: "48", state_name: "Texas", page_path: null, ...pop(2421115) },
  { geo_id: "state:48|county:015", geo_level: "COUNTY", geo_name: "Austin County", short_name: "Austin", match: "exact", state_fips: "48", state_name: "Texas", page_path: "/us/texas/austin-county", ...pop(30707) },
  { geo_id: "zcta:78701", geo_level: "ZCTA", geo_name: "ZCTA5 78701", short_name: "78701", match: "exact", page_path: null, population: null, population_year: null, population_metric_code: null },
];

describe("the finder's rows for a search answer", () => {
  const places = foundPlaces(AUSTIN);

  it("keeps the API's order: Austin city, Travis County, the Austin metro", () => {
    expect(places.map((place) => place.name)).toEqual([
      "Austin city, Texas",
      "Travis County, Texas",
      "Austin-Round Rock-San Marcos, TX Metro Area",
      "Austin County, Texas",
      "ZIP code 78701",
    ]);
    expect(places.map((place) => place.kind)).toEqual(["city or town", "county", "metro area", "county", "ZIP code area"]);
  });

  it("says why a place without the typed name was offered", () => {
    expect(places.map((place) => place.via)).toEqual([null, "includes Austin city", "includes Austin city", null, null]);
  });

  it("opens a page only where the API names one", () => {
    expect(places.map((place) => place.href)).toEqual(["/us/texas/austin-city", "/us/texas/travis-county", null, "/us/texas/austin-county", null]);
  });

  it("labels the ranking population with its survey years, and never shows an absent one as zero", () => {
    expect(places[0].population).toBe("967,862 people (2019–2023 survey)");
    expect(places[4].population).toBeNull();
    expect(foundPlaces([{ ...AUSTIN[0], population: 0 }])[0].population).toBe("0 people (2019–2023 survey)");
  });
});

describe("page addresses from the API", () => {
  it("admits only the site's own place addresses", () => {
    expect(safePagePath("/us")).toBe("/us");
    expect(safePagePath("/us/texas")).toBe("/us/texas");
    expect(safePagePath("/us/texas/austin-city")).toBe("/us/texas/austin-city");
    for (const bad of ["https://example.org/us/texas", "//evil.example/us", "/us/texas/../../admin", "/us/Texas", "/us/a/b/c", "/explore", "", null, 7]) {
      expect(safePagePath(bad)).toBeNull();
    }
  });
});

describe("the nation and the query minimum", () => {
  it("offers the nation's page for its names", () => {
    for (const typed of ["united", "United States", "USA", "us", "nation", "america"]) {
      expect(nationMatch(typed)?.href).toBe("/us");
    }
    expect(nationMatch("austin")).toBeNull();
    expect(nationMatch("u")).toBeNull();
  });

  it("asks the API only for two letters or digits, as it requires", () => {
    expect(searchable("a")).toBe(false);
    expect(searchable(" ,. a")).toBe(false);
    expect(searchable("au")).toBe(true);
    expect(searchable("é1")).toBe(true);
  });
});

describe("use my location", () => {
  it("rounds a coordinate to about 11 meters before it is sent", () => {
    expect(roundCoordinate(30.265012345)).toBe(30.265);
    expect(roundCoordinate(-97.747049)).toBe(-97.747);
    expect(roundCoordinate(-97.74706)).toBe(-97.7471);
  });

  const state = { geo_id: "state:48", geo_level: "STATE", geo_name: "Texas", page_path: "/us/texas" };
  const county = { geo_id: "state:48|county:453", geo_level: "COUNTY", geo_name: "Travis County", page_path: "/us/texas/travis-county" };

  it("lands on the city the point is in", () => {
    const city = { geo_id: "state:48|place:05000", geo_level: "PLACE", geo_name: "Austin city", page_path: "/us/texas/austin-city" };
    expect(locateLanding({ landing: city, state, county, place: city })).toEqual({
      href: "/us/texas/austin-city",
      message: "Opening Austin city, the city or town you are in.",
    });
  });

  it("lands on the county outside every city, and says so", () => {
    expect(locateLanding({ landing: county, state, county })).toEqual({
      href: "/us/texas/travis-county",
      message: "Opening Travis County. You are not inside a city or town, so this is your county.",
    });
  });

  it("falls back to the county, then the state, when the landing has no page", () => {
    const landing = { ...county, geo_level: "PLACE", page_path: null };
    expect(locateLanding({ landing, state, county }).href).toBe("/us/texas/travis-county");
    expect(locateLanding({ landing, state, county: null }).href).toBe("/us/texas");
    expect(locateLanding({ landing, state: { ...state, page_path: null } })).toBeNull();
  });
});
