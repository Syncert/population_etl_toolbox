// The web's address rule and the API's port of it agree (api-place-search).
//
// `/catalog/geographies/search` and `/locate` answer a `page_path` built by
// `apps/api/services/place_paths.py`. The place pages resolve an address with
// the functions below, so a path the API built by any other rule would be a
// link to a 404. Both suites assert this one fixture; changing either rule
// fails the other until both agree.
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  citySegment,
  countySegment,
  placePath,
  stateSegment,
} from "../../../apps/web/lib/placeChapters";
import { tractSegment } from "../../../apps/web/lib/tractPage";

const FIXTURE = join(
  dirname(fileURLToPath(import.meta.url)),
  "../../fixtures/shared/place_segments.json",
);

describe("place page addresses shared with the API", () => {
  const fixture = JSON.parse(readFileSync(FIXTURE, "utf8"));
  const { states, counties, places, tracts, expected_paths: expected } = fixture;

  function webPath(row) {
    const state = states.find((candidate) => candidate.state_fips === row.state_fips);
    const stateSeg = stateSegment(state, states);
    if (row.geo_level === "STATE") return placePath(stateSeg);
    const ownCounties = counties.filter((county) => county.state_fips === row.state_fips);
    if (row.geo_level === "COUNTY") return placePath(stateSeg, countySegment(row, ownCounties));
    if (row.geo_level === "TRACT") {
      // A tract's page sits under its county's (tract pages).
      const county = ownCounties.find((candidate) => candidate.county_fips === row.county_fips);
      return `${placePath(stateSeg, countySegment(county, ownCounties))}/${tractSegment(row.geo_id)}`;
    }
    const ownPlaces = places.filter((place) => place.state_fips === row.state_fips);
    return placePath(stateSeg, citySegment(row, ownPlaces, ownCounties));
  }

  it("Covers: API-171 — every fixture row answers the path the API answers", () => {
    const rows = [...states, ...counties, ...places, ...tracts];
    expect(rows.map((row) => row.geo_id).sort()).toEqual(Object.keys(expected).sort());
    for (const row of rows) {
      expect([row.geo_id, webPath(row)]).toEqual([row.geo_id, expected[row.geo_id]]);
    }
  });
});
