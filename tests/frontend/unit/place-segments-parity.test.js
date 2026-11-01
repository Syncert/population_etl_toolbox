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
  metroPath,
  metroSegment,
  placePath,
  stateSegment,
} from "../../../apps/web/lib/placeChapters";

const FIXTURE = join(
  dirname(fileURLToPath(import.meta.url)),
  "../../fixtures/shared/place_segments.json",
);

describe("place page addresses shared with the API", () => {
  const fixture = JSON.parse(readFileSync(FIXTURE, "utf8"));
  const { states, counties, places, metros, expected_paths: expected } = fixture;

  function webPath(row) {
    if (row.geo_level === "METRO") return metroPath(metroSegment(row, metros));
    const state = states.find((candidate) => candidate.state_fips === row.state_fips);
    const stateSeg = stateSegment(state, states);
    if (row.geo_level === "STATE") return placePath(stateSeg);
    const ownCounties = counties.filter((county) => county.state_fips === row.state_fips);
    if (row.geo_level === "COUNTY") return placePath(stateSeg, countySegment(row, ownCounties));
    const ownPlaces = places.filter((place) => place.state_fips === row.state_fips);
    return placePath(stateSeg, citySegment(row, ownPlaces, ownCounties));
  }

  // Covers: WEB-153 — a metro's address is its name's slug under /metro/, or its CBSA code on a clash.
  it("Covers: API-171 — every fixture row answers the path the API answers", () => {
    const rows = [...states, ...counties, ...places, ...metros];
    expect(rows.map((row) => row.geo_id).sort()).toEqual(Object.keys(expected).sort());
    for (const row of rows) {
      expect([row.geo_id, webPath(row)]).toEqual([row.geo_id, expected[row.geo_id]]);
    }
  });
});
