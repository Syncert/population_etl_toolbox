// Metro pages (web-metro-pages): the helpers that shape the one metro read.
import { describe, expect, it } from "vitest";

import {
  delineationText,
  derivationText,
  metroContainingRows,
  multiStateNote,
} from "../../../apps/web/lib/metroPages";
import { containingAreas, readingsFor } from "../../../apps/web/lib/groceriesAndGas";
import { compareHref } from "../../../apps/web/lib/placeComparison";
import { metroCode, metroSegment, resolvePlaceChapters } from "../../../apps/web/lib/placeChapters";
import { metroRouteTitle } from "../../../apps/web/lib/routeTitles";

const page = (states, extra = {}) => ({
  requested_segment: "texarkana-tx-ar-metro-area",
  metro: { geo_id: "cbsa:45500", geo_name: "Texarkana, TX-AR Metro Area", short_name: "Texarkana", cbsa_code: "45500", segment: "texarkana-tx-ar-metro-area", page_path: "/metro/texarkana-tx-ar-metro-area",
    principal_state_fips: "48", principal_state_name: "Texas", population: 147000, population_year: 2023, population_metric_code: "CENSUS_ACS:acs5:B01003_001" },
  delineation_vintage: 2023,
  evidence_source: "census_cbsa_delineation",
  member_counties: [],
  states,
  principal_state_areas: [
    { geo_id: "division:7", geo_level: "CENSUS_DIVISION", geo_name: "West South Central" },
    { geo_id: "region:3", geo_level: "CENSUS_REGION", geo_name: "South Region" },
    { geo_id: "us:1", geo_level: "NATIONAL", geo_name: "United States" },
  ],
  ...extra,
});
const TEXAS = { geo_id: "state:48", state_fips: "48", state_name: "Texas", page_path: "/us/texas", member_counties: 1, principal: true };
const ARKANSAS = { geo_id: "state:05", state_fips: "05", state_name: "Arkansas", page_path: "/us/arkansas", member_counties: 1, principal: false };

// Covers: WEB-153 — a metro page reads beside its principal state and the nation, says when it spans states, and labels a summed BEA figure.
describe("metro pages", () => {
  it("names the principal state of a metro that spans states, and why", () => {
    expect(multiStateNote(page([TEXAS, ARKANSAS]))).toBe(
      "Texarkana, TX-AR Metro Area reaches into 2 states: Texas and Arkansas. Its figures are read beside Texas's, the state of its most populous county.",
    );
    expect(multiStateNote(page([TEXAS]))).toBe("");
    expect(delineationText(page([TEXAS]))).toContain("2023 CBSA delineation");
  });

  it("gives the groceries and gas card its own price level and its state's larger areas", () => {
    const rows = metroContainingRows(page([TEXAS, ARKANSAS]));
    const areas = containingAreas("METRO", { geo_id: "cbsa:45500", geo_name: "Texarkana, TX-AR Metro Area" }, rows);
    expect(areas.metro?.geoId).toBe("cbsa:45500");
    expect(areas.state?.geoId).toBe("state:48");
    expect(areas.region?.geoId).toBe("region:3");
    const readings = readingsFor(areas);
    expect(readings.parity.map((reading) => reading.metricCode)).toEqual(["BEA:MARPP:1", "BEA:SARPP:1"]);
    expect(readings.food[0].metricCode).toBe("BLS:CUUR0370SAF11");
  });

  it("labels a BEA figure the warehouse summed from member counties, and nothing else", () => {
    expect(derivationText({ derivation: "sum_of_member_counties" })).toMatch(/^Derived: the sum of its member counties/);
    expect(derivationText({ derivation: "ratio_of_member_county_sums" })).toMatch(/summed income over their summed population/);
    expect(derivationText({ derivation: null })).toBeNull();
    expect(derivationText(null)).toBeNull();
  });

  it("addresses a metro by its name's slug, or its code on a clash", () => {
    const metros = [
      { geo_id: "cbsa:31540", geo_name: "Madison, WI Metro Area" },
      { geo_id: "cbsa:99901", geo_name: "Twin, TL Micro Area" },
      { geo_id: "cbsa:99902", geo_name: "Twin TL Micro Area" },
    ];
    expect(metroSegment(metros[0], metros)).toBe("madison-wi-metro-area");
    expect(metroSegment(metros[1], metros)).toBe("99901");
    expect(metroCode("cbsa:31540")).toBe("31540");
    expect(metroCode("state:55")).toBe("");
    expect(metroRouteTitle("madison-wi-metro-area")).toBe("Madison Wi Metro Area");
    expect(metroRouteTitle("31540")).toBe("Metro area 31540");
  });

  it("compares a metro by both identities, never under the /us pair form", () => {
    expect(compareHref({ geoId: "cbsa:31540", path: "/metro/madison-wi-metro-area" }, { geoId: "us:1", path: "/us" }))
      .toBe("/compare/places?a=cbsa%3A31540&b=us%3A1");
    expect(compareHref({ geoId: "state:55", path: "/us/wisconsin" }, { geoId: "us:1", path: "/us" })).toBe("/us/wisconsin/vs/us");
  });

  it("shows a chapter at metro grain only where the catalog publishes one", () => {
    const metric = (code, grains) => [code, { metric_code: code, source_code: code.split(":")[0], valid_geo_grains: grains }];
    const catalog = new Map([
      metric("CENSUS_ACS:acs5:B19013_001", ["METRO", "COUNTY", "STATE", "NATIONAL"]),
      metric("CDC:places_county:OBESITY:AgeAdjPrv", ["COUNTY", "NATIONAL"]),
    ]);
    const { shown, omitted } = resolvePlaceChapters("METRO", catalog);
    expect(shown.map((entry) => entry.chapter.id)).toEqual(["work-money"]);
    expect(omitted.find((entry) => entry.chapter.id === "health")?.reason).toBe("no published metro area values for this place");
    // A metro never borrows its state's crime figures as county pages do.
    expect(shown.some((entry) => entry.stateContext)).toBe(false);
  });
});
