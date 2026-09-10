import { describe, expect, test } from "vitest";

// Covers: WEB-028 — BLS LAUS is published per measure, so a BLS metric spans
// every state and county the program covers and the explorer's spatial
// presentations answer for it through the same capability-driven paths every
// other source uses. No client-side BLS special case is added or expected.

import { describeViewModes } from "../../../apps/web/lib/viewModes";
import { buildExplorerSources } from "../../../apps/web/lib/explorerSources";
import {
  datasetFacetOptions,
  metricSupportedGeoLevels,
  pickPreferredMetric,
  preferredGeoLevelForMetric,
} from "../../../apps/web/lib/explorerViewModel";

// The fields the published counties layer carries (infra/martin/martin.yml).
const TILE_FIELDS = [
  "geo_id",
  "geo_level",
  "state_fips",
  "county_fips",
  "state_name",
  "county_name",
  "latitude",
  "longitude",
];

const [bls] = buildExplorerSources([
  {
    source_code: "BLS",
    display_name: "Bureau of Labor Statistics",
    route_segment: "bls",
    served_by_neutral_routes: true,
    observation_filters: ["geo_id", "geo_level", "state_fips", "county_fips"],
    observation_routes: [
      {
        path: "/api/v1/observations",
        parameters: ["geo_id", "geo_level", "limit", "metric_code", "scope"],
      },
      {
        path: "/api/v1/observations/releases",
        parameters: ["limit", "metric_code", "offset"],
      },
    ],
  },
]);

const unemploymentRate = {
  metric_code: "BLS:LAU:UNEMP_RATE",
  metric_display_name: "Unemployment rate",
  source_code: "BLS",
  units: "Percent",
  valid_geo_grains: ["COUNTY", "STATE"],
  valid_time_grains: ["MONTHLY"],
  freshness_state: "current",
};

// A national CES series: one fixed-coded series, no spatial grain. It sorts
// ahead of every LAU code, so it is what a first-row default would pick.
const nationalPayrolls = {
  metric_code: "BLS:CES0000000001",
  metric_display_name: "Total Nonfarm Payroll Employment",
  source_code: "BLS",
  units: "Thousands of Persons",
  valid_geo_grains: ["NATIONAL"],
  valid_time_grains: ["MONTHLY"],
  freshness_state: "current",
};

const stateOnlyMeasure = {
  metric_code: "BLS:LAU:LFPR",
  metric_display_name: "Labor force participation rate",
  source_code: "BLS",
  units: "Percent",
  valid_geo_grains: ["STATE"],
  valid_time_grains: ["MONTHLY"],
  freshness_state: "current",
};

const catalog = [nationalPayrolls, unemploymentRate, stateOnlyMeasure];

describe("a LAUS measure is a spatial selection", () => {
  test("the map answers for a LAUS measure at the county grain", () => {
    const support = describeViewModes({
      metric: unemploymentRate,
      source: bls,
      geoLevel: "COUNTY",
      tileFields: TILE_FIELDS,
      rowCount: 3144,
    });

    expect(support.map.supported).toBe(true);
    expect(support.trend.supported).toBe(true);
    expect(support.table.supported).toBe(true);
  });

  test("the published grains are what narrow the geography selector", () => {
    expect(metricSupportedGeoLevels(unemploymentRate)).toEqual(["COUNTY", "STATE"]);
    expect(preferredGeoLevelForMetric(unemploymentRate)).toBe("COUNTY");
    // 07 to 09 are published at state only, and the selector follows the data.
    expect(metricSupportedGeoLevels(stateOnlyMeasure)).toEqual(["STATE"]);
    expect(preferredGeoLevelForMetric(stateOnlyMeasure)).toBe("STATE");
  });

  test("a national series is still correctly reported as non-spatial", () => {
    const support = describeViewModes({
      metric: nationalPayrolls,
      source: bls,
      geoLevel: "NATIONAL",
      tileFields: TILE_FIELDS,
      rowCount: 1,
    });

    expect(support.map.supported).toBe(false);
    expect(support.map.reason).toContain("no national geometry");
  });
});

describe("the BLS catalog's own shape drives the selectors", () => {
  test("BLS publishes one dataset facet, so the selector stays hidden", () => {
    // Only three-part codes carry a facet; the two-part CES/CPI/JOLTS codes
    // carry none. One option means SourceExplorerPage offers the full list.
    expect(datasetFacetOptions(catalog)).toEqual([{ value: "lau", label: "LAU" }]);
  });

  test("the default selection is a mappable measure, not a national series", () => {
    // With no dataset selector the explorer asks for a default over the whole
    // BLS catalog. A national series first would open the source on a
    // selection its map can never draw.
    expect(pickPreferredMetric(catalog, "")).toBe("BLS:LAU:UNEMP_RATE");
    expect(pickPreferredMetric(catalog, "lau")).toBe("BLS:LAU:UNEMP_RATE");
  });

  test("a catalog with only national series still resolves a default", () => {
    expect(pickPreferredMetric([nationalPayrolls], "")).toBe("BLS:CES0000000001");
  });
});
