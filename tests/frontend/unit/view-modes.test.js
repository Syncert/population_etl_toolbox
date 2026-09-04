import { describe, expect, test } from "vitest";

// Covers: WEB-017 and WEB-020 — presentation modes are offered only where the selected
// measure, its source's declared routes, and the discovered tile boundary
// can answer them, and every mode that is not offered names the published
// reason. A national or otherwise unmappable series gets an explicit
// non-spatial statement rather than a map that draws and declines to colour.

import {
  COMPARISON_VIEW_MODES,
  EXPLORER_VIEW_MODES,
  describeComparisonViewModes,
  describeViewModes,
  servesHistory,
  spatialGrains,
  supportedComparisonModes,
  supportedViewModes,
  unsupportedComparisonModes,
  unsupportedViewModes,
} from "../../../apps/web/lib/viewModes";
import { buildExplorerSources, findExplorerSource } from "../../../apps/web/lib/explorerSources";

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

const neutralRoutes = [
  {
    path: "/api/v1/observations",
    parameters: ["geo_id", "geo_level", "limit", "metric_code", "release", "scope"],
  },
  { path: "/api/v1/observations/releases", parameters: ["limit", "metric_code", "offset"] },
];

const [census] = buildExplorerSources([
  {
    source_code: "CENSUS_ACS",
    display_name: "Census American Community Survey",
    route_segment: "census",
    served_by_neutral_routes: true,
    observation_filters: ["geo_id", "geo_level", "state_fips"],
    observation_routes: [
      {
        path: "/api/v1/census/observations/latest",
        parameters: ["geo_level", "limit", "metric_code", "state_fips"],
      },
      {
        path: "/api/v1/census/observations/timeseries",
        parameters: ["end_date", "geo_id", "limit", "metric_code", "start_date"],
      },
      ...neutralRoutes,
    ],
  },
]);

const metric = {
  metric_code: "ACS:acs5:B01003_001",
  metric_display_name: "Total population",
  source_code: "CENSUS_ACS",
  units: "people",
  valid_geo_grains: ["STATE", "COUNTY"],
  valid_time_grains: ["ANNUAL"],
  freshness_state: "fresh",
};

const complete = {
  metric,
  source: census,
  geoLevel: "COUNTY",
  tileFields: TILE_FIELDS,
  rowCount: 12,
};

describe("spatial grains published by the tile boundary", () => {
  test("the grains come from the layer's own published fields", () => {
    expect(spatialGrains(TILE_FIELDS)).toEqual(["STATE", "COUNTY"]);
    // A boundary publishing only state attribution draws states, not counties.
    expect(spatialGrains(["geo_id", "state_fips"])).toEqual(["STATE"]);
    // Nothing published identifies a grain: there is no spatial presentation.
    expect(spatialGrains(["geo_id"])).toEqual([]);
    expect(spatialGrains(null)).toEqual([]);
  });

  test("no published field identifies a national geometry", () => {
    // A national series is not a map that failed to colour — the boundary
    // has no polygon for it at all.
    expect(spatialGrains(TILE_FIELDS)).not.toContain("NATIONAL");
  });
});

describe("history support comes from the declared routes", () => {
  test("a declared timeseries route or the neutral resource answers a history", () => {
    expect(servesHistory(census)).toBe(true);
    const [neutralOnly] = buildExplorerSources([
      {
        source_code: "CDC",
        display_name: "Centers for Disease Control and Prevention",
        route_segment: "cdc",
        observation_filters: ["geo_id"],
        observation_routes: neutralRoutes,
      },
    ]);
    expect(servesHistory(neutralOnly)).toBe(true);
    expect(servesHistory(null)).toBe(false);
  });
});

describe("view-mode support", () => {
  test("a fully answerable selection supports every mode", () => {
    const support = describeViewModes(complete);
    expect(supportedViewModes(support)).toEqual([...EXPLORER_VIEW_MODES]);
    expect(unsupportedViewModes(support)).toEqual([]);
    for (const mode of EXPLORER_VIEW_MODES) {
      expect(support[mode].reason).toBe("");
    }
  });

  test("a national series is explicitly non-spatial, not an uncoloured map", () => {
    const support = describeViewModes({ ...complete, geoLevel: "NATIONAL" });
    expect(support.map.supported).toBe(false);
    expect(support.map.reason).toContain("no national geometry");
    expect(support.map.reason).toContain("not spatial");
    // Every other mode still answers: the values are there, only the map is not.
    expect(supportedViewModes(support)).toEqual([
      "trend",
      "table",
      "metadata",
      "quality",
      "export",
    ]);
    expect(unsupportedViewModes(support)).toEqual([
      { mode: "map", reason: support.map.reason },
    ]);
  });

  test("an undiscovered tile boundary removes the map rather than blanking it", () => {
    const support = describeViewModes({ ...complete, tileFields: null });
    expect(support.map).toEqual({
      supported: false,
      reason: "no vector tile layer with published geography fields was discovered",
    });
    expect(describeViewModes({ ...complete, geoLevel: "" }).map.reason).toBe(
      "no geography grain is selected",
    );
  });

  test("a source declaring no history route offers no trend", () => {
    const [latestOnly] = buildExplorerSources([
      {
        source_code: "CENSUS_ACS",
        display_name: "Census American Community Survey",
        route_segment: "census",
        observation_filters: ["geo_id"],
        observation_routes: [
          {
            path: "/api/v1/census/observations/latest",
            parameters: ["geo_level", "limit", "metric_code"],
          },
          {
            path: "/api/v1/census/observations/timeseries",
            parameters: [],
          },
        ],
      },
    ]);
    const support = describeViewModes({ ...complete, source: latestOnly });
    expect(support.trend.supported).toBe(false);
    expect(support.trend.reason).toContain("declares no route");
    expect(support.trend.reason).toContain("Census American Community Survey");
  });

  test("quality is offered only where the measure publishes it", () => {
    // Freshness alone is enough...
    expect(
      describeViewModes({
        ...complete,
        metric: { metric_code: "M", freshness_state: "stale" },
      }).quality.supported,
    ).toBe(true);
    // ...and so is provenance alone.
    expect(
      describeViewModes({ ...complete, metric: { metric_code: "M", units: "people" } })
        .quality.supported,
    ).toBe(true);
    // A measure publishing neither gets no quality view: an empty one could
    // read as "nothing is wrong", which the source never said.
    const bare = describeViewModes({ ...complete, metric: { metric_code: "M" } });
    expect(bare.quality).toEqual({
      supported: false,
      reason: "this measure publishes no freshness or provenance state",
    });
  });

  test("table, export, and metadata follow what is actually loaded and selected", () => {
    const empty = describeViewModes({ ...complete, rowCount: 0, metric: null });
    expect(empty.table.reason).toBe("no observations are loaded for this selection");
    expect(empty.export.reason).toBe("no observations are loaded to export");
    expect(empty.metadata.reason).toContain("no measure is selected");
    expect(supportedViewModes(empty)).toEqual(["map", "trend"]);
  });

  test("an empty input set is answerable and offers nothing it cannot serve", () => {
    const support = describeViewModes();
    expect(supportedViewModes(support)).toEqual([]);
    expect(unsupportedViewModes(support).map((entry) => entry.mode)).toEqual([
      ...EXPLORER_VIEW_MODES,
    ]);
    expect(unsupportedViewModes(support).every((entry) => entry.reason)).toBe(true);
  });
});

describe("comparison view-mode support", () => {
  const comparable = {
    comparable: true,
    rowCount: 12,
    plottablePoints: 11,
    derivations: ["difference", "ratio"],
    geoLevel: "COUNTY",
    tileFields: TILE_FIELDS,
  };

  test("a comparable pair with rows and a mappable grain answers every mode", () => {
    const support = describeComparisonViewModes(comparable);
    expect(supportedComparisonModes(support)).toEqual([...COMPARISON_VIEW_MODES]);
    expect(unsupportedComparisonModes(support)).toEqual([]);
  });

  test("a blocked pair presents nothing, with the policy as the stated reason", () => {
    const support = describeComparisonViewModes({ ...comparable, comparable: false });
    expect(supportedComparisonModes(support)).toEqual([]);
    for (const { reason } of unsupportedComparisonModes(support)) {
      expect(reason).toContain("declared compatibility policy blocks this pair");
    }
    // The default input set is a blocked pair: nothing renders until the
    // API has said the comparison may be made.
    expect(supportedComparisonModes(describeComparisonViewModes())).toEqual([]);
  });

  test("a national comparison is explicitly non-spatial", () => {
    const support = describeComparisonViewModes({ ...comparable, geoLevel: "NATIONAL" });
    expect(support.map.supported).toBe(false);
    expect(support.map.reason).toContain("no national geometry");
    expect(support.map.reason).toContain("not spatial");
    expect(supportedComparisonModes(support)).toEqual(["chart", "table", "export"]);
  });

  test("a single plottable pair is not a plot", () => {
    // One point states nothing about how two measures relate across places.
    const support = describeComparisonViewModes({ ...comparable, plottablePoints: 1 });
    expect(support.chart.supported).toBe(false);
    expect(support.chart.reason).toContain("fewer than two geographies");
    // The table and map still answer: the values are there.
    expect(support.table.supported).toBe(true);
    expect(support.map.supported).toBe(true);
  });

  test("no derived field means nothing for a map to colour", () => {
    const support = describeComparisonViewModes({ ...comparable, derivations: [] });
    expect(support.map).toEqual({
      supported: false,
      reason: "the response named no derived field for a map to colour",
    });
  });

  test("no aligned geographies removes every presentation with one reason", () => {
    const support = describeComparisonViewModes({ ...comparable, rowCount: 0 });
    expect(supportedComparisonModes(support)).toEqual([]);
    for (const { reason } of unsupportedComparisonModes(support)) {
      expect(reason).toBe("no aligned geographies were published for this selection");
    }
  });
});
