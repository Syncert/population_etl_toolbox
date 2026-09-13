import { describe, expect, test } from "vitest";

// Covers: WEB-002 — metric, selection, choropleth, legend, and no-data models.

import {
  buildChoroplethMatchExpression,
  buildChoroplethModel,
  buildObservationIndex,
  buildSelectionFilter,
  distributionBins,
  metricOptions,
  pickPreferredMetric,
  preferredGeoLevelForMetric,
} from "../../../apps/web/components/SourceExplorerPage";
import {
  boundsOfFeatures,
  buildExtrusionHeightExpression,
  formatObservationValue,
  observationName,
  tileFilterForGeoLevel,
  tileFilterForSelection,
} from "../../../apps/web/lib/explorerViewModel";

const metrics = [
  { metric_code: "CENSUS_ACS:acs1:B19013_001", metric_display_name: "Income", source_code: "CENSUS_ACS" },
  { metric_code: "CENSUS_ACS:acs5:B01003_001", metric_display_name: "Population!!Total", source_code: "CENSUS_ACS" },
  { metric_code: "CENSUS_ACS:acs5:B19013_001", metric_display_name: "Income", source_code: "CENSUS_ACS" },
];

describe("explorer metric, selection, and legend contracts", () => {
  test("selects the requested dataset and canonical metric deterministically", () => {
    expect(pickPreferredMetric(metrics, "acs5")).toBe("CENSUS_ACS:acs5:B01003_001");
    expect(pickPreferredMetric(metrics, "acs1", "B19013_001")).toBe("CENSUS_ACS:acs1:B19013_001");
    expect(metricOptions(metrics)[1]).toMatchObject({
      value: "CENSUS_ACS:acs5:B01003_001",
      source: "CENSUS_ACS",
    });
  });

  test("chooses supported geography grain including ACS1 partial coverage", () => {
    expect(preferredGeoLevelForMetric({ valid_geo_grains: ["STATE", "COUNTY"] })).toBe("COUNTY");
    expect(preferredGeoLevelForMetric({ valid_geo_grains: ["NATIONAL"] })).toBe("NATIONAL");
  });

  // Covers: WEB-038 — the published grain vocabulary is five words, not
  // three. A measure declaring only PLACE or only AGENCY used to fall past
  // every branch and take the COUNTY fallback -- a grain it does not
  // publish -- so the explorer asked for nothing and reported "0 COUNTY
  // records" as though the measure published none.
  test("never prefers a grain the measure does not declare", () => {
    expect(preferredGeoLevelForMetric({ valid_geo_grains: ["AGENCY"] })).toBe("AGENCY");
    expect(preferredGeoLevelForMetric({ valid_geo_grains: ["PLACE"] })).toBe("PLACE");
    // The spatial three still win where the measure declares one of them.
    expect(
      preferredGeoLevelForMetric({ valid_geo_grains: ["PLACE", "COUNTY"] }),
    ).toBe("COUNTY");
    expect(
      preferredGeoLevelForMetric({ valid_geo_grains: ["AGENCY", "STATE"] }),
    ).toBe("STATE");
  });

  test("a measure declaring no grains keeps the caller's fallback", () => {
    // Unknown grains are not the same fact as no grains.
    expect(preferredGeoLevelForMetric({ valid_geo_grains: [] }, "COUNTY")).toBe("COUNTY");
    expect(preferredGeoLevelForMetric(null, "STATE")).toBe("STATE");
  });

  test("indexes hover/selection keys and produces an exact pinned outline filter", () => {
    const rows = [
      { geo_id: "state:55|county:025", value: "10" },
      { geo_id: "state:55|county:079", value: "20" },
    ];
    expect(buildObservationIndex(rows, "geo_id").get("state:55|county:025")).toBe(rows[0]);
    expect(buildSelectionFilter("state:55|county:025", "geo_id")).toEqual([
      "==",
      ["to-string", ["get", "geo_id"]],
      "state:55|county:025",
    ]);
    expect(buildSelectionFilter(null, "geo_id").at(-1)).toBe("__no_selected_county__");
  });

  test("uses API distribution bins for observation colors and reconciled legend counts", () => {
    // The response as the API publishes it: each bin carries its own bounds
    // (WEB-057). The fixture used to omit them, which is what let the model
    // recompute every boundary and nothing notice.
    const distribution = {
      min_value: 0,
      max_value: 20,
      bin_count: 2,
      total: 2,
      items: [
        { bin_index: 1, lower_bound: 0, upper_bound: 10, count: 1 },
        { bin_index: 2, lower_bound: 10, upper_bound: 20, count: 1 },
      ],
    };
    expect(distributionBins(distribution)).toHaveLength(2);
    const observations = [
      { geo_id: "a", value: 5 },
      { geo_id: "b", value: 15 },
    ];
    const model = buildChoroplethModel(observations, "geo_id", distribution);
    expect(model.usesDistribution).toBe(true);
    expect(model.valueCount).toBe(2);
    expect(model.legendItems.slice(0, 2).map((item) => item.count)).toEqual([1, 1]);
    expect(buildChoroplethMatchExpression(observations, "geo_id", distribution)).toEqual(model.expression);
  });

  test("no-data model stays explicit and uses the fallback color", () => {
    const model = buildChoroplethModel([], "geo_id", null, "Not published in ACS1");
    expect(model.valueCount).toBe(0);
    expect(model.legendItems).toEqual([{ color: "#9fb0ba", label: "Not published in ACS1" }]);
  });
});

describe("a value the source did not publish is never a zero", () => {
  // The API publishes `value: null` whenever a source published no usable
  // number, with `value_status` saying why. `Number(null)` is 0 and
  // `Number.isFinite(0)` is true, so every numeric path has to reject the
  // absent value explicitly or it silently becomes a published zero.
  const suppressed = [
    { geo_id: "state:55|county:025", value: "561504" },
    { geo_id: "state:55|county:001", value: null, value_status: "suppressed" },
    { geo_id: "state:55|county:003", value: "", value_status: "missing" },
  ];

  test("the choropleth colours only the geography that published a number", () => {
    const model = buildChoroplethModel(suppressed, "geo_id");
    expect(model.valueCount).toBe(1);
    // A suppressed geography must not appear in the colour expression at
    // all; leaving it out is what makes the map render it as no-data.
    expect(JSON.stringify(model.expression)).not.toContain("county:001");
    expect(JSON.stringify(model.expression)).not.toContain("county:003");
    // Its absence must not drag the scale to zero either.
    expect(model.minValue).toBe(561504);
  });

  test("extrusion heights exclude the geographies with no published value", () => {
    const expression = JSON.stringify(
      buildExtrusionHeightExpression(suppressed, "geo_id"),
    );
    expect(expression).toContain("county:025");
    expect(expression).not.toContain("county:001");
    expect(expression).not.toContain("county:003");
  });

  test("extrusion heights scale with zoom so a column stays visible at every zoom", () => {
    // Heights are metres drawn to scale, and the 12 km ceiling is under a
    // pixel at the national zoom. The per-feature match is bound once and
    // multiplied per zoom stop: 128x at zoom 3, 1x at the reference zoom 10.
    const expression = buildExtrusionHeightExpression(suppressed, "geo_id");
    expect(expression[0]).toBe("let");
    expect(expression[1]).toBe("height");
    expect(expression[2][0]).toBe("match");
    const [kind, , input, ...stops] = expression[3];
    expect(kind).toBe("interpolate");
    expect(input).toEqual(["zoom"]);
    const factorAt = (zoom) => stops[stops.indexOf(zoom) + 1][2];
    expect(factorAt(3)).toBe(128);
    expect(factorAt(7)).toBe(8);
    expect(factorAt(10)).toBe(1);
    // With nothing published there is no column to scale.
    expect(buildExtrusionHeightExpression([], "geo_id")).toEqual(["literal", 0]);
  });

  test("formatting an absent value states its absence rather than zero", () => {
    expect(formatObservationValue(null)).toBe("-");
    expect(formatObservationValue("")).toBe("-");
    expect(formatObservationValue(undefined)).toBe("-");
    // A published zero is still a published zero.
    expect(formatObservationValue(0)).toBe("0");
    expect(formatObservationValue("0")).toBe("0");
    expect(formatObservationValue("561504")).toBe("561,504");
  });
});

describe("a geography is named for a reader, not by its code", () => {
  test("a county names its state; a state does not repeat itself", () => {
    expect(
      observationName({
        geo_id: "state:06|county:037",
        geo_name: "Los Angeles County",
        county_name: "Los Angeles County",
        state_name: "California",
      }),
    ).toBe("Los Angeles County, California");
    expect(observationName({ geo_id: "state:01", geo_name: "Alabama", state_name: "Alabama" })).toBe(
      "Alabama",
    );
  });

  test("a row that publishes no name falls back to its identity", () => {
    expect(observationName({ geo_id: "state:06|county:037" })).toBe("state:06|county:037");
  });
});

describe("a selected state is the whole map", () => {
  test("a level is matched on geo_level, so the 32k places never show as spots", () => {
    // Places have no county_fips either; "not a county" is not "a state".
    expect(tileFilterForGeoLevel("STATE")).toEqual(["==", ["get", "geo_level"], "STATE"]);
    expect(tileFilterForGeoLevel("COUNTY")).toEqual(["==", ["get", "geo_level"], "COUNTY"]);
    expect(tileFilterForGeoLevel("NATIONAL")).toEqual([
      "in",
      ["get", "geo_level"],
      ["literal", ["STATE", "COUNTY"]],
    ]);
  });

  test("the selection filter keeps the geo level and narrows to the state", () => {
    expect(tileFilterForSelection("COUNTY", "")).toEqual(["==", ["get", "geo_level"], "COUNTY"]);
    expect(tileFilterForSelection("COUNTY", "06")).toEqual([
      "all",
      ["==", ["get", "geo_level"], "COUNTY"],
      ["==", ["to-string", ["get", "state_fips"]], "06"],
    ]);
  });

  test("the fit extent is the state's polygons, not the country's", () => {
    const square = (west, south, east, north) => [
      [[west, south], [east, south], [east, north], [west, north], [west, south]],
    ];
    const features = [
      { properties: { state_fips: "06" }, geometry: { type: "Polygon", coordinates: square(-124, 32, -114, 42) } },
      {
        properties: { state_fips: "06" },
        geometry: { type: "MultiPolygon", coordinates: [square(-120, 33, -118, 34.5)] },
      },
      { properties: { state_fips: "48" }, geometry: { type: "Polygon", coordinates: square(-106, 26, -93, 36) } },
    ];
    expect(boundsOfFeatures(features, "06")).toEqual([[-124, 32], [-114, 42]]);
    expect(boundsOfFeatures(features)).toEqual([[-124, 26], [-93, 42]]);
    expect(boundsOfFeatures(features, "99")).toBeNull();
    expect(boundsOfFeatures([], "06")).toBeNull();
  });
});

describe("a logarithmic value scale", () => {
  // Five decades and a zero. The API's equal-width bins over 0..100000 put
  // four of the five decades in the first bin, which is the population map
  // in one colour.
  const decades = [
    { geo_id: "a", value: "10" },
    { geo_id: "b", value: "100" },
    { geo_id: "c", value: "1000" },
    { geo_id: "d", value: "10000" },
    { geo_id: "e", value: "100000" },
    { geo_id: "z", value: "0" },
  ];
  // Every bin the caller asked for, with its bounds, empty ones included --
  // API-079's contract, which this fixture did not model (WEB-057). Five
  // equal-width bins over [0, 100000]: the first four decades all fall in the
  // first, which is the point this test makes about a linear scale.
  const distribution = {
    min_value: 0,
    max_value: 100000,
    bin_count: 5,
    total: 6,
    items: [
      { bin_index: 1, lower_bound: 0, upper_bound: 20000, count: 5 },
      { bin_index: 2, lower_bound: 20000, upper_bound: 40000, count: 0 },
      { bin_index: 3, lower_bound: 40000, upper_bound: 60000, count: 0 },
      { bin_index: 4, lower_bound: 60000, upper_bound: 80000, count: 0 },
      { bin_index: 5, lower_bound: 80000, upper_bound: 100000, count: 1 },
    ],
  };
  const colourOf = (model, key) => model.expression[model.expression.indexOf(key) + 1];

  test("spreads a long-tailed measure across every colour where linear bins cannot", () => {
    const linear = buildChoroplethModel(decades, "geo_id", distribution);
    const log = buildChoroplethModel(decades, "geo_id", distribution, "No observation", "log");
    const keys = ["a", "b", "c", "d", "e"];
    expect(new Set(keys.map((key) => colourOf(linear, key))).size).toBe(2);
    expect(new Set(keys.map((key) => colourOf(log, key))).size).toBe(5);
    expect(log.scale).toBe("log");
    expect(log.usesDistribution).toBe(false);
    // Zero has no logarithm; it sits in the lowest bin rather than vanishing.
    expect(colourOf(log, "z")).toBe(colourOf(log, "a"));
    expect(log.legendItems.map((item) => item.count)).toEqual([2, 1, 1, 1, 1, undefined]);
    // Five bins of equal width in log space over four decades: edges fall
    // at 10^1.8, 10^2.6, 10^3.4, 10^4.2, not on the decades themselves.
    expect(log.legendItems[0].label).toBe("Up to 63");
    expect(log.legendItems[4].label).toBe("15.8K and above");
    expect(log.minValue).toBe(10);
    expect(log.maxValue).toBeCloseTo(100000);
  });

  test("falls back to linear when nothing published is positive", () => {
    const model = buildChoroplethModel(
      [{ geo_id: "a", value: "0" }, { geo_id: "b", value: "-5" }],
      "geo_id",
      null,
      "No observation",
      "log",
    );
    expect(model.scale).toBe("linear");
  });

  test("extrusion heights follow the same scale", () => {
    const log = buildExtrusionHeightExpression(decades, "geo_id", "log");
    const heightOf = (expression, key) => {
      const match = expression[2];
      return match[match.indexOf(key) + 1];
    };
    // 1000 is the midpoint of 10..100000 in log space: half the height range.
    expect(heightOf(log, "c")).toBe(200 + 6000);
    expect(heightOf(log, "z")).toBe(200);
    expect(heightOf(buildExtrusionHeightExpression(decades, "geo_id"), "c")).toBe(
      Math.round(200 + ((1000 - 0) / 100000) * 12000),
    );
  });
});

// Covers: WEB-057 — the legend's bins are the bins the API measured.
//
// `/distribution/bins` publishes each bin whole and says why an absent bin
// and a bin holding zero geographies are different statements: "reporting it
// as the first makes every consumer rebuild the gaps from min/max". The model
// rebuilt them, and the degenerate answer the API documents -- one distinct
// value, one bin closing on itself -- came out as five bins over one point
// with the map coloured from a different one than the legend counted.
describe("the bins are the API's, not recomputed from its bounds", () => {
  const colourOf = (model, key) => model.expression[model.expression.indexOf(key) + 1];

  test("the published bounds are the bounds the legend shows", () => {
    const distribution = {
      total: 10,
      bin_count: 2,
      min_value: 0.1,
      max_value: 0.7,
      // Deliberately not the equal-width split of [0.1, 0.7]: the API owns
      // the binning rule, and a model that recomputes it cannot tell the
      // difference between reading the answer and agreeing with it.
      items: [
        { bin_index: 1, lower_bound: 0.1, upper_bound: 0.25, count: 4 },
        { bin_index: 2, lower_bound: 0.25, upper_bound: 0.7, count: 6 },
      ],
    };
    expect(distributionBins(distribution)).toEqual([
      { binIndex: 1, color: "#edcf63", lowerBound: 0.1, upperBound: 0.25, count: 4 },
      { binIndex: 2, color: "#9dc57d", lowerBound: 0.25, upperBound: 0.7, count: 6 },
    ]);
  });

  test("one distinct value is one bin, and the map colours it that bin", () => {
    // The answer the API sends for a metric every geography published the
    // same value for: `bin_count` is what the caller asked for, `items` is
    // what the query measured.
    const distribution = {
      total: 3,
      bin_count: 5,
      min_value: 4.2,
      max_value: 4.2,
      items: [{ bin_index: 1, lower_bound: 4.2, upper_bound: 4.2, count: 3 }],
    };
    const bins = distributionBins(distribution);
    expect(bins).toHaveLength(1);
    expect(bins[0].count).toBe(3);

    const observations = [
      { geo_id: "a", value: "4.2" },
      { geo_id: "b", value: "4.2" },
      { geo_id: "c", value: "4.2" },
    ];
    const model = buildChoroplethModel(observations, "geo_id", distribution);
    expect(model.usesDistribution).toBe(true);
    // The legend already had this case; the bin model never produced it.
    expect(model.legendItems[0].label).toBe("All numeric values");
    expect(model.legendItems[0].count).toBe(3);
    // The colour on the map is the colour beside the count in the legend.
    for (const key of ["a", "b", "c"]) {
      expect(colourOf(model, key)).toBe(model.legendItems[0].color);
    }
  });

  test("a gap in the published bins is refused, never filled with zeros", () => {
    // API-079 reports every bin, empty ones included. A response missing one
    // is a contract regression, and an absent bin is not a bin holding no
    // geographies -- so it is not rendered as one.
    expect(
      distributionBins({
        total: 6,
        bin_count: 3,
        min_value: 0,
        max_value: 30,
        items: [
          { bin_index: 1, lower_bound: 0, upper_bound: 10, count: 5 },
          { bin_index: 3, lower_bound: 20, upper_bound: 30, count: 1 },
        ],
      }),
    ).toEqual([]);
  });

  test("a bin with no published bounds is refused", () => {
    expect(
      distributionBins({
        total: 2,
        bin_count: 1,
        min_value: 0,
        max_value: 10,
        items: [{ bin_index: 1, count: 2 }],
      }),
    ).toEqual([]);
  });

  test("more bins than the palette can colour renders none", () => {
    const items = Array.from({ length: 6 }, (_unused, index) => ({
      bin_index: index + 1,
      lower_bound: index,
      upper_bound: index + 1,
      count: 1,
    }));
    expect(
      distributionBins({ total: 6, bin_count: 6, min_value: 0, max_value: 6, items }),
    ).toEqual([]);
  });
});
