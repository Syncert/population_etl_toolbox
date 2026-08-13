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

const metrics = [
  { metric_code: "ACS:acs1:B19013_001", metric_display_name: "Income", source_code: "CENSUS_ACS" },
  { metric_code: "ACS:acs5:B01003_001", metric_display_name: "Population!!Total", source_code: "CENSUS_ACS" },
  { metric_code: "ACS:acs5:B19013_001", metric_display_name: "Income", source_code: "CENSUS_ACS" },
];

describe("explorer metric, selection, and legend contracts", () => {
  test("selects the requested dataset and canonical metric deterministically", () => {
    expect(pickPreferredMetric(metrics, "acs5")).toBe("ACS:acs5:B01003_001");
    expect(pickPreferredMetric(metrics, "acs1", "B19013_001")).toBe("ACS:acs1:B19013_001");
    expect(metricOptions(metrics)[1]).toMatchObject({
      value: "ACS:acs5:B01003_001",
      source: "CENSUS_ACS",
    });
  });

  test("chooses supported geography grain including ACS1 partial coverage", () => {
    expect(preferredGeoLevelForMetric({ valid_geo_grains: ["STATE", "COUNTY"] })).toBe("COUNTY");
    expect(preferredGeoLevelForMetric({ valid_geo_grains: ["NATIONAL"] })).toBe("NATIONAL");
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
    const distribution = {
      min_value: 0,
      max_value: 20,
      bin_count: 2,
      total: 2,
      items: [{ bin_index: 1, count: 1 }, { bin_index: 2, count: 1 }],
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
