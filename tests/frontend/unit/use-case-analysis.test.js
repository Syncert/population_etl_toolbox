import { expect, test } from "vitest";
import { buildUseCaseHistory, useCaseChartState, verifyUseCaseRows } from "../../../apps/web/lib/useCaseAnalysis";

test("an answer must belong to the exact requested measure and geography", () => {
  const rows = [{ metric_code: "CENSUS_ACS:acs5:B25064_001", geo_id: "state:55|county:025", value: "1425" }];
  expect(() => verifyUseCaseRows(rows, "CENSUS_ACS:acs5:B25064_001", "state:55|county:025")).not.toThrow();
  expect(() => verifyUseCaseRows(rows, "CENSUS_ACS:acs5:B01003_001", "state:55|county:025")).toThrow(/different metric/);
  expect(() => verifyUseCaseRows(rows, "CENSUS_ACS:acs5:B25064_001", "state:27")).toThrow(/different geography/);
});

// Covers: WEB-123 — reusable use-case charts never widen a place request,
// collapse strata, treat withheld values as zero, or disguise incomplete reads.
const source = {
  sourceCode: "CENSUS_PEP", accessShape: "neutral", neutralFilters: ["geo_id", "geo_level"],
  requestFilters: ["geo_id", "geo_level"], supportsSettledHistory: true, supportsAsReleased: true,
  neutralDimensionFilters: [], dimensionFilters: [], publishedDimensions: [],
};
const metric = { metric_code: "CENSUS_PEP:POPESTIMATE", valid_geo_grains: ["COUNTY"] };

test("history uses the API's settled releases and exact selected geography", () => {
  const result = buildUseCaseHistory(source, metric, "state:55|county:025", "COUNTY");
  expect(result.reason).toBe("");
  expect(result.request.params).toMatchObject({ metric_code: metric.metric_code, geo_id: "state:55|county:025", scope: "as_released", newest_release_per_period: "true" });
});

test("a missing place, withheld grain, or unsupported geography filter cannot widen the request", () => {
  expect(buildUseCaseHistory(source, metric, "", "COUNTY").request).toBeNull();
  expect(buildUseCaseHistory(source, metric, "state:55", "STATE").reason).toContain("not published at STATE");
  expect(buildUseCaseHistory(source, { ...metric, valid_geo_grains: [] }, "state:55|county:025", "COUNTY").request).toBeNull();
  expect(buildUseCaseHistory({ ...source, neutralFilters: [] }, metric, "state:55|county:025", "COUNTY").reason).toContain("geo_id");
});

test("stratified or duplicate-period histories stay in a table", () => {
  const rows = [
    { geo_id: "state:55", period_start: "2023", value: "18", dimensions: { stratum_id: "all" } },
    { geo_id: "state:55", period_start: "2023", value: "20", dimensions: { stratum_id: "female" } },
  ];
  const cdc = { ...source, publishedDimensions: ["stratum_id"], neutralDimensionFilters: ["stratum_id"] };
  const state = useCaseChartState(cdc, rows, "latest");
  expect(state.drawable).toBe(false);
  expect(state.reason).toContain("separate series");
  expect(useCaseChartState(source, rows, "latest").drawable).toBe(false);
});

test("a genuine zero is drawable but a withheld value remains absent", () => {
  const state = useCaseChartState(source, [
    { geo_id: "state:55", period_start: "2023", value: "0" },
    { geo_id: "state:55", period_start: "2024", value: null, value_status: "withheld" },
  ], "latest");
  expect(state.drawable).toBe(true);
  expect(state.numericRows).toHaveLength(1);
  expect(state.numericRows[0].value).toBe("0");
});
