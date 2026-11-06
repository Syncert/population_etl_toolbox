import { describe, expect, it } from "vitest";

import {
  areaLabel,
  changeOverYear,
  containingAreas,
  formatChange,
  formatGas,
  formatMinutes,
  formatPrice,
  minutesRatioId,
  minutesText,
  PRICE_ITEMS,
  readingsFor,
  wageText,
} from "../../../apps/web/lib/groceriesAndGas";

// Covers: WEB-143 — groceries-and-gas-cards: a county's grocery and gas
// figures come from the areas the reference says contain it, each named,
// and a food price index is read only as its own area's change over a year.

const row = (geo_id, geo_level, geo_name) => ({ relationship: "part_of", geo_id, geo_level, geo_name, state_fips: null, geography_vintage: 2024, evidence_source: "x" });
const dane = { geo_id: "state:55|county:025", total: 5, items: [
  row("state:55", "STATE", "Wisconsin"),
  row("cbsa:31540", "METRO", "Madison, WI"),
  row("division:3", "CENSUS_DIVISION", "East North Central"),
  row("region:2", "CENSUS_REGION", "Midwest Region"),
  row("us:1", "NATIONAL", "us:1"),
  { ...row("state:55|county:105", "COUNTY", "Rock County"), relationship: "adjacent" },
] };

describe("the areas a county lies in", () => {
  const areas = containingAreas("COUNTY", { geo_id: "state:55|county:025" }, dane);

  it("come from part_of rows, by level and code", () => {
    expect(areas.state.name).toBe("Wisconsin");
    expect(areas.metro).toEqual({ geoId: "cbsa:31540", level: "METRO", name: "Madison, WI" });
    expect(areas.division.geoId).toBe("division:3");
    expect(areas.region.name).toBe("Midwest Region");
    expect(areas.nation.name).toBe("United States");
  });

  it("asks EIA for the state, then BLS for the region, then EIA for the nation", () => {
    const { gas } = readingsFor(areas);
    expect(gas.map((item) => [item.metricCode, item.area.geoId])).toEqual([
      ["EIA:EPMR", "state:55"],
      ["BLS:APU020074714", "region:2"],
      ["EIA:EPMR", "us:1"],
    ]);
  });

  it("builds the CPI identity from the division's and region's codes", () => {
    expect(readingsFor(areas).food.map((item) => item.metricCode)).toEqual([
      "BLS:CUUR0230SAF11",
      "BLS:CUUR0200SAF11",
      "BLS:CUUR0000SAF11",
    ]);
  });

  it("reads price parity for the metro area before the state, never the nation", () => {
    expect(readingsFor(areas).parity.map((item) => [item.metricCode, item.area.geoId])).toEqual([
      ["BEA:MARPP:1", "cbsa:31540"],
      ["BEA:SARPP:1", "state:55"],
    ]);
  });

  it("labels a larger area's figure as that area's", () => {
    expect(areaLabel(areas.region, "state:55|county:025")).toBe("Midwest Region figure");
    expect(areaLabel(areas.state, "state:55")).toBe("Wisconsin");
  });
});

describe("a county outside every metro area", () => {
  it("has no metro reading and falls back to the state", () => {
    const rural = { ...dane, items: dane.items.filter((item) => item.geo_level !== "METRO") };
    const { parity } = readingsFor(containingAreas("COUNTY", { geo_id: "state:55|county:001" }, rural));
    expect(parity.map((item) => item.metricCode)).toEqual(["BEA:SARPP:1"]);
  });

  it("names an area the catalog has not named by its kind, never its id", () => {
    const unnamed = { ...dane, items: [row("region:2", "CENSUS_REGION", "region:2")] };
    expect(containingAreas("COUNTY", { geo_id: "x" }, unnamed).region.name).toBe("the Census region");
  });

  it("ignores an area whose id is not its level's code", () => {
    const odd = { ...dane, items: [row("cbsa:Madison", "METRO", "Madison")] };
    expect(containingAreas("COUNTY", { geo_id: "x" }, odd).metro).toBeNull();
  });
});

describe("a state's and the nation's own pages", () => {
  it("use the page's own state and nation", () => {
    const state = containingAreas("STATE", { geo_id: "state:55", geo_name: "Wisconsin" }, { geo_id: "state:55", total: 1, items: [row("us:1", "NATIONAL", "us:1")] });
    expect(state.state).toEqual({ geoId: "state:55", level: "STATE", name: "Wisconsin" });
    const nation = containingAreas("NATIONAL", { geo_id: "us:1", geo_name: "us:1" }, { geo_id: "us:1", total: 0, items: [] });
    expect(readingsFor(nation).parity).toEqual([]);
    expect(readingsFor(nation).gas.map((item) => item.area.name)).toEqual(["United States"]);
  });
});

describe("food at home over a year", () => {
  const month = (period_start, value, value_status = "valid") => ({ period_start, value, value_status });

  it("compares the newest month with the same month a year earlier", () => {
    const change = changeOverYear([month("2025-08-01", "300"), month("2026-07-01", "305"), month("2026-08-01", "309")]);
    expect(change).toEqual({ percent: 3, period: "2025-08 to 2026-08", reason: "" });
    expect(formatChange(change.percent)).toBe("up 3.0% over the year");
  });

  it("says so when the earlier month is not published, rather than using another", () => {
    const change = changeOverYear([month("2025-07-01", "300"), month("2026-08-01", "309")]);
    expect(change.percent).toBeNull();
    expect(change.reason).toContain("2025-08 is not published");
  });

  it("says a withheld newest value is withheld, not zero", () => {
    const change = changeOverYear([month("2025-08-01", "300"), month("2026-08-01", null, "withheld")]);
    expect(change.percent).toBeNull();
    expect(change.reason).toBe("Published without a value (withheld).");
  });
});

it("formats gasoline in dollars a gallon", () => {
  expect(formatGas(3.1)).toBe("$3.10 a gallon");
});

// Covers: WEB-143 — cost-of-living-section: everyday prices come from the
// region BLS publishes them for, then the nation, each named.
describe("everyday prices", () => {
  const areas = containingAreas("COUNTY", { geo_id: "state:55|county:025" }, dane);

  it("asks for the region's average price, then the nation's", () => {
    const readings = readingsFor(areas);
    expect(readings.eggs.map((item) => [item.metricCode, item.area.geoId])).toEqual([
      ["BLS:APU0200708111", "region:2"],
      ["BLS:APU0000708111", "us:1"],
    ]);
    expect(readings.electricity.map((item) => item.metricCode)).toEqual(["BLS:APU020072610", "BLS:APU000072610"]);
  });

  it("lists the same items in the same order on every page", () => {
    expect(PRICE_ITEMS.map((spec) => spec.item)).toEqual(["eggs", "milk", "bread", "beef", "chicken", "electricity"]);
  });

  it("formats prices in the units people buy them in", () => {
    const eggs = PRICE_ITEMS.find((spec) => spec.item === "eggs");
    const power = PRICE_ITEMS.find((spec) => spec.item === "electricity");
    expect(formatPrice(3.117, eggs)).toBe("$3.12 a dozen");
    expect(formatPrice(0.179, power)).toBe("17.9\u00a2 a kilowatt-hour");
  });
});

// Covers: WEB-143 — cost-of-living-section CL-3: minutes of work name the
// served pairing for the price's own area and the page's own wage, and say
// why a figure is missing rather than show zero.
describe("minutes of work", () => {
  it("names the pairing for the price's BLS area", () => {
    expect(minutesRatioId({ measure: "eggs", metricCode: "BLS:APU0200708111" })).toBe("minutes_of_work_eggs:0200");
    expect(minutesRatioId({ measure: "electricity", metricCode: "BLS:APU000072610" })).toBe("minutes_of_work_electricity:0000");
    expect(minutesRatioId({ measure: "gas", metricCode: "EIA:EPMR" })).toBeNull();
  });

  it("words minutes as a reader says them", () => {
    expect(formatMinutes(6.36)).toBe("6 minutes");
    expect(formatMinutes(1)).toBe("1 minute");
    expect(formatMinutes(0.34)).toBe("20 seconds");
    expect(formatMinutes(65)).toBe("1 hour 5 minutes");
    expect(formatMinutes(120)).toBe("2 hours");
  });

  it("states the wage once, and says when it is the newest year rather than the price's", () => {
    const row = { value: "6.00", wage_geo_name: "Dane County", wage_year: "newest_published_year", denominator: { value: "1234.4", period_start: "2024-01-01" } };
    expect(wageText(row, "x")).toBe("Dane County's average wage, $1,234 a week in 2024, the newest year published, over a 40-hour week. Source: Bureau of Labor Statistics, QCEW, all jobs.");
    expect(wageText({ ...row, denominator: null }, "x")).toBe("");
  });

  it("says why a figure is missing, never zero", () => {
    expect(minutesText({ value: "6.36" })).toBe("6 minutes");
    expect(minutesText({ value: null, unavailable_reason: "denominator_missing" })).toBe("No wage loaded for this place yet.");
    expect(minutesText({ value: null, unavailable_reason: "different_period" })).toBe("No wage published for this price's year.");
    expect(minutesText({ value: null, unavailable_reason: "suppressed_input" })).toBe("Not computed: an input is withheld.");
  });
});
