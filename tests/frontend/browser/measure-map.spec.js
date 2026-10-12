import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { servedParameters } from "../support/servedContract.js";

// Covers: WEB-128 — one measure across every county: legend counts that add
// up, no painted or printed zero for a county without a value, only
// published periods, and an explanation instead of a map where a measure has
// no county grain. Synthetic UI fixture values, not published statistics.

const INCOME = "CENSUS_ACS:acs5:B19013_001";
const CRIME = "FBI_UCR:summarized_violent_crime:V:offense:rate";
const counties = [
  ["001", "Adams County"], ["003", "Ashland County"], ["025", "Dane County"], ["105", "Rock County"], ["127", "Walworth County"],
].map(([fips, name]) => ({ geo_id: `state:55|county:${fips}`, geo_level: "COUNTY", state_fips: "55", county_fips: fips, county_name: name, state_name: "Wisconsin" }));
const metrics = {
  [INCOME]: { metric_code: INCOME, metric_display_name: "Median household income (UI fixture)", source_code: "CENSUS_ACS", units: null, valid_geo_grains: ["COUNTY", "STATE", "NATIONAL"], measure_kind: "median", aggregation_characteristic: "non_additive" },
  [CRIME]: { metric_code: CRIME, metric_display_name: "Violent crime rate (UI fixture)", source_code: "FBI_UCR", units: "per_100000_population", valid_geo_grains: ["STATE", "NATIONAL"] },
};
const values = { "2024-01-01": { "001": "61000", "003": "52000", "025": null, "105": "70500" }, "2023-01-01": { "001": "60000", "003": "51000", "025": "88000", "105": "69000" } };

async function install(page) {
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    const path = url.pathname;
    if (path === "/api/v1/auth/refresh") return route.fulfill({ status: 401, json: { detail: "sign-in could not be completed" } });
    if (path === "/api/v1/catalog/capabilities") return route.fulfill({ json: { total: 2, items: ["CENSUS_ACS", "FBI_UCR"].map((source_code) => ({ source_code, display_name: source_code, route_segment: null, served_by_neutral_routes: true,
      publishes_value_status: true, publishes_aligned_reduction: true, observation_filters: ["geo_id", "geo_level", "state_fips"], observation_dimensions: [],
      observation_routes: ["/api/v1/observations", "/api/v1/observations/periods"].map((routePath) => ({ path: routePath, parameters: servedParameters(routePath) })) })) } });
    if (path === "/api/v1/catalog/sources") return route.fulfill({ json: [{ source_code: "CENSUS_ACS", source_name: "American Community Survey", reference_url: "https://example.org/acs" }] });
    if (path === "/api/v1/catalog/geographies") {
      const items = params.get("geo_level") === "COUNTY" ? counties : params.get("geo_level") === "STATE" ? [{ geo_id: "state:55", geo_level: "STATE", state_fips: "55", state_name: "Wisconsin" }] : [];
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path === "/api/v1/catalog/metrics") return route.fulfill({ json: { total: 1, limit: 8, offset: 0, items: [metrics[CRIME]] } });
    if (path.startsWith("/api/v1/catalog/metrics/")) {
      const code = decodeURIComponent(path.split("/metrics/")[1]);
      return metrics[code] ? route.fulfill({ json: metrics[code] }) : route.fulfill({ status: 404, json: { detail: "metric_code not found" } });
    }
    if (path === "/api/v1/observations/periods") return route.fulfill({ json: { metric_code: INCOME, total: 2, limit: 200, offset: 0, items: [
      { period_start: "2024-01-01", period_end: "2024-12-31", observation_count: 4 }, { period_start: "2023-01-01", period_end: "2023-12-31", observation_count: 4 },
    ] } });
    if (path === "/api/v1/observations") {
      const period = params.get("period_start") || "2024-01-01";
      const items = Object.entries(values[period] || {}).map(([fips, value]) => ({ metric_code: INCOME, source_code: "CENSUS_ACS", geo_id: `state:55|county:${fips}`, geo_level: "COUNTY",
        value, value_status: value === null ? "suppressed" : "valid", unit: "dollars", period_start: period, period_end: `${period.slice(0, 4)}-12-31`, release: "fixture", dimensions: {}, uncertainty: value === null ? null : { margin_of_error: "1500" } }));
      return route.fulfill({ json: { metric_code: INCOME, total: items.length, offset: 0, limit: 1000, items } });
    }
    return route.fulfill({ status: 503, json: { detail: "No UI fixture for this resource" } });
  });
}

test("one measure across every county, with a legend that adds up and no zero for a gap", async ({ page }) => {
  await install(page);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(`/map/${encodeURIComponent(INCOME)}`);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Median household income (UI fixture)");
  await expect(page.getByTestId("measure-map")).toHaveAttribute("data-period", "2024-01-01 – 2024-12-31");

  const legend = page.getByTestId("measure-map-legend").locator("li");
  const counts = (await legend.evaluateAll((items) => items.map((item) => Number(item.getAttribute("data-count")))));
  expect(counts.reduce((total, count) => total + count, 0)).toBe(counties.length);
  await expect(legend.last()).toContainText("no published value: 2 counties");

  const all = page.getByTestId("measure-map-all");
  await expect(all.locator("tbody tr")).toHaveCount(5);
  await expect(all.locator('tr[data-geo-id="state:55|county:025"] td').first()).toHaveText("Withheld (suppressed)");
  await expect(all.locator('tr[data-geo-id="state:55|county:127"] td').first()).toHaveText("Not published");
  for (const id of ["state:55|county:025", "state:55|county:127"]) {
    await expect(all.locator(`tr[data-geo-id="${id}"] td`).first()).not.toHaveText(/^0/);
  }
  await expect(page.getByTestId("measure-map-highest").locator("tbody tr").first()).toContainText("Rock County");
  await expect(page.getByTestId("measure-map-highest").getByRole("link", { name: "Rock County, Wisconsin" })).toHaveAttribute("href", "/us/wisconsin/rock-county");
  await expect(page.getByTestId("measure-map-coverage")).toContainText("median");

  await expect(page.getByTestId("measure-map-period").locator("option")).toHaveText(["2024-01-01 – 2024-12-31", "2023-01-01 – 2023-12-31"]);
  await page.getByTestId("measure-map-period").selectOption("2023-01-01");
  await expect(page.getByTestId("measure-map")).toHaveAttribute("data-period", "2023-01-01 – 2023-12-31");
  await expect(page).toHaveURL(/period=2023-01-01/);
  await expect(page.getByTestId("measure-map-legend").locator("li").last()).toContainText("no published value: 1 counties");

  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("a measure with no county grain explains itself and draws no map", async ({ page }) => {
  await install(page);
  await page.goto(`/map/${encodeURIComponent(CRIME)}`);
  await expect(page.getByTestId("measure-map-unavailable")).toBeVisible();
  await expect(page.getByTestId("measure-map-reason")).toContainText("is not published for counties");
  await expect(page.getByTestId("measure-map-legend")).toHaveCount(0);
  await expect(page.getByTestId("measure-map-alternatives").getByRole("link").first()).toHaveAttribute("href", /^\/map\//);
});

test("an unknown measure explains itself, and a malformed one is a 404", async ({ page }) => {
  await install(page);
  await page.goto("/map/NOPE%3A1");
  await expect(page.getByTestId("measure-map-reason")).toHaveText("No measure is published under NOPE:1.");
  const response = await page.goto("/map/%3Cscript%3E");
  expect(response.status()).toBe(404);
});

test("switching measure replaces the one measure", async ({ page }) => {
  await install(page);
  await page.goto(`/map/${encodeURIComponent(INCOME)}`);
  await expect(page.getByTestId("measure-map-legend")).toBeVisible();
  await page.getByTestId("measure-map-switch").fill("violent");
  await page.getByTestId("measure-map-switch-results").getByRole("button", { name: "Violent crime rate (UI fixture)" }).click();
  await expect(page).toHaveURL(new RegExp(`/map/${encodeURIComponent(CRIME)}$`));
  await expect(page.getByTestId("measure-map-unavailable")).toBeVisible();
});
