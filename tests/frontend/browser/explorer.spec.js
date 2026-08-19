import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

// Covers: WEB-004, WEB-005, WEB-006 — browser catalog/tile/selection/failure flows.

const MVT = Buffer.from(
  "GvEBCghjb3VudGllcxImEhAAAAEBAgIDAwQEBQUGBgcHGAMiEAm+FMQFGgDDBtQNAADEBg8aC2NvdW50eV9maXBzGgtjb3VudHlfbmFtZRoGZ2VvX2lkGglnZW9fbGV2ZWwaCGxhdGl0dWRlGglsb25naXR1ZGUaCnN0YXRlX2ZpcHMaCnN0YXRlX25hbWUiBQoDMDI1Ig0KC0RhbmUgQ291bnR5IhUKE3N0YXRlOjU1fGNvdW50eTowMjUiCAoGQ09VTlRZIgkZVFInoImIRUAiCRmamZmZmVlWwCIECgI1NSILCglXaXNjb25zaW4ogCB4Ag==",
  "base64",
);

const metrics = [
  {
    metric_code: "ACS:acs5:B01003_001",
    metric_display_name: "Total population",
    source_code: "CENSUS_ACS",
    valid_geo_grains: ["STATE", "COUNTY"],
    valid_time_grains: ["ANNUAL"],
  },
  {
    metric_code: "ACS:acs1:B01003_001",
    metric_display_name: "Total population ACS1",
    source_code: "CENSUS_ACS",
    valid_geo_grains: ["STATE", "COUNTY"],
    valid_time_grains: ["ANNUAL"],
  },
];

const county = {
  source_code: "CENSUS_ACS",
  source: "CENSUS_ACS",
  observation_date: "2023-01-01",
  period: "2023",
  duration_start: "2023-01-01",
  duration_end: "2023-12-31",
  as_of_date: "2024-01-01",
  release_date: "2024-01-01",
  updated_at: "2024-01-02T00:00:00Z",
  geo_id: "state:55|county:025",
  geo_level: "COUNTY",
  geo_name: "Dane County",
  state_fips: "55",
  county_fips: "025",
  state_name: "Wisconsin",
  county_name: "Dane County",
  geo_latitude: 43.0667,
  geo_longitude: -89.4,
  metric_code: "ACS:acs5:B01003_001",
  metric_display_name: "Total population",
  value: "561504",
  units: "people",
  unit: "people",
  dataset_code: "acs5",
  dataset: "acs5",
  vintage_year: 2023,
  margin_of_error: "1200",
  margin_of_error_pct: "0.21",
};

async function installRoutes(page, { failLatest = false } = {}) {
  let tileRequests = 0;
  await page.route("**/api/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/catalog/metrics?*", (route) => route.fulfill({
    json: { total: metrics.length, limit: 1000, offset: 0, items: metrics },
    headers: { "x-cache": "MISS" },
  }));
  await page.route("**/api/catalog/geographies?*", (route) => {
    const level = new URL(route.request().url()).searchParams.get("geo_level");
    const items = level === "STATE"
      ? [{ geo_id: "state:55", geo_level: "STATE", state_fips: "55", state_name: "Wisconsin", latitude: 44.5, longitude: -89.5 }]
      : [{ geo_id: county.geo_id, geo_level: "COUNTY", state_fips: "55", county_fips: "025", state_name: "Wisconsin", county_name: "Dane County", latitude: 43.0667, longitude: -89.4 }];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
  await page.route("**/api/census/observations/latest?*", (route) => {
    if (failLatest) return route.fulfill({ status: 503, json: { detail: "fallback unavailable" } });
    const metric = new URL(route.request().url()).searchParams.get("metric_code");
    const items = metric?.includes(":acs1:") ? [] : [{ ...county, metric_code: metric }];
    return route.fulfill({
      json: { total: items.length, limit: 4000, offset: 0, items },
      headers: { "x-cache": "MISS" },
    });
  });
  await page.route("**/api/census/observations/timeseries?*", (route) => route.fulfill({
    json: {
      total: 2,
      limit: 1000,
      offset: 0,
      items: [
        { ...county, observation_date: "2022-01-01", period: "2022", value: "555000" },
        county,
      ],
    },
  }));
  await page.route("**/api/distribution/bins?*", (route) => route.fulfill({
    json: {
      total: 1,
      bin_count: 1,
      min_value: 561504,
      max_value: 561504,
      items: [{ bin_index: 1, count: 1 }],
    },
  }));
  await page.route("**/tiles/catalog", (route) => route.fulfill({ json: { counties: {} } }));
  await page.route(/\/tiles\/counties$/, (route) => route.fulfill({
    json: {
      name: "counties",
      tiles: ["http://internal-martin:3000/counties/{z}/{x}/{y}"],
      vector_layers: [{
        id: "counties",
        fields: { geo_id: "String", state_fips: "String", county_fips: "String", county_name: "String" },
      }],
    },
  }));
  await page.route(/\/tiles\/counties\/\d+\/\d+\/\d+(?:\.pbf)?$/, (route) => {
    tileRequests += 1;
    return route.fulfill({
      status: 200,
      contentType: "application/vnd.mapbox-vector-tile",
      body: MVT,
    });
  });
  await page.route("**/tiles/counties/**", (route) => {
    tileRequests += 1;
    return route.fulfill({
      status: 200,
      contentType: "application/vnd.mapbox-vector-tile",
      body: MVT,
    });
  });
  return () => tileRequests;
}

test("catalog, observation coloring, Martin tile, selection, history, and keyboard flow", async ({ page }) => {
  const tileRequests = await installRoutes(page);
  await page.goto("/explore");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-metric-count", "2");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-colored-values", "1");
  await expect(page.getByLabel("Choropleth value legend")).toContainText("API distribution");
  await expect(page.getByTestId("tiles-status")).toContainText("healthy_tile=true");
  expect(tileRequests()).toBeGreaterThan(0);

  await page.getByTestId("state-select").selectOption("55");
  await page.getByTestId("county-select").selectOption(county.geo_id);
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", county.geo_id);
  await expect(page.getByText("2 historical observations")).toBeVisible();
  await expect(page.getByText("Dane County, Wisconsin")).toBeVisible();

  const map = page.getByTestId("map-canvas");
  await map.focus();
  await page.keyboard.press("Escape");
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", "");
  await page.keyboard.press("Enter");
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", county.geo_id);
});

test("ACS1 partial/no-data and API fallback states remain explicit", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-metric-count", "2");
  await page.getByTestId("dataset-select").selectOption("acs1");
  await expect(page.getByText(/ACS 1-year county coverage is partial/)).toBeVisible();
  await expect(page.getByTestId("observations-status")).toContainText("0 county records published");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "0");
  await expect(page.getByText("No observations available for selected metric.")).toHaveCount(1);

  const failing = await page.context().newPage();
  await installRoutes(failing, { failLatest: true });
  await failing.goto("/explore");
  await expect(failing.getByTestId("observations-status")).toContainText("status 503");
  await expect(failing.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "0");
  await expect(failing.getByText("No observations available for selected metric.")).toHaveCount(1);
  await failing.close();
});
