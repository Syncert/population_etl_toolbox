import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-110 — the table alternative reaches every loaded row.
//
// The explorer's table is the map's accessible alternative: the README says
// every value the map would show "remains available in the observation
// table". It rendered twelve rows under a heading with no caption, no count
// and nothing to click, and the comparison table did the same at twenty-five.
// A national county map colours 3,144 geographies, so a reader who cannot use
// the map reached twelve of them.
//
// The fixtures are the accessibility suite's, which is the suite that already
// treats this table as the map's alternative -- with one change: the neutral
// resource answers 120 rows rather than one, because a bound is invisible
// against a single row.

const ROW_COUNT = 120;

const MVT = Buffer.from(
  "GvEBCghjb3VudGllcxImEhAAAAEBAgIDAwQEBQUGBgcHGAMiEAm+FMQFGgDDBtQNAADEBg8aC2NvdW50eV9maXBzGgtjb3VudHlfbmFtZRoGZ2VvX2lkGglnZW9fbGV2ZWwaCGxhdGl0dWRlGglsb25naXR1ZGUaCnN0YXRlX2ZpcHMaCnN0YXRlX25hbWUiBQoDMDI1Ig0KC0RhbmUgQ291bnR5IhUKE3N0YXRlOjU1fGNvdW50eTowMjUiCAoGQ09VTlRZIgkZVFInoImIRUAiCRmamZmZmVlWwCIECgI1NSILCglXaXNjb25zaW4ogCB4Ag==",
  "base64",
);

const neutralRoutes = [
  {
    path: "/api/v1/observations",
    parameters: ["geo_id", "geo_level", "limit", "metric_code", "release", "scope", "state_fips"],
  },
  { path: "/api/v1/observations/releases", parameters: ["limit", "metric_code", "offset"] },
];

const capabilities = {
  total: 1,
  items: [
    {
      source_code: "CENSUS_ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      publishes_aligned_reduction: true,
      observation_filters: ["geo_id", "geo_level", "state_fips"],
      observation_routes: [
        {
          path: "/api/v1/census/observations/latest",
          parameters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
        },
        {
          path: "/api/v1/census/observations/timeseries",
          parameters: ["end_date", "geo_id", "limit", "metric_code", "start_date"],
        },
        ...neutralRoutes,
        { path: "/api/v1/distribution/bins", parameters: ["metric_code"] },
      ],
    },
  ],
};

const METRIC = "CENSUS_ACS:acs5:B01003_001";
const GEO_ID = "state:55|county:025";

const observation = {
  source_code: "CENSUS_ACS",
  source: "CENSUS_ACS",
  metric_code: METRIC,
  geo_id: GEO_ID,
  geo_level: "COUNTY",
  county_name: "Dane County",
  state_name: "Wisconsin",
  state_fips: "55",
  county_fips: "025",
  value: "561504",
  units: "people",
  unit: "people",
  period: "2023",
  observation_date: "2023-01-01",
  dataset_code: "acs5",
  margin_of_error: "1200",
};

/** One row per county, numbered so one page can be told from another. */
const observations = Array.from({ length: ROW_COUNT }, (_, index) => ({
  ...observation,
  geo_id: `state:55|county:${String(index).padStart(3, "0")}`,
  county_name: `County ${index}`,
  value: String(1000 + index),
}));

async function installRoutes(page, { failObservations = false } = {}) {
  await page.route("**/api/v1/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/v1/catalog/capabilities", (route) =>
    route.fulfill({ json: capabilities }),
  );
  await page.route("**/api/v1/catalog/metrics?*", (route) =>
    route.fulfill({
      json: {
        total: 1,
        limit: 1000,
        offset: 0,
        items: [
          {
            metric_code: METRIC,
            metric_display_name: "Total population",
            source_code: "CENSUS_ACS",
            units: "people",
            freshness_state: "fresh",
            valid_geo_grains: ["STATE", "COUNTY"],
            valid_time_grains: ["ANNUAL"],
          },
        ],
      },
    }),
  );
  await page.route("**/api/v1/catalog/geographies?*", (route) => {
    const level = new URL(route.request().url()).searchParams.get("geo_level");
    const items =
      level === "STATE"
        ? [
            {
              geo_id: "state:55",
              geo_level: "STATE",
              state_fips: "55",
              state_name: "Wisconsin",
              latitude: 44.5,
              longitude: -89.5,
            },
          ]
        : [
            {
              geo_id: GEO_ID,
              geo_level: "COUNTY",
              state_fips: "55",
              county_fips: "025",
              state_name: "Wisconsin",
              county_name: "Dane County",
              latitude: 43.0667,
              longitude: -89.4,
            },
          ];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
  await page.route("**/api/v1/census/observations/latest?*", (route) =>
    failObservations
      ? route.fulfill({ status: 503, json: { detail: "database unavailable" } })
      : route.fulfill({
          json: { total: ROW_COUNT, limit: 4000, offset: 0, items: observations },
        }),
  );
  await page.route("**/api/v1/census/observations/timeseries?*", (route) =>
    route.fulfill({
      json: {
        total: 2,
        limit: 1000,
        offset: 0,
        items: [{ ...observation, period: "2022", value: "555000" }, observation],
      },
    }),
  );
  // Every source reads through the neutral resource, so the failure the
  // recovery test injects belongs here rather than on the legacy pair.
  await page.route("**/api/v1/observations?*", (route) =>
    failObservations
      ? route.fulfill({ status: 503, json: { detail: "database unavailable" } })
      : route.fulfill({
          json: { total: ROW_COUNT, limit: 4000, offset: 0, items: observations },
        }),
  );
  await page.route("**/api/v1/observations/releases?*", (route) =>
    route.fulfill({ json: { total: 0, limit: 200, offset: 0, items: [] } }),
  );
  await page.route("**/api/v1/distribution/bins?*", (route) =>
    route.fulfill({
      json: {
        total: 1,
        bin_count: 1,
        min_value: 561504,
        max_value: 561504,
        // The bin's own bounds, as the API publishes them (WEB-057).
        items: [{ bin_index: 1, lower_bound: 561504, upper_bound: 561504, count: 1 }],
      },
    }),
  );
  await page.route("**/tiles/catalog", (route) =>
    route.fulfill({
      // Martin's real catalog shape: sources sit under section keys.
      json: {
        tiles: {
          counties: {
            content_type: "application/x-protobuf",
            description: "gold.dim_geo_latest.geo_geom",
          },
        },
        sprites: {},
        fonts: {},
        styles: {},
        settings: { rendering: false },
      },
    }),
  );
  await page.route(/\/tiles\/counties$/, (route) =>
    route.fulfill({
      json: {
        name: "counties",
        tiles: ["http://internal-martin:3000/counties/{z}/{x}/{y}"],
        vector_layers: [
          {
            id: "counties",
            fields: { geo_id: "String", state_fips: "String", county_fips: "String" },
          },
        ],
      },
    }),
  );
  await page.route("**/tiles/counties/**", (route) =>
    route.fulfill({ status: 200, contentType: "application/vnd.mapbox-vector-tile", body: MVT }),
  );
}

async function openTable(page) {
  await page.goto(`/explore?metric=${encodeURIComponent(METRIC)}`);
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByTestId("observation-table-caption")).toBeVisible();
}

test("the table says how many rows there are and which it is showing", async ({ page }) => {
  await installRoutes(page);
  await openTable(page);

  const caption = page.getByTestId("observation-table-caption");
  await expect(caption).toContainText("Showing 1–50 of 120 loaded rows");
  // Without the order, "rows 51 to 100" names no particular rows.
  await expect(caption).toContainText("in the order this resource declares");
  await expect(page.getByTestId("observation-table-page")).toContainText("Page 1 of 3");
});

test("a row past the twelfth is reachable by keyboard alone", async ({ page }) => {
  await installRoutes(page);
  await openTable(page);

  // The thirteenth row: the first one the old render could not reach at all.
  await expect(page.getByRole("cell", { name: "County 12, Wisconsin", exact: true })).toBeVisible();
  await expect(page.getByRole("cell", { name: "County 49, Wisconsin", exact: true })).toBeVisible();
  await expect(page.getByRole("cell", { name: "County 50, Wisconsin", exact: true })).toHaveCount(0);

  // Reached with the keyboard, not the mouse: this table is the alternative
  // for a reader who has no pointer.
  const next = page.getByTestId("observation-table-next");
  await next.focus();
  await expect(next).toBeFocused();
  await page.keyboard.press("Enter");

  await expect(page.getByTestId("observation-table-caption")).toContainText(
    "Showing 51–100 of 120 loaded rows",
  );
  await expect(page.getByRole("cell", { name: "County 50, Wisconsin", exact: true })).toBeVisible();
});

test("the page survives a reload, because the link carries it", async ({ page }) => {
  await installRoutes(page);
  await openTable(page);

  await page.getByTestId("observation-table-next").click();
  await page.getByTestId("observation-table-next").click();
  await expect(page.getByTestId("observation-table-page")).toContainText("Page 3 of 3");
  // The last page is short: 120 rows in fifties leaves 20.
  await expect(page.getByTestId("observation-table-caption")).toContainText(
    "Showing 101–120 of 120 loaded rows",
  );
  expect(page.url()).toContain("rows=3");

  await page.reload();
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByTestId("observation-table-page")).toContainText("Page 3 of 3");
  await expect(page.getByRole("cell", { name: "County 119, Wisconsin", exact: true })).toBeVisible();
});
