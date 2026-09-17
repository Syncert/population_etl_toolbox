import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-113 — the map's bytes arrive when the map does, and one
// catalog read answers every screen that asks for it at once.
//
// MapLibre is the largest dependency this application has. It was imported
// at the top of `useMapLibre`, so `/explore` and `/compare` each shipped
// roughly 1 MB more JavaScript than every other route -- to every reader,
// including one whose selection has no map at all, and including the first
// paint of the explorer, whose default tab is the map.
//
// The budget check (`scripts/check-bundle-budget.mjs`) measures the split
// from the build manifest. What it cannot see is whether the chunk is
// actually left unrequested at runtime, which is the claim that matters:
// these tests read what the browser asked for.

const MVT = Buffer.from(
  "GvEBCghjb3VudGllcxImEhAAAAEBAgIDAwQEBQUGBgcHGAMiEAm+FMQFGgDDBtQNAADEBg8aC2NvdW50eV9maXBzGgtjb3VudHlfbmFtZRoGZ2VvX2lkGglnZW9fbGV2ZWwaCGxhdGl0dWRlGglsb25naXR1ZGUaCnN0YXRlX2ZpcHMaCnN0YXRlX25hbWUiBQoDMDI1Ig0KC0RhbmUgQ291bnR5IhUKE3N0YXRlOjU1fGNvdW50eTowMjUiCAoGQ09VTlRZIgkZVFInoImIRUAiCRmamZmZmVlWwCIECgI1NSILCglXaXNjb25zaW4ogCB4Ag==",
  "base64",
);

const COUNTY_METRIC = "CENSUS_ACS:acs5:B01003_001";
// Published only at the national grain, and the vector boundary publishes no
// national geometry -- so this selection has no map (WEB-029).
const NATIONAL_METRIC = "CENSUS_ACS:acs5:B01003_001_US";
const GEO_ID = "state:55|county:025";

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

const metrics = [
  {
    metric_code: COUNTY_METRIC,
    metric_display_name: "Total population",
    source_code: "CENSUS_ACS",
    units: "people",
    freshness_state: "fresh",
    valid_geo_grains: ["STATE", "COUNTY"],
    valid_time_grains: ["ANNUAL"],
  },
  {
    metric_code: NATIONAL_METRIC,
    metric_display_name: "Total population, United States",
    source_code: "CENSUS_ACS",
    units: "people",
    freshness_state: "fresh",
    valid_geo_grains: ["NATIONAL"],
    valid_time_grains: ["ANNUAL"],
  },
];

const observation = {
  source_code: "CENSUS_ACS",
  source: "CENSUS_ACS",
  metric_code: COUNTY_METRIC,
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
};

function observationsFor(metricCode) {
  return metricCode === NATIONAL_METRIC
    ? [
        {
          ...observation,
          metric_code: NATIONAL_METRIC,
          geo_id: "nation:us",
          geo_level: "NATIONAL",
          county_name: "",
          state_name: "United States",
          value: "333287557",
        },
      ]
    : [observation];
}

async function installRoutes(page) {
  await page.route("**/api/v1/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/v1/catalog/capabilities", (route) =>
    route.fulfill({ json: capabilities }),
  );
  await page.route("**/api/v1/catalog/sources", (route) =>
    route.fulfill({
      json: {
        total: 1,
        items: [
          {
            source_code: "CENSUS_ACS",
            title: "Census American Community Survey",
            reference_url: "https://www.census.gov/programs-surveys/acs",
          },
        ],
      },
    }),
  );
  await page.route("**/api/v1/catalog/metrics?*", (route) =>
    route.fulfill({ json: { total: metrics.length, limit: 1000, offset: 0, items: metrics } }),
  );
  await page.route("**/api/v1/catalog/metrics/*", (route) => {
    const code = decodeURIComponent(new URL(route.request().url()).pathname.split("/").pop());
    const found = metrics.find((metric) => metric.metric_code === code);
    return found
      ? route.fulfill({ json: found })
      : route.fulfill({ status: 404, json: { detail: "metric_code not found" } });
  });
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
  for (const pattern of [
    "**/api/v1/census/observations/latest?*",
    "**/api/v1/observations?*",
  ]) {
    await page.route(pattern, (route) => {
      const metric = new URL(route.request().url()).searchParams.get("metric_code");
      const items = observationsFor(metric);
      return route.fulfill({ json: { total: items.length, limit: 4000, offset: 0, items } });
    });
  }
  await page.route("**/api/v1/census/observations/timeseries?*", (route) =>
    route.fulfill({ json: { total: 1, limit: 1000, offset: 0, items: [observation] } }),
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
        items: [{ bin_index: 1, lower_bound: 561504, upper_bound: 561504, count: 1 }],
      },
    }),
  );
  await page.route("**/tiles/catalog", (route) =>
    route.fulfill({
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

/**
 * Every JavaScript chunk the page fetches, and how many bytes each was.
 *
 * The chunk that carries MapLibre cannot be named: a production build hashes
 * its filenames, and the browser tier runs against a development server
 * locally and a production server in CI. So the test compares *sets* of
 * chunks between two states of the same page and weighs the difference,
 * which needs no name and holds under either build.
 */
function watchChunks(page) {
  const requested = new Set();
  const bytes = new Map();
  const settled = [];
  const isChunk = (url) => /\/_next\/static\/.*\.js$/.test(new URL(url).pathname);

  page.on("request", (request) => {
    if (isChunk(request.url())) {
      requested.add(new URL(request.url()).pathname);
    }
  });
  page.on("response", (response) => {
    if (!isChunk(response.url())) {
      return;
    }
    const path = new URL(response.url()).pathname;
    settled.push(
      response
        .body()
        .then((body) => bytes.set(path, body.length))
        .catch(() => bytes.set(path, 0)),
    );
  });

  return {
    snapshot: () => new Set(requested),
    async sizeOf(paths) {
      await Promise.all(settled);
      return [...paths].reduce((total, path) => total + (bytes.get(path) || 0), 0);
    },
  };
}

test("a selection with no map downloads no map", async ({ page }) => {
  await installRoutes(page);
  const chunks = watchChunks(page);

  // A national series: the tile boundary publishes no national geometry, so
  // the explorer renders no map at all and `useMapLibre` is never enabled.
  await page.goto(`/explore?metric=${encodeURIComponent(NATIONAL_METRIC)}`);
  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-map-supported", "false");
  await expect(page.getByTestId("map-canvas")).toHaveCount(0);
  // The page is fully settled before the reading is taken, or an absent
  // chunk would only mean the test was early.
  await expect(page.getByTestId("observation-table-caption")).toBeVisible();
  await page.waitForLoadState("networkidle");

  const withoutMap = chunks.snapshot();

  // The same page, same navigation, now with a mappable selection.
  await page.getByTestId("metric-select").selectOption(COUNTY_METRIC);
  await expect(dashboard).toHaveAttribute("data-map-supported", "true");
  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-map-ready", "true");
  await page.waitForLoadState("networkidle");

  const withMap = chunks.snapshot();
  const arrivedWithTheMap = [...withMap].filter((path) => !withoutMap.has(path));

  // Something arrived, and it is large: MapLibre is about a megabyte. A
  // floor rather than a range, because a development build is unminified and
  // a production one is not.
  expect(
    arrivedWithTheMap.length,
    "the map mounted and no new JavaScript was fetched, so its bytes were in the first load",
  ).toBeGreaterThan(0);
  expect(await chunks.sizeOf(arrivedWithTheMap)).toBeGreaterThan(300_000);
});

/**
 * Record the cache mode of every request the application makes.
 *
 * The mode is an argument to `fetch`, not anything a request carries on the
 * wire, so it is read where it is passed. Reuse from the browser's own cache
 * cannot be observed in this tier at all: every response here is fulfilled by
 * `page.route`, and an intercepted response is never written to Chromium's
 * HTTP cache. What this tier can prove is that the client stops refusing the
 * cache, which is the change; that the API sends the headers is API-139's.
 */
async function watchFetchModes(page) {
  await page.addInitScript(() => {
    window.__fetchModes = [];
    const original = window.fetch;
    window.fetch = (input, init) => {
      window.__fetchModes.push({
        url: typeof input === "string" ? input : String(input?.url ?? input),
        cache: init?.cache ?? null,
        authorized: Boolean(init?.headers?.Authorization),
      });
      return original(input, init);
    };
  });
}

test("a public catalog read is sent with the browser's cache rules, and once", async ({ page }) => {
  await watchFetchModes(page);
  await installRoutes(page);

  const catalogRequests = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname.startsWith("/api/v1/catalog/") && request.method() === "GET") {
      catalogRequests.push(`${url.pathname}${url.search}`);
    }
  });

  await page.goto(`/explore?metric=${encodeURIComponent(COUNTY_METRIC)}`);
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-map-supported", "true");
  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-map-ready", "true");
  await page.waitForLoadState("networkidle");

  const modes = await page.evaluate(() => window.__fetchModes);
  const catalogModes = modes.filter((entry) => entry.url.includes("/api/v1/catalog/"));
  // The reading means something only if the explorer did read the catalog.
  expect(catalogModes.length).toBeGreaterThan(1);
  expect(catalogModes.every((entry) => !entry.authorized)).toBe(true);
  expect(
    [...new Set(catalogModes.map((entry) => entry.cache))],
    "a public analytical read is sent with `default`, so the API's own TTL decides",
  ).toEqual(["default"]);

  // A full catalog page fetched twice in one navigation is 3,144 geographies
  // fetched twice. No screen in this application mounts another that reads
  // the same catalog today, so this is a standing guard rather than a
  // regression test: the de-duplication itself is proven in the unit tier,
  // where two overlapping reads can be arranged deterministically.
  const repeated = [...new Set(catalogRequests)].filter(
    (url) => catalogRequests.filter((seen) => seen === url).length > 1,
  );
  expect(repeated, "the same catalog page was fetched more than once").toEqual([]);
});
