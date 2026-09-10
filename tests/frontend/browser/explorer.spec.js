import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

// Covers: WEB-004, WEB-005, WEB-006, WEB-010, WEB-013, WEB-014, WEB-016,
// WEB-017, WEB-018 —
// browser catalog/tile/selection/failure flows, URL reproduction of the
// selected exploration state, capability-driven source discovery and
// switching, dispatch-shaped sources reached through the neutral
// /observations resource with capability-declared filters, as-released
// exploration over the published release listing, presentation modes
// offered only where the selection can answer them, and the retired
// demonstration dashboards redirecting into the live explorer.

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
  // A measure published only at the national grain. The vector boundary
  // publishes no national geometry, so this series has no map at all.
  {
    metric_code: "ACS:acs5:B01003_001_US",
    metric_display_name: "Total population, United States",
    source_code: "CENSUS_ACS",
    valid_geo_grains: ["NATIONAL"],
    valid_time_grains: ["ANNUAL"],
    units: "people",
    freshness_state: "fresh",
  },
];

const pepMetric = {
  metric_code: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
  metric_display_name: "Resident population estimate",
  source_code: "CENSUS_PEP",
  valid_geo_grains: ["STATE", "COUNTY"],
  valid_time_grains: ["ANNUAL"],
};

const cdcMetric = {
  metric_code: "CDC:cdc_places_county:OBESITY",
  metric_display_name: "Obesity prevalence",
  source_code: "CDC",
  valid_geo_grains: ["COUNTY"],
  valid_time_grains: ["ANNUAL"],
};

// Shaped like the served CapabilityListResponse. The explorer derives both
// its source tabs and how it reaches each source from these declarations:
// a source-scoped latest/timeseries pair, or the neutral /observations
// resource for a dispatch-shaped source.
const neutralRoutes = [
  {
    path: "/api/v1/observations",
    // The served parameter list (tests/fixtures/api/openapi_contract.json):
    // `scope` and `release` are what make the as-released surface reachable.
    parameters: [
      "adjustment_status",
      "county_fips",
      "domain_desc",
      "domaincat_desc",
      "geo_id",
      "geo_level",
      "limit",
      "metric_code",
      "offset",
      "release",
      "scope",
      "state_fips",
      "stratum_id",
      "subject_code",
      "subject_type",
      "year_from",
      "year_to",
    ],
  },
  { path: "/api/v1/observations/releases", parameters: ["limit", "metric_code", "offset"] },
];

const capabilityRoutes = (segment) => [
  {
    path: `/api/v1/${segment}/observations/latest`,
    parameters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
  },
  {
    path: `/api/v1/${segment}/observations/timeseries`,
    parameters: ["end_date", "geo_id", "limit", "metric_code", "start_date"],
  },
  ...neutralRoutes,
  { path: "/api/v1/distribution/bins", parameters: ["metric_code"] },
];

const capabilities = {
  total: 4,
  items: [
    {
      source_code: "CENSUS_ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      datasets: [],
      observation_filters: ["county_fips", "geo_id", "geo_level", "state_fips"],
      observation_routes: capabilityRoutes("census"),
    },
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      datasets: [],
      observation_filters: ["geo_id", "geo_level"],
      observation_routes: capabilityRoutes("pep"),
    },
    {
      source_code: "USDA_NASS",
      display_name: "USDA National Agricultural Statistics Service",
      route_segment: "usda-nass",
      served_by_neutral_routes: true,
      datasets: ["nass_crops_county"],
      observation_filters: ["domain_desc", "geo_id"],
      observation_routes: [
        ...neutralRoutes,
        { path: "/api/v1/usda-nass/observations", parameters: ["geo_id", "limit"] },
      ],
    },
    {
      source_code: "CDC",
      display_name: "Centers for Disease Control and Prevention",
      route_segment: "cdc",
      served_by_neutral_routes: true,
      datasets: ["cdc_places_county"],
      // No state_fips and no distribution route: the explorer must send
      // neither, and must not present a failed bins request as a fallback.
      observation_filters: [
        "adjustment_status",
        "geo_id",
        "geo_level",
        "stratum_id",
        "year_from",
        "year_to",
      ],
      observation_routes: neutralRoutes,
    },
  ],
};

// Provider-neutral envelope rows (NeutralObservation): period bounds rather
// than one date, `unit`, nested `dimensions`, and a suppressed value that
// must never render as a number.
const cdcRow = (stratumId, value, extra = {}) => ({
  metric_code: cdcMetric.metric_code,
  source_code: "CDC",
  geo_id: "state:55|county:025",
  geo_level: "COUNTY",
  value,
  value_status: value === null ? "suppressed" : "valid",
  unit: "percent",
  period_start: "2021-01-01",
  period_end: "2022-12-31",
  dimensions: { stratum_id: stratumId, adjustment_status: "age-adjusted" },
  ...extra,
});

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

// Census ACS published releases, as /observations/releases lists them:
// newest first, each with the identity `release=` accepts.
const acsReleases = [
  { release: "2023", as_of: "2024-01-01", observation_count: 3143 },
  { release: "2022", as_of: "2023-01-01", observation_count: 3142 },
];

// The same county as each release published it. Under scope=as_released
// both rows answer, so the geography carries one row per release.
const acsReleasedRow = (release, value) => ({
  ...county,
  release,
  as_of: release === "2023" ? "2024-01-01" : "2023-01-01",
  period_start: `${release}-01-01`,
  period_end: `${release}-12-31`,
  value,
});

async function installRoutes(
  page,
  { failLatest = false, neutralRequests = [], releaseRequests = [] } = {},
) {
  let tileRequests = 0;
  await page.route("**/api/v1/observations/releases?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    releaseRequests.push(Object.fromEntries(params));
    const items = params.get("metric_code")?.startsWith("ACS:") ? acsReleases : [];
    return route.fulfill({
      json: {
        metric_code: params.get("metric_code"),
        source_code: "CENSUS_ACS",
        total: items.length,
        limit: Number(params.get("limit") || 100),
        offset: 0,
        items,
      },
      headers: { "x-cache": "MISS" },
    });
  });
  await page.route("**/api/v1/observations?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    neutralRequests.push(Object.fromEntries(params));
    const metric = params.get("metric_code") || "";

    if (params.get("scope") === "as_released") {
      const pinned = params.get("release");
      const rows = [acsReleasedRow("2023", "561504"), acsReleasedRow("2022", "555000")];
      const items = metric.startsWith("ACS:")
        ? rows.filter((row) => !pinned || row.release === pinned)
        : [];
      return route.fulfill({
        json: {
          total: items.length,
          limit: Number(params.get("limit") || 100),
          offset: 0,
          scope: "as_released",
          release: pinned,
          metric_code: metric,
          source_code: "CENSUS_ACS",
          items,
        },
        headers: { "x-cache": "MISS" },
      });
    }

    // Latest scope. Every source now reads through this resource, so the
    // fixture dispatches on the metric's own published source prefix the way
    // the registry does. A single-geography request carries `geo_id`; a
    // cross-geography one carries `geo_level`.
    const geoId = params.get("geo_id");
    const answer = (items, sourceCode) =>
      route.fulfill({
        json: {
          total: items.length,
          limit: Number(params.get("limit") || 100),
          offset: 0,
          scope: params.get("scope") || "latest",
          metric_code: metric,
          source_code: sourceCode,
          items,
        },
        headers: { "x-cache": "MISS" },
      });

    if (metric.startsWith("CENSUS_PEP:")) {
      const pepRow = {
        ...county,
        metric_code: metric,
        source_code: "CENSUS_PEP",
        source: "CENSUS_PEP",
        dataset_code: "pep_cty_alldata",
        dataset: "pep_cty_alldata",
        value: "561800",
      };
      // PEP publishes no history for this geography in this fixture.
      return answer(geoId ? [] : [pepRow], "CENSUS_PEP");
    }

    if (metric.startsWith("ACS:")) {
      if (failLatest) {
        return route.fulfill({ status: 503, json: { detail: "fallback unavailable" } });
      }
      if (geoId) {
        return answer(
          [
            { ...county, metric_code: metric, observation_date: "2022-01-01", period: "2022", value: "555000" },
            { ...county, metric_code: metric },
          ],
          "CENSUS_ACS",
        );
      }
      // ACS1 publishes only counties above its population threshold, so this
      // selection legitimately answers with nothing.
      return answer(metric.includes(":acs1:") ? [] : [{ ...county, metric_code: metric }], "CENSUS_ACS");
    }

    const stratum = params.get("stratum_id");
    const rows = [cdcRow("overall", "32.4"), cdcRow("age_18_44", null)];
    const items = stratum ? rows.filter((row) => row.dimensions.stratum_id === stratum) : rows;
    return answer(items, "CDC");
  });
  await page.route("**/api/v1/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/v1/catalog/capabilities", (route) => route.fulfill({
    json: capabilities,
    headers: { "x-cache": "MISS" },
  }));
  await page.route("**/api/v1/catalog/metrics?*", (route) => {
    const sourceCode = new URL(route.request().url()).searchParams.get("source_code");
    const bySource = { CENSUS_PEP: [pepMetric], CDC: [cdcMetric] };
    const items = bySource[sourceCode] || metrics;
    return route.fulfill({
      json: { total: items.length, limit: 1000, offset: 0, items },
      headers: { "x-cache": "MISS" },
    });
  });
  await page.route("**/api/v1/catalog/geographies?*", (route) => {
    const level = new URL(route.request().url()).searchParams.get("geo_level");
    const items = level === "STATE"
      ? [{ geo_id: "state:55", geo_level: "STATE", state_fips: "55", state_name: "Wisconsin", latitude: 44.5, longitude: -89.5 }]
      : [{ geo_id: county.geo_id, geo_level: "COUNTY", state_fips: "55", county_fips: "025", state_name: "Wisconsin", county_name: "Dane County", latitude: 43.0667, longitude: -89.4 }];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
  await page.route("**/api/v1/census/observations/latest?*", (route) => {
    if (failLatest) return route.fulfill({ status: 503, json: { detail: "fallback unavailable" } });
    const metric = new URL(route.request().url()).searchParams.get("metric_code");
    const items = metric?.includes(":acs1:") ? [] : [{ ...county, metric_code: metric }];
    return route.fulfill({
      json: { total: items.length, limit: 4000, offset: 0, items },
      headers: { "x-cache": "MISS" },
    });
  });
  await page.route("**/api/v1/pep/observations/latest?*", (route) => {
    const metric = new URL(route.request().url()).searchParams.get("metric_code");
    const items = [{
      ...county,
      metric_code: metric,
      source_code: "CENSUS_PEP",
      source: "CENSUS_PEP",
      dataset_code: "pep_cty_alldata",
      dataset: "pep_cty_alldata",
      value: "561800",
    }];
    return route.fulfill({
      json: { total: items.length, limit: 4000, offset: 0, items },
      headers: { "x-cache": "MISS" },
    });
  });
  await page.route("**/api/v1/pep/observations/timeseries?*", (route) => route.fulfill({
    json: { total: 0, limit: 1000, offset: 0, items: [] },
  }));
  await page.route("**/api/v1/census/observations/timeseries?*", (route) => route.fulfill({
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
  await page.route("**/api/v1/distribution/bins?*", (route) => route.fulfill({
    json: {
      total: 1,
      bin_count: 1,
      min_value: 561504,
      max_value: 561504,
      items: [{ bin_index: 1, count: 1 }],
    },
  }));
  await page.route("**/tiles/catalog", (route) =>
    route.fulfill({
      // Martin's real catalog shape: sources sit under section keys rather
      // than at the top level (1.11.0, pinned in docker-compose.yml). The
      // flat `{counties:{}}` this previously mocked is a shape Martin does
      // not serve, which is how a broken discovery path stayed green.
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
  await expect(dashboard).toHaveAttribute("data-metric-count", "3");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-colored-values", "1");
  await expect(page.getByLabel("Choropleth value legend")).toContainText("API distribution");
  await expect(page.getByTestId("tiles-status")).toContainText("healthy_tile=true");
  expect(tileRequests()).toBeGreaterThan(0);

  await page.getByTestId("state-select").selectOption("55");
  await page.getByTestId("county-select").selectOption(county.geo_id);
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", county.geo_id);
  await expect(page.getByText("2 historical observations")).toBeVisible();
  // The selected geography is named in its own heading; the observation
  // table names it again in its Geo cell, so the assertion targets the heading.
  await expect(page.getByRole("heading", { name: "Dane County, Wisconsin" })).toBeVisible();

  // The URL reproduces the selected exploration state without navigation.
  await expect(page).toHaveURL(/metric=ACS%3Aacs5%3AB01003_001/);
  await expect(page).toHaveURL(/state=55/);
  await expect(page).toHaveURL(/geo=state%3A55%7Ccounty%3A025/);

  const map = page.getByTestId("map-canvas");
  await map.focus();
  await page.keyboard.press("Escape");
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", "");
  await page.keyboard.press("Enter");
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", county.geo_id);
});

test("source tabs derive from capability discovery and switch the explored source", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/explore");

  const dashboard = page.getByTestId("dashboard");
  // Every source whose declarations carry an access shape becomes a tab —
  // the source-scoped pair or the neutral /observations resource — and the
  // tab records which shape reaches it.
  await expect(dashboard).toHaveAttribute("data-source-count", "4");
  await expect(page.getByTestId("source-tab-census")).toHaveAttribute("aria-selected", "true");
  // Census declares its own route pair as well, and is still reached through
  // the neutral resource: the pair reads the legacy union views, which key
  // observations on that era's metric identity rather than the glossary
  // identity the catalog publishes, so it answers an empty page.
  await expect(page.getByTestId("source-tab-census"))
    .toHaveAttribute("data-access-shape", "neutral");
  await expect(page.getByTestId("source-tab-pep")).toBeVisible();
  await expect(page.getByTestId("source-tab-usda-nass"))
    .toHaveAttribute("data-access-shape", "neutral");
  await expect(page.getByTestId("source-tab-cdc"))
    .toHaveAttribute("data-access-shape", "neutral");

  await page.getByTestId("source-tab-pep").click();
  await expect(dashboard).toHaveAttribute("data-source-key", "pep");
  await expect(dashboard).toHaveAttribute("data-selected-metric", pepMetric.metric_code);
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await expect(page).toHaveURL(/source=pep/);

  await page.getByTestId("source-tab-census").click();
  await expect(dashboard).toHaveAttribute("data-selected-metric", "ACS:acs5:B01003_001");
  await expect(page).not.toHaveURL(/source=/);

  // A shared URL reproduces the non-default source directly.
  await page.goto("/explore?source=pep");
  await expect(dashboard).toHaveAttribute("data-source-key", "pep");
  await expect(dashboard).toHaveAttribute("data-selected-metric", pepMetric.metric_code);
});

test("ACS1 partial/no-data and API fallback states remain explicit", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-metric-count", "3");
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

test("a dispatch-shaped source is explored through the neutral resource", async ({ page }) => {
  const neutralRequests = [];
  await installRoutes(page, { neutralRequests });
  await page.goto("/explore");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-source-count", "4");

  await page.getByTestId("source-tab-cdc").click();
  await expect(dashboard).toHaveAttribute("data-access-shape", "neutral");
  await expect(dashboard).toHaveAttribute("data-selected-metric", cdcMetric.metric_code);
  await expect(dashboard).toHaveAttribute("data-observation-count", "2");

  // The request went to /observations with scope=latest, and carried only the
  // filters CDC declares — no state_fips, which would be a 422.
  const request = neutralRequests.at(-1);
  expect(request.metric_code).toBe(cdcMetric.metric_code);
  expect(request.scope).toBe("latest");
  expect(request.geo_level).toBe("COUNTY");
  expect(request.state_fips).toBeUndefined();

  // /distribution/bins is not declared for CDC, so it is not requested and
  // not reported as a failure.
  await expect(page.getByTestId("distribution-status")).toContainText("not declared for this source");

  // Two declared-dimension series per geography: the map declines to color
  // rather than keeping whichever row arrived last.
  await expect(dashboard).toHaveAttribute("data-stratified", "true");
  await expect(dashboard).toHaveAttribute("data-series-count", "2");
  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-colored-values", "0");
  await expect(page.getByTestId("stratification-note")).toContainText("stratum_id");

  // The published period range and the suppressed value stay exact.
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByRole("cell", { name: "2021-01-01 – 2022-12-31" }).first()).toBeVisible();
  await expect(page.getByRole("cell", { name: "suppressed" })).toBeVisible();

  // Narrowing the declared dimension filter resolves it to one series and
  // the map colors again.
  await page.getByTestId("dimension-select-stratum_id").selectOption("overall");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await expect(dashboard).toHaveAttribute("data-stratified", "false");
  expect(neutralRequests.at(-1).stratum_id).toBe("overall");
});

test("as-released exploration pins a published release and reproduces it", async ({ page }) => {
  const neutralRequests = [];
  const releaseRequests = [];
  await installRoutes(page, { neutralRequests, releaseRequests });
  await page.goto("/explore");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-selected-metric", "ACS:acs5:B01003_001");
  await expect(dashboard).toHaveAttribute("data-scope", "latest");

  // The release identities come from /observations/releases for the selected
  // metric; nothing infers them from a period or a vintage.
  await expect(page.getByTestId("releases-status")).toContainText("2 published releases");
  await expect(dashboard).toHaveAttribute("data-release-count", "2");
  expect(releaseRequests.at(-1).metric_code).toBe("ACS:acs5:B01003_001");

  // Reading every published release: the request moves to the neutral
  // resource with scope=as_released, and the geography now carries one row
  // per release, so the map declines to colour rather than showing whichever
  // release sorted last.
  await page.getByTestId("publication-select").selectOption("as_released");
  await expect(dashboard).toHaveAttribute("data-scope", "as_released");
  await expect(dashboard).toHaveAttribute("data-observation-count", "2");
  await expect(dashboard).toHaveAttribute("data-stratified", "true");
  await expect(page.getByTestId("map-canvas")).toHaveAttribute("data-colored-values", "0");
  await expect(page.getByTestId("stratification-note")).toContainText("release");
  await expect(page.getByTestId("as-released-note")).toContainText("every published release");

  let request = neutralRequests.at(-1);
  expect(request.scope).toBe("as_released");
  expect(request.release).toBeUndefined();
  expect(request.metric_code).toBe("ACS:acs5:B01003_001");

  // /distribution/bins declares no scope: its bins describe the latest
  // publication, so they are not requested for an as-released read and the
  // legend is not labelled with them.
  await expect(page.getByTestId("distribution-status"))
    .toContainText("API bins describe the latest publication only");

  // Pinning one release resolves it to a single series and reproduces the
  // analysis as that release published it.
  await page.getByTestId("publication-select").selectOption("release:2022");
  await expect(dashboard).toHaveAttribute("data-release", "2022");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");
  await expect(dashboard).toHaveAttribute("data-stratified", "false");
  request = neutralRequests.at(-1);
  expect(request.scope).toBe("as_released");
  expect(request.release).toBe("2022");

  // The pinned row is the value that release published, with its own release
  // identity visible in the table.
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByRole("cell", { name: "555000" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "2022", exact: true }).first()).toBeVisible();

  // The link carries the scope and the pin, so it reproduces the same
  // as-released analysis.
  await expect(page).toHaveURL(/scope=as_released/);
  await expect(page).toHaveURL(/release=2022/);

  const shared = await page.context().newPage();
  const sharedNeutral = [];
  await installRoutes(shared, { neutralRequests: sharedNeutral });
  await shared.goto("/explore?metric=ACS%3Aacs5%3AB01003_001&scope=as_released&release=2022");
  await expect(shared.getByTestId("dashboard")).toHaveAttribute("data-scope", "as_released");
  await expect(shared.getByTestId("dashboard")).toHaveAttribute("data-release", "2022");
  await expect(shared.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");
  expect(sharedNeutral.at(-1).release).toBe("2022");
  await shared.close();

  // Returning to the latest publication drops both from the request and the
  // link: `release` without `scope=as_released` is a 422 by contract.
  await page.getByTestId("publication-select").selectOption("latest");
  await expect(dashboard).toHaveAttribute("data-scope", "latest");
  await expect(dashboard).toHaveAttribute("data-release", "");
  await expect(page).not.toHaveURL(/release=/);
});

test("a national series gets the explicit non-spatial experience, not an empty map", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/explore");

  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-metric-count", "3");
  // The default county selection is mappable and offers every mode.
  await expect(dashboard).toHaveAttribute("data-map-supported", "true");
  await expect(page.getByRole("tab", { name: "map" })).toBeVisible();
  await expect(page.getByTestId("map-canvas")).toBeVisible();

  // A measure published only at the national grain resolves the geography
  // level to NATIONAL, and the tile boundary publishes no national geometry.
  await page.getByTestId("metric-select").selectOption("ACS:acs5:B01003_001_US");
  await expect(dashboard).toHaveAttribute("data-map-supported", "false");

  // The map is not rendered at all: an uncoloured map reads as "no data",
  // which is a different fact from "this series is not spatial".
  await expect(page.getByTestId("map-canvas")).toHaveCount(0);
  await expect(page.getByRole("tab", { name: "map" })).toHaveCount(0);
  await expect(dashboard).not.toHaveAttribute("data-view-modes", /(^|,)map(,|$)/);

  // The reason is stated, and the alternative paths to the same values are named.
  const note = page.getByTestId("non-spatial-note");
  await expect(note).toContainText("no national geometry");
  await expect(note).toContainText("observation table");
  await expect(page.getByTestId("unsupported-modes")).toContainText("map —");

  // Returning to a mappable measure brings the map back as the rendered
  // view without the user re-choosing it: the tab the user asked for is
  // remembered, so a mode that is briefly unavailable is not a lost one.
  await page.getByTestId("metric-select").selectOption("ACS:acs5:B01003_001");
  await expect(dashboard).toHaveAttribute("data-map-supported", "true");
  await expect(page.getByTestId("map-canvas")).toBeVisible();

  // Every non-spatial mode still answers, and the value is still retrievable.
  await page.getByTestId("metric-select").selectOption("ACS:acs5:B01003_001_US");
  await expect(page.getByRole("tab", { name: "table" })).toBeVisible();
  await expect(page.getByRole("tab", { name: "quality" })).toBeVisible();
  await expect(page.getByTestId("export-csv")).toBeEnabled();
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByRole("cell", { name: "561504" })).toBeVisible();

  // Quality is the measure's own published freshness and provenance.
  await page.getByRole("tab", { name: "quality" }).click();
  await expect(page.getByTestId("explorer-freshness")).toContainText("fresh");
  await expect(page.getByTestId("explorer-provenance")).toContainText("people");
});

test("the retired source dashboards land on the live explorer for their source", async ({ page }) => {
  await installRoutes(page);

  // These routes rendered demonstration dashboards whose charts, secondary
  // KPIs, ranked lists, and stylized maps were illustrative examples. They
  // are retired: the link stays valid and reaches the capability-driven
  // explorer for the same source, which answers from published data only.
  await page.goto("/census");
  await expect(page).toHaveURL(/\/explore/);
  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-source-key", "census");
  await expect(dashboard).toHaveAttribute("data-selected-metric", "ACS:acs5:B01003_001");
  // Nothing illustrative survives the retirement.
  await expect(page.getByTestId("demo-banner")).toHaveCount(0);
  // The site navigation, which the dashboards suppressed, is back.
  await expect(page.getByRole("navigation", { name: "Primary navigation" })).toBeVisible();

  // Each retired route keeps its own source identity through the redirect.
  await page.goto("/fred");
  await expect(page).toHaveURL(/source=fred/);
  await page.goto("/bls");
  await expect(page).toHaveURL(/source=bls/);
});

// Covers: WEB-022 — a saved view is a configuration on the account when the
// user is signed in, and a browser-local chart when they are not. The two are
// different facts with different consequences for whether the work still
// exists tomorrow, so the destination is stated before and after the save.
test("an explorer view saves to the account when signed in, and says so", async ({ page }) => {
  await installRoutes(page);

  const created = [];
  await page.route("**/api/v1/analysis-configurations", (route) => {
    const request = route.request();
    created.push({
      authorization: request.headers()["authorization"] || "",
      body: JSON.parse(request.postData() || "{}"),
      url: request.url(),
    });
    return route.fulfill({
      json: {
        configuration_id: 7,
        name: JSON.parse(request.postData() || "{}").name,
        version: 1,
        document: JSON.parse(request.postData() || "{}").document,
        validation: { valid: true, reasons: [] },
      },
    });
  });

  // The token the saved-analysis screen remembers for the tab.
  await page.addInitScript(() => {
    window.sessionStorage.setItem("economic-data-studio:api-token", "operator-token");
  });
  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");

  // The destination is stated before the click, not discovered after it.
  const save = page.getByTestId("save-view");
  await expect(save).toHaveAttribute("data-destination", "account");
  await save.click();

  const toast = page.getByTestId("save-toast");
  await expect(toast).toHaveAttribute("data-destination", "account");
  await expect(toast).toContainText("your account");

  expect(created).toHaveLength(1);
  // The token reaches the API only as a header, and never appears in the URL
  // it was sent to or in the address bar.
  expect(created[0].authorization).toBe("Bearer operator-token");
  expect(created[0].url).not.toContain("operator-token");
  expect(page.url()).not.toContain("operator-token");

  // The document is intent, not data: it names the measure and the filters
  // and carries no observation value.
  const document = created[0].body.document;
  expect(document.kind).toBe("observations");
  expect(document.metric_code).toBeTruthy();
  expect(JSON.stringify(document)).not.toContain("561504");
});

test("an explorer view saves in the browser when signed out, and names that limit", async ({
  page,
}) => {
  await installRoutes(page);
  let configurationWrites = 0;
  await page.route("**/api/v1/analysis-configurations", (route) => {
    configurationWrites += 1;
    return route.fulfill({ status: 401, json: { detail: "missing token" } });
  });

  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");

  const save = page.getByTestId("save-view");
  await expect(save).toHaveAttribute("data-destination", "browser");
  await save.click();

  const toast = page.getByTestId("save-toast");
  await expect(toast).toHaveAttribute("data-destination", "browser");
  // Not merely "Saved": a reader who is told only that would believe the work
  // survives the tab.
  await expect(toast).toContainText("this browser only");

  // Signed out, the account is never reached at all — an unauthenticated
  // write attempt would be a request the client knows the API must refuse.
  expect(configurationWrites).toBe(0);
  const stored = await page.evaluate(() =>
    window.localStorage.getItem("economic-data-studio:saved-charts:v1"),
  );
  expect(JSON.parse(stored || "[]")).toHaveLength(1);
});

test("a refused account save is reported and never falls back to the browser", async ({
  page,
}) => {
  await installRoutes(page);
  await page.route("**/api/v1/analysis-configurations", (route) =>
    route.fulfill({ status: 401, json: { detail: "token revoked" } }),
  );
  await page.addInitScript(() => {
    window.sessionStorage.setItem("economic-data-studio:api-token", "stale-token");
  });
  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");

  await page.getByTestId("save-view").click();

  const toast = page.getByTestId("save-toast");
  await expect(toast).toHaveAttribute("data-state", "unauthorized");
  await expect(toast).toContainText("not accepted");
  // The refusal is reported, not worked around: writing to the browser store
  // instead would tell the user their work is safe in a place they did not
  // choose and cannot see from their account.
  await expect(toast).not.toHaveAttribute("data-destination", "browser");
  const stored = await page.evaluate(() =>
    window.localStorage.getItem("economic-data-studio:saved-charts:v1"),
  );
  expect(JSON.parse(stored || "[]")).toHaveLength(0);
});

// Covers: WEB-002 — the view-level control offers only the grains the
// publisher declares for the selected measure.
//
// Offering all three unconditionally meant choosing one the measure does not
// publish sent a request the API answered with zero rows, after which a
// corrective effect snapped the selection back. The choice was offered,
// accepted, and silently discarded, which reads as the screen losing the
// click rather than as the measure not being published at that grain.
test("the view level offers only the grains the measure declares, and says why", async ({
  page,
}) => {
  const observationRequests = [];
  await installRoutes(page, { neutralRequests: observationRequests });
  await page.goto("/explore");
  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");

  const level = page.getByTestId("geo-level-select");

  // ACS county population declares STATE and COUNTY, so NATIONAL is absent
  // rather than offered and then revoked.
  await page.getByTestId("metric-select").selectOption("ACS:acs5:B01003_001");
  await expect(dashboard).toHaveAttribute("data-selected-metric", "ACS:acs5:B01003_001");
  await expect(level.locator("option")).toHaveCount(2);
  await expect(level.locator('option[value="STATE"]')).toHaveCount(1);
  await expect(level.locator('option[value="COUNTY"]')).toHaveCount(1);
  await expect(level.locator('option[value="NATIONAL"]')).toHaveCount(0);

  // The narrowing is attributed to the publisher, not left to be inferred
  // from a shorter list.
  await expect(page.getByTestId("geo-grain-note")).toContainText("published at");
  await expect(page.getByTestId("geo-grain-note")).toContainText("publisher");

  // No request is ever sent for a grain the measure does not declare.
  const requestedLevels = observationRequests.map((entry) => entry.geo_level).filter(Boolean);
  expect(requestedLevels).not.toContain("NATIONAL");
});
