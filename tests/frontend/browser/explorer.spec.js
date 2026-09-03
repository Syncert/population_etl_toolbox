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

    const stratum = params.get("stratum_id");
    const rows = [cdcRow("overall", "32.4"), cdcRow("age_18_44", null)];
    const items = stratum ? rows.filter((row) => row.dimensions.stratum_id === stratum) : rows;
    return route.fulfill({
      json: {
        total: items.length,
        limit: Number(params.get("limit") || 100),
        offset: 0,
        scope: params.get("scope") || "latest",
        metric_code: metric,
        source_code: "CDC",
        items,
      },
      headers: { "x-cache": "MISS" },
    });
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
  await expect(page.getByText("Dane County, Wisconsin")).toBeVisible();

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
  await expect(page.getByTestId("source-tab-census"))
    .toHaveAttribute("data-access-shape", "source-scoped");
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
