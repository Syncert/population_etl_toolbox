import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

// Covers: WEB-019 and WEB-020 — the comparison workspace in the browser. The declared
// compatibility verdict is presented before any comparison data is
// requested, a blocked pair is explained with alternatives and never
// queried, each side's published value and period survive into the table
// alongside the API-derived fields it labels as derived, and the link
// reproduces the pair without carrying a verdict. WEB-020 adds the aligned
// presentations: the scatter and the derived-value choropleth appear only
// where the comparison can answer them, and each names what it leaves out.

const MVT = Buffer.from(
  "GvEBCghjb3VudGllcxImEhAAAAEBAgIDAwQEBQUGBgcHGAMiEAm+FMQFGgDDBtQNAADEBg8aC2NvdW50eV9maXBzGgtjb3VudHlfbmFtZRoGZ2VvX2lkGglnZW9fbGV2ZWwaCGxhdGl0dWRlGglsb25naXR1ZGUaCnN0YXRlX2ZpcHMaCnN0YXRlX25hbWUiBQoDMDI1Ig0KC0RhbmUgQ291bnR5IhUKE3N0YXRlOjU1fGNvdW50eTowMjUiCAoGQ09VTlRZIgkZVFInoImIRUAiCRmamZmZmVlWwCIECgI1NSILCglXaXNjb25zaW4ogCB4Ag==",
  "base64",
);

const analysisRoutes = [
  {
    path: "/api/v1/comparison/preflight",
    parameters: ["metric_code_a", "metric_code_b"],
  },
  {
    path: "/api/v1/comparison",
    parameters: ["geo_level", "limit", "metric_code_a", "metric_code_b", "offset", "state_fips"],
  },
];

const neutralRoutes = [
  {
    path: "/api/v1/observations",
    parameters: ["geo_id", "geo_level", "limit", "metric_code", "release", "scope", "state_fips"],
  },
  { path: "/api/v1/observations/releases", parameters: ["limit", "metric_code", "offset"] },
];

const sourceRoutes = (segment) => [
  {
    path: `/api/v1/${segment}/observations/latest`,
    parameters: ["geo_level", "limit", "metric_code", "offset", "state_fips"],
  },
  {
    path: `/api/v1/${segment}/observations/timeseries`,
    parameters: ["end_date", "geo_id", "limit", "metric_code", "start_date"],
  },
  ...neutralRoutes,
];

const capabilities = {
  total: 3,
  items: [
    {
      source_code: "CENSUS_ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "geo_level", "state_fips"],
      observation_routes: [...sourceRoutes("census"), ...analysisRoutes],
    },
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "geo_level"],
      observation_routes: [...sourceRoutes("pep"), ...analysisRoutes],
    },
    {
      // The analysis routes decline this source by declared policy.
      source_code: "CDC",
      display_name: "Centers for Disease Control and Prevention",
      route_segment: "cdc",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "stratum_id"],
      observation_routes: neutralRoutes,
    },
  ],
};

const METRIC_A = "ACS:acs5:B01003_001";
const METRIC_B = "CENSUS_PEP:pep_cty_alldata:POPESTIMATE";
const METRIC_CDC = "CDC:cdc_places_county:OBESITY";

const metricsBySource = {
  CENSUS_ACS: [
    { metric_code: METRIC_A, metric_display_name: "Total population", source_code: "CENSUS_ACS" },
  ],
  CENSUS_PEP: [
    {
      metric_code: METRIC_B,
      metric_display_name: "Resident population estimate",
      source_code: "CENSUS_PEP",
    },
  ],
  CDC: [
    { metric_code: METRIC_CDC, metric_display_name: "Obesity prevalence", source_code: "CDC" },
  ],
};

// The served ComparisonPreflightResponse for a comparable pair: one rule the
// publication leaves unverifiable, which is a caveat rather than a block.
const comparableVerdict = {
  metric_code_a: METRIC_A,
  metric_code_b: METRIC_B,
  source_code_a: "CENSUS_ACS",
  source_code_b: "CENSUS_PEP",
  comparable: true,
  derivations: ["difference", "ratio"],
  rules: [
    {
      rule: "source_analysis_ready",
      status: "pass",
      reason: "both sources are served by the aligned analysis routes",
    },
    {
      rule: "units",
      status: "unknown",
      reason: "Census ACS publishes no units for measure A",
    },
    { rule: "time_grains", status: "pass", reason: "both publish ANNUAL" },
    { rule: "geo_grains", status: "pass", reason: "both publish COUNTY" },
  ],
  caveats: ["units could not be verified for measure A"],
};

const blockedVerdict = {
  metric_code_a: METRIC_CDC,
  metric_code_b: METRIC_B,
  source_code_a: "CDC",
  source_code_b: "CENSUS_PEP",
  comparable: false,
  derivations: [],
  rules: [
    {
      rule: "source_analysis_ready",
      status: "fail",
      reason:
        "measure A: source 'CDC' publishes stratified observations that an aligned one-value-per-geography analysis would silently collapse",
    },
    { rule: "time_grains", status: "pass", reason: "both publish ANNUAL" },
  ],
  caveats: [],
};

const comparisonPayload = {
  metric_code_a: METRIC_A,
  metric_code_b: METRIC_B,
  source_code_a: "CENSUS_ACS",
  source_code_b: "CENSUS_PEP",
  units_a: null,
  units_b: "people",
  derivations: ["difference", "ratio"],
  caveats: ["units could not be verified for measure A"],
  total: 2,
  limit: 1000,
  offset: 0,
  items: [
    {
      geo_id: "state:55|county:025",
      geo_level: "COUNTY",
      state_name: "Wisconsin",
      county_name: "Dane County",
      metric_code_a: METRIC_A,
      metric_code_b: METRIC_B,
      period_a: "2023",
      period_b: "2024",
      value_a: 561504,
      value_b: 568203,
      difference: -6699,
      ratio: 0.98821,
    },
    {
      geo_id: "state:55|county:001",
      geo_level: "COUNTY",
      state_name: "Wisconsin",
      county_name: "Adams County",
      metric_code_a: METRIC_A,
      metric_code_b: METRIC_B,
      period_a: "2023",
      period_b: "2023",
      value_a: null,
      value_b: 20567,
      difference: null,
      ratio: null,
    },
  ],
};

async function installRoutes(page, { preflightRequests = [], comparisonRequests = [] } = {}) {
  await page.route("**/api/v1/catalog/capabilities", (route) =>
    route.fulfill({ json: capabilities }),
  );
  await page.route("**/api/v1/catalog/metrics?*", (route) => {
    const sourceCode = new URL(route.request().url()).searchParams.get("source_code");
    const items = metricsBySource[sourceCode] || [];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
  await page.route("**/api/v1/catalog/geographies?*", (route) =>
    route.fulfill({
      json: {
        total: 1,
        limit: 1000,
        offset: 0,
        items: [
          {
            geo_id: "state:55",
            geo_level: "STATE",
            state_fips: "55",
            state_name: "Wisconsin",
          },
        ],
      },
    }),
  );
  await page.route("**/api/v1/comparison/preflight?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    preflightRequests.push(Object.fromEntries(params));
    const verdict =
      params.get("metric_code_a") === METRIC_CDC || params.get("metric_code_b") === METRIC_CDC
        ? blockedVerdict
        : comparableVerdict;
    return route.fulfill({ json: verdict });
  });
  await page.route("**/api/v1/comparison?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    comparisonRequests.push(Object.fromEntries(params));
    return route.fulfill({ json: comparisonPayload });
  });

  // The Martin boundary. Its published fields are what decide whether this
  // comparison is spatial at all.
  await page.route("**/tiles/catalog", (route) => route.fulfill({ json: { counties: {} } }));
  await page.route(/\/tiles\/counties$/, (route) =>
    route.fulfill({
      json: {
        name: "counties",
        tiles: ["http://internal-martin:3000/counties/{z}/{x}/{y}"],
        vector_layers: [
          {
            id: "counties",
            fields: {
              geo_id: "String",
              geo_level: "String",
              state_fips: "String",
              county_fips: "String",
              county_name: "String",
            },
          },
        ],
      },
    }),
  );
  await page.route(/\/tiles\/counties\/\d+\/\d+\/\d+(?:\.pbf)?$/, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/vnd.mapbox-vector-tile",
      body: MVT,
    }),
  );
  await page.route("**/tiles/counties/**", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/vnd.mapbox-vector-tile",
      body: MVT,
    }),
  );
}

test("a comparable pair is preflighted, then compared with inputs and derivations distinct", async ({
  page,
}) => {
  const preflightRequests = [];
  const comparisonRequests = [];
  await installRoutes(page, { preflightRequests, comparisonRequests });
  await page.goto("/compare?a=ACS%3Aacs5%3AB01003_001&b=CENSUS_PEP%3Apep_cty_alldata%3APOPESTIMATE&source_a=census&source_b=pep");

  const workspace = page.getByTestId("comparison-workspace");
  await expect(workspace).toHaveAttribute("data-metric-a", METRIC_A);
  await expect(workspace).toHaveAttribute("data-metric-b", METRIC_B);
  await expect(workspace).toHaveAttribute("data-comparable", "true");

  // The verdict is asked before any comparison data moves.
  expect(preflightRequests.at(-1)).toEqual({
    metric_code_a: METRIC_A,
    metric_code_b: METRIC_B,
  });

  // A rule the publication could not verify is a caution, never a pass:
  // the pill stays short of "ok" and the reason is shown as published.
  await expect(page.getByTestId("preflight-status")).toContainText("could not be verified");
  await expect(page.getByTestId("rule-units")).toContainText("unknown");
  await expect(page.getByTestId("rule-units")).toContainText("publishes no units");
  await expect(page.getByTestId("verdict-caveats")).toContainText("units could not be verified");

  // Only then is the comparison requested, scoped as selected.
  const request = comparisonRequests.at(-1);
  expect(request.metric_code_a).toBe(METRIC_A);
  expect(request.geo_level).toBe("COUNTY");
  await expect(workspace).toHaveAttribute("data-row-count", "2");

  // Each side keeps its own identity, published value, and period.
  const table = page.getByTestId("comparison-table-panel");
  await expect(table.getByRole("columnheader", { name: METRIC_A, exact: true })).toBeVisible();
  await expect(table.getByRole("columnheader", { name: METRIC_B, exact: true })).toBeVisible();
  await expect(
    table.getByRole("columnheader", { name: `${METRIC_A} period`, exact: true }),
  ).toBeVisible();
  await expect(table.getByRole("cell", { name: "561,504" })).toBeVisible();
  await expect(table.getByRole("cell", { name: "568,203" })).toBeVisible();

  // The API-derived fields are labelled derived wherever they appear.
  await expect(
    table.getByRole("columnheader", { name: "difference (API-derived)" }),
  ).toBeVisible();
  await expect(table.getByRole("columnheader", { name: "ratio (API-derived)" })).toBeVisible();
  await expect(page.getByTestId("derived-note")).toContainText("API-derived, not published");

  // Differing as-of periods are stated, not implied away: the API combines
  // each side's own newest value rather than aligning them.
  await expect(page.getByTestId("periods-differ").first()).toContainText("Different periods");

  // A side that published nothing is never rendered as zero.
  await expect(table.getByRole("cell", { name: "Not published" }).first()).toBeVisible();
  await expect(table.getByRole("cell", { name: "0", exact: true })).toHaveCount(0);
});

test("a pair the declared policy blocks is explained and never queried", async ({ page }) => {
  const preflightRequests = [];
  const comparisonRequests = [];
  await installRoutes(page, { preflightRequests, comparisonRequests });
  await page.goto("/compare");

  const workspace = page.getByTestId("comparison-workspace");
  await expect(workspace).toHaveAttribute("data-comparable", "true");

  // The default pair is comparable, so it was compared. Everything after
  // this point must add no further comparison request.
  const requestsBeforeSwitch = comparisonRequests.length;
  expect(requestsBeforeSwitch).toBeGreaterThan(0);

  // Switching one side to a source the analysis routes decline.
  await page.getByTestId("comparison-source-a").selectOption("cdc");
  await expect(workspace).toHaveAttribute("data-metric-a", METRIC_CDC);
  await expect(workspace).toHaveAttribute("data-comparable", "false");
  await expect(workspace).toHaveAttribute("data-blocking-rules", "source_analysis_ready");

  // The verdict reads as a failure-shaped state, never as healthy.
  await expect(page.getByTestId("preflight-status")).toContainText("not comparable");
  await expect(page.getByTestId("rule-source_analysis_ready")).toContainText("fail");
  await expect(page.getByTestId("rule-source_analysis_ready")).toContainText(
    "silently collapse",
  );

  // No comparison data was requested for the blocked pair: /comparison
  // answers it with a 422, and asking would turn a stated explanation into
  // a request failure.
  expect(comparisonRequests).toHaveLength(requestsBeforeSwitch);
  await expect(page.getByTestId("comparison-status")).toContainText("not requested");
  await expect(page.getByTestId("comparison-table-panel")).toHaveCount(0);

  // The reader is given actionable alternatives and a way to each measure.
  const explanation = page.getByTestId("incompatible-explanation");
  await expect(explanation).toContainText("Explore each measure on its own");
  await expect(explanation).toContainText("stratified");
  await expect(page.getByTestId("explore-a")).toHaveAttribute("href", /source=cdc/);
  await expect(page.getByTestId("explore-b")).toHaveAttribute("href", /source=pep/);

  // A blocked pair is not saveable as an analysis, and the reproducible
  // request names the preflight rather than a comparison never made.
  await expect(page.getByTestId("comparison-save")).toBeDisabled();
  await expect(page.getByTestId("comparison-api-query")).toContainText("/comparison/preflight");

  // The source picker says which sources the analysis routes have declined.
  await expect(page.getByTestId("comparison-source-a")).toContainText(
    "analysis routes not declared",
  );
});

test("the comparison link reproduces the pair and carries no verdict", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/compare");

  const workspace = page.getByTestId("comparison-workspace");
  await expect(workspace).toHaveAttribute("data-comparable", "true");
  await expect(page).toHaveURL(/a=ACS%3Aacs5%3AB01003_001/);
  await expect(page).toHaveURL(/b=CENSUS_PEP/);
  // The verdict belongs to the API and is re-asked on open, so a link can
  // never reproduce a stale "comparable".
  await expect(page).not.toHaveURL(/comparable/);

  await page.getByTestId("comparison-geo-level").selectOption("STATE");
  await expect(page).toHaveURL(/geo_level=STATE/);

  const reopened = await page.context().newPage();
  const preflightRequests = [];
  await installRoutes(reopened, { preflightRequests });
  await reopened.goto(page.url().replace(/^https?:\/\/[^/]+/, ""));
  await expect(reopened.getByTestId("comparison-workspace")).toHaveAttribute(
    "data-metric-a",
    METRIC_A,
  );
  await expect(reopened.getByTestId("comparison-geo-level")).toHaveValue("STATE");
  // Reopening re-asks the verdict rather than trusting the link.
  expect(preflightRequests.length).toBeGreaterThan(0);
  await reopened.close();
});

test("the aligned presentations appear only where the comparison can answer them", async ({
  page,
}) => {
  await installRoutes(page);
  await page.goto("/compare");

  const workspace = page.getByTestId("comparison-workspace");
  await expect(workspace).toHaveAttribute("data-comparable", "true");
  await expect(workspace).toHaveAttribute("data-row-count", "2");

  // Only one of the two geographies published a usable value on both sides,
  // so there is no pair to plot: one point states nothing about how two
  // measures relate across places.
  await expect(workspace).toHaveAttribute("data-plottable-points", "1");
  await expect(page.getByTestId("comparison-chart-panel")).toHaveCount(0);
  await expect(page.getByTestId("comparison-unsupported-modes")).toContainText(
    "fewer than two geographies",
  );

  // The map does answer: the boundary publishes county geometry and the
  // response named a derived field to colour.
  await expect(page.getByTestId("comparison-map-panel")).toBeVisible();
  await expect(page.getByTestId("map-derived-note")).toContainText("difference");
  await expect(page.getByTestId("map-derived-note")).toContainText(
    "not a value either source published",
  );
  const map = page.getByTestId("comparison-map");
  await expect(map).toHaveAttribute("data-map-ready", "true");
  // Exactly one geography could be coloured; the one missing a side stays
  // uncoloured rather than being coloured as zero.
  await expect(map).toHaveAttribute("data-colored-values", "1");
  await expect(page.getByLabel("difference · API-derived legend")).toBeVisible();

  // A national comparison has no geometry to draw at all, and says so
  // instead of rendering an empty map.
  await page.getByTestId("comparison-geo-level").selectOption("NATIONAL");
  await expect(page.getByTestId("comparison-map-panel")).toHaveCount(0);
  await expect(page.getByTestId("comparison-unsupported-modes")).toContainText(
    "no national geometry",
  );
  // The table and export still answer: the values are there, only the map
  // and the plot are not.
  await expect(page.getByTestId("comparison-table-panel")).toBeVisible();
  await expect(page.getByTestId("comparison-export")).toBeEnabled();
});

test("a blocked pair presents no aligned view at all", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/compare");
  await expect(page.getByTestId("comparison-workspace")).toHaveAttribute(
    "data-comparable",
    "true",
  );

  await page.getByTestId("comparison-source-a").selectOption("cdc");
  await expect(page.getByTestId("comparison-workspace")).toHaveAttribute(
    "data-comparable",
    "false",
  );
  await expect(page.getByTestId("comparison-workspace")).toHaveAttribute("data-view-modes", "");
  await expect(page.getByTestId("comparison-map-panel")).toHaveCount(0);
  await expect(page.getByTestId("comparison-chart-panel")).toHaveCount(0);
  await expect(page.getByTestId("comparison-table-panel")).toHaveCount(0);
  await expect(page.getByTestId("comparison-export")).toBeDisabled();
});
