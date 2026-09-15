import { expect, test } from "../support/servedRequests.js";

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
  total: 4,
  items: [
    {
      source_code: "CENSUS_ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      publishes_aligned_reduction: true,
      observation_filters: ["geo_id", "geo_level", "state_fips"],
      observation_routes: [...sourceRoutes("census"), ...analysisRoutes],
    },
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      publishes_aligned_reduction: true,
      observation_filters: ["geo_id", "geo_level"],
      observation_routes: [...sourceRoutes("pep"), ...analysisRoutes],
    },
    {
      // The analysis routes decline this source by declared policy.
      source_code: "CDC",
      display_name: "Centers for Disease Control and Prevention",
      route_segment: "cdc",
      served_by_neutral_routes: true,
      publishes_aligned_reduction: false,
      observation_filters: ["geo_id", "stratum_id"],
      observation_routes: neutralRoutes,
    },
    {
      // Published at the agency grain, and declined by the analysis routes.
      // Both facts matter: which grains a pair offers is what its measures
      // publish, and is decided separately from whether the pair is
      // comparable (WEB-074).
      source_code: "FBI_UCR",
      display_name: "FBI Uniform Crime Reporting",
      route_segment: null,
      served_by_neutral_routes: true,
      publishes_aligned_reduction: false,
      observation_filters: ["geo_id", "subject_type"],
      observation_routes: neutralRoutes,
    },
  ],
};

const METRIC_A = "CENSUS_ACS:acs5:B01003_001";
const METRIC_B = "CENSUS_PEP:pep_cty_alldata:POPESTIMATE";
const METRIC_B2 = "CENSUS_PEP:pep_cty_alldata:BIRTHS";
const METRIC_CDC = "CDC:cdc_places_county:OBESITY";
const METRIC_FBI = "FBI_UCR:summarized:VIOLENT_CRIME";

// `/catalog/metrics` publishes `valid_geo_grains`, and the grain control is
// built from it: a fixture without them models a weaker contract than the one
// that ships, and the client then goes untested for the narrowing (WEB-043,
// WEB-074). Census PEP publishes places where Census ACS does not, which is
// why `PLACE` is reachable data on the analysis routes and why a pair of one
// each cannot be read there.
const metricsBySource = {
  CENSUS_ACS: [
    {
      metric_code: METRIC_A,
      metric_display_name: "Total population",
      source_code: "CENSUS_ACS",
      valid_geo_grains: ["NATIONAL", "STATE", "COUNTY"],
    },
  ],
  CENSUS_PEP: [
    {
      metric_code: METRIC_B,
      metric_display_name: "Resident population estimate",
      source_code: "CENSUS_PEP",
      valid_geo_grains: ["NATIONAL", "STATE", "COUNTY", "PLACE"],
    },
    {
      metric_code: METRIC_B2,
      metric_display_name: "Births",
      source_code: "CENSUS_PEP",
      valid_geo_grains: ["NATIONAL", "STATE", "COUNTY", "PLACE"],
    },
  ],
  CDC: [
    {
      metric_code: METRIC_CDC,
      metric_display_name: "Obesity prevalence",
      source_code: "CDC",
      valid_geo_grains: ["COUNTY"],
    },
  ],
  FBI_UCR: [
    {
      metric_code: METRIC_FBI,
      metric_display_name: "Violent crime, actual count",
      source_code: "FBI_UCR",
      valid_geo_grains: ["AGENCY"],
    },
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

async function installRoutes(
  page,
  { preflightRequests = [], comparisonRequests = [], truncate = false, payload = null } = {},
) {
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
    const declined = new Set([METRIC_CDC, METRIC_FBI]);
    const verdict =
      declined.has(params.get("metric_code_a")) || declined.has(params.get("metric_code_b"))
        ? blockedVerdict
        : comparableVerdict;
    return route.fulfill({ json: verdict });
  });
  await page.route("**/api/v1/comparison?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    comparisonRequests.push(Object.fromEntries(params));
    if (truncate) {
      // More aligned geographies than the client's page bound can reach:
      // one row per page against a total no number of pages will meet.
      const offset = Number(params.get("offset") || 0);
      return route.fulfill({
        json: {
          ...comparisonPayload,
          total: 9999,
          offset,
          items: [{ ...comparisonPayload.items[0], geo_id: `county:${offset}` }],
        },
      });
    }
    return route.fulfill({ json: payload || comparisonPayload });
  });

  // The Martin boundary. Its published fields are what decide whether this
  // comparison is spatial at all.
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
  await page.goto("/compare?a=CENSUS_ACS%3Aacs5%3AB01003_001&b=CENSUS_PEP%3Apep_cty_alldata%3APOPESTIMATE&source_a=census&source_b=pep");

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
  await expect(page).toHaveURL(/a=CENSUS_ACS%3Aacs5%3AB01003_001/);
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

test("the view levels are the ones both measures publish", async ({ page }) => {
  // Covers: WEB-074 — the control offered a hard-coded NATIONAL/STATE/COUNTY
  // and ignored `valid_geo_grains` on either side. Census PEP publishes
  // places and Census ACS does not, so the pair cannot be read at PLACE —
  // and a `?geo_level=PLACE` link put exactly that value into the selection,
  // leaving the control showing one grain while the request sent another.
  const comparisonRequests = [];
  await installRoutes(page, { comparisonRequests });
  await page.goto("/compare?geo_level=PLACE");

  const level = page.getByTestId("comparison-geo-level");
  await expect(level.locator("option")).toHaveText(["National", "State", "County"]);
  // Reported, not silently held: the link asked for a grain the pair does not
  // both publish, and the screen says so and shows what it can.
  await expect(page.getByTestId("comparison-grain-unavailable")).toContainText("Place");
  await expect(level).toHaveValue("COUNTY");
  await expect(page.getByTestId("comparison-grain-note")).toContainText(
    "not offered for the pair",
  );
  const grains = comparisonRequests.map((entry) => entry.geo_level).filter(Boolean);
  expect(grains.length).toBeGreaterThan(0);
  expect(grains).not.toContain("PLACE");
});

test("a pair that publishes places is compared at places, and a link says so", async ({
  page,
}) => {
  // Covers: WEB-074 — two Census PEP measures publish places, so PLACE is a
  // level the pair can be read at and a copied link reopens there.
  const comparisonRequests = [];
  await installRoutes(page, { comparisonRequests });
  await page.goto(
    `/compare?a=${encodeURIComponent(METRIC_B)}` +
      `&source_a=pep&b=${encodeURIComponent(METRIC_B2)}` +
      "&source_b=pep&geo_level=PLACE",
  );

  const level = page.getByTestId("comparison-geo-level");
  await expect(level.locator("option")).toHaveText([
    "National",
    "State",
    "County",
    "Place",
  ]);
  await expect(level).toHaveValue("PLACE");
  await expect(page.getByTestId("comparison-grain-unavailable")).toHaveCount(0);
  await expect(page).toHaveURL(/geo_level=PLACE/);
  await expect
    .poll(() =>
      comparisonRequests.filter((entry) => entry.geo_level === "PLACE").length,
    )
    .toBeGreaterThan(0);
});

test("an agency-grain pair is offered its own grain, declined or not", async ({ page }) => {
  // Covers: WEB-074 — which grains a pair offers is what its measures
  // publish; whether the pair is comparable is the API's separate verdict.
  // FBI UCR publishes agency-grain facts and the analysis routes decline it,
  // and both statements have to survive together.
  await installRoutes(page);
  await page.goto(
    `/compare?a=${encodeURIComponent(METRIC_FBI)}` +
      `&source_a=FBI_UCR&b=${encodeURIComponent(METRIC_FBI)}` +
      "&source_b=FBI_UCR&geo_level=AGENCY",
  );

  const level = page.getByTestId("comparison-geo-level");
  await expect(level.locator("option")).toHaveText(["Agency"]);
  await expect(level).toHaveValue("AGENCY");
  await expect(page.getByTestId("comparison-workspace")).toHaveAttribute(
    "data-comparable",
    "false",
  );
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

// Covers: WEB-022 — the comparison workspace saves through the same
// destination decision as the explorer, so the two screens cannot drift into
// different ideas of when a save reaches the account.
test("a comparison saves to the account when signed in, storing the pair and not the verdict", async ({
  page,
}) => {
  await installRoutes(page);

  const created = [];
  await page.route("**/api/v1/analysis-configurations", (route) => {
    const body = JSON.parse(route.request().postData() || "{}");
    created.push({ authorization: route.request().headers()["authorization"] || "", body });
    return route.fulfill({
      json: {
        configuration_id: 11,
        name: body.name,
        version: 1,
        document: body.document,
        validation: { valid: true, reasons: [] },
      },
    });
  });
  await page.addInitScript(() => {
    window.sessionStorage.setItem("economic-data-studio:api-token", "operator-token");
  });

  await page.goto(
    "/compare?a=CENSUS_ACS%3Aacs5%3AB01003_001&b=CENSUS_PEP%3Apep_cty_alldata%3APOPESTIMATE&source_a=census&source_b=pep",
  );
  const save = page.getByTestId("comparison-save");
  await expect(save).toBeEnabled();
  await expect(save).toHaveAttribute("data-destination", "account");
  await save.click();

  await expect(page.getByTestId("save-toast")).toHaveAttribute("data-destination", "account");
  expect(created).toHaveLength(1);
  expect(created[0].authorization).toBe("Bearer operator-token");

  // The configuration stores the pair and the geography. The verdict, the
  // derived fields, and the caveats stay the API's to publish: a stored copy
  // could outlive the compatibility rules that produced it.
  const document = created[0].body.document;
  expect(document.kind).toBe("comparison");
  expect(document.metric_code_a).toBeTruthy();
  expect(document.metric_code_b).toBeTruthy();
  expect(document).not.toHaveProperty("derivations");
  expect(document).not.toHaveProperty("caveats");
  expect(document).not.toHaveProperty("verdict");
});

test("a comparison too large for the page bound says so, and is not reported healthy", async ({
  page,
}) => {
  // Covers: WEB-039 — `/comparison` caps `limit` at 1000 and a national
  // county comparison aligns 3,144 geographies. One request held the first
  // thousand rows ordered by geo_id and reported them as `ok`, so a scatter
  // plot of alphabetically-first counties read as the comparison.
  const comparisonRequests = [];
  await installRoutes(page, { comparisonRequests, truncate: true });
  await page.goto(
    `/compare?metric_a=${encodeURIComponent(METRIC_A)}&metric_b=${encodeURIComponent(METRIC_B)}`,
  );

  const status = page.getByTestId("comparison-status");
  await expect(status).toContainText("the page bound cut the answer short");
  await expect(status).toContainText("of 9999 aligned geographies");
  // A partial answer is never green.
  await expect(status).toHaveClass(/pill bad/);

  // It paged rather than asking once, and each page asked for the next rows.
  const offsets = comparisonRequests.map((entry) => Number(entry.offset));
  expect(offsets.length).toBeGreaterThan(1);
  expect(offsets[0]).toBe(0);
  expect(offsets[1]).toBe(1);
});

test("the file of a page-bounded comparison says so in its own name", async ({
  page,
}) => {
  // Covers: WEB-067 — the pill said "the page bound cut the answer short";
  // the file said nothing, and the file is what a reader keeps. WEB-059 made
  // the explorer's export name its own shortfall for the same reason.
  await installRoutes(page, { truncate: true });
  await page.goto(
    `/compare?metric_a=${encodeURIComponent(METRIC_A)}&metric_b=${encodeURIComponent(METRIC_B)}`,
  );
  await expect(page.getByTestId("comparison-status")).toContainText(
    "the page bound cut the answer short",
  );

  // The export is never refused: a reader may want the rows they have.
  const exportButton = page.getByTestId("comparison-export");
  await expect(exportButton).toBeEnabled();
  const [download] = await Promise.all([
    page.waitForEvent("download"),
    exportButton.click(),
  ]);
  const name = download.suggestedFilename();
  expect(name).toContain("-partial-");
  expect(name).toContain("-of-9999.csv");
});

test("an aligned view says when a pair is not contemporaneous", async ({ page }) => {
  // Covers: WEB-049 — the route combines each side's own newest value rather
  // than aligning them to a shared period, and carries both periods so that
  // is visible. The table marked it; the scatter drew a pair four years apart
  // as a point like any other, and the map coloured it by a difference
  // computed across those years, with nothing on either panel saying so.
  const payload = {
    ...comparisonPayload,
    total: 3,
    items: [
      // Not contemporaneous.
      comparisonPayload.items[0],
      {
        ...comparisonPayload.items[0],
        geo_id: "state:55|county:009",
        county_name: "Brown County",
        period_a: "2023",
        period_b: "2023",
        value_a: 268740,
        value_b: 270000,
        difference: -1260,
        ratio: 0.99533,
      },
      comparisonPayload.items[1],
    ],
  };
  await installRoutes(page, { payload });
  await page.goto("/compare");

  const workspace = page.getByTestId("comparison-workspace");
  await expect(workspace).toHaveAttribute("data-plottable-points", "2");

  // The chart marks the pair, counts it, and keeps it: both values are
  // published, so dropping it would answer a narrower question.
  await expect(page.getByTestId("scatter-point-differing")).toHaveCount(1);
  await expect(page.getByTestId("scatter-point")).toHaveCount(1);
  const note = page.getByTestId("scatter-differing-periods");
  await expect(note).toContainText("1 of 2 plotted geographies pairs values");
  await expect(note).toContainText("different periods");

  // And the map, which colours one API-derived number per polygon, says how
  // many of those numbers span two publications.
  await expect(page.getByTestId("map-period-note")).toContainText(
    "1 of 2 coloured geographies combine values published for different periods",
  );
});

test("a comparison whose sides share a period says nothing extra", async ({ page }) => {
  // Covers: WEB-049 — the note is a fact about this answer, not a standing
  // disclaimer on every comparison.
  const contemporaneous = {
    ...comparisonPayload,
    items: comparisonPayload.items.map((row) => ({ ...row, period_b: row.period_a })),
  };
  await installRoutes(page, { payload: contemporaneous });
  await page.goto("/compare");

  await expect(page.getByTestId("comparison-map-panel")).toBeVisible();
  await expect(page.getByTestId("map-period-note")).toHaveCount(0);
  await expect(page.getByTestId("scatter-differing-periods")).toHaveCount(0);
});

test("the screen says what its geographies are an intersection of", async ({ page }) => {
  // Covers: WEB-050 — the route joins its two reduced sides on geography
  // identity with an inner join. "500 aligned geographies" reads as the
  // universe when it is 500 of 3,143, and the map and the scatter draw only
  // the intersection with nothing saying so.
  const narrowed = {
    ...comparisonPayload,
    total: 2,
    geographies_a: 3143,
    geographies_b: 2,
  };
  await installRoutes(page, { payload: narrowed });
  await page.goto("/compare");

  const note = page.getByTestId("comparison-coverage-note");
  await expect(note).toContainText("2 geographies are paired here");
  await expect(note).toContainText("publishes 3,143");
  await expect(note).toContainText("not in this comparison");
  // A different fact from the page-bound shortfall, which this comparison
  // does not have: the status stays what it was.
  await expect(page.getByTestId("comparison-status")).toContainText("aligned geographies");
});

test("a comparison that paired everything says nothing extra", async ({ page }) => {
  // Covers: WEB-050 — a fact about this answer, not a standing disclaimer.
  await installRoutes(page, {
    payload: { ...comparisonPayload, total: 2, geographies_a: 2, geographies_b: 2 },
  });
  await page.goto("/compare");

  await expect(page.getByTestId("comparison-table-panel")).toBeVisible();
  await expect(page.getByTestId("comparison-coverage-note")).toHaveCount(0);
});
