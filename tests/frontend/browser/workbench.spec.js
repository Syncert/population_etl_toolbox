import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-086 — the workbench in the browser. Two measures from two
// sources at two grains join one chart; the legend names each one's
// publisher, measure, grain and geography; two published units become two
// axes with the note that their scales differ; a period that published no
// value is counted rather than drawn; a stratified source is refused until
// its declared dimension is pinned; and the address bar carries the
// composition so the same chart reopens from the link.

const NEUTRAL_ROUTES = [
  {
    path: "/api/v1/observations",
    parameters: [
      "geo_id",
      "geo_level",
      "limit",
      "metric_code",
      "newest_release_per_period",
      "offset",
      "release",
      "scope",
      "state_fips",
    ],
  },
  { path: "/api/v1/observations/releases", parameters: ["limit", "metric_code", "offset"] },
];

const capabilities = {
  total: 3,
  items: [
    {
      source_code: "FRED",
      display_name: "Federal Reserve Economic Data",
      route_segment: "fred",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "geo_level", "state_fips"],
      observation_routes: NEUTRAL_ROUTES,
    },
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "geo_level", "state_fips"],
      observation_routes: NEUTRAL_ROUTES,
    },
    {
      source_code: "CDC",
      display_name: "Centers for Disease Control and Prevention",
      route_segment: "cdc",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "stratum_id"],
      observation_routes: NEUTRAL_ROUTES,
    },
  ],
};

const METRIC_FRED = "FRED:UNRATE";
const METRIC_PEP = "CENSUS_PEP:POP";
const METRIC_CDC = "CDC:cdi:X:crude";

const metricsBySource = {
  FRED: [
    {
      metric_code: METRIC_FRED,
      metric_display_name: "Unemployment rate",
      source_code: "FRED",
      units: "Percent",
      valid_geo_grains: ["NATIONAL"],
      valid_time_grains: ["MONTHLY"],
    },
  ],
  CENSUS_PEP: [
    {
      metric_code: METRIC_PEP,
      metric_display_name: "Resident population",
      source_code: "CENSUS_PEP",
      units: "People",
      valid_geo_grains: ["STATE"],
      valid_time_grains: ["ANNUAL"],
    },
  ],
  CDC: [
    {
      metric_code: METRIC_CDC,
      metric_display_name: "Chronic disease indicator",
      source_code: "CDC",
      units: "percent",
      valid_geo_grains: ["STATE"],
      valid_time_grains: ["ANNUAL"],
    },
  ],
};

function observation(metricCode, periodStart, value, release) {
  return {
    metric_code: metricCode,
    geo_id: metricCode === METRIC_FRED ? "NATIONAL" : "state:55",
    geo_level: metricCode === METRIC_FRED ? "NATIONAL" : "STATE",
    period_start: periodStart,
    period_end: periodStart,
    value,
    release,
    unit: metricCode === METRIC_FRED ? "Percent" : "People",
  };
}

const observationsByMetric = {
  [METRIC_FRED]: [
    observation(METRIC_FRED, "2021-01-01", "6.4", "2024-01-05"),
    observation(METRIC_FRED, "2022-01-01", "4.0", "2024-01-05"),
    observation(METRIC_FRED, "2023-01-01", "3.6", "2024-01-05"),
  ],
  [METRIC_PEP]: [
    observation(METRIC_PEP, "2021-01-01", "5895908", "v2023"),
    // A period the source published without a number: counted, never a zero.
    observation(METRIC_PEP, "2022-01-01", null, "v2023"),
    observation(METRIC_PEP, "2023-01-01", "5910955", "v2023"),
  ],
  [METRIC_CDC]: [],
};

async function installRoutes(page) {
  await page.route("**/api/v1/catalog/capabilities", (route) =>
    route.fulfill({ json: capabilities }),
  );
  await page.route("**/api/v1/catalog/metrics?*", (route) => {
    const sourceCode = new URL(route.request().url()).searchParams.get("source_code");
    const items = metricsBySource[sourceCode] || [];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
  // One measure by code: what a restored composition reads for the series
  // whose source is not the one the picker happens to be on.
  await page.route(/\/api\/v1\/catalog\/metrics\/[^?]+$/, (route) => {
    const code = decodeURIComponent(
      new URL(route.request().url()).pathname.split("/").pop(),
    );
    const metric = Object.values(metricsBySource)
      .flat()
      .find((entry) => entry.metric_code === code);
    return metric
      ? route.fulfill({ json: metric })
      : route.fulfill({ status: 404, json: { detail: "metric_code not found" } });
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
  await page.route("**/api/v1/observations?*", (route) => {
    const metricCode = new URL(route.request().url()).searchParams.get("metric_code");
    const items = observationsByMetric[metricCode] || [];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
}

async function addSeries(page, { source, metric, grain, state, geography }) {
  await page.getByTestId("workbench-source").selectOption(source);
  await expect(page.getByTestId("workbench-metric")).toContainText(metric.label);
  await page.getByTestId("workbench-metric").selectOption(metric.code);
  await page.getByTestId("workbench-grain").selectOption(grain);
  if (state) {
    await page.getByTestId("workbench-state").selectOption(state);
  }
  if (geography) {
    await expect(page.getByTestId("workbench-geography")).toBeEnabled();
    await page.getByTestId("workbench-geography").selectOption(geography);
  }
  await page.getByTestId("workbench-add-series").click();
}

test("two measures from two sources at two grains join one chart", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/workbench");

  await expect(page.getByTestId("workbench-sources-status")).toContainText("3 published");

  await addSeries(page, {
    source: "fred",
    metric: { code: METRIC_FRED, label: "Unemployment rate" },
    grain: "NATIONAL",
  });
  await addSeries(page, {
    source: "pep",
    metric: { code: METRIC_PEP, label: "Resident population" },
    grain: "STATE",
    geography: "state:55",
  });

  await expect(page.locator("[data-testid='workbench']")).toHaveAttribute(
    "data-series-count",
    "2",
  );

  // The legend names each series' publisher, measure, grain and geography.
  const legend = page.getByTestId("workbench-legend");
  await expect(legend).toContainText("FRED");
  await expect(legend).toContainText("Unemployment rate");
  await expect(legend).toContainText("CENSUS_PEP");
  await expect(legend).toContainText("Resident population");
  await expect(legend).toContainText("Wisconsin");

  // Two published units, two axes, and a note that the scales are separate.
  const axes = page.getByTestId("workbench-axis");
  await expect(axes).toHaveCount(2);
  await expect(page.getByTestId("workbench-axis-note")).toContainText(
    "relative heights carry no meaning",
  );

  // The period Census PEP published without a number is counted, not drawn.
  await expect(page.getByTestId("legend-dropped-periods")).toContainText(
    "1 period published no value",
  );

  // And every plotted value is readable without the chart.
  const table = page.getByTestId("workbench-table");
  await expect(table.locator("tbody tr")).toHaveCount(5);
});

test("a stratified source is refused until its declared dimension is pinned", async ({
  page,
}) => {
  await installRoutes(page);
  await page.goto("/workbench");

  await page.getByTestId("workbench-source").selectOption("cdc");
  await page.getByTestId("workbench-metric").selectOption(METRIC_CDC);
  await page.getByTestId("workbench-grain").selectOption("STATE");
  await page.getByTestId("workbench-geography").selectOption("state:55");

  await expect(page.getByTestId("workbench-admission-reason")).toContainText("stratum_id");
  await expect(page.getByTestId("workbench-add-series")).toBeDisabled();

  await page.getByTestId("workbench-dimension-stratum_id").fill("OVR");
  await expect(page.getByTestId("workbench-add-series")).toBeEnabled();
  await page.getByTestId("workbench-add-series").click();
  await expect(page.locator("[data-testid='workbench']")).toHaveAttribute(
    "data-series-count",
    "1",
  );
});

test("the address carries the composition, and the link reopens it", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/workbench");

  await addSeries(page, {
    source: "fred",
    metric: { code: METRIC_FRED, label: "Unemployment rate" },
    grain: "NATIONAL",
  });

  await expect(page).toHaveURL(/s=/);
  const shared = page.url();

  await page.goto(shared);
  await expect(page.locator("[data-testid='workbench']")).toHaveAttribute(
    "data-series-count",
    "1",
  );
  await expect(page.getByTestId("workbench-legend")).toContainText("Unemployment rate");
});

test("an unanswerable presentation is listed with its reason, not hidden", async ({
  page,
}) => {
  await installRoutes(page);
  await page.goto("/workbench");

  await addSeries(page, {
    source: "fred",
    metric: { code: METRIC_FRED, label: "Unemployment rate" },
    grain: "NATIONAL",
  });

  const withheld = page.getByTestId("workbench-unavailable-presentations");
  await expect(withheld).toContainText("Scatter");
  await expect(withheld).toContainText("Correlation");
  await expect(page.getByTestId("workbench-presentation-line")).toBeVisible();
  await expect(page.getByTestId("workbench-presentation-bar")).toBeVisible();
});

// --- WB-2: the cross-sectional pair and the matrix heatmap -----------------
//
// Covers: WEB-090 — the workbench reads a comparable pair at the shared
// grain, refuses an incomparable one without issuing a single `/comparison`
// request, ranks by either side, and lays one measure out as geographies by
// periods with the cells that published no value hatched and counted.

const ANALYSIS_ROUTES = [
  {
    path: "/api/v1/comparison/preflight",
    parameters: ["metric_code_a", "metric_code_b"],
  },
  {
    path: "/api/v1/comparison",
    parameters: [
      "geo_level",
      "limit",
      "metric_code_a",
      "metric_code_b",
      "offset",
      "state_fips",
    ],
  },
];

const METRIC_ACS = "CENSUS_ACS:acs5:B01003_001";
const METRIC_PEP_STATE = "CENSUS_PEP:pep_state:POPESTIMATE";

const alignedCapabilities = {
  total: 2,
  items: [
    {
      source_code: "CENSUS_ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "geo_level", "state_fips"],
      observation_routes: [...NEUTRAL_ROUTES, ...ANALYSIS_ROUTES],
    },
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "geo_level", "state_fips"],
      observation_routes: [...NEUTRAL_ROUTES, ...ANALYSIS_ROUTES],
    },
  ],
};

const alignedMetrics = {
  CENSUS_ACS: [
    {
      metric_code: METRIC_ACS,
      metric_display_name: "Total population",
      source_code: "CENSUS_ACS",
      units: "People",
      valid_geo_grains: ["STATE", "COUNTY"],
      valid_time_grains: ["ANNUAL"],
    },
  ],
  CENSUS_PEP: [
    {
      metric_code: METRIC_PEP_STATE,
      metric_display_name: "Resident population estimate",
      source_code: "CENSUS_PEP",
      units: "People",
      valid_geo_grains: ["STATE"],
      valid_time_grains: ["ANNUAL"],
    },
  ],
};

const comparableVerdict = {
  metric_code_a: METRIC_ACS,
  metric_code_b: METRIC_PEP_STATE,
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
    { rule: "units", status: "pass", reason: "both metrics publish 'People'" },
    { rule: "time_grains", status: "pass", reason: "both publish ANNUAL" },
    { rule: "geo_grains", status: "pass", reason: "shared geography grains: STATE" },
    {
      rule: "aggregation",
      status: "unknown",
      reason:
        "aggregation characteristics are not fully published; do not sum derived values across geographies",
    },
  ],
  caveats: [
    "aggregation characteristics are not fully published; do not sum derived values across geographies",
  ],
};

const alignedComparison = {
  metric_code_a: METRIC_ACS,
  metric_code_b: METRIC_PEP_STATE,
  source_code_a: "CENSUS_ACS",
  source_code_b: "CENSUS_PEP",
  units_a: "People",
  units_b: "People",
  derivations: ["difference", "ratio"],
  caveats: comparableVerdict.caveats,
  total: 3,
  // Each side publishes more than the pairing: the coverage note must say so.
  geographies_a: 52,
  geographies_b: 51,
  limit: 1000,
  offset: 0,
  items: [
    {
      geo_id: "state:06",
      geo_level: "STATE",
      state_fips: "06",
      state_name: "California",
      metric_code_a: METRIC_ACS,
      metric_code_b: METRIC_PEP_STATE,
      period_a: "2023",
      period_b: "2023",
      value_a: 39000000,
      value_b: 39100000,
      difference: -100000,
      ratio: 0.997,
    },
    {
      geo_id: "state:36",
      geo_level: "STATE",
      state_fips: "36",
      state_name: "New York",
      metric_code_a: METRIC_ACS,
      metric_code_b: METRIC_PEP_STATE,
      period_a: "2023",
      period_b: "2022",
      value_a: 19600000,
      value_b: 19700000,
      difference: -100000,
      ratio: 0.995,
    },
    {
      // A geography one side published no number for: not a bar, and counted.
      geo_id: "state:55",
      geo_level: "STATE",
      state_fips: "55",
      state_name: "Wisconsin",
      metric_code_a: METRIC_ACS,
      metric_code_b: METRIC_PEP_STATE,
      period_a: "2023",
      period_b: "2023",
      value_a: null,
      value_b: 5900000,
      difference: null,
      ratio: null,
    },
  ],
};

const alignedObservations = {
  [METRIC_ACS]: [
    {
      metric_code: METRIC_ACS,
      geo_id: "state:06",
      geo_level: "STATE",
      period_start: "2022",
      period_end: "2022",
      value: "38900000",
      release: "acs5:2022",
    },
    {
      metric_code: METRIC_ACS,
      geo_id: "state:06",
      geo_level: "STATE",
      period_start: "2023",
      period_end: "2023",
      value: "39000000",
      release: "acs5:2023",
    },
    {
      metric_code: METRIC_ACS,
      geo_id: "state:36",
      geo_level: "STATE",
      period_start: "2022",
      period_end: "2022",
      value: "19700000",
      release: "acs5:2022",
    },
    {
      // Published without a number, with the source's own reason.
      metric_code: METRIC_ACS,
      geo_id: "state:36",
      geo_level: "STATE",
      period_start: "2023",
      period_end: "2023",
      value: null,
      value_status: "suppressed",
      release: "acs5:2023",
    },
  ],
  [METRIC_PEP_STATE]: [
    {
      metric_code: METRIC_PEP_STATE,
      geo_id: "state:06",
      geo_level: "STATE",
      period_start: "2023",
      period_end: "2023",
      value: "39100000",
      release: "v2023",
    },
  ],
};

async function installAlignedRoutes(page, { preflight = comparableVerdict } = {}) {
  await page.route("**/api/v1/catalog/capabilities", (route) =>
    route.fulfill({ json: alignedCapabilities }),
  );
  await page.route("**/api/v1/catalog/metrics?*", (route) => {
    const sourceCode = new URL(route.request().url()).searchParams.get("source_code");
    const items = alignedMetrics[sourceCode] || [];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
  await page.route(/\/api\/v1\/catalog\/metrics\/[^?]+$/, (route) => {
    const code = decodeURIComponent(
      new URL(route.request().url()).pathname.split("/").pop(),
    );
    const metric = Object.values(alignedMetrics)
      .flat()
      .find((entry) => entry.metric_code === code);
    return metric
      ? route.fulfill({ json: metric })
      : route.fulfill({ status: 404, json: { detail: "metric_code not found" } });
  });
  await page.route("**/api/v1/catalog/geographies?*", (route) =>
    route.fulfill({
      json: {
        total: 2,
        limit: 1000,
        offset: 0,
        items: [
          {
            geo_id: "state:06",
            geo_level: "STATE",
            state_fips: "06",
            state_name: "California",
          },
          {
            geo_id: "state:36",
            geo_level: "STATE",
            state_fips: "36",
            state_name: "New York",
          },
        ],
      },
    }),
  );
  await page.route("**/api/v1/observations?*", (route) => {
    const metricCode = new URL(route.request().url()).searchParams.get("metric_code");
    const items = alignedObservations[metricCode] || [];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
  await page.route("**/api/v1/comparison/preflight?*", (route) =>
    route.fulfill({ json: preflight }),
  );
  await page.route("**/api/v1/comparison?*", (route) =>
    route.fulfill({ json: alignedComparison }),
  );
}

async function addAlignedPair(page) {
  await page.getByTestId("workbench-source").selectOption("census");
  await page.getByTestId("workbench-metric").selectOption(METRIC_ACS);
  await page.getByTestId("workbench-grain").selectOption("STATE");
  await page.getByTestId("workbench-geography").selectOption("state:06");
  await page.getByTestId("workbench-add-series").click();

  await page.getByTestId("workbench-source").selectOption("pep");
  await page.getByTestId("workbench-metric").selectOption(METRIC_PEP_STATE);
  await page.getByTestId("workbench-grain").selectOption("STATE");
  await page.getByTestId("workbench-geography").selectOption("state:06");
  await page.getByTestId("workbench-add-series").click();
}

test("a comparable pair draws a scatter at the shared grain", async ({ page }) => {
  await installAlignedRoutes(page);
  await page.goto("/workbench");
  await addAlignedPair(page);

  await page.getByTestId("workbench-presentation-scatter").click();

  // The grain offer is the intersection: ACS publishes STATE and COUNTY, PEP
  // only STATE, so only STATE is offered and the note names PEP.
  const grainOptions = page
    .getByTestId("workbench-alignment-grain")
    .locator("option");
  await expect(grainOptions).toHaveCount(1);
  await expect(grainOptions.first()).toHaveText("State");
  await expect(page.getByTestId("workbench-grain-note")).toContainText(
    METRIC_PEP_STATE,
  );

  await expect(page.getByTestId("workbench-preflight-status")).toContainText(
    "comparable",
  );
  // The rule the publication could not verify travels as a caveat, unchanged.
  await expect(page.getByTestId("workbench-preflight-caveats")).toContainText(
    "do not sum derived values across geographies",
  );

  // Two of the three rows have both sides published; the third is counted.
  await expect(page.locator('[data-testid="scatter-point"]')).toHaveCount(1);
  await expect(
    page.locator('[data-testid="scatter-point-differing"]'),
  ).toHaveCount(1);
  await expect(page.getByTestId("scatter-excluded")).toContainText(
    "1 geography is not plotted",
  );

  // And the partial pairing is visible against each side's own count.
  await expect(page.getByTestId("workbench-coverage")).toContainText("52");
});

test("an incomparable pair is explained and never queried", async ({ page }) => {
  await installAlignedRoutes(page, {
    preflight: {
      ...comparableVerdict,
      comparable: false,
      derivations: [],
      rules: [
        {
          rule: "units",
          status: "fail",
          reason:
            "units differ ('People' vs 'Percent'); a difference or ratio of unlike units would present incomparable quantities as comparable",
        },
      ],
    },
  });
  const comparisonRequests = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname.endsWith("/api/v1/comparison")) {
      comparisonRequests.push(url.toString());
    }
  });

  await page.goto("/workbench");
  await addAlignedPair(page);
  await expect(page.getByTestId("workbench-preflight-status")).toContainText(
    "not comparable",
  );
  await expect(page.getByTestId("workbench-unavailable-presentations")).toContainText(
    "units differ",
  );
  expect(comparisonRequests).toEqual([]);
});

test("the ranking sorts by the chosen side and counts the unpublished", async ({
  page,
}) => {
  await installAlignedRoutes(page);
  await page.goto("/workbench");
  await addAlignedPair(page);

  await page.getByTestId("workbench-presentation-ranking").click();
  await expect(page.locator('[data-testid="workbench-bar"]')).toHaveCount(2);
  await expect(page.getByTestId("bar-unpublished")).toContainText(
    "1 geography published no value",
  );

  // Sorting by the other side brings Wisconsin back: PEP published it.
  await page.getByTestId("workbench-rank-by").selectOption("b");
  await expect(page.locator('[data-testid="workbench-bar"]')).toHaveCount(3);
});

test("the heatmap hatches a period that published no value", async ({ page }) => {
  await installAlignedRoutes(page);
  await page.goto("/workbench");

  await page.getByTestId("workbench-source").selectOption("census");
  await page.getByTestId("workbench-metric").selectOption(METRIC_ACS);
  await page.getByTestId("workbench-grain").selectOption("STATE");
  await page.getByTestId("workbench-geography").selectOption("state:06");
  await page.getByTestId("workbench-add-series").click();

  await page.getByTestId("workbench-presentation-heatmap").click();

  const heatmap = page.getByTestId("workbench-heatmap");
  await expect(heatmap).toHaveAttribute("data-geography-count", "2");
  await expect(heatmap).toHaveAttribute("data-period-count", "2");
  await expect(page.locator('[data-testid="heatmap-cell"]')).toHaveCount(3);
  await expect(
    page.locator('[data-testid="heatmap-cell-unpublished"]'),
  ).toHaveCount(1);

  // The legend carries the withheld set as its own row, counted, in a colour
  // that is never one on the scale.
  const withheldRow = heatmap
    .locator(".map-legend .legend-row")
    .filter({ hasText: "No published value" });
  await expect(withheldRow).toHaveCount(1);
  await expect(withheldRow).toContainText("(1)");
});

// --- WB-5: the correlation on screen ---------------------------------------
//
// Covers: WEB-094 — the correlation panel leads with the association caveat,
// labels every coefficient API-derived, shows a null coefficient's reason
// rather than a blank, and reports coverage and contemporaneity from the
// answer. The matrix draws a declined cell off its scale with the failed rule
// in its tooltip, and a longitudinal composition is refused with the reason.

const CORRELATION_ROUTES = [
  {
    path: "/api/v1/comparison/correlation",
    parameters: ["geo_level", "metric_code_a", "metric_code_b", "state_fips", "year"],
  },
  {
    path: "/api/v1/comparison/matrix",
    parameters: ["geo_level", "limit", "metric_codes", "offset", "state_fips", "year"],
  },
];

const CAUSATION =
  "association, not causation: a coefficient describes how two published measures move together across geographies, never that one causes the other; a third measure, a shared geography effect, or the way each source defines its universe can produce any coefficient here";

const correlationAnswer = {
  metric_code_a: METRIC_ACS,
  metric_code_b: METRIC_PEP_STATE,
  source_code_a: "CENSUS_ACS",
  source_code_b: "CENSUS_PEP",
  units_a: "People",
  units_b: "People",
  derived: true,
  geo_level: "STATE",
  state_fips: null,
  year: null,
  n: 48,
  geographies_a: 52,
  geographies_b: 51,
  contemporaneous_pairs: 30,
  pearson_r: 0.9871,
  spearman_rho: 0.9123,
  period_a: "2023",
  period_b: null,
  periods_differ: true,
  derivations: ["pearson_r", "spearman_rho"],
  caveats: [
    CAUSATION,
    "aggregation characteristics are not fully published; do not sum derived values across geographies",
    "coverage: 48 of the 52 geographies either side published were paired; a geography one side publishes and the other does not is absent from the coefficient entirely",
    "18 of 48 pairs combine two different periods, because each side reduces to its own newest published value; pin a year to ask for a same-year answer, at the cost of the coverage that answer will report",
  ],
};

async function installCorrelationRoutes(page, { answer = correlationAnswer } = {}) {
  await page.route("**/api/v1/catalog/capabilities", (route) =>
    route.fulfill({
      json: {
        total: 2,
        items: alignedCapabilities.items.map((entry) => ({
          ...entry,
          observation_routes: [...entry.observation_routes, ...CORRELATION_ROUTES],
        })),
      },
    }),
  );
  await page.route("**/api/v1/comparison/correlation?*", (route) =>
    route.fulfill({ json: answer }),
  );
}

test("the correlation panel leads with the caveat and labels the coefficients", async ({
  page,
}) => {
  await installAlignedRoutes(page);
  await installCorrelationRoutes(page);
  await page.goto("/workbench");
  await addAlignedPair(page);

  await page.getByTestId("workbench-presentation-correlation").click();

  const panel = page.getByTestId("workbench-correlation");
  await expect(panel).toBeVisible();

  // The association sentence is first, in full, and not behind a control.
  await expect(page.getByTestId("workbench-correlation-causation")).toContainText(
    "association, not causation",
  );

  // `n` is the first reading, so the coefficient is read against it.
  const readings = page.getByTestId("correlation-reading");
  await expect(readings.first()).toContainText("Paired geographies");
  await expect(readings.first()).toContainText("48");

  // Both coefficients, at three decimal places, each labelled API-derived.
  await expect(panel).toContainText("0.987");
  await expect(panel).toContainText("0.912");
  await expect(page.getByTestId("correlation-derived")).toHaveCount(2);

  // Coverage and contemporaneity come from the answer, not from a guess.
  await expect(panel).toContainText("48 paired of 52 and 51 published");
  await expect(panel).toContainText("30 of 48");

  // And every remaining caveat is listed.
  await expect(page.getByTestId("workbench-correlation-caveats")).toContainText(
    "pin a year to ask for a same-year answer",
  );
});

test("a null coefficient shows the reason the API gave", async ({ page }) => {
  await installAlignedRoutes(page);
  await installCorrelationRoutes(page, {
    answer: {
      ...correlationAnswer,
      n: 2,
      pearson_r: null,
      spearman_rho: null,
      caveats: [
        CAUSATION,
        "no coefficient is reported: 2 paired geographies is fewer than the 3 a correlation needs to carry any information",
      ],
    },
  });
  await page.goto("/workbench");
  await addAlignedPair(page);
  await page.getByTestId("workbench-presentation-correlation").click();

  const panel = page.getByTestId("workbench-correlation");
  await expect(panel).toContainText("fewer than the 3 a correlation needs");
  // Both coefficient readings carry the reason, and neither is a bare dash
  // where a number should be.
  const derivedReadings = page
    .getByTestId("correlation-reading")
    .filter({ hasText: "API-derived" });
  await expect(derivedReadings).toHaveCount(2);
  for (const reading of await derivedReadings.all()) {
    const text = (await reading.textContent()) || "";
    expect(text).toContain("no coefficient is reported");
    expect(text.trim()).not.toMatch(/(API-derived)\s*[—–-]\s*$/);
  }
});

test("the panel says the coefficient is across geographies, not across time", async ({
  page,
}) => {
  await installAlignedRoutes(page);
  await installCorrelationRoutes(page);
  await page.goto("/workbench");
  await addAlignedPair(page);

  await page.getByTestId("workbench-presentation-correlation").click();

  // The statistic this plan declines to offer is named rather than silently
  // absent: a reader arriving from a time chart is told what this coefficient
  // is measured over, and what it is not.
  await expect(page.getByTestId("workbench-correlation-scope")).toContainText(
    "across geographies at the shared grain",
  );
  await expect(page.getByTestId("workbench-correlation-scope")).toContainText(
    "shared time trend",
  );
});

test("the year pin is off by default and its effect is read from the answer", async ({
  page,
}) => {
  await installAlignedRoutes(page);
  const pinned = {
    ...correlationAnswer,
    year: 2023,
    n: 30,
    contemporaneous_pairs: 30,
    period_a: "2023",
    period_b: "2023",
    periods_differ: false,
    caveats: [
      CAUSATION,
      "coverage: 30 of the 52 geographies either side published were paired; a geography one side publishes and the other does not is absent from the coefficient entirely",
    ],
  };
  await installCorrelationRoutes(page, { answer: correlationAnswer });
  // Registered after the blanket handler on purpose: Playwright resolves the
  // most recently registered matching route first, so this one answers.
  await page.route("**/api/v1/comparison/correlation?*", (route) => {
    const year = new URL(route.request().url()).searchParams.get("year");
    return route.fulfill({ json: year ? pinned : correlationAnswer });
  });
  await page.goto("/workbench");
  await addAlignedPair(page);
  await page.getByTestId("workbench-presentation-correlation").click();

  await expect(page.getByTestId("workbench-year-pin")).toHaveValue("");
  await expect(page.getByTestId("workbench-correlation-periods")).toContainText(
    "own newest published value",
  );

  await page.getByTestId("workbench-year-pin").selectOption("2023");
  // The coverage the pin cost is reported from the answer, never predicted.
  await expect(page.getByTestId("workbench-correlation")).toContainText(
    "30 paired of 52 and 51 published",
  );
  await expect(page.getByTestId("workbench-correlation-periods")).toContainText(
    "reduced within 2023",
  );
});

// --- WB-6: saving the composition ------------------------------------------
//
// Covers: WEB-097 — the save control names its destination before and after
// the save, a refused account save is reported where the reader asked for it
// and never redirected to the browser store, and a saved composition reopens
// as the same chart.

test("the save states its destination, and a refusal is not redirected", async ({
  page,
}) => {
  await installAlignedRoutes(page);
  await installCorrelationRoutes(page);

  const posted = [];
  await page.route("**/api/v1/analysis-configurations", (route) => {
    posted.push(JSON.parse(route.request().postData() || "{}"));
    return route.fulfill({
      status: 403,
      json: { detail: "this token cannot write configurations" },
    });
  });

  // `sessionStorage`, never `localStorage`: the token's only home in the
  // browser, and deliberately not beside the public saved-chart store.
  await page.addInitScript(() => {
    window.sessionStorage.setItem(
      "economic-data-studio:api-token",
      "operator-token",
    );
  });
  await page.goto("/workbench");
  await addAlignedPair(page);

  const save = page.getByTestId("workbench-save");
  await expect(save).toHaveAttribute("data-destination", "account");
  await expect(save).toContainText("Save to account");
  await save.click();

  // The refusal is reported where the reader asked for it. It is never
  // quietly written to the browser store instead: a save they were told went
  // to their account and silently did not is worse than one that failed.
  const toast = page.getByTestId("workbench-save-toast");
  await expect(toast).toBeVisible();
  await expect(toast).not.toHaveAttribute("data-destination", "browser");

  // And what it tried to store is a workbench document with one series each.
  expect(posted).toHaveLength(1);
  expect(posted[0].document.kind).toBe("workbench");
  expect(posted[0].document.series).toHaveLength(2);
  expect(posted[0].document.series[0].metric_code).toBe(METRIC_ACS);
  expect(posted[0].document.filters).toEqual({});
});

test("a composition saved in the browser reopens as the same chart", async ({
  page,
}) => {
  await installAlignedRoutes(page);
  await installCorrelationRoutes(page);
  await page.goto("/workbench");
  await addAlignedPair(page);

  const save = page.getByTestId("workbench-save");
  await expect(save).toHaveAttribute("data-destination", "browser");
  await save.click();
  await expect(page.getByTestId("workbench-save-toast")).toBeVisible();

  const stored = await page.evaluate(() =>
    JSON.parse(
      window.localStorage.getItem("economic-data-studio:saved-charts:v1") || "[]",
    ),
  );
  expect(stored).toHaveLength(1);
  expect(stored[0].chartType).toBe("workbench");
  // One envelope per series, so a packet's completeness rule sees each one.
  expect(stored[0].series).toHaveLength(2);
  expect(stored[0].series[0].source).toBe("CENSUS_ACS");
  expect(stored[0].series[0].geoLevel).toBe("STATE");
  expect(stored[0].series[0].unit).toBe("People");
  expect(stored[0].document.kind).toBe("workbench");
  expect(stored[0].document.series[0].filters).toMatchObject({
    geo_level: "STATE",
    geo_id: "state:06",
  });

  // And the page's own link reopens the same composition.
  const shared = page.url();
  await page.goto(shared);
  await expect(page.locator("[data-testid='workbench']")).toHaveAttribute(
    "data-series-count",
    "2",
  );
});
