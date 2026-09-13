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
