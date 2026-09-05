import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

// Covers: WEB-021 — the community conditions profile in the browser. The
// product is configuration over published catalog identities: each filled
// slot shows the identity that answered with its own source, period, unit,
// uncertainty, and freshness, a slot the catalog cannot fill states the gap
// instead of disappearing, a measure the place did not publish is never a
// zero, and the link reproduces the product and place.

const neutralRoutes = [
  {
    path: "/api/v1/observations",
    parameters: ["geo_id", "geo_level", "limit", "metric_code", "release", "scope", "state_fips"],
  },
  { path: "/api/v1/observations/releases", parameters: ["limit", "metric_code", "offset"] },
];

const capabilities = {
  total: 3,
  items: [
    {
      source_code: "ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "geo_level", "state_fips"],
      observation_routes: neutralRoutes,
    },
    {
      source_code: "CENSUS_PEP",
      display_name: "Census Population Estimates Program",
      route_segment: "pep",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "geo_level"],
      observation_routes: neutralRoutes,
    },
    {
      source_code: "BLS",
      display_name: "Bureau of Labor Statistics",
      route_segment: "bls",
      served_by_neutral_routes: true,
      observation_filters: ["geo_id", "geo_level"],
      observation_routes: neutralRoutes,
    },
  ],
};

// Only some of the community profile's candidate identities are published
// here: health, safety, and rural context are not, which the product must
// state rather than hide.
const publishedMetrics = {
  "ACS:acs5:B01003_001": {
    metric_code: "ACS:acs5:B01003_001",
    metric_display_name: "Total population",
    source_code: "ACS",
    units: "people",
    freshness_state: "fresh",
    valid_geo_grains: ["COUNTY"],
  },
  "CENSUS_PEP:pep_cty_alldata:POPESTIMATE": {
    metric_code: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
    metric_display_name: "Resident population estimate",
    source_code: "CENSUS_PEP",
    units: "people",
    freshness_state: "stale",
    valid_geo_grains: ["COUNTY"],
  },
  "BLS:LAU:UNEMP_RATE": {
    metric_code: "BLS:LAU:UNEMP_RATE",
    metric_display_name: "Unemployment rate",
    source_code: "BLS",
    units: "percent",
    freshness_state: "fresh",
    valid_geo_grains: ["COUNTY"],
  },
  "ACS:acs5:B19013_001": {
    metric_code: "ACS:acs5:B19013_001",
    metric_display_name: "Median household income",
    source_code: "ACS",
    units: "dollars",
    freshness_state: "fresh",
    valid_geo_grains: ["COUNTY"],
  },
};

const GEO_ID = "state:55|county:025";

// The observations each published measure answers with for this place. The
// median-income slot answers with a suppressed row: a value the source did
// not publish, which must never render as a number.
const observationsByMetric = {
  "ACS:acs5:B01003_001": [
    {
      metric_code: "ACS:acs5:B01003_001",
      source_code: "ACS",
      geo_id: GEO_ID,
      geo_level: "COUNTY",
      value: "561504",
      value_status: "valid",
      unit: "people",
      period_start: "2023-01-01",
      period_end: "2023-12-31",
      uncertainty: { margin_of_error: "1200" },
    },
  ],
  "CENSUS_PEP:pep_cty_alldata:POPESTIMATE": [
    {
      metric_code: "CENSUS_PEP:pep_cty_alldata:POPESTIMATE",
      source_code: "CENSUS_PEP",
      geo_id: GEO_ID,
      geo_level: "COUNTY",
      value: "568203",
      value_status: "valid",
      unit: "people",
      period_start: "2024-01-01",
      period_end: "2024-12-31",
    },
  ],
  "ACS:acs5:B19013_001": [
    {
      metric_code: "ACS:acs5:B19013_001",
      source_code: "ACS",
      geo_id: GEO_ID,
      geo_level: "COUNTY",
      value: null,
      value_status: "suppressed",
      unit: "dollars",
      period_start: "2023-01-01",
      period_end: "2023-12-31",
    },
  ],
  // The unemployment slot resolves in the catalog but published nothing for
  // this place, which is a different fact from a suppressed value.
  "BLS:LAU:UNEMP_RATE": [],
};

async function installRoutes(page, { observationRequests = [] } = {}) {
  await page.route("**/api/v1/catalog/capabilities", (route) =>
    route.fulfill({ json: capabilities }),
  );
  await page.route("**/api/v1/catalog/metrics/*", (route) => {
    const path = new URL(route.request().url()).pathname;
    const code = decodeURIComponent(path.split("/catalog/metrics/")[1] || "");
    const metric = publishedMetrics[code];
    // The API's stable answer for an identity it does not publish.
    return metric
      ? route.fulfill({ json: metric })
      : route.fulfill({ status: 404, json: { detail: "metric_code not found" } });
  });
  await page.route("**/api/v1/catalog/geographies?*", (route) => {
    const level = new URL(route.request().url()).searchParams.get("geo_level");
    const items =
      level === "STATE"
        ? [{ geo_id: "state:55", geo_level: "STATE", state_fips: "55", state_name: "Wisconsin" }]
        : [
            {
              geo_id: GEO_ID,
              geo_level: "COUNTY",
              state_fips: "55",
              county_fips: "025",
              state_name: "Wisconsin",
              county_name: "Dane County",
            },
          ];
    return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
  });
  await page.route("**/api/v1/observations?*", (route) => {
    const params = new URL(route.request().url()).searchParams;
    observationRequests.push(Object.fromEntries(params));
    const items = observationsByMetric[params.get("metric_code")] || [];
    return route.fulfill({
      json: {
        metric_code: params.get("metric_code"),
        source_code: "ACS",
        scope: "latest",
        total: items.length,
        limit: 50,
        offset: 0,
        items,
      },
    });
  });
}

test("the community profile reads a place through published identities", async ({ page }) => {
  const observationRequests = [];
  await installRoutes(page, { observationRequests });
  await page.goto("/profiles");

  const product = page.getByTestId("profile-product");
  await expect(product).toHaveAttribute("data-template", "community-conditions");
  // Four of the product's candidate identities are published; the rest are
  // not, and the count is stated rather than the slots being dropped.
  await expect(product).toHaveAttribute("data-available-measures", "4");
  await expect(page.getByTestId("profile-coverage-note")).toContainText("not published");
  await expect(page.getByTestId("template-limits")).toContainText(
    "Nothing here is combined into a score",
  );

  await page.getByTestId("profile-place").selectOption(GEO_ID);
  await expect(product).toHaveAttribute("data-geo-id", GEO_ID);

  // A filled slot shows the value with its own unit, and names the identity
  // that answered rather than only the slot's label.
  await expect(page.getByTestId("measure-value-total-population")).toContainText("561,504");
  await expect(page.getByTestId("measure-total-population")).toContainText(
    "ACS:acs5:B01003_001",
  );
  await expect(page.getByTestId("measure-total-population")).toContainText("Source: ACS");
  await expect(page.getByTestId("measure-total-population")).toContainText("2023-01-01");
  await expect(page.getByTestId("measure-total-population")).toContainText("1,200");

  // Two population measures from different programs sit side by side, each
  // with its own period and freshness — never merged into one number.
  await expect(page.getByTestId("measure-value-population-estimate")).toContainText("568,203");
  await expect(page.getByTestId("measure-freshness-population-estimate")).toContainText("stale");
  await expect(page.getByTestId("measure-freshness-total-population")).toContainText("fresh");

  // A suppressed value is stated, never rendered as a number or a zero.
  await expect(page.getByTestId("measure-value-median-household-income")).toContainText(
    "Not published for this place",
  );
  await expect(page.getByTestId("measure-status-median-household-income")).toContainText(
    "suppressed",
  );

  // A measure that published nothing for this place is a different fact,
  // and says so.
  await expect(page.getByTestId("measure-answer-unemployment-rate")).toContainText(
    "not published for this place",
  );

  // A slot no published identity satisfies states what it looked for.
  await expect(page.getByTestId("measure-cdc-indicator")).toHaveAttribute(
    "data-available",
    "false",
  );
  await expect(page.getByTestId("measure-reason-cdc-indicator")).toContainText(
    "CDC:cdi:ALC1_1:crude",
  );
  // Its section is still rendered: an absent measure is not a place with
  // nothing to report.
  await expect(page.getByTestId("section-health")).toBeVisible();
  await expect(page.getByTestId("section-safety")).toBeVisible();

  // Every request carried the place through the source's declared filter.
  expect(observationRequests.every((request) => request.geo_id === GEO_ID)).toBe(true);
  expect(observationRequests.every((request) => request.scope === "latest")).toBe(true);

  // Each measure keeps a direct path into the explorer.
  await expect(page.getByTestId("measure-explore-total-population")).toHaveAttribute(
    "href",
    /metric=ACS%3Aacs5%3AB01003_001/,
  );

  // The link reproduces the product and the place.
  await expect(page).toHaveURL(/place=state%3A55%7Ccounty%3A025/);
});

test("the products are configuration: switching rebuilds the same screen", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/profiles");
  const product = page.getByTestId("profile-product");

  await page.getByTestId("template-select").selectOption("workforce");
  await expect(product).toHaveAttribute("data-template", "workforce");
  await expect(page).toHaveURL(/template=workforce/);
  await expect(page.getByRole("heading", { level: 1 })).toContainText("Workforce");
  // The same slot machinery answers a different product: the labor section
  // resolves, the population base partly resolves, and the unresolved slots
  // state their candidates.
  await expect(page.getByTestId("section-labor-force")).toBeVisible();
  await expect(page.getByTestId("measure-participation")).toHaveAttribute(
    "data-available",
    "false",
  );
  await expect(page.getByTestId("measure-reason-participation")).toContainText("FRED:CIVPART");

  // A shared product link reopens the same product for the same place.
  await page.goto("/profiles?template=population-growth&place=state%3A55%7Ccounty%3A025");
  await expect(product).toHaveAttribute("data-template", "population-growth");
  await expect(product).toHaveAttribute("data-geo-id", GEO_ID);
  await expect(page.getByTestId("measure-value-population-estimate")).toContainText("568,203");
});
