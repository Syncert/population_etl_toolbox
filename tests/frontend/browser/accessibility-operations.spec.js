import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-025 — the cross-workflow accessibility, responsive, and
// operational audit. Every core workflow is reachable and operable by
// keyboard, every control carries an accessible name, every map and chart
// has a table or textual alternative, state changes are announced,
// analytical context survives a small viewport, an unavailable API leaves a
// distinct recoverable state rather than a blank screen, and the served
// security headers are the ones the deployment declares.

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
      : route.fulfill({ json: { total: 1, limit: 4000, offset: 0, items: [observation] } }),
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
      : route.fulfill({ json: { total: 1, limit: 4000, offset: 0, items: [observation] } }),
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

// `/workbench` is here because `analytics-workbench` added the route and not
// the entry, and nothing noticed: `workbench.spec.js` checks none of the
// landmark, heading or status-row rules. The auditor spec beside this one
// derives its own list from the app directory for exactly that reason; this
// list stays hand-written because these assertions are this repository's own
// rules rather than WCAG's, and each is worth stating deliberately.
const CORE_ROUTES = [
  "/",
  "/catalog",
  "/explore",
  "/compare",
  "/workbench",
  "/profiles",
  "/quality",
  "/articles",
  "/builder",
  "/saved",
];

test("every core route serves the declared security headers", async ({ page }) => {
  await installRoutes(page);
  for (const route of ["/", "/explore", "/saved"]) {
    const response = await page.goto(route);
    const headers = response.headers();
    expect(headers["x-content-type-options"]).toBe("nosniff");
    expect(headers["x-frame-options"]).toBe("SAMEORIGIN");
    expect(headers["referrer-policy"]).toBe("strict-origin-when-cross-origin");
    const csp = headers["content-security-policy"];
    // Same-origin only: the API and tiles are reached through this server's
    // own rewrites, so nothing needs a third-party connect or script origin.
    expect(csp).toContain("default-src 'self'");
    expect(csp).toContain("connect-src 'self'");
    expect(csp).toContain("object-src 'none'");
    expect(csp).toContain("frame-ancestors 'self'");
    // MapLibre builds its workers and tile images from blob URLs.
    expect(csp).toContain("worker-src 'self' blob:");
  }
});

test("every core route has one main landmark and a single top-level heading", async ({ page }) => {
  await installRoutes(page);
  for (const route of CORE_ROUTES) {
    await page.goto(route);
    await expect(page.locator("main")).toHaveCount(1);
    await expect(page.getByRole("heading", { level: 1 })).toHaveCount(1);
    // Navigation is a named landmark on every page that renders it.
    await expect(page.getByRole("navigation", { name: "Primary navigation" })).toBeVisible();
  }
});

test("every control on the core workflows carries an accessible name", async ({ page }) => {
  await installRoutes(page);
  // Every core route, not four of them, and buttons and links as well as the
  // form controls: a button with no name is as unusable as an unlabelled
  // select, and this check could not see one.
  for (const route of CORE_ROUTES) {
    await page.goto(route);
    const unnamed = await page.evaluate(() => {
      const controls = [
        ...document.querySelectorAll("select, input, textarea, button, a[href]"),
      ];
      return controls
        .filter((control) => {
          if (control.getAttribute("aria-hidden") === "true") return false;
          const labelled =
            control.getAttribute("aria-label") ||
            control.getAttribute("aria-labelledby") ||
            (control.id && document.querySelector(`label[for="${CSS.escape(control.id)}"]`)) ||
            control.closest("label") ||
            // A button or a link is named by its own text content, which a
            // select never is.
            (control.textContent || "").trim() ||
            control.getAttribute("title");
          return !labelled;
        })
        .map((control) => `${control.tagName.toLowerCase()}#${control.id || "(no id)"}`);
    });
    expect(unnamed, `unnamed controls on ${route}`).toEqual([]);
  }
});

test("the active navigation link says so, and the skip link reaches main", async ({ page }) => {
  // Covers: WEB-111 — the active link was marked by a class alone, which says
  // nothing to a reader who is not looking at the colour; and ten navigation
  // links stood between the top of every page and the analysis on it.
  await installRoutes(page);
  await page.goto("/explore");

  const active = page.getByRole("link", { name: "Explore", exact: true });
  await expect(active).toHaveAttribute("aria-current", "page");
  await expect(page.getByRole("link", { name: "Compare", exact: true })).not.toHaveAttribute(
    "aria-current",
    "page",
  );

  // The skip link is the first thing a keyboard reaches, and it lands on the
  // content rather than scrolling to it and leaving focus behind.
  await page.keyboard.press("Tab");
  const skip = page.getByRole("link", { name: "Skip to main content" });
  await expect(skip).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.locator("#main-content")).toBeFocused();
});

test("the map and the trend both have a table alternative to the same values", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");

  // The map is a labelled region and is not the only way to reach a value.
  const map = page.getByTestId("map-canvas");
  await expect(map).toHaveAttribute("aria-label", /arrow keys/i);
  await page.getByRole("tab", { name: "table" }).click();
  await expect(page.getByRole("cell", { name: "561504" })).toBeVisible();

  // An alternative that shows an unstated subset is not one: the caption says
  // how many rows there are and which of them this is, so a reader who cannot
  // use the map knows whether they have seen everything (WEB-110).
  await expect(page.getByTestId("observation-table-caption")).toContainText(
    "Showing all 1 loaded row,",
  );

  // The legend states its bins in text, so colour is not the only carrier.
  await page.getByRole("tab", { name: "map" }).click();
  await expect(page.getByLabel("Choropleth value legend")).toContainText("API distribution");
});

test("selection state is announced and reachable by keyboard alone", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/explore");
  const dashboard = page.getByTestId("dashboard");
  await expect(dashboard).toHaveAttribute("data-observation-count", "1");

  // The selected-geography panel is a live region, so a selection made by
  // keyboard is announced rather than only drawn.
  const panel = page.locator(".county-panel");
  await expect(panel).toHaveAttribute("aria-live", "polite");

  const map = page.getByTestId("map-canvas");
  await map.focus();
  await page.keyboard.press("Enter");
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", GEO_ID);
  await expect(panel).toContainText("Dane County");
  await page.keyboard.press("Escape");
  await expect(dashboard).toHaveAttribute("data-selected-geo-id", "");
});

test("analytical context survives a small viewport", async ({ page }) => {
  await installRoutes(page);
  await page.setViewportSize({ width: 390, height: 780 });
  await page.goto("/explore");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "1");

  // Source and caveat context is not hidden on a phone: the whole point of
  // the context is that it travels with the value.
  await expect(page.getByTestId("observations-status")).toBeVisible();
  await expect(page.getByText(/ACS 5-year estimates provide complete county coverage/)).toBeVisible();

  // Nothing overflows the viewport horizontally.
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth - document.documentElement.clientWidth,
  );
  expect(overflow).toBeLessThanOrEqual(1);
});

test("an unavailable API leaves a distinct, recoverable state rather than a blank page", async ({
  page,
}) => {
  await installRoutes(page, { failObservations: true });
  await page.goto("/explore");

  // The failure is named with its status, and no stale value is presented
  // as current.
  await expect(page.getByTestId("observations-status")).toContainText("status 503");
  await expect(page.getByTestId("dashboard")).toHaveAttribute("data-observation-count", "0");
  // The rest of the workflow still works: the user can change the selection
  // and retry rather than reloading into the same wall.
  await expect(page.getByTestId("metric-select")).toBeEnabled();
  await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
});

// Covers: WEB-037 — the outcome of a request the reader triggered is
// announced. Every data screen reports through a `.status-row`, and the
// catalog through its own summary; none of them spoke, so a reader using a
// screen reader saw nothing change when a comparison came back incompatible
// or a catalog search returned nothing.
test("every screen's request status is a polite live region", async ({ page }) => {
  await installRoutes(page);
  for (const route of CORE_ROUTES) {
    await page.goto(route);
    const rows = page.locator(".status-row");
    const count = await rows.count();
    for (let index = 0; index < count; index += 1) {
      const row = rows.nth(index);
      await expect(row).toHaveAttribute("role", "status");
      // Polite, never assertive: a failed read is not an interruption that
      // should cut off what the reader is already listening to.
      const live = await row.getAttribute("aria-live");
      expect(live === null || live === "polite").toBe(true);
    }
  }
});

test("the catalog's result state is announced as one region", async ({ page }) => {
  await installRoutes(page);
  await page.goto("/catalog");
  const status = page.getByTestId("catalog-status");
  await expect(status).toHaveAttribute("role", "status");
  // Always present, so the first render is the baseline and only later
  // changes speak.
  await expect(status).toHaveCount(1);
  await expect(status).toContainText("matching metrics");
});

test("a status pill is not itself a live region", async ({ page }) => {
  // A catalog page renders one freshness pill per metric row; making the
  // shared component live would turn a list render into dozens of
  // announcements. The row is the region, the pills are its content.
  await installRoutes(page);
  await page.goto("/catalog");
  const pills = page.locator(".pill");
  const count = await pills.count();
  for (let index = 0; index < count; index += 1) {
    await expect(pills.nth(index)).not.toHaveAttribute("role", "status");
    await expect(pills.nth(index)).not.toHaveAttribute("aria-live", /.*/);
  }
});
