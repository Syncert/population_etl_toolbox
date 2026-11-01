import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { servedParameters } from "../support/servedContract.js";
import { polygonAround, routeSyntheticTiles } from "../support/syntheticTiles.js";

// Covers: WEB-148 — one measure drawn on Texas cities and towns and on metro
// areas: the page offers only the grains the catalog publishes, draws each on
// the layer and boundary vintage /catalog/tile-layers lists, names that
// vintage, keeps equal-count bands and a legend that adds up, leaves a
// geography without a value uncoloured and says so, never prints a zero for
// a gap, and always draws the most recent boundaries held, marking the map
// with an asterisk and when its boundaries were last updated whenever they
// are not from the period's year. Synthetic UI fixture values and fixture shapes (a polygon around
// each city's point), not published statistics or boundaries.

const INCOME = "CENSUS_ACS:acs5:B19013_001";
const SCREENSHOTS = process.env.PLACE_MAP_SCREENSHOT_DIR || "";

const states = [
  { geo_id: "state:48", geo_level: "STATE", state_fips: "48", state_name: "Texas", latitude: 31.2, longitude: -99.3 },
  { geo_id: "state:55", geo_level: "STATE", state_fips: "55", state_name: "Wisconsin", latitude: 44.6, longitude: -89.9 },
];
const counties = [
  { geo_id: "state:48|county:453", geo_level: "COUNTY", state_fips: "48", county_fips: "453", county_name: "Travis County", state_name: "Texas" },
  { geo_id: "state:55|county:025", geo_level: "COUNTY", state_fips: "55", county_fips: "025", county_name: "Dane County", state_name: "Wisconsin" },
];
// [place fips, name, lon, lat, value]: null is a withheld value, undefined no row at all.
const texasPlaces = [
  ["05000", "Austin city", -97.74, 30.27, 91461],
  ["35000", "Houston city", -95.37, 29.76, 62894],
  ["19000", "Dallas city", -96.8, 32.78, 67760],
  ["65000", "San Antonio city", -98.49, 29.42, 62917],
  ["24000", "El Paso city", -106.44, 31.76, 58734],
  ["27000", "Fort Worth city", -97.33, 32.75, 76602],
  ["45000", "Lubbock city", -101.86, 33.58, 61911],
  ["03000", "Amarillo city", -101.83, 35.22, 63417],
  ["17000", "Corpus Christi city", -97.4, 27.8, 66632],
  ["41464", "Laredo city", -99.49, 27.53, 60542],
  ["76000", "Waco city", -97.15, 31.55, 49853],
  ["01000", "Abilene city", -99.73, 32.45, 58340],
  ["48072", "Midland city", -102.08, 32.0, null],
  ["74144", "Tyler city", -95.3, 32.35, undefined],
];
const places = texasPlaces.map(([fips, name, lon, lat]) => ({
  geo_id: `state:48|place:${fips}`, geo_level: "PLACE", state_fips: "48", place_fips: fips, place_name: name, geo_name: name, state_name: "Texas", longitude: lon, latitude: lat,
}));
// [cbsa, name, lon, lat, value]
const metroList = [
  ["12420", "Austin-Round Rock-San Marcos, TX", -97.75, 30.3, 97579],
  ["26420", "Houston-Pasadena-The Woodlands, TX", -95.37, 29.76, 80458],
  ["19100", "Dallas-Fort Worth-Arlington, TX", -96.9, 32.8, 87155],
  ["16980", "Chicago-Naperville-Elgin, IL-IN", -87.9, 41.9, 85994],
  ["35620", "New York-Newark-Jersey City, NY-NJ", -74.0, 40.7, 93610],
  ["31080", "Los Angeles-Long Beach-Anaheim, CA", -118.2, 34.05, 93525],
  ["42660", "Seattle-Tacoma-Bellevue, WA", -122.3, 47.6, 112594],
  ["19740", "Denver-Aurora-Centennial, CO", -104.9, 39.7, 98975],
  ["12060", "Atlanta-Sandy Springs-Roswell, GA", -84.4, 33.75, 83251],
  ["33100", "Miami-Fort Lauderdale-West Palm Beach, FL", -80.2, 26.1, 73481],
  ["31540", "Madison, WI", -89.4, 43.07, 84297],
  ["38060", "Phoenix-Mesa-Chandler, AZ", -112.0, 33.45, 84703],
  ["14460", "Boston-Cambridge-Newton, MA-NH", -71.06, 42.36, 112484],
  ["33460", "Minneapolis-St. Paul-Bloomington, MN-WI", -93.27, 44.98, 95116],
  ["41860", "San Francisco-Oakland-Fremont, CA", -122.3, 37.8, undefined],
];
const metros = metroList.map(([cbsa, name]) => ({ geo_id: `cbsa:${cbsa}`, geo_level: "METRO", geo_name: name, cbsa_code: cbsa }));

const tileLayers = {
  uncoloured_rule: "A geography with no published value is left uncoloured, never drawn as zero.",
  layers: [
    { layer_id: "counties", source_layer: "counties", grains: ["STATE", "COUNTY"], versioned: false, minzoom: 0, maxzoom: 12, join_key: "geo_id", name_field: "county_name",
      tile_template: "/tiles/counties/{z}/{x}/{y}", vintages: [], default_vintage: null, available: true },
    { layer_id: "tracts", source_layer: "tracts", grains: ["TRACT"], versioned: false, minzoom: 6, maxzoom: 14, join_key: "geo_id", name_field: "area_name",
      tile_template: "/tiles/tracts/{z}/{x}/{y}", vintages: [], default_vintage: null, available: true },
    { layer_id: "places", source_layer: "places", grains: ["PLACE"], versioned: true, minzoom: 6, maxzoom: 14, join_key: "geo_id", name_field: "place_name",
      tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2024", default_vintage: 2024, available: true,
      vintages: [
        { boundary_vintage: 2024, feature_count: 32350, is_default: true, tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2024" },
        { boundary_vintage: 2020, feature_count: 31900, is_default: false, tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2020" },
      ] },
    { layer_id: "metros", source_layer: "metros", grains: ["METRO"], versioned: true, minzoom: 0, maxzoom: 12, join_key: "geo_id", name_field: "metro_name",
      tile_template: "/tiles/metros/{z}/{x}/{y}?vintage=2023", default_vintage: 2023, available: true,
      vintages: [{ boundary_vintage: 2023, feature_count: 925, is_default: true, tile_template: "/tiles/metros/{z}/{x}/{y}?vintage=2023" }] },
  ],
};

const PERIODS = [
  { period_start: "2020-01-01", period_end: "2024-12-31", observation_count: 30 },
  { period_start: "2008-01-01", period_end: "2012-12-31", observation_count: 30 },
];

function row(geoId, geoLevel, value, period) {
  return {
    metric_code: INCOME, source_code: "CENSUS_ACS", geo_id: geoId, geo_level: geoLevel, value: value === null ? null : String(value),
    value_status: value === null ? "suppressed" : "valid", unit: "dollars", period_start: period.period_start, period_end: period.period_end,
    release: period.period_end.slice(0, 4), dimensions: {}, uncertainty: value === null ? null : { margin_of_error: "2100" },
  };
}

// OFFICE_LAB's case for an older period: the latest publication still lists
// 2018–2022 because a few dissolved places keep it as their newest value.
const STALE_PERIODS = [
  { period_start: "2020-01-01", period_end: "2024-12-31", observation_count: 30000 },
  { period_start: "2018-01-01", period_end: "2022-12-31", observation_count: 12 },
];
const RELEASES = [
  { release: "2024", as_of: null, observation_count: 30000, newest_period_start: "2020-01-01", newest_period_end: "2024-12-31" },
  { release: "2022", as_of: null, observation_count: 29800, newest_period_start: "2018-01-01", newest_period_end: "2022-12-31" },
];

async function install(page, { observationRequests = [], releases = false } = {}) {
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    const path = url.pathname;
    if (path === "/api/v1/auth/refresh") return route.fulfill({ status: 401, json: { detail: "sign-in could not be completed" } });
    if (path === "/api/v1/catalog/capabilities") return route.fulfill({ json: { total: 1, items: [{ source_code: "CENSUS_ACS", display_name: "CENSUS_ACS", route_segment: null, served_by_neutral_routes: true,
      publishes_value_status: true, publishes_aligned_reduction: true, observation_filters: ["geo_id", "geo_level", "state_fips"], observation_dimensions: [],
      observation_routes: ["/api/v1/observations", "/api/v1/observations/periods", ...(releases ? ["/api/v1/observations/releases"] : [])].map((routePath) => ({ path: routePath, parameters: servedParameters(routePath) })) }] } });
    if (path === "/api/v1/catalog/sources") return route.fulfill({ json: [{ source_code: "CENSUS_ACS", source_name: "American Community Survey", reference_url: "https://example.org/acs" }] });
    if (path === "/api/v1/catalog/tile-layers") return route.fulfill({ json: tileLayers });
    if (path === "/api/v1/catalog/derived-ratios") return route.fulfill({ json: { total: 0, items: [] } });
    if (path === "/api/v1/catalog/geographies") {
      const level = params.get("geo_level");
      const items = level === "STATE" ? states : level === "COUNTY" ? counties : level === "METRO" ? metros
        : level === "PLACE" ? places.filter((item) => !params.get("state_fips") || item.state_fips === params.get("state_fips")) : [];
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path.startsWith("/api/v1/catalog/metrics/")) {
      return route.fulfill({ json: { metric_code: INCOME, metric_display_name: "Median household income (UI fixture)", source_code: "CENSUS_ACS", units: null,
        valid_geo_grains: ["NATIONAL", "STATE", "COUNTY", "METRO", "PLACE"], measure_kind: "median", aggregation_characteristic: "non_additive" } });
    }
    if (path === "/api/v1/observations/periods") {
      const items = releases ? STALE_PERIODS : PERIODS;
      return route.fulfill({ json: { metric_code: INCOME, total: items.length, limit: 200, offset: 0, items } });
    }
    if (path === "/api/v1/observations/releases") return route.fulfill({ json: { metric_code: INCOME, total: RELEASES.length, limit: 200, offset: 0, items: RELEASES } });
    if (path === "/api/v1/observations" && params.get("scope") === "as_released") {
      observationRequests.push(Object.fromEntries(params));
      const period = { period_start: "2018-01-01", period_end: "2022-12-31" };
      const items = params.get("release") === "2022" && params.get("geo_level") === "PLACE"
        ? texasPlaces.filter((entry) => entry[4] !== undefined).map(([fips, , , , value]) => ({ ...row(`state:48|place:${fips}`, "PLACE", value, period), release: "2022" }))
        : [];
      return route.fulfill({ json: { metric_code: INCOME, total: items.length, offset: 0, limit: 1000, items } });
    }
    if (path === "/api/v1/observations") {
      observationRequests.push(Object.fromEntries(params));
      // Read from the latest publication, the old period holds no Texas city.
      if (releases && params.get("period_start") === "2018-01-01") return route.fulfill({ json: { metric_code: INCOME, total: 0, offset: 0, limit: 1000, items: [] } });
      const period = PERIODS.find((item) => item.period_start === params.get("period_start")) || PERIODS[0];
      const level = params.get("geo_level");
      let items = [];
      if (level === "PLACE") items = texasPlaces.filter((entry) => entry[4] !== undefined).map(([fips, , , , value]) => row(`state:48|place:${fips}`, "PLACE", value, period));
      if (level === "METRO") items = metroList.filter((entry) => entry[4] !== undefined).map(([cbsa, , , , value]) => row(`cbsa:${cbsa}`, "METRO", value, period));
      if (level === "COUNTY") items = [row("state:48|county:453", "COUNTY", 97000, period), row("state:55|county:025", "COUNTY", 84000, period)];
      return route.fulfill({ json: { metric_code: INCOME, total: items.length, offset: 0, limit: 1000, items } });
    }
    return route.fulfill({ status: 503, json: { detail: "No UI fixture for this resource" } });
  });
}

const placeLayer = [{ name: "places", features: texasPlaces.map(([fips, name, lon, lat]) => ({
  ring: polygonAround(lon, lat, 0.38), properties: { geo_id: `state:48|place:${fips}`, geo_level: "PLACE", state_fips: "48", place_name: name, boundary_vintage: 2024 },
})) }];
const metroLayer = [{ name: "metros", features: metroList.map(([cbsa, name, lon, lat]) => ({
  ring: polygonAround(lon, lat, 1.1), properties: { geo_id: `cbsa:${cbsa}`, geo_level: "METRO", cbsa_code: cbsa, metro_name: name, boundary_vintage: 2023 },
})) }];

async function legendCounts(page) {
  return page.getByTestId("measure-map-legend").locator("li").evaluateAll((items) => items.map((item) => Number(item.getAttribute("data-count"))));
}

test("a measure drawn on Texas cities and towns, on the boundary vintage it names", async ({ page }) => {
  const observationRequests = [];
  const tileRequests = [];
  await install(page, { observationRequests });
  await routeSyntheticTiles(page, "places", placeLayer, tileRequests);
  await page.setViewportSize({ width: 1440, height: 1100 });

  // Cities are drawn one state at a time: no state, no read.
  await page.goto(`/map/${encodeURIComponent(INCOME)}?grain=PLACE`);
  await expect(page.getByTestId("measure-map-choose-state")).toBeVisible();
  await expect(page.getByTestId("measure-map-grain").locator("option")).toHaveText(["Counties", "Metro areas", "Cities and towns"]);
  expect(observationRequests.filter((params) => params.geo_level === "PLACE")).toEqual([]);
  await page.getByTestId("measure-map-state").selectOption("48");

  const shell = page.getByTestId("measure-map");
  await expect(shell).toHaveAttribute("data-grain", "PLACE");
  await expect(shell).toHaveAttribute("data-state", "48");
  await expect(page).toHaveURL(/grain=PLACE/);
  await expect(page).toHaveURL(/state=48/);
  await expect(page.locator(".section-kicker").first()).toHaveText("One measure, every city or town in Texas");
  expect(observationRequests.find((params) => params.geo_level === "PLACE")).toMatchObject({ state_fips: "48" });

  await expect(page.getByTestId("measure-map-vintage")).toHaveText("Drawn on the 2024 city and town boundaries.");
  await expect(page.getByTestId("measure-map-vintage")).toHaveAttribute("data-vintage", "2024");
  await expect(page.getByTestId("measure-map-vintage")).toHaveAttribute("data-marked", "false");
  const note = page.getByTestId("map-legend-boundary");
  await expect(note).toHaveText("City and town boundaries last updated 2024, the most recent held.");
  await expect(note).toHaveAttribute("data-marked", "false");
  const counts = await legendCounts(page);
  expect(counts.reduce((total, count) => total + count, 0)).toBe(texasPlaces.length);
  expect(counts.slice(0, -1).length).toBe(5);
  await expect(page.getByTestId("measure-map-legend").locator("li").last()).toHaveText("Uncoloured, no published value: 2 cities and towns");
  await expect(page.getByTestId("measure-map-uncoloured")).toHaveText("2 of 14 cities and towns have no published value and are left uncoloured, not shown as zero.");

  const all = page.getByTestId("measure-map-all");
  await expect(all.locator("thead th").first()).toHaveText("City or town");
  await expect(all.locator('tr[data-geo-id="state:48|place:48072"] td').first()).toHaveText("Withheld (suppressed)");
  await expect(all.locator('tr[data-geo-id="state:48|place:74144"] td').first()).toHaveText("Not published");
  await expect(page.getByTestId("measure-map-highest").getByRole("link", { name: "Austin city, Texas" })).toHaveAttribute("href", "/us/texas/austin-city");

  const canvas = page.getByTestId("measure-map-canvas");
  await expect(canvas).toHaveAttribute("data-geo-level", "PLACE");
  await expect(canvas).toHaveAttribute("data-tile-template", "/tiles/places/{z}/{x}/{y}?vintage=2024");
  await expect(canvas).toHaveAttribute("data-colored-values", "12");
  await expect(canvas).toHaveAttribute("data-map-ready", "true");
  await expect.poll(() => tileRequests.length).toBeGreaterThan(0);
  expect(tileRequests.every((request) => request.endsWith("?vintage=2024"))).toBe(true);
  if (SCREENSHOTS) {
    await page.waitForTimeout(1500);
    await page.locator("#map-heading").scrollIntoViewIfNeeded();
    await page.locator("section.analysis-panel").first().screenshot({ path: `${SCREENSHOTS}/place-map-texas.png` });
  }

  // An old period is drawn on the most recent boundaries too, marked with an
  // asterisk and the year they were last updated, never withheld.
  await page.getByTestId("measure-map-period").selectOption("2008-01-01");
  await expect(shell).toHaveAttribute("data-period", "2008-01-01 – 2012-12-31");
  await expect(page.getByTestId("measure-map-no-map")).toHaveCount(0);
  await expect(page.getByTestId("measure-map-canvas")).toHaveAttribute("data-tile-template", "/tiles/places/{z}/{x}/{y}?vintage=2024");
  await expect(page.getByTestId("measure-map-canvas")).toHaveAttribute("title", "City and town boundaries last updated 2024, the most recent held.");
  await expect(page.getByTestId("measure-map-vintage")).toHaveText(
    "* Drawn on the 2024 city and town boundaries, the most recent held; the values are for 2012, so a few cities and towns may differ from the shapes they were measured on.",
  );
  await expect(page.getByTestId("measure-map-vintage")).toHaveAttribute("data-marked", "true");
  await expect(note).toHaveText("* City and town boundaries last updated 2024, the most recent held.");
  await expect(note).toHaveAttribute("title", "City and town boundaries last updated 2024, the most recent held.");
  await expect(page.locator(".map-legend .legend-title")).toHaveText(/ \*$/);
  await expect(all.locator("tbody tr")).toHaveCount(14);

  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("the same measure drawn on metro areas, nationally, on the 2023 delineation", async ({ page }) => {
  const observationRequests = [];
  const tileRequests = [];
  await install(page, { observationRequests });
  await routeSyntheticTiles(page, "metros", metroLayer, tileRequests);
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.goto(`/map/${encodeURIComponent(INCOME)}`);
  const shell = page.getByTestId("measure-map");
  await expect(shell).toHaveAttribute("data-grain", "COUNTY");
  await expect(page.getByTestId("measure-map-state")).toHaveCount(0);

  await page.getByTestId("measure-map-grain").selectOption("METRO");
  await expect(shell).toHaveAttribute("data-grain", "METRO");
  await expect(page).toHaveURL(/grain=METRO/);
  await expect(page).not.toHaveURL(/state=/);
  expect(observationRequests.find((params) => params.geo_level === "METRO")?.state_fips).toBeUndefined();
  await expect(page.getByTestId("measure-map-vintage")).toHaveText(
    "* Drawn on the 2023 metro area boundaries, the most recent held; the values are for 2024, so a few metro areas may differ from the shapes they were measured on.",
  );
  await expect(page.getByTestId("map-legend-boundary")).toHaveText("* Metro area boundaries last updated 2023, the most recent held.");
  const counts = await legendCounts(page);
  expect(counts.reduce((total, count) => total + count, 0)).toBe(metroList.length);
  await expect(page.getByTestId("measure-map-uncoloured")).toHaveText("1 of 15 metro areas have no published value and are left uncoloured, not shown as zero.");
  const all = page.getByTestId("measure-map-all");
  await expect(all.locator('tr[data-geo-id="cbsa:41860"] td').first()).toHaveText("Not published");
  await expect(all.locator('tr[data-geo-id="cbsa:41860"] td').first()).not.toHaveText(/^0/);
  // Each metro links to its page by CBSA code (WEB-153).
  await expect(page.getByTestId("measure-map-highest").getByRole("link").first()).toHaveAttribute("href", /^\/metro\/\d{5}$/);

  const canvas = page.getByTestId("measure-map-canvas");
  await expect(canvas).toHaveAttribute("data-tile-template", "/tiles/metros/{z}/{x}/{y}?vintage=2023");
  await expect(canvas).toHaveAttribute("data-colored-values", "14");
  await expect(canvas).toHaveAttribute("data-map-ready", "true");
  await expect.poll(() => tileRequests.length).toBeGreaterThan(0);
  expect(tileRequests.every((request) => request.endsWith("?vintage=2023"))).toBe(true);
  if (SCREENSHOTS) {
    await page.waitForTimeout(1500);
    await page.locator("#map-heading").scrollIntoViewIfNeeded();
    await page.locator("section.analysis-panel").first().screenshot({ path: `${SCREENSHOTS}/place-map-metros.png` });
  }

  // A shared link opens on the grain it names.
  await page.goto(`/map/${encodeURIComponent(INCOME)}?grain=METRO`);
  await expect(shell).toHaveAttribute("data-grain", "METRO");
});

test("a grain the catalog does not publish is not offered", async ({ page }) => {
  await install(page);
  await page.goto(`/map/${encodeURIComponent(INCOME)}?grain=TRACT`);
  // Tracts are not among this measure's grains, so the county map opens.
  await expect(page.getByTestId("measure-map")).toHaveAttribute("data-grain", "COUNTY");
  await expect(page.getByTestId("measure-map-grain").locator("option")).not.toContainText(["Tracts"]);
  await expect(page).not.toHaveURL(/grain=/);
});

test("cities are drawn on the most recent boundaries held, marked with when they were last updated", async ({ page }) => {
  const tileRequests = [];
  await install(page);
  // OFFICE_LAB's case: the place layer holds only 2025 boundaries and the
  // newest ACS period ends in 2024. Registered after install(), so it wins.
  const only2025 = { ...tileLayers, layers: tileLayers.layers.map((layer) => (layer.layer_id === "places"
    ? { ...layer, tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2025", default_vintage: 2025,
      vintages: [{ boundary_vintage: 2025, feature_count: 32629, is_default: true, tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2025" }] }
    : layer)) };
  await page.route("**/api/v1/catalog/tile-layers", (route) => route.fulfill({ json: only2025 }));
  await routeSyntheticTiles(page, "places", placeLayer, tileRequests);
  await page.goto(`/map/${encodeURIComponent(INCOME)}?grain=PLACE&state=48`);
  await expect(page.getByTestId("measure-map-vintage")).toHaveText(
    "* Drawn on the 2025 city and town boundaries, the most recent held; the values are for 2024, so a few cities and towns may differ from the shapes they were measured on.",
  );
  await expect(page.getByTestId("map-legend-boundary")).toHaveText("* City and town boundaries last updated 2025, the most recent held.");
  await expect(page.getByTestId("measure-map-canvas")).toHaveAttribute("title", "City and town boundaries last updated 2025, the most recent held.");
  await expect(page.getByTestId("measure-map-vintage")).toHaveAttribute("data-vintage", "2025");
  await expect(page.getByTestId("measure-map-canvas")).toHaveAttribute("data-tile-template", "/tiles/places/{z}/{x}/{y}?vintage=2025");
  await expect(page.getByTestId("measure-map-canvas")).toHaveAttribute("data-colored-values", "12");
  if (SCREENSHOTS) {
    await page.waitForTimeout(1500);
    await page.locator("#map-heading").scrollIntoViewIfNeeded();
    await page.locator("section.analysis-panel").first().screenshot({ path: `${SCREENSHOTS}/place-map-texas-marked.png` });
  }
  // Thirteen years from the only held vintage still draws, on 2025, marked.
  await page.getByTestId("measure-map-period").selectOption("2008-01-01");
  await expect(page.getByTestId("measure-map-no-map")).toHaveCount(0);
  await expect(page.getByTestId("measure-map-canvas")).toHaveAttribute("data-tile-template", "/tiles/places/{z}/{x}/{y}?vintage=2025");
  await expect(page.getByTestId("measure-map-vintage")).toContainText("* Drawn on the 2025 city and town boundaries, the most recent held; the values are for 2012");
  await expect(page.getByTestId("measure-map-all").locator("tbody tr")).toHaveCount(14);
});

test("an older period of the cities is read from the release that published it, not from a few leftover rows", async ({ page }) => {
  const observationRequests = [];
  const tileRequests = [];
  await install(page, { observationRequests, releases: true });
  await routeSyntheticTiles(page, "places", placeLayer, tileRequests);
  // An address naming the leftover latest period opens on its release.
  await page.goto(`/map/${encodeURIComponent(INCOME)}?grain=PLACE&state=48&period=2018-01-01`);
  const shell = page.getByTestId("measure-map");
  await expect(shell).toHaveAttribute("data-period", "2018-01-01 – 2022-12-31");
  await expect(page.getByTestId("measure-map-period")).toHaveValue("release:2022");
  await expect(page.getByTestId("measure-map-period").locator("option")).toHaveText([
    "2020-01-01 – 2024-12-31",
    "2018-01-01 – 2022-12-31 (2022 release, as published)",
  ]);
  await expect(page).toHaveURL(/release=2022/);
  const read = observationRequests.find((params) => params.scope === "as_released");
  expect(read).toMatchObject({ release: "2022", geo_level: "PLACE", state_fips: "48" });
  expect(observationRequests.some((params) => params.period_start === "2018-01-01")).toBe(false);
  await expect(page.getByTestId("measure-map-uncoloured")).toHaveText("2 of 14 cities and towns have no published value and are left uncoloured, not shown as zero.");
  await expect(page.getByTestId("measure-map-canvas")).toHaveAttribute("data-colored-values", "12");
  await expect(page.getByTestId("measure-map-vintage")).toContainText("the values are for 2022");
});
