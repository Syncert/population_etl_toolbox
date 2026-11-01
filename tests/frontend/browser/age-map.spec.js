import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { polygonAround, routeSyntheticTiles } from "../support/syntheticTiles.js";
import { SCHEME } from "../support/ageScenarios.js";

// Covers: WEB-156 — the age indicators on the map (web-age-and-sex-structure,
// API-191): `/map/age/<indicator>` draws one indicator from
// `/age-structure/map` for every county (or metro area), and for cities and
// tracts one state at a time; the same equal-count bands and a legend that
// adds up; the boundary vintage named in the legend note and the map's
// tooltip, marked when it is not the period's year; the nation's own value
// named as a reference; a withheld value listed with its reason, never zero
// and never coloured; the measure select swaps the one indicator; the
// one-measure map's switcher finds the indicators by their words. Synthetic
// UI fixture values and shapes, not published statistics or boundaries.

const states = [
  { geo_id: "state:48", geo_level: "STATE", state_fips: "48", state_name: "Texas", latitude: 31.2, longitude: -99.3 },
  { geo_id: "state:55", geo_level: "STATE", state_fips: "55", state_name: "Wisconsin", latitude: 44.6, longitude: -89.9 },
];
// [county fips, name, lon, lat, median age, 65+ share]: null is withheld, undefined no row.
const wisconsin = [
  ["025", "Dane County", -89.42, 43.07, 35.4, 14.9],
  ["079", "Milwaukee County", -87.97, 43.01, 35.0, 15.1],
  ["009", "Brown County", -88.0, 44.47, 38.2, 16.2],
  ["133", "Waukesha County", -88.3, 43.02, 43.1, 21.0],
  ["105", "Rock County", -89.07, 42.67, 40.4, 18.9],
  ["139", "Winnebago County", -88.64, 44.07, 39.6, 18.0],
  ["087", "Outagamie County", -88.46, 44.42, 39.0, 16.8],
  ["101", "Racine County", -88.06, 42.75, 41.2, 19.4],
  ["059", "Kenosha County", -87.84, 42.58, 38.6, 16.0],
  ["073", "Marathon County", -89.76, 44.9, 41.0, 19.7],
  ["063", "La Crosse County", -91.12, 43.9, 37.1, 17.6],
  ["035", "Eau Claire County", -91.29, 44.73, 36.0, 16.5],
  ["078", "Menominee County", -88.67, 44.99, null, null],
  ["003", "Ashland County", -90.68, 46.32, undefined, undefined],
];
const counties = wisconsin.map(([fips, name]) => ({
  geo_id: `state:55|county:${fips}`, geo_level: "COUNTY", state_fips: "55", county_fips: fips, county_name: name, geo_name: `${name}, Wisconsin`, state_name: "Wisconsin",
}));
// [place fips, name, lon, lat, median age]
const wisconsinPlaces = [
  ["48000", "Madison city", -89.4, 43.07, 31.6],
  ["53000", "Milwaukee city", -87.91, 43.04, 32.4],
  ["31000", "Green Bay city", -88.02, 44.52, 35.5],
  ["40775", "La Crosse city", -91.24, 43.8, 29.4],
  ["22300", "Eau Claire city", -91.5, 44.81, 31.9],
  ["84675", "Waukesha city", -88.23, 43.01, 37.0],
];
const places = wisconsinPlaces.map(([fips, name]) => ({
  geo_id: `state:55|place:${fips}`, geo_level: "PLACE", state_fips: "55", place_fips: fips, place_name: name, geo_name: name, state_name: "Wisconsin",
}));

const tileLayers = {
  uncoloured_rule: "A geography with no published value is left uncoloured, never drawn as zero.",
  layers: [
    { layer_id: "counties", source_layer: "counties", grains: ["STATE", "COUNTY"], versioned: false, minzoom: 0, maxzoom: 12, join_key: "geo_id", name_field: "county_name",
      tile_template: "/tiles/counties/{z}/{x}/{y}", vintages: [], default_vintage: null, available: true },
    { layer_id: "places", source_layer: "places", grains: ["PLACE"], versioned: true, minzoom: 6, maxzoom: 14, join_key: "geo_id", name_field: "place_name",
      tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2025", default_vintage: 2025, available: true,
      vintages: [{ boundary_vintage: 2025, feature_count: 32629, is_default: true, tile_template: "/tiles/places/{z}/{x}/{y}?vintage=2025" }] },
  ],
};

const LABELS = { median_age: ["Median age", "years"], age_65_plus: ["65 and over (share of everyone)", "percent"] };

function answer(indicator, level, stateFips, items) {
  const [label, unit] = LABELS[indicator];
  return {
    indicator, label, unit, derived: true, dataset: "acs5", geo_level: level, state_fips: stateFips, release: "2023",
    period_start: "2019-01-01", period_end: "2023-12-31", scheme: { ...SCHEME, status: "approved" }, total: items.length, items,
  };
}

function ageMap(params) {
  const indicator = params.get("indicator");
  const level = params.get("geo_level");
  const column = indicator === "age_65_plus" ? 5 : 4;
  if (level === "NATIONAL") return answer(indicator, "NATIONAL", null, [{ geo_id: "us:1", value: indicator === "age_65_plus" ? "17.30" : "38.7", moe: indicator === "age_65_plus" ? "0.02" : "0.1", unavailable_reason: null }]);
  if (level === "COUNTY") {
    return answer(indicator, "COUNTY", null, wisconsin.filter((entry) => entry[column] !== undefined).map((entry) => ({
      geo_id: `state:55|county:${entry[0]}`,
      value: entry[column] === null ? null : String(entry[column]),
      moe: entry[column] === null ? null : "0.4",
      unavailable_reason: entry[column] === null && indicator === "age_65_plus" ? "suppressed_input" : null,
    })));
  }
  if (level === "PLACE") {
    return answer(indicator, "PLACE", params.get("state_fips"), wisconsinPlaces.map(([fips, , , , value]) => ({ geo_id: `state:55|place:${fips}`, value: String(value), moe: "0.6", unavailable_reason: null })));
  }
  return answer(indicator, level, params.get("state_fips"), []);
}

async function install(page, { ageRequests = [] } = {}) {
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    const path = url.pathname;
    if (path === "/api/v1/auth/refresh") return route.fulfill({ status: 401, json: { detail: "sign-in could not be completed" } });
    if (path === "/api/v1/catalog/tile-layers") return route.fulfill({ json: tileLayers });
    if (path === "/api/v1/catalog/geographies") {
      const level = params.get("geo_level");
      const items = level === "STATE" ? states : level === "COUNTY" ? counties : level === "PLACE" ? places.filter((item) => item.state_fips === params.get("state_fips")) : [];
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path === "/api/v1/age-structure/map") {
      ageRequests.push(Object.fromEntries(params));
      return route.fulfill({ json: ageMap(params) });
    }
    return route.fulfill({ status: 503, json: { detail: "No UI fixture for this resource" } });
  });
}

const countyLayer = [{ name: "counties", features: wisconsin.map(([fips, name, lon, lat]) => ({
  ring: polygonAround(lon, lat, 0.3), properties: { geo_id: `state:55|county:${fips}`, geo_level: "COUNTY", state_fips: "55", county_name: name },
})) }];
const placeLayer = [{ name: "places", features: wisconsinPlaces.map(([fips, name, lon, lat]) => ({
  ring: polygonAround(lon, lat, 0.2), properties: { geo_id: `state:55|place:${fips}`, geo_level: "PLACE", state_fips: "55", place_name: name, boundary_vintage: 2025 },
})) }];

async function legendCounts(page) {
  return page.getByTestId("age-map-legend").locator("li").evaluateAll((items) => items.map((item) => Number(item.getAttribute("data-count"))));
}

test("median age by county: equal-count bands, the nation as reference, a gap listed and never coloured", async ({ page }) => {
  const ageRequests = [];
  await install(page, { ageRequests });
  await routeSyntheticTiles(page, "counties", countyLayer);
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.goto("/map/age/median_age");

  const shell = page.getByTestId("age-map");
  await expect(shell).toHaveAttribute("data-grain", "COUNTY");
  await expect(shell).toHaveAttribute("data-release", "2023");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Median age");
  await expect(page.getByRole("heading", { name: "Median age, 2019–2023" })).toBeVisible();
  await expect(page.getByTestId("age-map-nation")).toHaveText("United States, 2019–2023: 38.7 years (± 0.1).");
  await expect(page.getByTestId("age-map-vintage")).toHaveText("Drawn on the current county boundaries.");
  await expect(page.getByTestId("age-map-canvas")).toHaveAttribute("title", "County boundaries: current.");

  // Fourteen counties listed; twelve valued; the legend adds up to all of them.
  const counts = await legendCounts(page);
  expect(counts.reduce((sum, count) => sum + count, 0)).toBe(14);
  expect(counts.at(-1)).toBe(2);
  await expect(page.getByTestId("age-map-uncoloured")).toHaveText("2 of 14 counties have no published value and are left uncoloured, not shown as zero.");
  await expect(page.getByTestId("age-map-canvas")).toHaveAttribute("data-colored-values", "12");
  await expect(page.getByTestId("age-map-highest").locator("tbody tr").first()).toContainText("Waukesha County, Wisconsin");
  await expect(page.getByTestId("age-map-highest").locator("tbody tr").first()).toContainText("43.1 years");
  const menominee = page.getByTestId("age-map-all").locator("tr[data-geo-id='state:55|county:078']");
  await expect(menominee).toContainText("Not available");
  await expect(menominee).not.toContainText("0.0");
  await expect(page.getByTestId("age-map-all").locator("tr[data-geo-id='state:55|county:003']")).toContainText("No value published");
  await expect(page.getByTestId("age-map-all").locator("tbody tr a").first()).toHaveAttribute("href", /^\/us\/wisconsin\//);

  // One read of the grain and one of the nation; no catalog measure is asked for.
  expect(ageRequests).toEqual(expect.arrayContaining([
    expect.objectContaining({ indicator: "median_age", geo_level: "NATIONAL" }),
    expect.objectContaining({ indicator: "median_age", geo_level: "COUNTY" }),
  ]));
  expect(ageRequests).toHaveLength(2);

  const results = await new AxeBuilder({ page }).include("[data-testid='age-map']").withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("the measure select swaps the one indicator, and a withheld share says why", async ({ page }) => {
  await install(page);
  await routeSyntheticTiles(page, "counties", countyLayer);
  await page.goto("/map/age/median_age");
  await expect(page.getByTestId("age-map")).toHaveAttribute("data-indicator", "median_age");
  await page.getByTestId("age-map-indicator").selectOption("age_65_plus");
  await expect(page).toHaveURL(/\/map\/age\/age_65_plus$/);
  await expect(page.getByTestId("age-map")).toHaveAttribute("data-indicator", "age_65_plus");
  await expect(page.getByTestId("age-map-nation")).toHaveText("United States, 2019–2023: 17.3% (± 0.02).");
  await expect(page.getByTestId("age-map-all").locator("tr[data-geo-id='state:55|county:078']")).toContainText(
    "The Census Bureau withheld one of the published lines this group is made of",
  );
});

test("cities are drawn one state at a time, on the newest boundaries, marked with their year", async ({ page }) => {
  const ageRequests = [];
  await install(page, { ageRequests });
  await routeSyntheticTiles(page, "places", placeLayer);
  await page.goto("/map/age/median_age?grain=PLACE");
  await expect(page.getByTestId("age-map-choose-state")).toBeVisible();
  expect(ageRequests.filter((params) => params.geo_level === "PLACE")).toEqual([]);
  await page.getByTestId("age-map-state").selectOption("55");
  await expect(page.getByTestId("age-map")).toHaveAttribute("data-state", "55");
  await expect(page).toHaveURL(/\/map\/age\/median_age\?grain=PLACE&state=55$/);
  expect(ageRequests.find((params) => params.geo_level === "PLACE")).toMatchObject({ state_fips: "55" });
  await expect(page.locator(".section-kicker").first()).toHaveText("Age and sex, every city or town in Wisconsin");
  await expect(page.getByTestId("age-map-vintage")).toHaveAttribute("data-marked", "true");
  await expect(page.getByTestId("age-map-vintage")).toContainText("* Drawn on the 2025 city and town boundaries, the most recent held; the values are for 2023");
  await expect(page.getByTestId("map-legend-boundary")).toHaveText("* City and town boundaries last updated 2025, the most recent held.");
  await expect(page.getByTestId("age-map-canvas")).toHaveAttribute("title", "City and town boundaries last updated 2025, the most recent held.");
});

test("an indicator the API does not serve is not a page", async ({ page }) => {
  await install(page);
  const response = await page.goto("/map/age/B01001_011");
  expect(response?.status()).toBe(404);
});
