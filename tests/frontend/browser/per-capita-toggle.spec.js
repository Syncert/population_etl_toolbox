import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { servedParameters } from "../support/servedContract.js";
import { DERIVED_RATIOS, installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-145 — per-capita and share views (web-per-capita-toggle,
// ADR-0009): a chapter switches its counts between the count, a per-capita
// view and a share only where a served pairing allows it; each derived value
// names its denominator, its period and its margin's method beside the
// county, its state and the nation; a value the API could not compute reads
// as its reason; an approved pairing carries no draft mark, and a draft one
// is marked "Draft view awaiting review"; the
// FBI's rate names its denominator, and a county reads as unpublished rather
// than as a county rate the FBI never published; the county map offers the
// same views and keeps the view in its address. The pairings are the
// registry's, all approved by Nick on 2026-10-31; the map test serves them as
// drafts, as DERIVED_RATIOS_SERVE_DRAFTS=1 would before an approval;
// every value is a synthetic UI fixture, not a published statistic.

async function ready(page, path) {
  await installPlaceFixtures(page);
  await page.goto(path);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
}

async function noViolations(page) {
  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
}

test("reported violent crime: a county reads against its state on the FBI's own denominator", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const safety = page.getByTestId("chapter-safety");
  await safety.scrollIntoViewIfNeeded();
  // Covers: WEB-150 — the FBI publishes no county figure and nothing divides
  // its counts by a Census population (ADR-0009 rule 2); the county's rate is
  // computed from its own agencies' reports (ADR-0012) and says, beside the
  // figure, how many people they cover and what share that is.
  const county = page.getByTestId("card-violent-crime-rate-county");
  await expect(county).toContainText("Dane County");
  const rate = page.getByTestId("card-violent-crime-rate-county-agency-rate");
  await expect(rate).toHaveAttribute("data-available", "true");
  await expect(rate).toHaveAttribute("data-derived", "true");
  await expect(rate.locator("strong")).toHaveText("11.24 per 100,000 people covered by reporting agencies in this county");
  await expect(rate).toContainText("computed from agency reports");
  await expect(page.getByTestId("card-violent-crime-rate-county-agency-rate-coverage")).toHaveText(" · covers 115,648 people, 100 % of the population its agencies serve");
  await expect(rate).toHaveAttribute("title", "2 reporting agencies; 1 agency left out because it also serves other counties");
  await expect(rate.locator("[data-pairing-status='draft']")).toHaveText("(Draft view awaiting review)");
  await expect(county).not.toContainText("Not published at county grain");
  await expect(page.getByTestId("card-violent-crime-rate-state")).toContainText("Wisconsin");
  const denominator = page.getByTestId("card-violent-crime-rate-denominator");
  await expect(denominator).toHaveText(/Per 100,000 people, denominator: population covered by reporting agencies, as the publisher computed it\./);
  await expect(denominator.locator("[data-pairing-status='draft']")).toHaveCount(0);
  // No count of offenses is on the page to divide, so no toggle is offered.
  await expect(page.getByTestId("chapter-safety-view")).toHaveCount(0);
  await safety.screenshot({ path: "test-results/per-capita/county-violent-crime.png" });
});

test("a county whose agencies cover too few people reads as refused, with its coverage", async ({ page }) => {
  // Covers: WEB-150 — under the 50 % floor the month has no rate, never a
  // zero, and still says whom the agencies that did report cover.
  await page.setViewportSize({ width: 390, height: 900 });
  await ready(page, "/us/wisconsin/rock-county");
  const rate = page.getByTestId("card-violent-crime-rate-county-agency-rate");
  await rate.scrollIntoViewIfNeeded();
  await expect(rate).toHaveAttribute("data-available", "false");
  await expect(rate).toHaveAttribute("data-unavailable-reason", "coverage_below_floor");
  await expect(rate.locator("strong")).toHaveText("Too few agencies reported to give a rate");
  await expect(page.getByTestId("card-violent-crime-rate-county-agency-rate-coverage")).toHaveText(" · covers 9,000 people, 21 % of the population its agencies serve");
  await expect(rate).not.toContainText(/\b0 per 100,000/);
  await expect(rate).toHaveAttribute("title", "1 reporting agency; 2 agencies that did not report; 1 agency left out because it also serves other counties");
  await page.getByTestId("chapter-safety").screenshot({ path: "test-results/per-capita/county-agency-rate-refused.png" });
  const results = await new AxeBuilder({ page }).include("[data-testid='chapter-safety']").withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
});

test("a share names its denominator for the county, its state and the nation", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const toggle = page.getByTestId("chapter-people-view");
  await expect(toggle.getByRole("button")).toHaveText(["Count", "Share"]);
  await expect(toggle.getByRole("button", { name: "Count" })).toHaveAttribute("aria-pressed", "true");
  await toggle.getByRole("button", { name: "Share" }).click();
  await expect(toggle.getByRole("button", { name: "Share" })).toHaveAttribute("aria-pressed", "true");

  const row = page.getByTestId("depth-under-18");
  await expect(row.locator("dt")).toContainText("Share of residents under 18");
  await expect(row.locator("dt")).toContainText("Universe: total population (survey)");
  for (const [level, name, value] of [["county", "Dane County", "10.1 percent"], ["state", "Wisconsin", "11 percent"], ["national", "United States", "70 percent"]]) {
    const line = page.getByTestId(`depth-under-18-${level}`);
    await expect(line).toContainText(`${name}:`);
    await expect(line).toContainText(value);
    await expect(line).toContainText("of total population (survey)");
    await expect(line).toContainText("2019-01-01 – 2023-12-31");
    await expect(line).toContainText("margin of error ±0.8 (Census Bureau formula for a derived proportion)");
    await expect(line.locator("[data-pairing-status='draft']")).toHaveCount(0);
  }
  // A measure the registry pairs with nothing keeps its count, and says so.
  await expect(page.getByTestId("chapter-people").locator("[data-testid$='-count-only']").first()).toContainText("showing the count");
  await page.getByTestId("chapter-people").screenshot({ path: "test-results/per-capita/county-people-share.png" });
  await noViolations(page);

  await toggle.getByRole("button", { name: "Count" }).click();
  await expect(page.getByTestId("depth-under-18").locator("dt")).not.toContainText("Share of residents under 18");
});

test("each share shows as it arrives; a slow state read never holds up the county", async ({ page }) => {
  await installPlaceFixtures(page);
  let release;
  const held = new Promise((resolve) => { release = resolve; });
  // Registered after the fixtures, so it is asked first: the state's reads
  // wait until the county's answer is already on the page.
  await page.route((url) => url.pathname === "/api/v1/observations/derived", async (route) => {
    if (new URL(route.request().url()).searchParams.get("geo_id") === "state:55") await held;
    await route.fallback();
  });
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  await page.getByTestId("chapter-people-view").getByRole("button", { name: "Share" }).click();
  await expect(page.getByTestId("depth-under-18-county")).toContainText("10.1 percent");
  await expect(page.getByTestId("depth-under-18-state")).toContainText("Reading…");
  release();
  await expect(page.getByTestId("depth-under-18-state")).toContainText("11 percent");
  // Switching back and forth keeps what was read rather than reading it again.
  await page.getByTestId("chapter-people-view").getByRole("button", { name: "Count" }).click();
  await page.getByTestId("chapter-people-view").getByRole("button", { name: "Share" }).click();
  await expect(page.getByTestId("depth-under-18-county")).toContainText("10.1 percent");
});

test("a share the API cannot compute reads as its reason, never as zero", async ({ page }) => {
  await ready(page, "/us/wisconsin/rock-county");
  await page.getByTestId("chapter-people-view").getByRole("button", { name: "Share" }).click();
  const county = page.getByTestId("depth-under-18-county");
  await expect(county).toContainText("No denominator for this period and release");
  await expect(county.locator("[data-ratio-id]")).toHaveAttribute("data-unavailable-reason", "different_period");
  await expect(county).not.toContainText(/\b0 percent/);
});

test("births per 1,000 residents are the Census Bureau's own rate, named as such", async ({ page }) => {
  await ready(page, "/us/wisconsin/dane-county");
  const toggle = page.getByTestId("chapter-change-view");
  await expect(toggle.getByRole("button")).toHaveText(["Count", "Per 1,000 residents"]);
  await toggle.getByRole("button", { name: "Per 1,000 residents" }).click();
  const births = page.getByTestId("depth-births-county");
  await expect(births).toContainText("per 1,000 residents");
  await expect(births).toContainText("denominator: average resident population over the estimate year (PEP), as the publisher computed it");
  await expect(births.locator("[data-ratio-id]")).toHaveAttribute("data-derived", "false");
});

const POVERTY = "CENSUS_ACS:acs5:B17001_002";
const counties = [["001", "Adams County"], ["025", "Dane County"], ["105", "Rock County"]].map(([fips, name]) => ({
  geo_id: `state:55|county:${fips}`, geo_level: "COUNTY", state_fips: "55", county_fips: fips, county_name: name, state_name: "Wisconsin" }));

async function installMap(page) {
  await page.route("**/api/v1/**", (route) => {
    const url = new URL(route.request().url());
    const params = url.searchParams;
    const path = url.pathname;
    if (path === "/api/v1/auth/refresh") return route.fulfill({ status: 401, json: { detail: "sign-in could not be completed" } });
    if (path === "/api/v1/catalog/capabilities") return route.fulfill({ json: { total: 1, items: [{ source_code: "CENSUS_ACS", display_name: "CENSUS_ACS", route_segment: null, served_by_neutral_routes: true,
      publishes_value_status: true, publishes_aligned_reduction: true, observation_filters: ["geo_id", "geo_level", "state_fips"], observation_dimensions: [],
      observation_routes: ["/api/v1/observations", "/api/v1/observations/periods"].map((routePath) => ({ path: routePath, parameters: servedParameters(routePath) })) }] } });
    if (path === "/api/v1/catalog/sources") return route.fulfill({ json: [{ source_code: "CENSUS_ACS", source_name: "American Community Survey" }] });
    if (path === "/api/v1/catalog/geographies") {
      const items = params.get("geo_level") === "COUNTY" ? counties : params.get("geo_level") === "STATE" ? [{ geo_id: "state:55", geo_level: "STATE", state_fips: "55", state_name: "Wisconsin" }] : [];
      return route.fulfill({ json: { total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path === `/api/v1/catalog/metrics/${encodeURIComponent(POVERTY)}` || path === `/api/v1/catalog/metrics/${POVERTY}`) return route.fulfill({ json: {
      metric_code: POVERTY, metric_display_name: "People below the poverty level (UI fixture)", metric_label: "People below the poverty level", metric_label_status: "draft",
      source_code: "CENSUS_ACS", units: null, valid_geo_grains: ["COUNTY", "STATE", "NATIONAL"], measure_kind: "count", aggregation_characteristic: "additive" } });
    if (path === "/api/v1/catalog/derived-ratios") {
      const items = DERIVED_RATIOS.filter((ratio) => ratio.numerator === params.get("numerator")).map((ratio) => ({ ...ratio, status: "draft" }));
      return route.fulfill({ json: { total: items.length, items } });
    }
    if (path === "/api/v1/observations/periods") return route.fulfill({ json: { metric_code: POVERTY, total: 1, limit: 200, offset: 0, items: [{ period_start: "2019-01-01", period_end: "2023-12-31", observation_count: 3 }] } });
    const period = { period_start: "2019-01-01", period_end: "2023-12-31", release: "2023" };
    if (path === "/api/v1/observations/derived") {
      const ratio = { ...DERIVED_RATIOS.find((item) => item.ratio_id === params.get("ratio_id")), status: "draft" };
      const items = Object.entries({ "001": "9.10", "025": "10.42", "105": null }).map(([fips, value]) => ({
        ratio_id: ratio.ratio_id, derived: true, pairing_status: "draft", pairing_version: 1, geo_id: `state:55|county:${fips}`, geo_level: "COUNTY", ...period,
        value, unit: "percent", scale: 100, margin_of_error: value ? "0.81" : null, margin_of_error_method: value ? "proportion" : "unavailable",
        unavailable_reason: value ? null : "suppressed_input", numerator: null, denominator: null, provider_rate: null }));
      return route.fulfill({ json: { ratio, scope: "latest", release: null, total: items.length, limit: 1000, offset: 0, items } });
    }
    if (path === "/api/v1/observations") {
      const items = Object.entries({ "001": "1200", "025": "56000", "105": "17000" }).map(([fips, value]) => ({ metric_code: POVERTY, source_code: "CENSUS_ACS", geo_id: `state:55|county:${fips}`, geo_level: "COUNTY",
        value, value_status: "valid", unit: "people", ...period, dimensions: {}, uncertainty: { margin_of_error: "300" } }));
      return route.fulfill({ json: { metric_code: POVERTY, total: items.length, offset: 0, limit: 1000, items } });
    }
    return route.fulfill({ status: 503, json: { detail: "No UI fixture for this resource" } });
  });
}

test("the county map shows a count as a share, names the denominator, and keeps the view in the address", async ({ page }) => {
  await installMap(page);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(`/map/${encodeURIComponent(POVERTY)}`);
  await expect(page.getByTestId("measure-map")).toHaveAttribute("data-view", "count");
  await expect(page.getByTestId("measure-map-all").locator('tr[data-geo-id="state:55|county:025"] td').first()).toContainText("56,000");

  await page.getByTestId("measure-map-view").getByRole("button", { name: "Share" }).click();
  await expect(page.getByTestId("measure-map")).toHaveAttribute("data-view", "share");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Share of people below the poverty level");
  await expect(page.getByTestId("measure-map-denominator")).toContainText("Every value is percent of people whose poverty status is determined");
  await expect(page.getByTestId("measure-map-draft")).toHaveText("(Draft view awaiting review)");
  const all = page.getByTestId("measure-map-all");
  await expect(all.locator("thead")).toContainText("Percent of people whose poverty status is determined");
  await expect(all.locator('tr[data-geo-id="state:55|county:025"] td').first()).toHaveText("10.42 percent");
  await expect(all.locator('tr[data-geo-id="state:55|county:105"] td').first()).toHaveText("An input is withheld or not a usable number");
  await expect(page.getByTestId("measure-map-coverage")).toContainText("people whose poverty status is determined");
  await expect.poll(() => new URL(page.url()).searchParams.get("view")).toBe("share");
  await page.screenshot({ path: "test-results/per-capita/county-map-share.png", fullPage: true });

  // The address reopens the same view.
  await page.reload();
  await expect(page.getByTestId("measure-map")).toHaveAttribute("data-view", "share");
  await noViolations(page);
});
