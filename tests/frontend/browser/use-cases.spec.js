import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { scenarios, installUseCaseFixtures } from "../support/useCaseScenarios.js";

// Covers: WEB-123, WEB-025, WEB-111 — all twenty pages, nested keyboard
// navigation, exact scenario selections, source guardrails, chart/table
// alternatives, source-grain gaps, failures, mobile layout, and accessibility.
const screenshotDirectory = process.env.USE_CASE_SCREENSHOT_DIR;

for (const example of scenarios) {
  test(`use case ${example.rank}: ${example.id} answers its example scenario`, async ({ page }) => {
    await installUseCaseFixtures(page);
    await page.setViewportSize({ width: 1440, height: 1100 });
    const params = new URLSearchParams({ place: example.geoId });
    if (example.grain !== "COUNTY") params.set("grain", example.grain);
    await page.goto(`/use-cases/${example.id}?${params}`);
    await expect(page.locator("main")).toHaveCount(1);
    await expect(page.getByRole("heading", { level: 1 })).toHaveCount(1);
    await expect(page.getByTestId("use-case-hero")).toBeVisible();
    await expect(page.getByTestId("template-limits")).toBeVisible();
    if (example.metric) {
      await expect(page.getByTestId("use-case-measure")).toBeEnabled();
      await page.getByTestId("use-case-measure").selectOption(example.metric);
      await expect(page.getByTestId("use-case-history-status")).toContainText("6 historical observations");
      await expect(page.getByTestId("profile-product")).toHaveAttribute("data-geo-id", example.geoId);
      await expect(page.getByRole("img", { name: /6 time-series/ })).toBeVisible();
      await page.getByRole("button", { name: "Table", exact: true }).click();
      await expect(page.getByTestId("use-case-visualizations").locator(".use-case-table tbody tr")).toHaveCount(6);
      await page.getByRole("button", { name: "Trend", exact: true }).click();
      if ([3, 12, 16, 17].includes(example.rank)) {
        await page.getByRole("button", { name: "Peers", exact: true }).click();
        await page.getByRole("combobox", { name: "Add a peer" }).selectOption(example.grain === "STATE" ? "state:27" : "state:55|county:105");
        await page.getByRole("textbox", { name: "Why these peers?" }).fill(example.grain === "STATE" ? "Neighboring Upper Midwest states; same measure, own published periods." : "Neighboring Wisconsin counties; same measure and geography grain.");
        await expect(page.getByTestId("use-case-peer-chart")).toHaveAttribute("data-bar-count", "2");
        await expect(page.getByRole("table", { name: "Selected peer evidence" }).locator("tbody tr")).toHaveCount(2);
      }
    } else {
      await expect(page.getByTestId("quality-explorer")).toHaveAttribute("data-source-count", "7");
      await page.getByTestId("quality-select-BLS").click();
      await expect(page.getByTestId("quality-metrics-panel")).toBeVisible();
    }
    if (example.rank === 1) {
      await expect(page.getByTestId("source-report-health").locator("tbody tr")).toHaveCount(6);
      const safety = page.getByTestId("source-report-safety");
      await safety.getByRole("button", { name: "Load Wisconsin state report" }).click();
      await expect(safety.locator("tbody tr")).toHaveCount(6);
      await expect(safety).toContainText("these are not Dane County, Wisconsin figures");
      // Covers: WEB-124 — the county selection offers the derived roll-up,
      // explicitly run, labeled derived, with the multi-county agency
      // counted in full and the non-additivity consequence stated.
      const rollup = page.getByTestId("county-crime-rollup");
      await expect(rollup.locator("table")).toHaveCount(0);
      await rollup.getByRole("button", { name: "Load derived county roll-up" }).click();
      await expect(rollup.locator("tbody tr")).toHaveCount(6);
      await expect(rollup).toContainText("derived sum");
      await expect(rollup).toContainText("WI0130000, WI0137000, WI0540300");
      await expect(page.getByTestId("county-rollup-coverage")).toContainText("3 of 3 mapped agencies reported");
      await expect(page.getByTestId("county-rollup-multi-county")).toContainText("not additive to state totals");
      await expect(page.getByTestId("county-rollup-caveats")).toContainText("not a provider-published county figure");
    }
    if ([1, 2].includes(example.rank)) {
      await page.getByRole("button", { name: "Run population scenario" }).click();
      await expect(page.getByTestId("population-scenario").locator("tbody tr")).toHaveCount(10);
      await expect(page.getByTestId("population-scenario")).toContainText("2034");
    }
    const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
    expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
    if (screenshotDirectory) {
      mkdirSync(screenshotDirectory, { recursive: true });
      await page.evaluate((text) => {
        const banner = document.createElement("p");
        banner.className = "use-case-guardrail";
        banner.textContent = `Example scenario: ${text}. Deterministic UI fixture — illustrative values, not live statistics.`;
        document.querySelector(".use-case-hero").after(banner);
      }, example.scenario);
      await page.evaluate(async () => {
        window.scrollTo({ top: 0, behavior: "instant" });
        await new Promise((done) => requestAnimationFrame(() => requestAnimationFrame(done)));
      });
      await page.screenshot({ path: resolve(screenshotDirectory, `${String(example.rank).padStart(2, "0")}-${example.id}.png`), fullPage: true });
      writeFileSync(resolve(screenshotDirectory, "scenarios.json"), JSON.stringify(scenarios, null, 2));
    }
    await page.setViewportSize({ width: 390, height: 844 });
    await expect(page.getByTestId("template-limits")).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  });
}

test("the directory and nested top menu reach all twenty pages by keyboard", async ({ page }) => {
  await installUseCaseFixtures(page);
  await page.goto("/use-cases");
  await expect(page.getByTestId("use-case-link")).toHaveCount(20);
  const audit = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(audit.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
  await page.getByRole("searchbox", { name: "Search use cases" }).fill("housing");
  await expect(page.getByTestId("use-case-link")).toHaveCount(1);
  await page.getByRole("searchbox", { name: "Search use cases" }).fill("no-such-topic");
  await expect(page.getByText(/No use cases match/)).toBeVisible();
  const menu = page.getByTestId("use-case-menu");
  await menu.locator(":scope > summary").focus();
  await page.keyboard.press("Enter");
  await expect(menu).toHaveAttribute("open", "");
  for (const summary of await menu.locator(".use-case-submenu > summary").all()) { await summary.focus(); await page.keyboard.press("Enter"); }
  await expect(menu.locator('a[href^="/use-cases/"]')).toHaveCount(20);
  await page.keyboard.press("Escape");
  await expect(menu).not.toHaveAttribute("open", "");
  await expect(menu.locator(":scope > summary")).toBeFocused();
  await page.setViewportSize({ width: 390, height: 844 });
  await menu.locator(":scope > summary").click();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
});

test("a stratified health answer stays tabular and a history failure remains explicit", async ({ page }) => {
  await installUseCaseFixtures(page, { stratified: true });
  await page.goto("/use-cases/disease-capacity-watch?grain=STATE&place=state%3A55");
  await expect(page.getByTestId("use-case-history-status")).toContainText("12 historical observations");
  await expect(page.getByText(/These rows describe separate series/)).toBeVisible();
  await expect(page.getByRole("img", { name: /time-series/ })).toHaveCount(0);
  await expect(page.getByTestId("use-case-visualizations").locator(".use-case-table tbody tr")).toHaveCount(12);
  await installUseCaseFixtures(page, { failHistory: true });
  await page.goto("/use-cases/population-growth?place=state%3A55%7Ccounty%3A025");
  await expect(page.getByTestId("use-case-history-status")).toContainText("503");
  await expect(page.getByText("The source could not be read. See the error above.")).toBeVisible();
});

test("national context is never relabeled as a county figure and exports retain withholding", async ({ page }) => {
  await installUseCaseFixtures(page, { withheld: true });
  await page.goto("/use-cases/cost-of-living?place=state%3A55%7Ccounty%3A025");
  await page.getByTestId("use-case-measure").selectOption("FRED:CPIAUCSL");
  await expect(page.getByTestId("use-case-history-status")).toContainText("not published at COUNTY");
  await page.goto("/use-cases/population-growth?place=state%3A55%7Ccounty%3A025");
  await expect(page.getByTestId("use-case-history-status")).toContainText("6 historical observations");
  const [download] = await Promise.all([page.waitForEvent("download"), page.getByRole("button", { name: "Export history" }).click()]);
  const chunks = [];
  for await (const chunk of await download.createReadStream()) chunks.push(chunk);
  const csv = Buffer.concat(chunks).toString("utf8");
  expect(csv).toContain('"suppressed"');
  expect(csv).toContain("newest_release_per_period=true");
  expect(csv).toContain("api_query");
});

test("a missing peer remains in the evidence and export with its selection criteria", async ({ page }) => {
  await installUseCaseFixtures(page);
  await page.route("**/api/v1/observations?**", (route) => {
    const params = new URL(route.request().url()).searchParams;
    if (params.get("geo_id") !== "state:27") return route.fallback();
    return route.fulfill({ json: { total: 0, offset: 0, limit: 1, items: [] } });
  });
  await page.goto("/use-cases/peer-benchmarking?grain=STATE&place=state%3A55");
  await expect(page.getByTestId("use-case-history-status")).toContainText("6 historical observations");
  await page.getByRole("button", { name: "Peers", exact: true }).click();
  await page.getByRole("combobox", { name: "Add a peer" }).selectOption("state:27");
  await page.getByRole("textbox", { name: "Why these peers?" }).fill("Neighboring states; retain unpublished peers.");
  const evidence = page.getByRole("table", { name: "Selected peer evidence" });
  await expect(evidence.locator("tbody tr")).toHaveCount(2);
  await expect(evidence).toContainText("Not published for this peer.");
  await expect(page.getByTestId("use-case-peer-chart")).toHaveAttribute("data-bar-count", "1");
  const [download] = await Promise.all([page.waitForEvent("download"), page.getByRole("button", { name: "Export selected peers" }).click()]);
  const chunks = [];
  for await (const chunk of await download.createReadStream()) chunks.push(chunk);
  const csv = Buffer.concat(chunks).toString("utf8");
  expect(csv).toContain('"state:27"');
  expect(csv).toContain("Not published for this peer.");
  expect(csv).toContain("Neighboring states; retain unpublished peers.");
  expect(csv).toContain("api_query");
  await page.getByTestId("use-case-measure").selectOption("CDC:cdi:ALC06:AGEADJPREV");
  await expect(page.getByText(/This source does not declare an aligned value per geography/)).toBeVisible();
  await expect(page.getByTestId("use-case-peer-chart")).toHaveCount(0);
});

test("housing cards show their requested dollar and household measures, never a population fallback", async ({ page }) => {
  await installUseCaseFixtures(page);
  await page.goto("/use-cases/housing-affordability?place=state%3A55%7Ccounty%3A025");
  await expect(page.getByTestId("measure-value-median-gross-rent")).toHaveText("1,456 dollars");
  await expect(page.getByTestId("measure-value-median-monthly-housing-cost")).toHaveText("1,848 dollars");
  await expect(page.getByTestId("measure-median-monthly-housing-cost")).toContainText("CENSUS_ACS:acs5:B25105_001");
  await expect(page.getByTestId("measure-value-rent-share-of-income")).toHaveText("97,440 households");
  await expect(page.getByTestId("measure-value-occupancy")).toHaveText("280,000 housing units");
  await expect(page.getByTestId("measure-median-home-value")).toContainText("CENSUS_ACS:acs5:B25077_001");
});

test("a housing response for a different metric is refused in both the card and history", async ({ page }) => {
  await installUseCaseFixtures(page);
  await page.route("**/api/v1/observations?**", (route) => {
    const params = new URL(route.request().url()).searchParams;
    if (params.get("metric_code") !== "CENSUS_ACS:acs5:B25064_001") return route.fallback();
    return route.fulfill({ json: { total: 1, items: [{ metric_code: "CENSUS_ACS:acs5:B01003_001", source_code: "CENSUS_ACS", geo_id: params.get("geo_id"), period_start: "2024", value: "616000", unit: "people" }] } });
  });
  await page.goto("/use-cases/housing-affordability?place=state%3A55%7Ccounty%3A025");
  await expect(page.getByTestId("measure-answer-median-gross-rent")).toContainText("different metric");
  await expect(page.getByTestId("measure-value-median-gross-rent")).not.toContainText("616,000");
  await expect(page.getByTestId("use-case-history-status")).toContainText("different metric");
  await expect(page.getByRole("img", { name: /time-series/ })).toHaveCount(0);
});

test("population scenario assumptions reach the API, export, and changing place clears the answer", async ({ page }) => {
  await installUseCaseFixtures(page);
  await page.goto("/use-cases/population-growth?place=state%3A55%7Ccounty%3A025");
  await page.getByRole("spinbutton", { name: "Assumed annual population change (%)" }).fill("-1");
  await page.getByRole("spinbutton", { name: "Years after baseline" }).fill("20");
  await page.getByRole("button", { name: "Run population scenario" }).click();
  const scenario = page.getByTestId("population-scenario");
  await expect(scenario.locator("tbody tr")).toHaveCount(20);
  await expect(scenario).toContainText("2044");
  await expect(scenario).toContainText("-1% per year for 20 years");
  const [download] = await Promise.all([page.waitForEvent("download"), page.getByRole("button", { name: "Export derived scenario" }).click()]);
  const chunks = [];
  for await (const chunk of await download.createReadStream()) chunks.push(chunk);
  const csv = Buffer.concat(chunks).toString("utf8");
  expect(csv).toContain("compound-growth-v1");
  expect(csv).toContain("annual_change_percent=-1");
  expect(csv).toContain("horizon_years=20");
  await page.getByTestId("profile-place").selectOption("state:55|county:105");
  await expect(scenario.locator("tbody tr")).toHaveCount(0);
});
