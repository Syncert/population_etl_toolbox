import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-129 — two counties side by side by chapter, one shared period
// per row with state and nation marks, preflight refusals listed rather than
// hidden, swapping sides, and mixed grains offered the parent instead.

const PAIR = "/us/wisconsin/dane-county/vs/us/wisconsin/rock-county";

test("two counties compare chapter by chapter, and a refused measure is listed", async ({ page }) => {
  await installPlaceFixtures(page, { nationLagsMedianAge: false });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto(PAIR);
  await expect(page.getByTestId("compare-places")).toHaveAttribute("data-ready", "true");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Dane County, Wisconsin and Rock County, Wisconsin");
  const population = page.getByTestId("compare-row-population-estimate");
  await expect(population).toHaveAttribute("data-period", "2024-01-01 – 2024-12-31");
  await expect(population.locator(".paired-bar")).toHaveCount(2);
  await expect(population).toContainText("Reference marks: Wisconsin");
  await expect(page.getByTestId("compare-chapter-people-trend")).toBeVisible();
  // FBI rates are not published for counties at all; preflight refuses
  // nothing here because they are never offered. NASS has no Dane cell.
  await expect(page.getByTestId("compare-refused-corn-acres-harvested")).toContainText("No published value for Dane County, Wisconsin.");
  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("a measure preflight refuses appears under Not comparable here with its reason", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.goto("/us/wisconsin/vs/us/minnesota");
  await expect(page.getByTestId("compare-places")).toHaveAttribute("data-ready", "true");
  await expect(page.getByTestId("compare-refused-violent-crime-rate")).toContainText("FBI UCR subjects are not canonical geographies (UI fixture)");
  await expect(page.getByTestId("compare-row-violent-crime-rate")).toHaveCount(0);
});

test("swapping exchanges the sides and the address", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.goto(PAIR);
  await page.getByTestId("compare-places-swap").click();
  await expect(page).toHaveURL(/\/us\/wisconsin\/rock-county\/vs\/us\/wisconsin\/dane-county$/);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Rock County, Wisconsin and Dane County, Wisconsin");
});

test("a county and a state are not compared; the county's state is offered", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.goto("/us/wisconsin/dane-county/vs/us/minnesota");
  await expect(page.getByTestId("compare-places-grain")).toBeVisible();
  await expect(page.getByTestId("compare-places-parent-offer")).toHaveAttribute("href", "/us/wisconsin/vs/us/minnesota");
  await expect(page.locator(".paired-row")).toHaveCount(0);
});

test("the compare picker offers neighbouring counties first", async ({ page }) => {
  // Covers: WEB-130 — the compare default is a neighbour.
  await installPlaceFixtures(page);
  await page.goto("/us/wisconsin/rock-county/vs/us/minnesota/hennepin-county");
  await expect(page.getByTestId("compare-places-picker-results")).toContainText("Dane County, Wisconsin · neighbouring county");
  await page.getByTestId("compare-places-picker-results").getByRole("button", { name: "Dane County, Wisconsin" }).click();
  // The neighbour is addressed by its FIPS, which the route resolves.
  await expect(page).toHaveURL(/\/us\/wisconsin\/rock-county\/vs\/us\/wisconsin\/55025$/);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Rock County, Wisconsin and Dane County, Wisconsin");
});
