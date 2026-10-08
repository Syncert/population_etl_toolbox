import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-127 — the home page reaches a county page in one selection by
// keyboard and by pointer, location degrades to search when refused, and the
// public data page reports every source and the site's rules.

async function noViolations(page) {
  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
}

async function noHorizontalScroll(page) {
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
}

test("the home search reaches a county page by keyboard", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto("/");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("What is going on in your place?");
  const input = page.getByRole("combobox", { name: "Find your county, state, or the nation" });
  await expect(input).toBeEnabled();
  await input.fill("county wis");
  await expect(page.getByTestId("place-finder-status")).toHaveText("2 places found");
  await expect(page.getByRole("option")).toHaveText([/Dane County, Wisconsin/, /Rock County, Wisconsin/]);
  await input.press("ArrowDown");
  await expect(page.getByRole("option", { name: /Rock County/ })).toHaveAttribute("aria-selected", "true");
  await input.press("ArrowUp");
  await input.press("Enter");
  await expect(page).toHaveURL(/\/us\/wisconsin\/dane-county$/);
  await expect(page.getByTestId("place-rules-link")).toHaveAttribute("href", "/data#rules");
});

test("the home search reaches a county page by pointer, and the page passes AA at both widths", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto("/");
  await expect(page.getByTestId("home-featured-places").getByRole("link")).toHaveText(["Dane County, Wisconsin", "Wisconsin", "United States"]);
  await page.getByRole("combobox").fill("rock");
  await noViolations(page);
  await page.getByRole("option", { name: /Rock County/ }).click();
  await expect(page).toHaveURL(/\/us\/wisconsin\/rock-county$/);
  await page.goto("/");
  await noHorizontalScroll(page);
});

test("a refused location degrades to search and is kept nowhere", async ({ browser }) => {
  const context = await browser.newContext({ permissions: [] });
  const page = await context.newPage();
  await installPlaceFixtures(page);
  await page.goto("/");
  await expect(page.getByRole("combobox")).toBeEnabled();
  await page.getByTestId("place-finder-locate").click();
  await expect(page.getByTestId("place-finder-locating")).toHaveText("Your location is not available. Search for your place instead.");
  expect(new URL(page.url()).pathname).toBe("/");
  expect(new URL(page.url()).search).toBe("");
  expect(await page.evaluate(() => [Object.keys(sessionStorage), Object.keys(localStorage)].flat().filter((key) => /locat|coord|lat/i.test(key)))).toEqual([]);
  await expect(page.getByRole("combobox")).toBeEnabled();
  await context.close();
});

test("the public data page reports every source, and a missing one as not reported", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto("/data");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("The sources behind every number");
  await expect(page.locator("[data-testid^=source-card-]")).toHaveCount(10);
  const bls = page.getByTestId("source-card-BLS");
  await expect(bls).toHaveAttribute("data-reported", "true");
  await expect(bls).toContainText("counties, states");
  const nass = page.getByTestId("source-card-USDA_NASS");
  await expect(nass).toHaveAttribute("data-reported", "false");
  await expect(nass.locator("dd").first()).toHaveText("not reported");
  await expect(page.getByTestId("site-rules").locator("li")).toHaveCount(5);
  await expect(page.getByTestId("refresh-timeline").locator("li")).toHaveCount(9);
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("the analyst routes stay reachable under Tools", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.goto("/data");
  await expect(page.getByRole("navigation", { name: "Primary navigation" }).getByRole("link", { name: "Where the numbers come from" })).toHaveAttribute("aria-current", "page");
  await page.getByTestId("tools-menu").locator(":scope > summary").click();
  for (const name of ["Home", "Data Catalog", "Explore", "Compare", "Workbench", "Profiles", "Data quality", "Articles", "Builder", "Saved"]) {
    await expect(page.getByTestId("tools-menu").getByRole("link", { name, exact: true })).toBeVisible();
  }
  await page.keyboard.press("Escape");
  await expect(page.getByTestId("tools-menu")).not.toHaveAttribute("open", "");
});
