import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { LOCATIONS, installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-127 — the home search over the API's place search (D-1) offers
// Austin city, Travis County and the Austin metro for "Austin" and reaches a
// city or county page in one selection by keyboard and by pointer; "use my
// location" lands on the city the point is in, or its county outside every
// city, and degrades to search when refused or outside coverage; and the
// public data page reports every source and the site's rules.

async function noViolations(page) {
  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
}

async function noHorizontalScroll(page) {
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
}

const FINDER = "Find your city, county, or state";

test("typing Austin offers Austin city, Travis County and the Austin metro", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto("/");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("What is going on in your place?");
  await page.getByRole("combobox", { name: FINDER }).fill("Austin");
  await expect(page.getByTestId("place-finder-status")).toHaveText("4 places found");
  await expect(page.getByRole("option")).toHaveText([
    /^Austin city, Texas · city or town · 967,862 people \(2019–2023 survey\)$/,
    /^Travis County, Texas · county · includes Austin city/,
    /^Austin-Round Rock-San Marcos, TX Metro Area · metro area · includes Austin city · .*(?<!no page yet)$/,
    /^Austin County, Texas · county/,
  ]);
  // A metro has its page (D-5, WEB-153): it is offered and can be opened.
  await expect(page.getByRole("option", { name: /Metro Area/ })).not.toHaveAttribute("aria-disabled", "true");
  await expect(page.getByRole("option").first()).toHaveAttribute("aria-selected", "true");
  await expect(page.getByTestId("place-finder")).toHaveAttribute("data-search", "api");
});

test("the home search reaches a city page by keyboard", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto("/");
  const input = page.getByRole("combobox", { name: FINDER });
  await expect(input).toBeEnabled();
  await input.fill("madison");
  await expect(page.getByRole("option")).toHaveText([/^Madison city, Wisconsin/, /^Dane County, Wisconsin/, /^Madison, WI Metro Area/]);
  await input.press("ArrowDown");
  await expect(page.getByRole("option", { name: /Dane County/ })).toHaveAttribute("aria-selected", "true");
  await input.press("ArrowDown");
  await expect(page.getByRole("option", { name: /Metro Area/ })).toHaveAttribute("aria-selected", "true");
  await input.press("ArrowUp");
  await input.press("ArrowUp");
  await input.press("Enter");
  await expect(page).toHaveURL(/\/us\/wisconsin\/madison-city$/);
});

test("the home search reaches a county page by pointer, and the page passes AA at both widths", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto("/");
  await expect(page.getByTestId("home-featured-places").getByRole("link")).toHaveText(["Dane County, Wisconsin", "Wisconsin", "United States"]);
  await page.getByRole("combobox").fill("madison");
  await expect(page.getByRole("option")).toHaveCount(3);
  await noViolations(page);
  // A press on the metro, which has no page, goes nowhere.
  await page.getByRole("option", { name: /Metro Area/ }).click({ force: true });
  expect(new URL(page.url()).pathname).toBe("/");
  await page.getByRole("combobox").fill("rock");
  await page.getByRole("option", { name: /Rock County/ }).click();
  await expect(page).toHaveURL(/\/us\/wisconsin\/rock-county$/);
  await expect(page.getByTestId("place-rules-link")).toHaveAttribute("href", "/data#rules");
  await page.goto("/");
  await noHorizontalScroll(page);
});

test("the nation is offered by name", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.goto("/");
  await page.getByRole("combobox", { name: FINDER }).fill("united");
  await expect(page.getByRole("option")).toHaveText([/^United States · nation$/]);
  await page.getByRole("combobox").press("Enter");
  await expect(page).toHaveURL(/\/us$/);
});

test("a search the API cannot answer falls back to the catalog's counties and states", async ({ page }) => {
  await installPlaceFixtures(page, { searchUnavailable: true });
  await page.goto("/");
  const input = page.getByRole("combobox", { name: FINDER });
  await input.fill("county wis");
  await expect(page.getByTestId("place-finder")).toHaveAttribute("data-search", "catalog");
  await expect(page.getByTestId("place-finder-fallback")).toBeVisible();
  await expect(page.getByTestId("place-finder-status")).toHaveText("2 places found");
  await expect(page.getByRole("option")).toHaveText([/Dane County, Wisconsin/, /Rock County, Wisconsin/]);
  await input.press("Enter");
  await expect(page).toHaveURL(/\/us\/wisconsin\/dane-county$/);
});

async function locatedPage(browser, where, options = {}) {
  const context = await browser.newContext({ permissions: ["geolocation"], geolocation: LOCATIONS[where] });
  const page = await context.newPage();
  const sent = [];
  await installPlaceFixtures(page, { onLocate: (search) => sent.push(search), ...options });
  await page.goto("/");
  await page.getByTestId("place-finder-locate").click();
  return { context, page, sent };
}

async function keptNowhere(page) {
  expect(await page.evaluate(() => [Object.keys(sessionStorage), Object.keys(localStorage)].flat().filter((key) => /locat|coord|lat/i.test(key)))).toEqual([]);
  expect(new URL(page.url()).search).toBe("");
}

test("use my location lands on the city the reader is in, sending a rounded point", async ({ browser }) => {
  const { context, page, sent } = await locatedPage(browser, "madison");
  await expect(page).toHaveURL(/\/us\/wisconsin\/madison-city$/);
  expect(sent).toEqual(["?lat=43.0747&lon=-89.3841"]);
  await keptNowhere(page);
  await context.close();
});

test("use my location outside every city lands on the county, and says so", async ({ browser }) => {
  const { context, page } = await locatedPage(browser, "rockCounty");
  await expect(page).toHaveURL(/\/us\/wisconsin\/rock-county$/);
  await keptNowhere(page);
  await context.close();
});

test("a location outside every covered place degrades to search", async ({ browser }) => {
  const { context, page } = await locatedPage(browser, "atSea");
  await expect(page.getByTestId("place-finder-locating")).toHaveText("Your location is outside the United States places this site covers. Search for your place instead.");
  expect(new URL(page.url()).pathname).toBe("/");
  await keptNowhere(page);
  await expect(page.getByRole("combobox")).toBeEnabled();
  await context.close();
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
  await keptNowhere(page);
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
