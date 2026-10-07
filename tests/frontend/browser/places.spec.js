import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-125 — place-pages: the nation, a state, and a county render the shared chapters
// over the published API, with three-level cards, stated omissions, state
// context for county safety, and no horizontal scroll at 390px.

async function ready(page, path) {
  await installPlaceFixtures(page);
  await page.goto(path);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
}

async function noViolations(page) {
  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
}

async function noHorizontalScroll(page) {
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
}

test("the nation's page reads every chapter it publishes and lists the states", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us");
  await expect(page.locator("main")).toHaveCount(1);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("United States");
  await expect(page.getByTestId("chapter-rail").getByRole("link")).toHaveText(["People", "Work and Money", "Housing", "Health", "Safety", "Land and Farms", "Change"]);
  // BLS publishes no national unemployment rate in this catalog.
  await expect(page.getByTestId("card-unemployment-rate-national")).toContainText("Not published at national grain");
  await expect(page.getByTestId("place-children").getByRole("link", { name: "Wisconsin" })).toHaveAttribute("href", "/us/wisconsin");
  await expect(page.getByTestId("chapter-people-trend")).toHaveAttribute("data-scale", "index");
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("a state page says which headline measures publish no state grain", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Wisconsin");
  // CDC PLACES publishes counties and the nation; the ACS insurance counts in
  // the same chapter publish states, so the chapter stays and the card says so.
  await expect(page.getByTestId("card-obesity-state")).toContainText("Not published at state grain");
  await expect(page.getByTestId("card-obesity-national")).toContainText("%");
  await expect(page.getByTestId("card-population-estimate-state")).toContainText("persons");
  await expect(page.getByTestId("card-population-estimate-national")).toBeVisible();
  await expect(page.getByTestId("place-children").getByRole("link", { name: "Dane County" })).toHaveAttribute("href", "/us/wisconsin/dane-county");
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("a county page draws three levels, labels state context, and states its gaps", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  await expect(page.locator("main")).toHaveCount(1);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Dane County, Wisconsin");

  const card = page.getByTestId("card-population-estimate");
  await expect(card.locator("tbody tr")).toHaveCount(3);
  await expect(card).toHaveAttribute("data-period", "2024-01-01 – 2024-12-31");
  // The nation's newest median age is a different year: said, not shown.
  await expect(page.getByTestId("card-median-age-national")).toContainText("Not published for 2024-01-01 – 2024-12-31 (newest published: 2023-01-01 – 2023-12-31)");
  await expect(page.getByTestId("card-median-age-county")).toContainText("margin of error");

  const safety = page.getByTestId("chapter-safety");
  await expect(safety).toHaveAttribute("data-read-grain", "STATE");
  await expect(page.getByTestId("chapter-safety-state-context")).toContainText("The figures below are Wisconsin's, not Dane County's");
  await expect(page.getByTestId("card-violent-crime-rate-county")).toContainText("Not published at county grain");
  await expect(page.getByTestId("card-violent-crime-rate-state")).toContainText("(state context)");

  // NASS publishes no Dane County cell in the fixture.
  await expect(page.getByTestId("chapter-land-farms")).toHaveCount(0);
  await expect(page.getByTestId("place-omissions")).toContainText("Land and Farms: no published values for this place");

  await expect(page.getByTestId("depth-living-alone")).toContainText("Universe: households");
  await expect(page.getByTestId("chapter-people-footer")).toContainText("Source: CENSUS_PEP, CENSUS_ACS");
  const explore = page.getByTestId("chapter-people-explore");
  const href = new URL(await explore.getAttribute("href"), "http://localhost");
  expect(href.pathname).toBe("/explore");
  expect(href.searchParams.get("metric")).toBe("CENSUS_PEP:POPESTIMATE");
  expect(href.searchParams.get("geo_level")).toBe("COUNTY");
  expect(href.searchParams.get("state")).toBe("55");

  await noViolations(page);
  await noHorizontalScroll(page);
  await expect(page.getByTestId("chapter-rail")).toBeVisible();
});

// Covers: WEB-137 — bea-regional-accounts: BEA per capita income and real GDP
// sit beside the ACS income, each labelled with how it was counted, and the
// earnings table states a withheld sector.
test("BEA income and GDP sit beside the survey income, labelled apart, with earnings by industry", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  await expect(page.getByTestId("card-bea-per-capita-income")).toHaveAttribute("data-available", "true");
  await expect(page.getByTestId("card-bea-per-capita-income-basis")).toHaveText("BEA personal income account, current dollars");
  await expect(page.getByTestId("card-median-household-income-basis")).toHaveText("Survey of households (ACS)");
  await expect(page.getByTestId("card-bea-real-gdp-basis")).toHaveText("BEA county GDP, chained 2017 dollars");
  await expect(page.getByTestId("card-median-household-income")).not.toContainText("BEA:");

  const earnings = page.getByTestId("bea-earnings");
  await expect(earnings.locator("tbody tr")).toHaveCount(21);
  await expect(page.getByTestId("bea-earnings-basis")).toContainText("BEA personal income account");
  await expect(page.getByTestId("bea-earnings-bea-earnings-1600")).toContainText("Health care and social assistance");
  await expect(page.getByTestId("bea-earnings-bea-earnings-200")).toContainText("Published without a value: withheld");
  await expect(page.getByTestId("chapter-work-money-footer")).toContainText("never combined with the survey's income");
  await noViolations(page);
  await noHorizontalScroll(page);
});

test("an address by FIPS settles on the named address", async ({ page }) => {
  await ready(page, "/us/55/025");
  await expect(page).toHaveURL(/\/us\/wisconsin\/dane-county$/);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Dane County, Wisconsin");
});

test("an unknown place is a not-found page with search and no chart", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.goto("/us/wisconsin/atlantis-county");
  await expect(page.getByTestId("place-not-found")).toBeVisible();
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("There is no place at this address");
  await expect(page.locator(".place-trend, .place-card")).toHaveCount(0);
  await page.getByRole("searchbox", { name: "Search places" }).fill("rock");
  await expect(page.getByTestId("place-search-results").getByRole("link", { name: "Rock County, Wisconsin" })).toHaveAttribute("href", "/us/wisconsin/rock-county");
  await noViolations(page);

  const response = await page.goto("/us/Not%20A%20Place");
  expect(response.status()).toBe(404);
  await expect(page.getByTestId("route-not-found")).toBeVisible();
});
