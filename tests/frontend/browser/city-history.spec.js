import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-154 — web-city-history: a city page draws its median household
// income, rent and home value over every period published, beside its
// county, its state and the nation, with each point's margin of error; a
// period the city was not published for says "Not published that year"; a
// measure the city does not publish draws no trend; and every chapter's
// trend lines are read as one settled-history batch.

function apiCalls(page) {
  const calls = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname.startsWith("/api/v1/")) calls.push(url);
  });
  return calls;
}

async function madison(page, { after = null } = {}) {
  await installPlaceFixtures(page);
  // Routes added after the fixtures take precedence over them.
  if (after) await after(page);
  const calls = apiCalls(page);
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.goto("/us/wisconsin/madison-city");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  // The county arrives with the relationships; the trends then draw four lines.
  await expect(page.getByTestId("chapter-work-money-trend-median-household-income")).toContainText("Dane County (reference)");
  return calls;
}

test("a city page draws income, rent and home value beside its county, state and the nation, with margins", async ({ page }) => {
  await madison(page);
  const income = page.getByTestId("chapter-work-money-trend-median-household-income");
  for (const name of ["Madison city (this page)", "Dane County (reference)", "Wisconsin (reference)", "United States (reference)"]) {
    await expect(income.locator(".place-trend-legend")).toContainText(name);
  }
  // Each published margin is a bar through its point and a ± in the table.
  await expect(income.locator('[data-moe-series="state:55|place:48000"] [data-moe-period]')).toHaveCount(6);
  await income.locator("summary").click();
  await expect(income.locator("tbody tr").first()).toContainText("±12");
  await expect(income.getByTestId("chapter-work-money-trend-median-household-income-note").first()).toContainText("margin of error");

  await expect(page.getByTestId("chapter-housing-trend")).toContainText("Madison city (this page)");
  const value = page.getByTestId("chapter-housing-trend-median-home-value");
  await expect(value).toContainText("Madison city (this page)");
  // The city's home value starts two years later than its county's: those
  // years are named as not published, never drawn or filled in.
  await expect(value.getByTestId("chapter-housing-trend-median-home-value-not-published")).toContainText("Madison city");
  await value.locator("summary").click();
  const firstRow = value.locator("tbody tr").first();
  await expect(firstRow.locator("td").first()).toHaveText("Not published that year");
  await expect(firstRow.locator("td").nth(1)).toContainText("±12");

  // BLS publishes no city unemployment rate: no county line poses as the city's.
  await expect(page.getByTestId("chapter-work-money-trend")).toHaveCount(0);

  const results = await new AxeBuilder({ page }).include('[data-testid="chapter-housing"]').withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("a city page reads its trend lines as one batch per chapter, never one read per line", async ({ page }) => {
  const calls = await madison(page);
  const batches = calls.filter((url) => url.pathname === "/api/v1/observations/history/batch");
  expect(batches.length).toBeGreaterThan(0);
  const pairs = new Set(batches.flatMap((url) => {
    const codes = url.searchParams.get("metric_codes").split(",");
    const places = url.searchParams.get("geo_ids").split(",");
    return codes.flatMap((code) => places.map((place) => `${code}|${place}`));
  }));
  expect(pairs.has("CENSUS_ACS:acs5:B19013_001|state:55|place:48000")).toBe(true);
  expect(pairs.has("CENSUS_ACS:acs5:B25077_001|state:55|county:025")).toBe(true);
  expect(pairs.has("CENSUS_ACS:acs5:B25064_001|us:1")).toBe(true);
  // The chapters' lines only: the groceries-and-gas section reads its own.
  const chapterCodes = new Set(batches.flatMap((url) => url.searchParams.get("metric_codes").split(",")));
  const singles = calls.filter(
    (url) => url.pathname === "/api/v1/observations"
      && url.searchParams.get("newest_release_per_period") === "true"
      && (chapterCodes.has(url.searchParams.get("metric_code")) || url.searchParams.get("metric_code").startsWith("CENSUS_")),
  );
  expect(singles.map((url) => url.search)).toEqual([]);
});

test("when the history batch cannot be read each line is read on its own", async ({ page }) => {
  const calls = await madison(page, {
    after: (target) => target.route("**/api/v1/observations/history/batch**", (route) =>
      route.fulfill({ status: 503, json: { detail: "Database service is temporarily unavailable." } }),
    ),
  });
  const singles = calls.filter(
    (url) => url.pathname === "/api/v1/observations" && url.searchParams.get("newest_release_per_period") === "true",
  );
  expect(singles.some((url) => url.searchParams.get("metric_code") === "CENSUS_ACS:acs5:B19013_001")).toBe(true);
  await expect(page.getByTestId("chapter-housing-trend-median-home-value")).toContainText("Madison city (this page)");
});
