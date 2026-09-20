import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-107 — a route that fails, and an address that does not exist,
// both land on a page this application owns.
//
// `apps/web/app` had no `error.js`, `not-found.js` or `global-error.js`, so
// either case fell through to Next's own built-in page: no site header, no
// navigation, no heading structure, and nothing to click. The reader was
// dropped out of the application by a failure in one route.

const sources = [
  { source_code: "CENSUS_ACS", source_name: "Census ACS" },
  { source_code: "FRED", source_name: "Federal Reserve Economic Data" },
];

const metrics = {
  total: 4210,
  items: [
    {
      metric_code: "CENSUS_ACS:acs5:B01003_001",
      metric_display_name: "Total population",
      source_code: "CENSUS_ACS",
      units: "people",
      valid_geo_grains: ["STATE", "COUNTY"],
    },
  ],
};

async function installRoutes(page) {
  await page.route("**/api/v1/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/v1/catalog/sources", (route) => route.fulfill({ json: sources }));
  await page.route("**/api/v1/catalog/metrics?*", (route) => route.fulfill({ json: metrics }));
}

test("an address the site does not serve gets the site's own page", async ({ page }) => {
  await installRoutes(page);
  const response = await page.goto("/no-such-route");

  // Still a 404 to anything reading the status, and a page of this site's to
  // anything reading it with eyes.
  expect(response?.status()).toBe(404);
  await expect(page.getByTestId("route-not-found")).toBeVisible();

  // The landmarks the accessibility audit asserts on every core route. A
  // failure page is a route a reader can be on, so it holds them too.
  await expect(page.locator("main")).toHaveCount(1);
  await expect(page.getByRole("heading", { level: 1 })).toHaveCount(1);
  await expect(page.getByRole("navigation", { name: "Primary navigation" })).toBeVisible();

  // And a way back that is part of the application, not the browser's back
  // button.
  await page.getByRole("link", { name: "Browse the catalog" }).click();
  await expect(page).toHaveURL(/\/catalog$/);
});

test("a route that throws while rendering shows the boundary, and recovers", async ({ page }) => {
  await installRoutes(page);

  // A real render error, injected where the page actually formats: the home
  // page renders `formatNumber(metrics.total)` once the catalog answers.
  //
  // It throws on every render until the test clears it, which is deliberate
  // twice over. React retries a failed concurrent render synchronously, so a
  // throw armed to fire once is swallowed by that retry and the page renders
  // as though nothing happened. And it is what a reader actually experiences:
  // retrying does not help while the cause is still there. Clearing the flag
  // before the retry is what makes the recovery assertion mean something.
  await page.addInitScript(() => {
    const real = Number.prototype.toLocaleString;
    // Scoped to the catalog total: anything broader fires on the first number
    // any code formats, including code that catches, and spends itself
    // somewhere harmless.
    // eslint-disable-next-line no-extend-native
    Number.prototype.toLocaleString = function patched(...args) {
      if (!window.__renderFailureCleared && Number(this) === 4210) {
        throw new Error("render failure under test");
      }
      return real.apply(this, args);
    };
  });

  await page.goto("/");

  const boundary = page.getByTestId("route-error");
  await expect(boundary).toBeVisible();
  await expect(page.locator("main")).toHaveCount(1);
  await expect(page.getByRole("heading", { level: 1 })).toHaveCount(1);
  // The shell survived: the boundary renders inside the root layout, so the
  // site header and its navigation are still there.
  await expect(page.getByRole("navigation", { name: "Primary navigation" })).toBeVisible();

  // The address bar is untouched by a boundary, so a successful retry reopens
  // the route the reader was on rather than sending them somewhere else.
  await expect(page).toHaveURL(/\/$/);
  await page.evaluate(() => {
    window.__renderFailureCleared = true;
  });
  await page.getByRole("button", { name: "Try this page again" }).click();
  await expect(boundary).toBeHidden();
  await expect(page.getByRole("heading", { level: 1, name: "Economic Data Studio" })).toBeVisible();
});
