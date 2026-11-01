import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";
import { KEPT, SPLIT, installTractFixtures } from "../support/tractScenarios.js";

// Covers: WEB-153 — a Census tract has its own page under its county
// (tract pages, D-4). The example is a Travis County tract split in 2020:
// the page says so, links the tract the rest of the land became, shows the
// tract beside its county, state and the nation, and marks every earlier
// count that was derived across the redraw. A tract that kept its boundary
// shows its earlier published values with no derived mark. Nothing is read
// for a tract until the address resolves, and the county's "within this
// county" table opens each tract's page. Values are synthetic UI fixtures.

const SCREENSHOTS = process.env.TRACT_SCREENSHOT_DIR || "";

const api = (seen, prefix) => seen.filter((entry) => entry.startsWith(prefix));

test("a tract split in 2020 says so and marks its derived history", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  const seen = [];
  await installTractFixtures(page, { seen });
  await page.goto("/us/texas/travis-county/tract-18-31");
  const main = page.getByTestId("tract-page");
  await expect(main).toHaveAttribute("data-ready", "true");
  await expect(main).toHaveAttribute("data-geo-id", SPLIT.geo_id);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Census Tract 18.31, Travis County, Texas");
  await expect(page.getByRole("navigation", { name: "Place hierarchy" }).getByRole("link", { name: "Travis County" })).toHaveAttribute("href", "/us/texas/travis-county");

  const boundary = page.getByTestId("tract-boundary");
  await expect(boundary).toHaveAttribute("data-status", "split");
  await expect(boundary.getByRole("heading")).toHaveText("This tract was split off in 2020");
  await expect(boundary).toContainText("Census Tract 18.05 was split");
  await expect(page.getByTestId("tract-boundary-also-became").getByRole("link", { name: "Census Tract 18.32" })).toHaveAttribute(
    "href",
    "/us/texas/travis-county/tract-18-32",
  );

  // This tract beside its county, state and the nation, with margins.
  const population = page.getByTestId("tract-card-population");
  await expect(population.getByTestId("tract-card-population-this-tract")).toContainText("1,500");
  await expect(population.getByTestId("tract-card-population-this-tract")).toContainText("±410");
  await expect(population.getByTestId("tract-card-population-county")).toContainText("Travis County");
  await expect(population.getByTestId("tract-card-population-nation")).toContainText("United States");

  // The history: published from 2020, derived before, an incomplete year with no value.
  const history = page.getByTestId("tract-history");
  await expect(history.getByTestId("tract-history-row-2024")).toHaveAttribute("data-derived", "false");
  await expect(history.getByTestId("tract-history-row-2024")).toContainText("Published for this tract");
  await expect(history.getByTestId("tract-history-row-2015")).toHaveAttribute("data-derived", "true");
  await expect(history.getByTestId("tract-history-row-2015")).toContainText("Derived from Census Tract 18.05, by share of land area");
  await expect(history.getByTestId("tract-history-row-2015").getByTestId("derived-tag")).toBeVisible();
  await expect(history.getByTestId("tract-history-row-2012")).toContainText("No value");
  // A median is not carried across the redraw, and says so.
  await page.getByTestId("tract-history-measure").selectOption({ label: "Median household income" });
  await expect(page.getByTestId("tract-history-note")).toContainText("starts in 2020");
  await expect(history.locator("[data-derived='true']")).toHaveCount(0);

  // Two reads about the tract, each for this tract only, after it resolved.
  expect(api(seen, "/api/v1/place/tract-history")).toHaveLength(1);
  expect(api(seen, "/api/v1/place/tract-history")[0]).toContain(encodeURIComponent(SPLIT.geo_id));
  const batches = api(seen, "/api/v1/observations/newest/batch");
  expect(batches).toHaveLength(1);
  expect(decodeURIComponent(batches[0])).toContain(`geo_ids=${SPLIT.geo_id},state:48|county:453,state:48,us:1`);
  const tractReads = api(seen, "/api/v1/catalog/geographies?").filter((entry) => entry.includes("geo_level=TRACT"));
  expect(tractReads.every((entry) => entry.includes("county_fips=453"))).toBe(true);

  if (SCREENSHOTS) await page.screenshot({ path: `${SCREENSHOTS}/tract-split-top.png` });
  if (SCREENSHOTS) await history.screenshot({ path: `${SCREENSHOTS}/tract-split-history.png` });

  const results = await new AxeBuilder({ page }).include("[data-testid='tract-page']").withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("a tract that kept its boundary shows its earlier published values, none derived", async ({ page }) => {
  await installTractFixtures(page);
  await page.goto("/us/texas/travis-county/tract-11");
  await expect(page.getByTestId("tract-page")).toHaveAttribute("data-ready", "true");
  await expect(page.getByTestId("tract-boundary")).toHaveAttribute("data-status", "unchanged");
  await expect(page.getByTestId("tract-boundary").getByRole("heading")).toHaveText("Same boundary since 2010");
  await expect(page.getByTestId("tract-history-row-2015")).toHaveAttribute("data-status", "unchanged");
  await expect(page.getByTestId("tract-history").locator("[data-derived='true']")).toHaveCount(0);
  await expect(page.getByTestId("tract-history-row-2015")).toContainText("same boundary");
  if (SCREENSHOTS) await page.screenshot({ path: `${SCREENSHOTS}/tract-unchanged-top.png` });
});

test("a tract's code form settles on its number, and an unknown tract points to its county", async ({ page }) => {
  const seen = [];
  await installTractFixtures(page, { seen });
  await page.goto("/us/texas/travis-county/tract-001831");
  await expect(page).toHaveURL(/\/us\/texas\/travis-county\/tract-18-31$/);
  await expect(page.getByTestId("tract-page")).toHaveAttribute("data-geo-id", SPLIT.geo_id);

  const before = seen.length;
  await page.goto("/us/texas/travis-county/tract-18-05");
  await expect(page.getByTestId("tract-not-found")).toContainText("Travis County has no current Census tract under that number.");
  await expect(page.getByRole("link", { name: "Every tract in Travis County" })).toHaveAttribute("href", "/us/texas/travis-county#within-county-heading");
  // A tract that does not resolve is never read.
  expect(seen.slice(before).filter((entry) => entry.startsWith("/api/v1/place/tract-history") || entry.startsWith("/api/v1/observations"))).toEqual([]);
  expect(KEPT.geo_id).toContain("tract:001100");
});

test("the county's within-county table opens each tract's page", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.goto("/us/wisconsin/dane-county");
  const table = page.getByTestId("within-county-table");
  await expect(table).toBeVisible();
  await expect(table.getByRole("link", { name: "Census Tract 2.01" })).toHaveAttribute("href", "/us/wisconsin/dane-county/tract-2-01");
  await expect(page.getByTestId("within-county-measure").locator("option")).not.toHaveCount(5);
});
