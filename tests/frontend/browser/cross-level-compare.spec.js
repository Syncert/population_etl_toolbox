import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installCrossLevelFixtures } from "../support/crossLevelScenarios.js";

// Covers: WEB-146 — two places at different levels compare on the web
// (web-cross-level-compare, ADR-0010) over `/comparison/places`: a city
// beside its county, a county beside its state, a metro beside the nation.
// Medians and rates sit side by side in one period with how the places
// relate and no difference between nested or overlapping places; a total
// across levels is refused in plain words and its served share is shown
// instead, marked as a draft view; a total with no share says it is left
// out; each chapter's "Show counts as" switch asks for the chosen view; a
// metro, which has no page, is addressed by identity, and an identity
// address of two places with pages settles on the place-page form; place
// pages link to each related place's comparison, and the picker finds a
// place at any level. Values are synthetic UI fixtures.

async function open(page, path) {
  await installCrossLevelFixtures(page);
  await page.goto(path);
  await expect(page.getByTestId("compare-places")).toHaveAttribute("data-ready", "true");
}

test("a city beside its county: medians side by side, totals refused in plain words with the share instead", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await open(page, "/us/wisconsin/madison-city/vs/us/wisconsin/dane-county");
  await expect(page.getByTestId("compare-places")).toHaveAttribute("data-cross-level", "true");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Madison city, Wisconsin and Dane County, Wisconsin");
  await expect(page.getByTestId("compare-places-levels")).toContainText("Madison city, Wisconsin is a city or town and Dane County, Wisconsin is a county.");

  const income = page.getByTestId("compare-row-median-household-income");
  await expect(income).toHaveAttribute("data-state", "ok");
  await expect(income.locator(".paired-bar")).toHaveCount(2);
  await expect(income).toContainText("2019-01-01 – 2023-12-31");
  await expect(page.getByTestId("compare-row-median-household-income-containment")).toHaveText(
    "100% of Madison city, Wisconsin's area lies inside Dane County, Wisconsin, so the two figures partly describe the same people. They are shown side by side; no difference is worked out between them.",
  );
  await expect(page.getByTestId("compare-row-median-household-income-difference")).toHaveCount(0);

  // A total with a served share: refused, and the share shown instead.
  const poverty = page.getByTestId("compare-row-below-poverty");
  await expect(poverty).toHaveAttribute("data-refusal", "counts_differ_in_size");
  await expect(page.getByTestId("compare-row-below-poverty-refusal")).toContainText(
    "This is a total, and Madison city, Wisconsin is a city or town and Dane County, Wisconsin is a county, so they differ in size by their nature",
  );
  await expect(page.getByTestId("compare-row-below-poverty-instead")).toHaveText("Shown instead: Share of people below the poverty level (Draft view awaiting review)");
  await expect(poverty).toHaveAttribute("data-ratio-id", "poverty_share:acs5");
  await expect(poverty.locator(".paired-bar")).toHaveCount(2);

  // A total with none: said, not shown.
  const population = page.getByTestId("compare-row-population-estimate");
  await expect(population).toHaveAttribute("data-state", "refused");
  await expect(population).toContainText("No share or per-resident version of this total is published yet, so it is left out.");
  await expect(population.locator(".paired-bar")).toHaveCount(0);

  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("a county beside its state says one lies inside the other, and offers no difference", async ({ page }) => {
  await open(page, "/us/wisconsin/dane-county/vs/us/wisconsin");
  await expect(page.getByTestId("compare-row-unemployment-rate-containment")).toHaveText(
    "Dane County, Wisconsin lies inside Wisconsin, so Wisconsin's figure already includes Dane County, Wisconsin. They are shown side by side; no difference is worked out between them.",
  );
  await expect(page.getByTestId("compare-row-unemployment-rate-difference")).toHaveCount(0);
  // The state's figures beside another state are one level, not this one.
  await expect(page.getByTestId("compare-places-parent-offer")).toHaveCount(0);
});

test("a county beside another state is answered across levels, with the one-level pair offered", async ({ page }) => {
  await open(page, "/us/wisconsin/dane-county/vs/us/minnesota");
  await expect(page.getByTestId("compare-places")).toHaveAttribute("data-cross-level", "true");
  await expect(page.getByTestId("compare-places-parent-offer")).toHaveAttribute("href", "/us/wisconsin/vs/us/minnesota");
  // Independent places in one period: the API's difference is shown.
  await expect(page.getByTestId("compare-row-unemployment-rate-difference")).toHaveText(/^Dane County, Wisconsin minus Minnesota: -8\.5 /);
});

test("a metro beside the nation is addressed by identity and swaps by identity", async ({ page }) => {
  await open(page, "/compare/places?a=cbsa:31540&b=us:1");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Madison, WI and United States");
  await expect(page.getByTestId("compare-places-levels")).toContainText("Madison, WI is a metro area and United States is the nation.");
  await expect(page.getByTestId("compare-row-median-household-income")).toHaveAttribute("data-state", "ok");
  await expect(page.getByTestId("compare-row-median-household-income-containment")).toContainText("Madison, WI lies inside United States");
  await expect(page.getByTestId("compare-places-swap")).toHaveAttribute("href", "/compare/places?a=us%3A1&b=cbsa%3A31540");
});

test("an identity address of two places with pages settles on the place-page address", async ({ page }) => {
  await open(page, "/compare/places?a=state:55%7Ccounty:025&b=state:55");
  await expect(page).toHaveURL(/\/us\/wisconsin\/dane-county\/vs\/us\/wisconsin$/);
});

test("an unknown identity is not found", async ({ page }) => {
  await installCrossLevelFixtures(page);
  const response = await page.goto("/compare/places?a=cbsa:31540&b=nowhere");
  expect(response?.status()).toBe(404);
});

test("Show counts as asks for the chosen view across levels", async ({ page }) => {
  await open(page, "/us/wisconsin/dane-county/vs/us/wisconsin");
  const people = page.getByTestId("compare-chapter-people");
  const toggle = page.getByTestId("compare-chapter-people-view");
  await expect(toggle).toBeVisible();
  await toggle.getByRole("button", { name: "Share" }).click();
  await expect(people).toHaveAttribute("data-view", "share");
  const under18 = page.getByTestId("compare-row-under-18");
  await expect(under18).toHaveAttribute("data-ratio-id", "under_18_share:acs5");
  await expect(under18.getByRole("heading", { level: 3 })).toHaveText("Share of residents under 18");
  // Asked as the share itself, so nothing was refused on the way.
  await expect(page.getByTestId("compare-row-under-18-refusal")).toHaveCount(0);
});

test("at one level the switch shows a chapter's counts as shares", async ({ page }) => {
  await open(page, "/us/wisconsin/dane-county/vs/us/wisconsin/rock-county");
  await expect(page.getByTestId("compare-places")).toHaveAttribute("data-cross-level", "false");
  await expect(page.getByTestId("compare-row-under-18")).toHaveCount(0);
  await page.getByTestId("compare-chapter-people-view").getByRole("button", { name: "Share" }).click();
  await expect(page.getByTestId("compare-row-under-18")).toHaveAttribute("data-state", "ok");
  await expect(page.getByTestId("compare-row-population-estimate")).toBeVisible();
});

test("a place page links each related place to its comparison", async ({ page }) => {
  await installCrossLevelFixtures(page);
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  const partOf = page.getByTestId("place-nearby-part-of");
  await expect(partOf.getByRole("link", { name: "Compare with Wisconsin" })).toHaveAttribute("href", "/us/wisconsin/dane-county/vs/us/wisconsin");
  await expect(partOf.getByRole("link", { name: "Compare with Madison, WI" })).toHaveAttribute("href", "/compare/places?a=state%3A55%7Ccounty%3A025&b=cbsa%3A31540");
  await expect(partOf.getByRole("link", { name: "Compare with United States" })).toHaveAttribute("href", "/us/wisconsin/dane-county/vs/us");
  // A Census division has no comparison here.
  await expect(partOf.getByRole("link", { name: "Compare with East North Central" })).toHaveCount(0);
  await expect(page.getByTestId("place-nearby-within").getByRole("link", { name: "Compare with Crossing city, Wisconsin" })).toBeVisible();
});

test("the picker finds a place at any level, a metro included", async ({ page }) => {
  await open(page, "/us/wisconsin/rock-county/vs/us/wisconsin");
  await page.getByTestId("compare-places-picker").fill("madison");
  const results = page.getByTestId("compare-places-picker-results");
  await expect(results).toContainText("Madison city, Wisconsin · city or town");
  await results.getByRole("button", { name: "Madison, WI" }).click();
  await expect(page).toHaveURL(/\/compare\/places\?a=state%3A55%7Ccounty%3A105&b=cbsa%3A31540$/);
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Rock County, Wisconsin and Madison, WI");
});

test("a side with no published figure is refused in plain words, and the other side still shows", async ({ page }) => {
  // LAUS publishes no national unemployment rate: the API refuses with
  // `not_published` (#187) rather than comparing a figure with nothing.
  await open(page, "/compare/places?a=cbsa:31540&b=us:1");
  const row = page.getByTestId("compare-row-unemployment-rate");
  await expect(row).toHaveAttribute("data-state", "refused");
  await expect(row).toHaveAttribute("data-refusal", "not_published");
  await expect(page.getByTestId("compare-row-unemployment-rate-refusal")).toHaveText(
    "There is no published figure for United States on this measure, so there is nothing to compare. A missing figure is not zero.",
  );
  await expect(row.locator('.paired-bar[data-geo-id="cbsa:31540"]')).toHaveAttribute("data-published", "true");
  await expect(row.locator('.paired-bar[data-geo-id="us:1"]')).toContainText("Not published for this place");
  await expect(page.getByTestId("compare-row-unemployment-rate-difference")).toHaveCount(0);
  await expect(page.getByTestId("compare-row-unemployment-rate-containment")).toHaveCount(0);
});

test("a metro beside the nation still lists the measures the catalog lists for one side", async ({ page }) => {
  // OFFICE_LAB: the catalog lists no metro grain for the survey or LAUS, so
  // filtering on both sides' grains left this page with no rows at all.
  await open(page, "/compare/places?a=cbsa:31540&b=us:1");
  await expect(page.getByTestId("compare-places-empty")).toHaveCount(0);
  await expect(page.getByTestId("compare-row-median-household-income")).toHaveAttribute("data-state", "ok");
  await expect(page.getByTestId("compare-row-unemployment-rate")).toHaveAttribute("data-refusal", "not_published");
});

test("a canonical address resolves through the place search, without paging a state's places", async ({ page }) => {
  const listed = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname === "/api/v1/catalog/geographies" && ["COUNTY", "PLACE"].includes(url.searchParams.get("geo_level") || "")) listed.push(url.searchParams.get("geo_level"));
  });
  await open(page, "/us/wisconsin/madison-city/vs/us/wisconsin/dane-county");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Madison city, Wisconsin and Dane County, Wisconsin");
  expect(listed).toEqual([]);
});

test("a place page always offers its state and the nation to compare with", async ({ page }) => {
  await installCrossLevelFixtures(page);
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  const offer = page.getByTestId("place-compare-with");
  await expect(offer).toHaveText("Compare this place with Wisconsin or the United States");
  await expect(offer.getByRole("link", { name: "Wisconsin" })).toHaveAttribute("href", "/us/wisconsin/dane-county/vs/us/wisconsin");
  await expect(offer.getByRole("link", { name: "the United States" })).toHaveAttribute("href", "/us/wisconsin/dane-county/vs/us");
});
