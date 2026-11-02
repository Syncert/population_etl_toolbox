import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";
import { installAgeFixtures } from "../support/ageScenarios.js";

// Covers: WEB-152 — a place page draws who lives there by age and sex
// (web-age-and-sex-structure, ADR-0013): a pyramid of eighteen five-year
// bands, men left and women right, as shares of everyone, with the "Compare
// with" toggle Nick asked for (city: County · State · Nation · None, county
// selected; county: State · Nation · None). One choice redraws the outline,
// the cards' comparison values and the difference bars with no new request;
// the history is read once, only when opened; unreliable bands are hatched,
// absent ones said; the compare page shares one axis; a single B01001 line
// is not offered as a map. Values are synthetic UI fixtures.

async function open(page, path, seen = []) {
  await installPlaceFixtures(page);
  await installAgeFixtures(page, { seen });
  await page.goto(path);
  await expect(page.getByTestId("age-section")).toHaveAttribute("data-state", "ready");
}

const ageRequests = (seen) => seen.filter((entry) => entry.startsWith("/api/v1/age-structure"));

test("a city compares against its county by default, and the toggle redraws everything with no new request", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  const seen = [];
  await open(page, "/us/wisconsin/madison-city", seen);
  const section = page.getByTestId("age-section");
  const toggle = page.getByTestId("age-section-reference");
  await expect(toggle.getByRole("button")).toHaveText([
    "County: Dane County, Wisconsin",
    "State: Wisconsin",
    "Nation: United States",
    "None",
  ]);
  await expect(toggle.getByRole("button", { name: "County: Dane County, Wisconsin" })).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByTestId("age-section-pyramid")).toHaveAttribute("data-reference", "state:55|county:025");
  await expect(page.getByTestId("age-section-pyramid-reference-men")).toHaveCount(1);
  await expect(page.getByTestId("age-section-cards-median-age-reference")).toHaveText("Dane County, Wisconsin: 35.4");
  await expect(page.getByRole("heading", { name: "Where Madison city, Wisconsin differs from Dane County, Wisconsin" })).toBeVisible();
  // Eighteen bands per side, men and women.
  await expect(page.locator("[data-testid^='age-section-pyramid-bar-']")).toHaveCount(36);
  // The scheme is a draft, so the computed cards say so; the published median does not.
  await expect(page.getByTestId("age-section-cards-65-plus-draft")).toHaveText("draft");
  await expect(page.getByTestId("age-section-cards-median-age-draft")).toHaveCount(0);
  expect(ageRequests(seen)).toHaveLength(1);

  await toggle.getByRole("button", { name: "Nation: United States" }).click();
  await expect(section).toHaveAttribute("data-reference", "us:1");
  await expect(page.getByTestId("age-section-pyramid")).toHaveAttribute("data-reference", "us:1");
  await expect(page.getByTestId("age-section-cards-median-age-reference")).toHaveText("United States: 38.7");
  await expect(page.getByTestId("age-section-difference")).toHaveAttribute("data-reference", "us:1");

  await toggle.getByRole("button", { name: "None" }).click();
  await expect(page.getByTestId("age-section-pyramid")).toHaveAttribute("data-reference", "none");
  await expect(page.getByTestId("age-section-pyramid-reference-men")).toHaveCount(0);
  await expect(page.getByTestId("age-section-cards-median-age-reference")).toHaveCount(0);
  await expect(page.getByTestId("age-section-differs")).toHaveCount(0);
  // Every switch drew from the one answer already held.
  expect(ageRequests(seen)).toHaveLength(1);

  const results = await new AxeBuilder({ page }).include("[data-testid='age-section']").withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("a county offers its state, the nation and none, the state selected", async ({ page }) => {
  const seen = [];
  await open(page, "/us/wisconsin/dane-county", seen);
  // Asked for the county only, never for the state while the county resolved.
  expect(ageRequests(seen)).toEqual(["/api/v1/age-structure?geo_id=state%3A55%7Ccounty%3A025"]);
  const toggle = page.getByTestId("age-section-reference");
  await expect(toggle.getByRole("button")).toHaveText(["State: Wisconsin", "Nation: United States", "None"]);
  await expect(toggle.getByRole("button", { name: "State: Wisconsin" })).toHaveAttribute("aria-pressed", "true");
  // The section links its figures to the age map of every county.
  await expect(page.getByTestId("age-section-map-link")).toHaveAttribute("href", "/map/age/median_age");
});

test("the nation has nothing to compare against and no toggle", async ({ page }) => {
  await open(page, "/us");
  await expect(page.getByTestId("age-section-reference")).toHaveCount(0);
  await expect(page.getByTestId("age-section-pyramid")).toHaveAttribute("data-reference", "none");
});

test("a city that overlaps its county names the overlap", async ({ page }) => {
  await open(page, "/us/wisconsin/crossing-city");
  await expect(page.getByTestId("age-section-overlap")).toHaveText(
    "60% of Crossing city, Wisconsin's area lies in Dane County, Wisconsin, so the comparison covers some people outside it.",
  );
});

test("keyboard focus reaches a bar and announces band, sex, share, count and margin; the table holds the same numbers", async ({ page }) => {
  await open(page, "/us/wisconsin/dane-county");
  const hit = page.getByTestId("age-section-pyramid-hit-20-24-women");
  await hit.focus();
  await expect(hit).toHaveAttribute("aria-label", /^Women aged 20 to 24: \d+\.\d% of Dane County, Wisconsin, [\d,]+ people, plus or minus [\d,]+\. Wisconsin, 2019–2023: \d+\.\d%\.$/);
  await expect(page.getByTestId("age-section-pyramid-tooltip")).toContainText("Women, 20 to 24");
  await page.getByTestId("age-section-view").getByRole("button", { name: "Table" }).click();
  const table = page.getByTestId("age-section-table");
  await expect(table.locator("tbody tr")).toHaveCount(18);
  await expect(table.locator("tbody tr").first().locator("th")).toHaveText("85 and over");
});

test("history is read once, only when the reader opens it, and feeds the earlier outline too", async ({ page }) => {
  const seen = [];
  await open(page, "/us/wisconsin/dane-county", seen);
  expect(ageRequests(seen).filter((entry) => entry.includes("/history"))).toHaveLength(0);
  await page.getByTestId("age-section-history-open").click();
  await expect(page.getByTestId("age-section-history")).toHaveAttribute("data-periods", "2013,2018,2023");
  await page.getByTestId("age-section-earlier").getByRole("checkbox").check();
  await expect(page.getByTestId("age-section-pyramid")).toHaveAttribute("data-earlier", "2013");
  await expect(page.getByTestId("age-section-pyramid-earlier-men")).toHaveCount(1);
  await page.getByTestId("age-section-earlier-release").selectOption("2018");
  await expect(page.getByTestId("age-section-pyramid")).toHaveAttribute("data-earlier", "2018");
  expect(ageRequests(seen).filter((entry) => entry.includes("/history"))).toHaveLength(1);
});

test("cohorts follow each band forward five years in ADR-0011's verbs, with the caveat", async ({ page }) => {
  const seen = [];
  await open(page, "/us/wisconsin/dane-county", seen);
  await page.getByTestId("age-section-cohorts-open").click();
  await expect(page.getByTestId("age-section-cohorts-caveat")).toContainText("it is not one group of people followed over time");
  expect(seen.find((entry) => entry.includes("/cohorts"))).toContain("from_release=2018&to_release=2023");
  const sentence = page.getByTestId("age-section-cohorts-sentence-men-25-29");
  await expect(sentence).toHaveText(/(rose|fell|was unchanged|was not distinguishable from it)/);
  await expect(sentence).not.toHaveText(/sharply|because|due to|better|worse/);
});

test("the compare page draws both pyramids on one axis, each outlining the other", async ({ page }) => {
  const seen = [];
  await installPlaceFixtures(page);
  await installAgeFixtures(page, { seen });
  await page.goto("/us/wisconsin/dane-county/vs/us/wisconsin/rock-county");
  await expect(page.getByTestId("compare-age-a")).toHaveAttribute("data-reference", "state:55|county:105");
  await expect(page.getByTestId("compare-age-b")).toHaveAttribute("data-reference", "state:55|county:025");
  expect(ageRequests(seen).filter((entry) => entry.includes("compare_geo_ids=state%3A55%7Ccounty%3A105"))).toHaveLength(1);
});

test("a single age line is not offered as a map, and points to the explorer", async ({ page }) => {
  await installPlaceFixtures(page);
  await page.goto("/map/CENSUS_ACS%3Aacs5%3AB01001_011");
  await expect(page.getByTestId("measure-map-age-line")).toBeVisible();
  await expect(page.getByTestId("measure-map-age-line-explore")).toHaveAttribute("href", /\/explore\?/);
});
