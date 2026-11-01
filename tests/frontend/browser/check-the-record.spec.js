import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-155 — check-the-record: every place page answers the four
// fixed questions under "What the record shows" with sentences built from
// published numbers, a chart, who reported (for crime), the sources and
// caveats, and a link to the rows; a county reads its crime rate history from
// agency reports in one request and says when coverage moved; a city sends
// its crime and unemployment questions to its county; nothing is read with a
// placeholder geography, and no generated sentence uses a forbidden word.

const FORBIDDEN = /\b(sharply|dramatically|significantly|soared|plummeted|because|due to|driven by|led to|better|worse|safer|dangerous)\b/i;

function apiCalls(page) {
  const calls = [];
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname.startsWith("/api/v1/")) calls.push(url);
  });
  return calls;
}

async function open(page, path) {
  await installPlaceFixtures(page);
  const calls = apiCalls(page);
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.goto(path);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  await expect(page.getByTestId("place-record")).toBeVisible();
  return calls;
}

test("a county page answers the four questions from published figures", async ({ page }) => {
  const calls = await open(page, "/us/wisconsin/dane-county");
  const record = page.getByTestId("place-record");
  await expect(record.getByRole("heading", { level: 2 })).toHaveText("What the record shows");
  await expect(record.getByRole("heading", { level: 3 })).toHaveText([
    "Has violent crime gone up here in ten years?",
    "Are rents rising faster than incomes?",
    "Is the population growing or shrinking, and why: births, deaths or migration?",
    "How does unemployment here compare with the state?",
  ]);

  // Crime: December 2024 against December 2014, with who reported in each.
  const crime = page.getByTestId("record-crime");
  await expect(crime).toHaveAttribute("data-status", "answered");
  await expect(crime.getByTestId("record-crime-answer")).toContainText(
    "The violent crime rate computed from agency reports in Dane County rose from 9 per 100,000 people its reporting agencies serve in December 2014 to 11.2 per 100,000 people its reporting agencies serve in December 2024",
  );
  const coverage = crime.getByTestId("record-crime-coverage");
  await expect(coverage).toContainText("In December 2014, 1 agency reported");
  await expect(coverage).toContainText("In December 2024, 2 agencies reported");
  await expect(coverage).toContainText("may reflect which agencies reported");
  await expect(crime.getByTestId("record-crime-chart-crime")).toBeVisible();
  await expect(crime.getByTestId("record-crime-link")).toHaveAttribute("href", /\/api\/v1\/observations\/derived\?.*newest_release_per_period=true/);

  // One read for the county's history, by the county's own id.
  const history = calls.filter((url) => url.pathname === "/api/v1/observations/derived" && url.searchParams.get("newest_release_per_period") === "true");
  expect(history.map((url) => [url.searchParams.get("ratio_id"), url.searchParams.get("geo_id"), url.searchParams.get("scope")]))
    .toEqual([["violent_crime_county_agency_rate", "state:55|county:025", "as_released"]]);

  // Rents and incomes: two series, then their percentages.
  const rent = page.getByTestId("record-rent-income-answer");
  await expect(rent).toContainText("Median gross rent in Dane County");
  await expect(rent).toContainText("Median household income in Dane County");
  await expect(rent).toContainText("the percent change in median gross rent");
  await page.getByTestId("record-rent-income-working").locator("summary").click();
  await expect(page.getByTestId("record-rent-income-caveats")).toContainText("no affordability index");

  // Population, with the program's own components.
  const population = page.getByTestId("record-population-answer");
  await expect(population).toContainText("The estimated resident population in Dane County rose from 1,000 in 2019 to 1,100 in 2024");
  await expect(population).toContainText("the program estimates");
  await expect(population).toContainText("births");
  await expect(population).toContainText("net international migration of");

  // Unemployment beside the state.
  await expect(page.getByTestId("record-unemployment-answer")).toContainText("Dane County's rate was lower than Wisconsin's.");

  const text = await record.innerText();
  expect(text).not.toMatch(FORBIDDEN);
  const results = await new AxeBuilder({ page }).include('[data-testid="place-record"]').withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("a county whose agencies fall under the floor reads no rate, never zero", async ({ page }) => {
  await open(page, "/us/wisconsin/rock-county");
  const crime = page.getByTestId("record-crime");
  await expect(crime).toHaveAttribute("data-status", "unanswered");
  await expect(crime.getByTestId("record-crime-answer")).toContainText("too few agencies reported to give a rate");
  await expect(crime.getByTestId("record-crime-answer")).not.toContainText(/\b0 per 100,000/);
});

test("a city answers rents and population, and sends crime and unemployment to its county", async ({ page }) => {
  const calls = await open(page, "/us/wisconsin/madison-city");
  const crime = page.getByTestId("record-crime");
  await expect(crime).toHaveAttribute("data-status", "elsewhere");
  await expect(crime.getByTestId("record-crime-link")).toHaveAttribute("href", "/us/wisconsin/dane-county#record");
  await expect(page.getByTestId("record-unemployment")).toHaveAttribute("data-status", "elsewhere");
  await expect(page.getByTestId("record-rent-income-answer")).toContainText("Madison city");
  await expect(page.getByTestId("record-population-answer")).toContainText("not for cities and towns; Dane County's components are on its page");
  // A city reads no county crime history of its own.
  expect(calls.filter((url) => url.pathname === "/api/v1/observations/derived" && url.searchParams.get("newest_release_per_period") === "true")).toEqual([]);
});

test("a state answers crime from the FBI's own rate and unemployment beside the nation", async ({ page }) => {
  await open(page, "/us/wisconsin");
  await expect(page.getByTestId("record-crime-answer")).toContainText("The FBI's violent crime rate in Wisconsin rose from");
  await expect(page.getByTestId("record-crime-coverage")).toContainText("the FBI published no coverage for this rate");
  await expect(page.getByTestId("record-unemployment").getByRole("heading", { level: 3 })).toHaveText("How does unemployment here compare with the nation?");
});
