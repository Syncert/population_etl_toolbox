import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-160 — every trend chart says what it draws (UAT on the Madison
// metro page, where the work chart showed only "14" and "1.3"): a heading
// naming the measure in plain words, its unit, how often it is published and
// the span, its source, a y-axis label, the legend above the plot, and links
// between the chart and the headline card it draws.

async function ready(page, path) {
  await installPlaceFixtures(page);
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.goto(path);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
}

test("a metro page's trend charts each name their measure, unit, frequency, source and card", async ({ page }) => {
  await ready(page, "/metro/madison-wi-metro-area");
  const charts = page.locator("figure.place-trend");
  await expect(charts.first()).toBeVisible();
  const count = await charts.count();
  expect(count).toBeGreaterThan(0);
  for (let index = 0; index < count; index += 1) {
    const chart = charts.nth(index);
    const testId = await chart.getAttribute("data-testid");
    const chapterTrend = testId.startsWith("chapter-");
    // A title in words, never a metric code.
    if (chapterTrend) await expect(chart.getByTestId(`${testId}-title`)).toHaveText(/ over time$/);
    await expect(chart.getByTestId(`${testId}-title`)).not.toHaveText("");
    await expect(chart.getByTestId(`${testId}-title`)).not.toHaveText(/[A-Z_]+:[A-Za-z0-9_]+/);
    await expect(chart.getByTestId(`${testId}-meta`)).toHaveText(/(monthly|quarterly|yearly|five-year estimates|multi-year)?.*\d{4}/);
    await expect(chart.getByTestId(`${testId}-source`)).toHaveText(/^Source: \S/);
    await expect(chart.getByTestId(`${testId}-y-label`)).not.toHaveText("");
    // The legend is above the plot and inside the chart's box.
    const legend = await chart.locator(".place-trend-legend").boundingBox();
    const plot = await chart.locator('svg[role="img"]').boundingBox();
    const box = await chart.boundingBox();
    expect(legend.y + legend.height).toBeLessThanOrEqual(plot.y + 1);
    expect(legend.y + legend.height).toBeLessThanOrEqual(box.y + box.height);
    // A chapter's chart names its card and links to it.
    if (!chapterTrend) continue;
    const toCard = chart.getByTestId(`${testId}-for`).getByRole("link");
    const anchor = (await toCard.getAttribute("href")).slice(1);
    await expect(page.locator(`#${anchor}`)).toHaveCount(1);
  }
  const unemployment = page.getByTestId("chapter-work-money-trend");
  await expect(unemployment.getByTestId("chapter-work-money-trend-title")).toHaveText("Unemployment rate over time");
  await expect(unemployment.getByTestId("chapter-work-money-trend-source")).toHaveText(
    "Source: Bureau of Labor Statistics, Local Area Unemployment Statistics (LAUS)",
  );
  await expect(unemployment.getByTestId("chapter-work-money-trend-y-label")).toHaveText("Percent");
  const results = await new AxeBuilder({ page }).include("figure.place-trend").withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
});

test("a city's history charts are titled and sourced the same way", async ({ page }) => {
  await ready(page, "/us/wisconsin/madison-city");
  const income = page.getByTestId("chapter-work-money-trend-median-household-income");
  await expect(income.getByTestId("chapter-work-money-trend-median-household-income-title")).toHaveText("Median household income over time");
  await expect(income.getByTestId("chapter-work-money-trend-median-household-income-source")).toHaveText(
    "Source: U.S. Census Bureau, American Community Survey 5-year estimates",
  );
  await expect(income.getByTestId("chapter-work-money-trend-median-household-income-meta")).toHaveText(/^dollars · (yearly|five-year estimates), \d{4}/i);
  await expect(income.getByTestId("chapter-work-money-trend-median-household-income-for").getByRole("link")).toHaveAttribute(
    "href",
    "#chapter-work-money-card-median-household-income",
  );
});
