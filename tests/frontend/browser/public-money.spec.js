import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-168 — the Work and Money chapter's "Taxes and public money"
// group and a state's governor table (taxes-and-public-money T-6): dot strips
// for place, state and nation on one scale, lines labelled at their ends, the
// county-area tax burden said in words, governor changes in neutral grey, the
// ADR-0014 wording, empty groups said as not loaded, maps one link away.

async function ready(page, path) {
  await installPlaceFixtures(page);
  await page.goto(path);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
}

async function noViolations(page, selector) {
  const results = await new AxeBuilder({ page }).include(selector).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
}

test("a county reads payments, income sources, tax rate, tax burden and property tax beside its state and the nation", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const group = page.getByTestId("public-money");
  await expect(group).toBeVisible();
  await expect(page.locator("#work-money").getByTestId("public-money")).toHaveCount(1);
  await expect(group.getByTestId("public-money-legend").getByRole("listitem")).toHaveText(["Dane County", "Wisconsin", "United States"]);

  const total = group.getByTestId("public-money-payments-strips-total");
  await expect(total.getByRole("rowheader")).toHaveText("All payments");
  await expect(total).toContainText("13.1%");
  await expect(total.locator(".public-money-dot")).toHaveCount(3);
  // The nation's unpublished program draws no dot and is never a zero.
  await expect(group.getByTestId("public-money-payments-strips-unemployment").locator(".public-money-dot")).toHaveCount(2);
  await expect(group.getByTestId("public-money-payments-gap")).toHaveText("Dane County: payments are 4.8 points below the nation.");
  await expect(group.getByTestId("public-money-payments-map")).toHaveAttribute("href", "/map/BEA%3ACAINC35%3A1000?view=share");

  await expect(group.getByTestId("public-money-income-strips-earnings")).toContainText("67.0%");
  await expect(group.getByTestId("public-money-tax-rate-lines-this-place")).toContainText("Dane County 13.4%");
  await expect(group.getByTestId("public-money-tax-rate-lines-nation")).toContainText("United States 11.4%");
  await expect(group.getByTestId("public-money-tax-rate-map")).toHaveAttribute("href", "/map/IRS_SOI%3Aincome_tax_amount?view=per_capita");

  // State and local taxes: the state and the nation as lines; the county
  // area's local taxes are another measure, said in words with Census's years.
  await expect(group.getByTestId("public-money-tax-burden-lines-this-place")).toHaveCount(0);
  await expect(group.getByTestId("public-money-tax-burden-lines-state")).toBeVisible();
  await expect(group.getByTestId("public-money-county-burden")).toContainText("4.1% of residents' income in fiscal 2022");
  await expect(group.getByTestId("public-money-county-burden")).toContainText("years ending in 2 and 7");

  await expect(group.getByTestId("public-money-property-strips-property")).toContainText("1.6%");
  await expect(group).toContainText("none is a score");
  await expect(page.getByTestId("governor-terms")).toHaveCount(0);

  // Hover reads every level's value for the year under the pointer.
  const lines = group.getByTestId("public-money-tax-rate-lines").locator("svg");
  await lines.scrollIntoViewIfNeeded();
  const box = await lines.boundingBox();
  await lines.hover({ position: { x: box.width * 0.6, y: box.height / 2 } });
  await expect(group.getByTestId("public-money-tax-rate-lines-readout")).toContainText(/20\d\d: Dane County/);

  await noViolations(page, "[data-testid=public-money]");
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("a state page adds its governors, with changes against the nation in grey and the standing wording", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin");
  await expect(page.getByTestId("public-money-legend").getByRole("listitem")).toHaveText(["Wisconsin", "United States"]);
  const table = page.getByTestId("governor-table");
  await expect(table.getByRole("row")).toHaveCount(4);
  await expect(page.getByTestId("governor-walker")).toContainText("Scott Walker");
  await expect(page.getByTestId("governor-walker")).toContainText("2010–2018");
  await expect(page.getByTestId("governor-evers")).toContainText("2018–2024 so far");
  await expect(page.getByTestId("governor-walker-tax_burden")).toContainText("−0.9");
  await expect(page.getByTestId("governor-doyle-tax_burden")).toHaveText("Not published for the first year compared");
  const bar = page.getByTestId("governor-walker-tax_burden").locator(".governor-change-bar");
  await expect(bar).toHaveCSS("background-color", "rgb(134, 133, 127)");
  await expect(page.getByTestId("governor-mix-walker").locator(".governor-mix-bar span")).toHaveCount(4);
  await expect(page.getByTestId("governor-terms")).toContainText("comparisons, not causes");
  await expect(page.getByTestId("governor-terms")).not.toContainText(/score|rank/i);
  await noViolations(page, "[data-testid=governor-terms]");
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("the nation reads its own history, and an unloaded county says so group by group", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us");
  await expect(page.getByTestId("public-money-legend")).toHaveCount(0);
  await expect(page.getByTestId("public-money-payments-strips-total")).toContainText("17.9%");

  await ready(page, "/us/wisconsin/rock-county");
  for (const id of ["payments", "income", "tax-rate", "tax-burden", "property"]) {
    await expect(page.getByTestId(`public-money-${id}-empty`)).toHaveText("Not loaded into this site yet.");
  }
});
