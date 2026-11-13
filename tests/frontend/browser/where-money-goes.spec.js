import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-176 — the Work and Money chapter's "Where the money goes" group
// (where-public-money-goes slice 5): one card per government level with its
// largest lines as shares of that level's own total and the rest as one bar,
// amounts in dollars, a missing amount "not published", no share where none
// was published, and the separate-figures line.

async function ready(page, path) {
  await installPlaceFixtures(page);
  await page.goto(path);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
}

async function noViolations(page, selector) {
  const results = await new AxeBuilder({ page }).include(selector).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
}

test("a county shows its local governments' spending and the federal awards recorded there, never added", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin/dane-county");
  const group = page.locator("#work-money").getByTestId("where-money");
  await expect(group).toBeVisible();

  const local = group.getByTestId("where-money-state_local_spending");
  await expect(local.getByRole("heading")).toHaveText("What local governments in the county area spend, fiscal year ending in 2022");
  await expect(local.getByTestId("where-money-state_local_spending-total")).toHaveText("Direct general spending: $3.00 billion.");
  const education = local.getByTestId("where-money-state_local_spending-bar-spend_elementary_secondary_education");
  await expect(education).toContainText("46.9%");
  await expect(education).toContainText("$1.41 billion");
  // Six lines, then the rest of Census's own total as one bar.
  await expect(local.getByTestId("where-money-state_local_spending-bars").getByRole("row")).toHaveCount(7);
  await expect(local.getByTestId("where-money-state_local_spending-bar-rest")).toContainText("Everything else");
  await local.getByTestId("where-money-state_local_spending-all").locator("summary").click();
  await expect(local.getByTestId("where-money-state_local_spending-all").getByRole("row", { name: /Fire protection/ })).toContainText("not published");

  // The county's grants are shown as published, with no share: no all-award total.
  const byType = group.getByTestId("where-money-federal_awards_by_type");
  await expect(byType.getByTestId("where-money-federal_awards_by_type-bars")).toHaveCount(0);
  await expect(byType.getByTestId("where-money-federal_awards_by_type-unshared")).toContainText("all-award total not published");
  // With no share to draw, the amounts are shown open.
  await expect(byType.getByTestId("where-money-federal_awards_by_type-all").locator("summary")).toHaveCount(0);
  await expect(byType.getByRole("row", { name: /Grants/ })).toContainText("$2.98 billion");
  await expect(group.getByTestId("where-money-federal_awards_by_agency-empty")).toHaveText("Not published for this place in fiscal 2024.");

  await expect(group.getByTestId("where-money-separate")).toContainText("never added");
  await expect(group).not.toContainText(/score|rank/i);
  await noViolations(page, "[data-testid=where-money]");
});

test("a state shows its federal award shares and the nation its federal outlays", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1100 });
  await ready(page, "/us/wisconsin");
  const state = page.getByTestId("where-money");
  await expect(state.getByTestId("where-money-federal_awards_by_type-total")).toHaveText("All federal award obligations recorded here: $112 billion.");
  // A negative group (loan deobligations) draws no bar; it is in the table.
  await expect(state.getByTestId("where-money-federal_awards_by_type-bar-loans")).toHaveCount(0);
  await expect(state.getByTestId("where-money-federal_awards_by_type-bar-direct_payments")).toContainText("49.8%");
  // Agencies add to less than the total: what no agency holds is one bar of the same total.
  await expect(state.getByTestId("where-money-federal_awards_by_agency-bar-rest")).toContainText("Not in any line here");
  await expect(state.getByTestId("where-money-federal_awards_by_agency-bar-rest")).toContainText("19.1%");

  await ready(page, "/us");
  const nation = page.getByTestId("where-money");
  await expect(nation.getByTestId("where-money-federal_outlays")).toBeVisible();
  await expect(nation.getByTestId("where-money-federal_outlays-total")).toHaveText("Total outlays: $7.01 trillion.");
  await expect(nation.getByTestId("where-money-federal_awards_by_type")).toHaveCount(0);
});

test("a phone keeps the bars and shares inside the page", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 900 });
  await ready(page, "/us/wisconsin/dane-county");
  const group = page.getByTestId("where-money");
  await group.scrollIntoViewIfNeeded();
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(overflow).toBeLessThanOrEqual(0);
  await expect(group.getByTestId("where-money-state_local_spending-bar-spend_highways")).toContainText("10.0%");
});
