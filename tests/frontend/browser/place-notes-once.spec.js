import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-181 — a note two charts or two spending levels share is said
// once (website UX review 2026-11, C3), and a basis line is not dressed as a
// link (B8).

test("a chapter's charts share a note once, and spending levels share theirs once", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await installPlaceFixtures(page);
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");

  // Housing draws two survey trends: the five-year-estimate note sits under
  // the first, and the second links to it.
  const housing = page.getByTestId("chapter-housing");
  const fiveYear = housing.getByText(/^Each point is a five-year estimate/);
  await expect(fiveYear).toHaveCount(1);
  await expect(page.getByTestId("chapter-housing-trend").getByText(/^Each point is a five-year estimate/)).toBeVisible();
  const above = page.getByTestId("chapter-housing-trend-median-home-value-notes-above");
  await expect(above).toContainText("apply here too");
  await expect(above.getByRole("link")).toHaveAttribute("href", "#chapter-housing-trend");
  // A note about a chart's own bars stays with each chart that draws them.
  await expect(housing.getByText(/^The bar through each point/)).toHaveCount(2);

  // The place-of-performance note belongs to more than one spending level.
  const money = page.getByTestId("where-money");
  await expect(money.getByText(/^Recorded where the awarded work is performed/)).toHaveCount(1);
  await expect(page.getByTestId("where-money-shared-note")).toHaveCount(1);

  // A basis line reads as a sentence: body ink, not the link colour.
  const basis = page.locator(".place-basis").first();
  const link = page.locator("main a.text-link").first();
  expect(await basis.evaluate((node) => getComputedStyle(node).color)).not.toBe(await link.evaluate((node) => getComputedStyle(node).color));
});
