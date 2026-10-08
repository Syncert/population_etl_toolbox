import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installUseCaseFixtures } from "../support/useCaseScenarios.js";

// Covers: WEB-126 — the explainer index and one explainer at desktop and
// 390px, the worked example's session-only place, an unknown slug, and a
// profile card linking its caveat.

async function noViolations(page) {
  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
}

test("the index lists every explainer", async ({ page }) => {
  await installUseCaseFixtures(page);
  await page.goto("/explain");
  await expect(page.locator("main")).toHaveCount(1);
  await expect(page.getByRole("heading", { level: 1 })).toHaveCount(1);
  await expect(page.getByTestId("explainer-link")).toHaveCount(12);
  await expect(page).toHaveTitle(/Explainers/);
  await noViolations(page);
});

test("an explainer with no last place shows the national value and says so", async ({ page }) => {
  await installUseCaseFixtures(page);
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.goto("/explain/margin-of-error");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText("Why does a survey estimate have a margin of error?");
  await expect(page.getByRole("heading", { level: 2 })).toHaveText(["Short answer", "What it is not", "Worked example", "Where it is used"]);
  await expect(page.getByTestId("explainer-example-note")).toContainText("national value");
  await expect(page.getByTestId("explainer-example-nation")).toContainText("United States");
  await expect(page.getByTestId("explainer-example-place")).toHaveCount(0);
  await expect(page).toHaveTitle(/margin of error/);
  await noViolations(page);
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("the worked example reads the last place from session state, never the URL", async ({ page }) => {
  await installUseCaseFixtures(page);
  await page.goto("/profiles?place=state%3A55%7Ccounty%3A025");
  await expect(page.getByTestId("profile-product")).toHaveAttribute("data-geo-id", "state:55|county:025");
  const link = page.getByTestId("measure-explainer-unemployment-rate");
  await expect(link).toHaveAttribute("href", "/explain/unemployment-rate");
  await link.click();
  await expect(page).toHaveURL(/\/explain\/unemployment-rate$/);
  await expect(page.getByTestId("explainer-example-place")).toContainText("Dane County");
  await expect(page.getByTestId("explainer-example-place")).toContainText("2024");
  expect(new URL(page.url()).search).toBe("");
});

test("an unknown explainer is a 404", async ({ page }) => {
  await installUseCaseFixtures(page);
  const response = await page.goto("/explain/no-such-explainer");
  expect(response.status()).toBe(404);
  await expect(page.getByTestId("route-not-found")).toBeVisible();
});
