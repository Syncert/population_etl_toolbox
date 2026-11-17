import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";
import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-186 — a place page's chapters show their headline cards and
// fold everything else behind one "More about ..." control each (website UX
// review C1; Nick 2026-11-16). Nothing is removed: a fold opens by its own
// control, by a link to something inside it, and all at once by one switch
// the browser remembers.

async function ready(page, path) {
  await installPlaceFixtures(page, { folded: true });
  await page.goto(path);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
}

test("a first visit shows headline cards with each chapter's detail folded, and a fold opens by its control", async ({ page }) => {
  await ready(page, "/us/wisconsin/dane-county");
  const chapter = page.getByTestId("chapter-people");
  const fold = page.getByTestId("chapter-people-more");
  await expect(chapter.locator(".place-cards").first()).toBeVisible();
  await expect(fold.locator(":scope > summary")).toHaveText("More about people");
  await expect(fold).not.toHaveAttribute("open", "");
  await expect(page.getByTestId("chapter-people-trend")).toBeHidden();
  // The period and source line stays outside the fold.
  await expect(page.getByTestId("chapter-people-footer")).toBeVisible();
  const folds = page.locator("details.place-chapter-more:not([hidden])");
  expect(await folds.count()).toBeGreaterThan(2);
  expect(await folds.evaluateAll((items) => items.filter((item) => item.open).length)).toBe(0);

  const foldedHeight = await page.evaluate(() => document.documentElement.scrollHeight);
  await fold.locator(":scope > summary").click();
  await expect(page.getByTestId("chapter-people-trend")).toBeVisible();
  // Only that chapter opened.
  expect(await folds.evaluateAll((items) => items.filter((item) => item.open).length)).toBe(1);
  expect(await page.evaluate(() => document.documentElement.scrollHeight)).toBeGreaterThan(foldedHeight);

  const results = await new AxeBuilder({ page }).include("#people").withTags(["wcag2a", "wcag2aa", "wcag21aa"]).analyze();
  expect(results.violations.map((item) => `${item.id}: ${item.help}`)).toEqual([]);
});

test("one switch opens every chapter's detail, folds it again, and is remembered", async ({ page }) => {
  await ready(page, "/us/wisconsin/dane-county");
  const folds = page.locator("details.place-chapter-more:not([hidden])");
  const open = () => folds.evaluateAll((items) => items.filter((item) => item.open).length);
  const total = await folds.count();
  const folded = await page.evaluate(() => document.documentElement.scrollHeight);
  const toggle = page.getByTestId("place-folds-toggle");
  await expect(toggle).toHaveText("Open every chapter's detail");
  await toggle.click();
  await expect(toggle).toHaveText("Fold every chapter's detail");
  expect(await open()).toBe(total);
  // Opening everything makes the page at least half as long again as its first read.
  expect(await page.evaluate(() => document.documentElement.scrollHeight)).toBeGreaterThan(folded * 1.5);

  await page.reload();
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  await expect(page.getByTestId("place-folds-toggle")).toHaveText("Fold every chapter's detail");
  expect(await open()).toBe(await folds.count());

  // A fold the reader opened by hand closes with the rest.
  await page.getByTestId("place-folds-toggle").click();
  expect(await open()).toBe(0);
  await page.getByTestId("chapter-people-more").locator(":scope > summary").click();
  await page.getByTestId("place-folds-toggle").click();
  await page.getByTestId("place-folds-toggle").click();
  expect(await open()).toBe(0);
});

test("an address or a link that names something inside a fold opens that fold", async ({ page }) => {
  await ready(page, "/us/wisconsin/dane-county#chapter-people-trend");
  await expect(page.getByTestId("chapter-people-more")).toHaveAttribute("open", "");
  await expect(page.getByTestId("chapter-people-trend")).toBeInViewport();
  // No other chapter opened.
  expect(await page.locator("details.place-chapter-more[open]").count()).toBe(1);

  // An in-page link: the change chapter's note points into People.
  await page.evaluate(() => {
    const link = document.createElement("a");
    link.href = "#chapter-work-money-trend";
    link.textContent = "to the chart";
    link.dataset.testid = "probe-link";
    document.querySelector("[data-testid='chapter-rail']").append(link);
  });
  await page.getByTestId("probe-link").click();
  await expect(page.getByTestId("chapter-work-money-more")).toHaveAttribute("open", "");
  await expect(page.getByTestId("chapter-work-money-trend")).toBeVisible();
});

test("a phone shows the folded page with a control a finger can hit", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await ready(page, "/us/wisconsin/dane-county");
  const box = await page.getByTestId("chapter-people-more").locator(":scope > summary").boundingBox();
  expect(box.height).toBeGreaterThanOrEqual(44);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});
