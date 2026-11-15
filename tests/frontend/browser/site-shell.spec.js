import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-178 — the header and a place page's chapter links leave the
// page to the reader on a phone and stay in reach on a long page.
//
// The website UX review (2026-11: A3, A4, C2, F1) measured a 148 px header
// stuck over a 844 px phone screen, chapter links that scrolled away on a
// page tens of screens long, and a nation page with no search.

async function ready(page, path) {
  await installPlaceFixtures(page);
  await page.goto(path);
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
}

const railTop = (page) => page.getByTestId("chapter-rail").evaluate((rail) => rail.getBoundingClientRect().top);

test("on a phone the header is one short block that scrolls away, and the chapter links stay", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await ready(page, "/us/wisconsin/dane-county");

  const header = page.locator(".site-header");
  expect((await header.boundingBox()).height).toBeLessThan(110);
  const nav = page.getByRole("navigation", { name: "Primary navigation" });
  // One row: every entry sits on the same line, the long label shortened.
  const tops = await nav.locator(":scope > .nav-link, :scope > details > summary").evaluateAll((links) => links.map((link) => Math.round(link.getBoundingClientRect().top)));
  expect(new Set(tops).size).toBe(1);
  await expect(nav.getByRole("link", { name: "Sources", exact: true })).toHaveAttribute("href", "/data");
  await expect(page.getByRole("button", { name: "Sign in" })).toBeVisible();
  // A menu opens inside the screen.
  await page.getByTestId("tools-menu").locator("summary").click();
  const panel = await page.locator(".tools-dropdown").boundingBox();
  expect(panel.x).toBeGreaterThanOrEqual(0);
  expect(panel.x + panel.width).toBeLessThanOrEqual(390);
  await page.keyboard.press("Escape");

  await page.evaluate(() => window.scrollTo(0, 4000));
  await expect.poll(() => header.evaluate((node) => node.getBoundingClientRect().bottom)).toBeLessThan(0);
  await expect.poll(() => railTop(page)).toBe(0);
  // One row of links, scrolled sideways inside the bar, never the page.
  expect((await page.getByTestId("chapter-rail").boundingBox()).height).toBeLessThan(70);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("on a desktop the chapter links stay under the header, and a link lands its chapter below them", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await ready(page, "/us/wisconsin/dane-county");
  await expect(page.getByRole("navigation", { name: "Primary navigation" }).getByRole("link", { name: "Where the numbers come from" })).toBeVisible();

  await page.evaluate(() => window.scrollTo(0, 4000));
  const headerBottom = await page.locator(".site-header").evaluate((node) => node.getBoundingClientRect().bottom);
  await expect.poll(() => railTop(page)).toBeGreaterThanOrEqual(headerBottom - 1);
  expect(await railTop(page)).toBeLessThan(headerBottom + 2);

  await page.getByTestId("chapter-rail").getByRole("link", { name: "Housing" }).click();
  const railBottom = await page.getByTestId("chapter-rail").evaluate((rail) => rail.getBoundingClientRect().bottom);
  await expect.poll(() => page.locator("#housing").evaluate((node) => node.getBoundingClientRect().top)).toBeGreaterThanOrEqual(railBottom - 1);
});

test("the nation's page offers the place search; a county's does not repeat it", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await ready(page, "/us");
  await expect(page.getByTestId("place-finder-input")).toBeVisible();
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  await expect(page.getByTestId("place-finder")).toHaveCount(0);
});

test("the Tools menu is not marked as the current section on the home page", async ({ page }) => {
  await page.route("**/api/v1/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/v1/catalog/sources", (route) => route.fulfill({ json: [] }));
  await page.goto("/");
  await expect(page.getByTestId("tools-menu").locator("summary")).not.toHaveClass(/active/);
  await page.goto("/catalog");
  await expect(page.getByTestId("tools-menu").locator("summary")).toHaveClass(/active/);
});
