import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";
import { installAgeFixtures } from "../support/ageScenarios.js";

// Covers: WEB-182 — on a phone, a place page and the header draw no text
// under 12 px and no link or toggle under 24 px in either dimension
// (website UX review 2026-11: G1, G2). Text inside a chart is sized by the
// chart (WEB-184, below).

test("a county page on a phone has no text under 12 px and no control under 24 px", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installPlaceFixtures(page);
  // The age section brings the page's tick box and its "draft" marks.
  await installAgeFixtures(page, { seen: [] });
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");

  const found = await page.evaluate(() => {
    const name = (node) => `${node.tagName.toLowerCase()}.${String(node.className?.baseVal ?? node.className).split(" ")[0]} "${node.textContent.trim().slice(0, 30)}"`;
    const small = [];
    const controls = [];
    for (const node of document.querySelectorAll("body *")) {
      if (node.closest("svg") || !node.offsetParent) continue;
      const ownText = [...node.childNodes].some((child) => child.nodeType === 3 && child.textContent.trim());
      if (ownText && parseFloat(getComputedStyle(node).fontSize) < 12) small.push(name(node));
      if (node.matches("a, button, summary, input, select")) {
        const box = node.getBoundingClientRect();
        if (box.width > 0 && (box.height < 24 || box.width < 24)) controls.push(`${name(node)} ${Math.round(box.width)}x${Math.round(box.height)}`);
      }
    }
    return { small, controls };
  });
  expect(found.small).toEqual([]);
  expect(found.controls).toEqual([]);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

// Covers: WEB-184 — on a phone, the words and numbers inside a place page's
// charts are drawn at about 10 px or more: the drawing's own text size times
// the scale the drawing is shown at (website UX review D1, live check on
// 64ecfb6a: 5.1 px on the public money lines, 7.3 px on the age charts).
test("text inside a county page's charts is drawn at 10 px or more on a phone", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installPlaceFixtures(page);
  await installAgeFixtures(page, { seen: [] });
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");
  await expect(page.getByTestId("public-money-tax-rate-lines").locator("svg")).toHaveAttribute("data-narrow", "true");
  // On a phone the end label is the value in the line's colour; the legend names the colours.
  await expect(page.getByTestId("public-money-tax-rate-lines-this-place").locator("text")).toHaveText("13.4%");

  const small = await page.evaluate(() => {
    const found = [];
    for (const svg of document.querySelectorAll(".place-page svg[viewBox]")) {
      const box = svg.getBoundingClientRect();
      const width = svg.viewBox.baseVal.width;
      if (!box.width || !width) continue;
      for (const text of svg.querySelectorAll("text")) {
        if (!text.textContent.trim()) continue;
        const drawn = parseFloat(getComputedStyle(text).fontSize) * (box.width / width);
        if (drawn < 9.8) found.push(`${String(svg.closest("[data-testid]")?.dataset.testid)} "${text.textContent.trim().slice(0, 20)}" ${drawn.toFixed(1)}`);
      }
    }
    return found;
  });
  expect(small).toEqual([]);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});
