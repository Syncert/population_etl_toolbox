import { expect, test } from "../support/servedRequests.js";
import { installPlaceFixtures } from "../support/placeScenarios.js";

// Covers: WEB-182 — on a phone, a place page and the header draw no text
// under 12 px and no link or toggle under 24 px in either dimension
// (website UX review 2026-11: G1, G2). The wordmark's badge is a logo, and
// text inside a chart is sized by the chart (WEB-180).

test("a county page on a phone has no text under 12 px and no control under 24 px", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await installPlaceFixtures(page);
  await page.goto("/us/wisconsin/dane-county");
  await expect(page.getByTestId("place-page")).toHaveAttribute("data-ready", "true");

  const found = await page.evaluate(() => {
    const name = (node) => `${node.tagName.toLowerCase()}.${String(node.className?.baseVal ?? node.className).split(" ")[0]} "${node.textContent.trim().slice(0, 30)}"`;
    const small = [];
    const controls = [];
    for (const node of document.querySelectorAll("body *")) {
      if (node.closest("svg") || node.closest(".wordmark-mark") || !node.offsetParent) continue;
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
