import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-032, WEB-035 — the Content-Security-Policy carries a per-request nonce
// and no longer admits inline scripts wholesale. Every response's policy
// names a fresh nonce under `script-src` with `'strict-dynamic'` and without
// `'unsafe-inline'`; every inline script the framework emits carries that
// same nonce; two responses never share one; and the routes that load the
// most script -- the explorer with MapLibre and its blob workers, the
// composer -- render and run with no `securitypolicyviolation` event at all,
// which is the only evidence that the policy admits exactly what the page
// needs and nothing it does not.
//
// WEB-035 splits styles the same way, as far as they can be split. CSP3
// separates `<style>` elements (`style-src-elem`) from `style=""` attributes
// (`style-src-attr`), and only one of those doors can be closed:
//
//   - No page emits or creates a `<style>` element. Measured on the
//     production server on 2026-09-12: zero on `/`, `/explore` (with the map
//     rendered and hovered), and `/builder`. So production admits `'self'`
//     and nothing else. Under `next dev` there is exactly one -- Next's own
//     `@font-face` block for the dev overlay font -- which is why the
//     development policy keeps the exception and why the assertion below is
//     written as "every style element is Next's own" rather than "there are
//     none": it then holds in both modes, vacuously where there are none.
//   - Attribute styles cannot be removed. MapLibre writes them on the canvas
//     it creates, Next writes one on its route announcer, and three of this
//     application's own styles are values rather than rules -- the legend
//     swatch's colour, a coverage segment's width, the map tooltip's pointer
//     position. A nonce cannot cover an attribute and `'unsafe-hashes'` needs
//     one hash per exact declaration, which a data-driven value cannot have.
//
// The production policy is graded by `scripts/check-csp-nonce.mjs`, which
// reads the built middleware: this suite runs against `next dev`, where the
// element exception is live, so it cannot see the shipped policy itself.

const scriptSrcOf = (csp) =>
  (csp || "")
    .split(";")
    .map((directive) => directive.trim())
    .find((directive) => directive.startsWith("script-src ")) || "";

const nonceOf = (csp) => {
  const match = scriptSrcOf(csp).match(/'nonce-([A-Za-z0-9+/=]+)'/);
  return match ? match[1] : "";
};

const styleSrcOf = (csp, directive) =>
  (csp || "")
    .split(";")
    .map((part) => part.trim())
    .find((part) => part.startsWith(`${directive} `)) || "";

/** Every `<style>` element in the DOM, with the text that identifies its owner. */
const styleElements = (page) =>
  page.evaluate(() =>
    [...document.querySelectorAll("style")].map((element) =>
      (element.textContent || "").replace(/\s+/g, " ").slice(0, 80),
    ),
  );

/**
 * What carries a `style` attribute, as `tag.first-class-name`, falling back to
 * the first `data-` attribute when an element has no class. An unclassed
 * `<span style="">` is otherwise indistinguishable from any other, and this
 * list is only meaningful if each entry names one thing.
 */
const styleAttributeOwners = (page) =>
  page.evaluate(() =>
    [...document.querySelectorAll("[style]")].map((element) => {
      const name = element.className;
      const text = typeof name === "string" ? name : (name && name.baseVal) || "";
      const tag = element.tagName.toLowerCase();
      if (text.trim()) {
        return `${tag}.${text.trim().split(" ")[0]}`;
      }
      const data = Object.keys(element.dataset)[0];
      return data ? `${tag}[data-${data}]` : `${tag}`;
    }),
  );

/** The owners this application accepts an attribute style from, and why. */
const ALLOWED_STYLE_ATTRIBUTE_OWNERS = [
  // MapLibre writes these itself, on elements it creates.
  "canvas.maplibregl-canvas",
  "div.maplibregl-canvas-container",
  "div.maplibregl-ctrl",
  "div.maplibregl-control-container",
  // Next's own accessibility announcer, and its child.
  "next-route-announcer",
  "p",
  // Dev only, and it says so itself: the overlay Next injects under
  // `next dev` positions itself with a style attribute. The production
  // server serves none -- measured on 2026-09-12, `script[style]` is empty
  // there, which is why production needs no exception for it.
  "script[data-nextjsDevOverlay]",
  "nextjs-portal",
  // This application's three, each a value rather than a rule: a colour from
  // the choropleth scale, a coverage bar segment's share, and the pointer
  // position of the map tooltip. None can become a class.
  "span.legend-swatch",
  "span[data-segment]",
  "div.county-tooltip",
];

async function collectViolations(page) {
  await page.addInitScript(() => {
    window.__cspViolations = [];
    document.addEventListener("securitypolicyviolation", (event) => {
      window.__cspViolations.push({
        directive: event.violatedDirective,
        blocked: event.blockedURI,
        sample: event.sample,
      });
    });
  });
}

test("every page response names a fresh nonce and forbids inline scripts", async ({ page }) => {
  await collectViolations(page);
  const nonces = [];
  for (const route of ["/", "/builder", "/"]) {
    const response = await page.goto(route);
    const csp = response.headers()["content-security-policy"];
    const scriptSrc = scriptSrcOf(csp);

    const nonce = nonceOf(csp);
    expect(nonce, `${route} carries a nonce`).not.toBe("");
    expect(scriptSrc).toContain("'strict-dynamic'");
    // The wide door is closed: no inline script runs without the nonce.
    expect(scriptSrc).not.toContain("'unsafe-inline'");
    // The rest of the policy is unchanged from what WEB-025 gates.
    expect(csp).toContain("default-src 'self'");
    expect(csp).toContain("object-src 'none'");
    expect(csp).toContain("worker-src 'self' blob:");
    nonces.push(nonce);

    // Every executable inline script in the HTML the server emits carries
    // that response's nonce. Fetched as a plain document request so the
    // whole body and its own policy header are read together: the served
    // document is what the policy is enforced against, and scripts a nonced
    // script later creates through the DOM run under 'strict-dynamic' by
    // design and carry none, so the hydrated DOM is not the thing to
    // inspect. A JSON payload is data, not code.
    const served = await page.request.get(route);
    const servedNonce = nonceOf(served.headers()["content-security-policy"]);
    expect(servedNonce).not.toBe("");
    const html = await served.text();
    const inlineTags = [...html.matchAll(/<script([^>]*)>/g)]
      .map((match) => match[1])
      .filter((attributes) => !/src=/.test(attributes))
      .filter((attributes) => !/type="application\/json"/.test(attributes));
    expect(inlineTags.length, `${route} emits inline scripts`).toBeGreaterThan(0);
    for (const attributes of inlineTags) {
      expect(attributes, `${route}: <script${attributes}>`).toContain(`nonce="${servedNonce}"`);
    }
    nonces.push(servedNonce);
    // The page under the browser's own response hydrated, so the framework's
    // nonced scripts ran under the policy that named them.
    await expect(page.locator("main")).toHaveCount(1);
  }
  // No two responses share a nonce: three navigations and three fetches.
  expect(new Set(nonces).size).toBe(6);
});

test("the explorer, with MapLibre and its blob workers, runs under the policy without a violation", async ({
  page,
}) => {
  await collectViolations(page);
  await page.route("**/api/v1/**", (route) =>
    route.fulfill({ status: 503, json: { detail: "unavailable for this check" } }),
  );
  await page.route("**/tiles/**", (route) =>
    route.fulfill({ status: 503, json: { detail: "unavailable for this check" } }),
  );
  await page.goto("/explore");
  // The page rendered and its script ran: the unavailable API is reported
  // as a named state, which only a running client can produce.
  await expect(page.getByTestId("dashboard")).toBeVisible();
  await page.waitForTimeout(500);
  const violations = await page.evaluate(() => window.__cspViolations);
  expect(violations, JSON.stringify(violations)).toEqual([]);
});

test("the composer and the reader run under the policy without a violation", async ({ page }) => {
  await collectViolations(page);
  for (const route of ["/builder", "/articles", "/saved"]) {
    await page.goto(route);
    await expect(page.locator("main")).toHaveCount(1);
    await page.waitForTimeout(300);
    const violations = await page.evaluate(() => window.__cspViolations);
    expect(violations, `${route}: ${JSON.stringify(violations)}`).toEqual([]);
  }
});

test("the style policy closes the element door and names what it keeps open", async ({ page }) => {
  await collectViolations(page);
  await page.route("**/api/v1/**", (route) =>
    route.fulfill({ status: 503, json: { detail: "unavailable for this check" } }),
  );

  for (const route of ["/", "/explore", "/builder"]) {
    const response = await page.goto(route);
    const csp = response.headers()["content-security-policy"];

    // The two directives exist at all: without them both fall back to
    // `style-src`, which admits inline styles wholesale and is exactly the
    // door this closes.
    const elementPolicy = styleSrcOf(csp, "style-src-elem");
    const attributePolicy = styleSrcOf(csp, "style-src-attr");
    expect(elementPolicy, `${route} declares style-src-elem`).not.toBe("");
    expect(elementPolicy).toContain("'self'");
    expect(attributePolicy, `${route} declares style-src-attr`).toBe(
      "style-src-attr 'unsafe-inline'",
    );

    // Nothing the server sends is an inline style element, in either mode.
    // The production build carries no `<style>` at all; what makes that true
    // is that the application has one stylesheet and imports it.
    const served = await page.request.get(route);
    const html = await served.text();
    expect(
      (html.match(/<style[\s>]/g) || []).length,
      `${route} serves an inline <style> element`,
    ).toBe(0);

    await expect(page.locator("main")).toHaveCount(1);
    await page.waitForTimeout(300);

    // And nothing the page creates at runtime is one either, except Next's
    // own dev-overlay font block. In a production build this list is empty
    // and the assertion holds vacuously -- which is the point: the same test
    // states the same rule in both modes.
    for (const text of await styleElements(page)) {
      expect(text, `${route} created a <style> element this policy would block`).toContain(
        "__nextjs",
      );
    }

    // The attribute door stays open, so say precisely who walks through it.
    // An owner appearing here that is not on the list is a new inline style
    // to justify or remove, not a test to widen without reading it.
    const owners = [...new Set(await styleAttributeOwners(page))];
    for (const owner of owners) {
      expect(
        ALLOWED_STYLE_ATTRIBUTE_OWNERS,
        `${route}: '${owner}' carries a style attribute and is not a recorded source`,
      ).toContain(owner);
    }
  }

  const violations = await page.evaluate(() => window.__cspViolations);
  expect(violations, JSON.stringify(violations)).toEqual([]);
});
