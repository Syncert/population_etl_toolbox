import { expect, test } from "../../../apps/web/node_modules/@playwright/test/index.mjs";

// Covers: WEB-032 — the Content-Security-Policy carries a per-request nonce
// and no longer admits inline scripts wholesale. Every response's policy
// names a fresh nonce under `script-src` with `'strict-dynamic'` and without
// `'unsafe-inline'`; every inline script the framework emits carries that
// same nonce; two responses never share one; and the routes that load the
// most script -- the explorer with MapLibre and its blob workers, the
// composer -- render and run with no `securitypolicyviolation` event at all,
// which is the only evidence that the policy admits exactly what the page
// needs and nothing it does not.

const scriptSrcOf = (csp) =>
  (csp || "")
    .split(";")
    .map((directive) => directive.trim())
    .find((directive) => directive.startsWith("script-src ")) || "";

const nonceOf = (csp) => {
  const match = scriptSrcOf(csp).match(/'nonce-([A-Za-z0-9+/=]+)'/);
  return match ? match[1] : "";
};

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
