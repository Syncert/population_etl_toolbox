import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-114 — the browser reaches the sink, in a browser.
//
// The unit tier proves what each report says and how it is sent; this proves
// the wiring those two rest on: that the listeners are registered on every
// route, that the sink is reachable at a same-origin path the CSP admits,
// and that a violation of that very policy is itself reported.
//
// The CSP is the thing under test here as much as the reporters are. A
// reporting endpoint that `connect-src` forbids reports nothing, and that
// failure is invisible until a production incident nobody hears about.

const SINK = "/client-report";

/** Every report the page posts, decoded, in the order they were sent. */
function watchReports(page) {
  const posted = [];
  page.on("request", (request) => {
    if (!new URL(request.url()).pathname.startsWith(SINK)) {
      return;
    }
    let payload = null;
    try {
      payload = JSON.parse(request.postData() || "null");
    } catch {
      payload = { unparsed: request.postData() };
    }
    posted.push({ method: request.method(), payload });
  });
  return posted;
}

/**
 * Take `sendBeacon` away, so the reports arrive by `fetch` instead.
 *
 * Not a convenience: Playwright cannot read the body of a beacon, because
 * Chromium does not expose a `Blob` payload to the protocol. A spec that
 * asserted only "a request was made" would pass just as well on a report
 * carrying a bearer token, and this spec exists to check what is *in* the
 * report. The beacon path is preferred in a real browser and is proven in the
 * unit tier, where the transport is injectable; the test below this one
 * checks the beacon still reaches the sink when it is available.
 */
async function reportByFetch(page) {
  await page.addInitScript(() => {
    Object.defineProperty(Navigator.prototype, "sendBeacon", {
      configurable: true,
      value: undefined,
    });
  });
}

async function settle(page) {
  // The reports are sent from listeners, not from the render, so the page
  // being loaded is not the same as the report having been sent.
  await page.waitForTimeout(500);
}

test("an uncaught error reaches the sink, named by its type", async ({ page }) => {
  await reportByFetch(page);
  const reports = watchReports(page);
  await page.goto("/");
  await expect(page.getByRole("heading", { level: 1 })).toBeVisible();

  // Thrown out of a task rather than inside `evaluate`, so it reaches
  // `window.onerror` as an uncaught error instead of returning a rejection
  // to Playwright.
  await page.evaluate(() => {
    setTimeout(() => {
      throw new TypeError("synthetic failure for WEB-114");
    }, 0);
  });
  await settle(page);

  const errors = reports.filter((entry) => entry.payload?.kind === "error");
  expect(errors.length).toBeGreaterThan(0);
  const [report] = errors;
  expect(report.method).toBe("POST");
  expect(report.payload.name).toBe("TypeError");
  expect(report.payload.route).toBe("/");
  expect(report.payload).toHaveProperty("buildId");
  // The closed shape, on the wire.
  expect(Object.keys(report.payload).sort()).toEqual([
    "buildId",
    "kind",
    "message",
    "name",
    "route",
  ]);
});

test("a policy violation reports itself, and the query never travels", async ({ page }) => {
  await reportByFetch(page);
  const reports = watchReports(page);
  // A route with a query string, so a report that leaked one would show it.
  await page.goto("/explore?metric=CENSUS_ACS%3Aacs5%3AB01003_001");
  await expect(page.getByTestId("dashboard")).toBeVisible();

  // `img-src 'self' data: blob:` forbids this in development and in
  // production alike -- unlike an inline style, which the development policy
  // still admits.
  await page.evaluate(() => {
    const image = document.createElement("img");
    image.src = "https://blocked.invalid/pixel.png";
    document.body.appendChild(image);
  });
  await settle(page);

  const violations = reports.filter((entry) => entry.payload?.kind === "csp");
  expect(violations.length).toBeGreaterThan(0);
  const [report] = violations;
  expect(report.payload.name).toContain("img-src");
  expect(report.payload.route).toBe("/explore");
  for (const entry of reports) {
    const serialized = JSON.stringify(entry.payload);
    expect(serialized, "a report carried a query string").not.toContain("B01003_001");
    expect(serialized).not.toContain("metric=");
  }
});

test("the sink answers 204 to anything, including a report it refuses", async ({ page }) => {
  await page.goto("/");

  const answered = await page.evaluate(async () => {
    const post = (body, type) =>
      fetch("/client-report", {
        method: "POST",
        headers: { "Content-Type": type },
        body,
      }).then((response) => response.status);
    return {
      // A report outside the closed shape.
      refused: await post(
        JSON.stringify({ kind: "error", name: "E", token: "secret" }),
        "application/json",
      ),
      // Not JSON at all.
      garbage: await post("not json", "application/json"),
      // A valid one.
      accepted: await post(
        JSON.stringify({ kind: "error", route: "/", name: "E", message: "", buildId: "b" }),
        "application/json",
      ),
      // And a browser's own violation shape.
      violation: await post(
        JSON.stringify({ "csp-report": { "violated-directive": "img-src" } }),
        "application/csp-report",
      ),
    };
  });

  // A reporter that is told "400" retries, and a retrying reporter against a
  // broken sink is a loop running in every reader's browser at once.
  expect(answered).toEqual({ refused: 204, garbage: 204, accepted: 204, violation: 204 });
});

test("the policy names the sink, so a browser can report without being asked", async ({
  page,
}) => {
  const response = await page.goto("/");
  const headers = response.headers();
  const policy = headers["content-security-policy"] || "";

  expect(policy).toContain(`report-uri ${SINK}`);
  expect(policy).toContain("report-to csp-endpoint");
  expect(headers["reporting-endpoints"]).toBe(`csp-endpoint="${SINK}"`);
  // And the sink is same-origin, so the policy did not have to be widened to
  // reach it.
  expect(policy).toContain("connect-src 'self'");
  expect(policy).not.toContain("connect-src 'self' http");
});

test("a real browser reports by beacon, which survives the navigation", async ({ page }) => {
  // Without the init script above, so `sendBeacon` is what the browser has.
  const posted = [];
  page.on("request", (request) => {
    if (new URL(request.url()).pathname.startsWith(SINK)) {
      posted.push(request.method());
    }
  });
  await page.goto("/");
  await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  await page.evaluate(() => {
    setTimeout(() => {
      throw new TypeError("synthetic failure for WEB-114");
    }, 0);
  });
  await settle(page);

  // The body is not readable here -- Chromium does not expose a beacon's Blob
  // to the protocol, which is why the two tests above take the beacon away.
  // What this proves is the part those cannot: the path a real reader's
  // browser takes reaches the sink.
  expect(posted.length).toBeGreaterThan(0);
  expect(new Set(posted)).toEqual(new Set(["POST"]));
});
