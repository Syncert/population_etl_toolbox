import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join } from "node:path";

// By path, for the same reason `support/servedRequests.js` imports
// `@playwright/test` that way: these specs live outside `apps/web`, so Node
// resolves from `tests/frontend/browser` upward and never reaches
// `apps/web/node_modules`.
import AxeBuilder from "../../../apps/web/node_modules/@axe-core/playwright/dist/index.mjs";

import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-111 — an automated auditor runs on every route, and the route
// list is read from the application rather than kept by hand.
//
// WEB-025 promises that "every core workflow" is audited, and
// `accessibility-operations.spec.js` does that by hand against a nine-route
// list that `analytics-workbench` never added `/workbench` to. Hand-written
// assertions also cannot see a contrast failure, a duplicate id, a button
// with no accessible name or a table without headers -- an auditor reports
// all four for free.

/** The repository's `apps/web`, found by walking up. */
function webRoot() {
  let directory = process.cwd();
  for (;;) {
    try {
      statSync(join(directory, "apps", "web", "package.json"));
      return join(directory, "apps", "web");
    } catch {
      const parent = dirname(directory);
      if (parent === directory) throw new Error(`apps/web not found from ${process.cwd()}`);
      directory = parent;
    }
  }
}

const WEB = webRoot();

/**
 * Every route the application serves, from the directory that defines them.
 *
 * Derived, not listed: a route added later is audited because it exists, not
 * because someone remembered to add it here. The three retired routes that
 * only `redirect()` are excluded -- auditing them audits their destination
 * twice and reports its findings against the wrong path.
 */
function applicationRoutes() {
  const app = join(WEB, "app");
  const routes = ["/"];
  for (const entry of readdirSync(app)) {
    const page = join(app, entry, "page.js");
    let source;
    try {
      source = readFileSync(page, "utf8");
    } catch {
      continue;
    }
    if (/redirect\(/.test(source)) continue;
    routes.push(`/${entry}`);
  }
  return routes.sort();
}

/**
 * Findings this application cannot fix, each with the reason it stands.
 *
 * An allowlist of *findings*, never a disabled rule: disabling a rule hides
 * every future instance of it, including the ones that are this
 * application's fault. Each entry names the rule and the selector it is
 * allowed on.
 */
const ALLOWED = [
  {
    id: "region",
    selector: ".maplibregl-control-container",
    why: "MapLibre renders its own controls outside any landmark, and the container is not ours to place.",
  },
];

function unallowedViolations(results) {
  const found = [];
  for (const violation of results.violations) {
    for (const node of violation.nodes) {
      const target = node.target.join(" ");
      const allowed = ALLOWED.some(
        (entry) => entry.id === violation.id && target.includes(entry.selector),
      );
      if (!allowed) {
        found.push(`${violation.id} at ${target}: ${violation.help}`);
      }
    }
  }
  return found;
}

const sources = [{ source_code: "CENSUS_ACS", source_name: "Census ACS" }];

const capabilities = {
  total: 1,
  items: [
    {
      source_code: "CENSUS_ACS",
      display_name: "Census American Community Survey",
      route_segment: "census",
      served_by_neutral_routes: true,
      publishes_aligned_reduction: true,
      observation_filters: ["geo_id", "geo_level", "state_fips"],
      observation_routes: [
        {
          path: "/api/v1/observations",
          parameters: [
            "geo_id",
            "geo_level",
            "limit",
            "metric_code",
            "release",
            "scope",
            "state_fips",
          ],
        },
        { path: "/api/v1/observations/releases", parameters: ["limit", "metric_code", "offset"] },
      ],
    },
  ],
};

async function installRoutes(page) {
  await page.route("**/api/v1/health", (route) => route.fulfill({ json: { status: "ok" } }));
  await page.route("**/api/v1/catalog/capabilities", (route) =>
    route.fulfill({ json: capabilities }),
  );
  await page.route("**/api/v1/catalog/sources", (route) => route.fulfill({ json: sources }));
  await page.route("**/api/v1/catalog/**", (route) =>
    route.fulfill({ json: { total: 0, limit: 1000, offset: 0, items: [] } }),
  );
  await page.route("**/api/v1/**", (route) =>
    route.fulfill({ json: { total: 0, limit: 1000, offset: 0, items: [] } }),
  );
  await page.route("**/tiles/**", (route) => route.fulfill({ status: 404, body: "" }));
}

for (const route of applicationRoutes()) {
  test(`${route} passes the accessibility auditor`, async ({ page }) => {
    await installRoutes(page);
    await page.goto(route);
    // Every screen here renders its status surface on first paint, so the
    // audit runs against a settled page rather than a loading one.
    await expect(page.locator("main")).toHaveCount(1);

    const results = await new AxeBuilder({ page })
      .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
      .analyze();

    expect(unallowedViolations(results), `accessibility findings on ${route}`).toEqual([]);
  });
}

test("the route list is read from the application, not kept by hand", () => {
  // The nine-route list this replaces was missing `/workbench`, added by a
  // later plan. A derived list cannot be.
  const routes = applicationRoutes();
  expect(routes).toContain("/workbench");
  expect(routes).toContain("/");
  expect(routes.length).toBeGreaterThanOrEqual(10);
  // The retired redirect routes are not audited as themselves.
  for (const retired of ["/bls", "/census", "/fred"]) {
    expect(routes).not.toContain(retired);
  }
});
