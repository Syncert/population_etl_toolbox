import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, extname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";

// Covers: WEB-043 — a frontend fixture cannot describe a contract the API
// does not serve.
//
// The suites' fake `/catalog/capabilities` responses decide what every
// frontend test can observe, because the client sends only declared
// parameters. Three of those fixtures carried a comment claiming to be the
// served list while missing a parameter the API had gained, which does not
// fail -- it quietly stops testing the behaviour the fixture names.

import {
  requestComplaint,
  servedParameters,
  servedPathsMatching,
} from "../support/servedContract.js";

const FRONTEND_ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");

/** Every `{ path: "...", parameters: [...] }` literal in the fixtures. */
function declaredRoutes() {
  const found = [];
  const walk = (directory) => {
    for (const entry of readdirSync(directory)) {
      const candidate = join(directory, entry);
      if (statSync(candidate).isDirectory()) {
        walk(candidate);
        continue;
      }
      if (![".js", ".jsx"].includes(extname(candidate))) {
        continue;
      }
      const source = readFileSync(candidate, "utf8");
      const literal =
        /path:\s*[`"'](\/api\/v1\/[^`"']*)[`"']\s*,\s*(?:\/\/[^\n]*\n\s*)*parameters:\s*\[([^\]]*)\]/g;
      for (const match of source.matchAll(literal)) {
        found.push({
          file: candidate.slice(FRONTEND_ROOT.length + 1),
          path: match[1],
          parameters: [...match[2].matchAll(/["'`]([^"'`]+)["'`]/g)].map((name) => name[1]),
        });
      }
    }
  };
  walk(FRONTEND_ROOT);
  return found;
}

describe("frontend capability fixtures describe the served contract", () => {
  const routes = declaredRoutes();

  test("the guard actually examined the fixtures", () => {
    // A change to the fixture shape that stopped matching would otherwise
    // make every assertion below pass without reading anything. The floors
    // are well under the current counts so ordinary fixture edits do not
    // trip them, and far enough above zero that a silent miss cannot.
    expect(routes.length).toBeGreaterThanOrEqual(30);
    expect(new Set(routes.map((route) => route.file)).size).toBeGreaterThanOrEqual(6);
  });

  test("no fixture names a query parameter the API does not serve", () => {
    const invented = [];
    for (const route of routes) {
      const matching = servedPathsMatching(route.path);
      if (matching.length === 0) {
        // A route the snapshot does not declare at all; the next test owns it.
        continue;
      }
      for (const name of route.parameters) {
        // Every path the expression names must serve it: a per-source
        // template may not claim a parameter only one source declares.
        const missing = matching.filter((path) => !servedParameters(path).includes(name));
        if (missing.length > 0) {
          invented.push(`${route.file}: ${missing.join(", ")} do not serve ${name}`);
        }
      }
    }
    expect(invented).toEqual([]);
  });

  test("no fixture declares a route the API does not serve", () => {
    const unserved = routes
      .filter((route) => servedPathsMatching(route.path).length === 0)
      .map((route) => `${route.file}: ${route.path}`);
    // `/api/v1/future/measures` is deliberate: explorer-sources.test.js uses
    // it to prove a source declaring only an unknown route joins no explorer
    // tab. It is the one fixture that must name a path nothing serves.
    expect(unserved).toEqual([
      "unit/explorer-sources.test.js: /api/v1/future/measures",
    ]);
  });
});

// Covers: WEB-052 — the request guard the browser tier runs, checked here.
//
// `requestComplaint` is what every browser spec's automatic fixture applies
// to every request its page makes. Its own correctness is deterministic, so
// it is graded in the unit tier rather than only by the suite that uses it.
describe("requestComplaint", () => {
  test("passes a request whose parameters the contract declares", () => {
    expect(
      requestComplaint(
        "http://localhost:3100/api/v1/catalog/metrics?limit=10&offset=0&source_code=BLS",
      ),
    ).toBeNull();
  });

  test("names an undeclared parameter and the ones the operation accepts", () => {
    const complaint = requestComplaint(
      "http://localhost:3100/api/v1/catalog/metrics?limit=10&source_codes=BLS",
    );
    expect(complaint).toContain("source_codes");
    expect(complaint).toContain("GET /api/v1/catalog/metrics");
    // The accepted set comes from the snapshot, so the singular is named.
    expect(complaint).toContain("source_code");
  });

  test("matches a path the contract declares with a parameter in it", () => {
    expect(
      requestComplaint(
        "http://localhost:3100/api/v1/catalog/metrics/CENSUS_ACS%3AB01003_001E",
      ),
    ).toBeNull();
  });

  test("refuses a path the contract does not serve", () => {
    expect(requestComplaint("http://localhost:3100/api/v1/not-a-resource")).toContain(
      "not a path the reviewed contract serves",
    );
  });

  test("ignores a request that is not an API request", () => {
    expect(requestComplaint("http://localhost:3100/tiles/catalog?x=1")).toBeNull();
  });
});
