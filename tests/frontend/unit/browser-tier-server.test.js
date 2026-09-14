import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, expect, test } from "vitest";

// Covers: WEB-068 — the browser tier serves the build the job just made.
//
// `frontend.yml` runs `npm run build`, `check:bundle` and `check:csp`, and
// then ran the browser suite against `next dev`. A dev server compiles each
// route the first time a browser asks for it, so the first assertion on a
// route raced that compile against the suite's ten-second `expect` timeout:
// `articles.spec.js` failed on one push with `Expected: "unreadable"
// Received: "loading"` and passed on the next with nothing changed on that
// route. Neither the spec nor the component has a race.
//
// Read from the config rather than trusted, because the symptom is a flake:
// a revert to `next dev` would show up as an occasional red job months later
// rather than as a failing test now.

/** The repository root, found by walking up for the config this guards. */
function webRoot() {
  let directory = process.cwd();
  for (;;) {
    const candidate = join(directory, "apps", "web", "playwright.config.mjs");
    try {
      readFileSync(candidate);
      return join(directory, "apps", "web");
    } catch {
      const parent = dirname(directory);
      if (parent === directory) {
        throw new Error("playwright.config.mjs not found from " + process.cwd());
      }
      directory = parent;
    }
  }
}

const WEB_ROOT = webRoot();
const CONFIG = readFileSync(join(WEB_ROOT, "playwright.config.mjs"), "utf8");
const PACKAGE = JSON.parse(readFileSync(join(WEB_ROOT, "package.json"), "utf8"));

describe("the browser tier's server", () => {
  test("CI serves the production build, and only the local path compiles on demand", () => {
    // The `webServer.command` is a conditional on `process.env.CI`; the two
    // halves are read separately so neither can be dropped.
    const command = CONFIG.slice(CONFIG.indexOf("webServer"));
    const ci = command.slice(command.indexOf("process.env.CI"));
    const [ciCommand, localCommand] = [
      ci.slice(0, ci.indexOf(":")),
      ci.slice(ci.indexOf(":")),
    ];
    expect(ciCommand).toContain("next start");
    expect(ciCommand).not.toContain("next dev");
    // A developer still sees an edit without a build.
    expect(localCommand).toContain("next dev");
  });

  test("the served port is the one the suite navigates to", () => {
    // A mismatch would start a server the suite never reaches and time out
    // waiting for a URL nothing answers.
    expect(CONFIG).toContain("next start -p 3100");
    expect(CONFIG).toContain("http://localhost:3100/");
    expect(PACKAGE.scripts.start).toContain("-p 3100");
  });

  test("the workflow builds before it browses", () => {
    // `next start` serves `.next`; without the build step ahead of it the
    // tier would have nothing to serve, and the failure would read as a
    // server that never came up.
    const workflow = readFileSync(
      join(WEB_ROOT, "..", "..", ".github", "workflows", "frontend.yml"),
      "utf8",
    );
    const build = workflow.indexOf("npm run build");
    const browser = workflow.indexOf("npm run test:browser");
    expect(build).toBeGreaterThan(-1);
    expect(browser).toBeGreaterThan(build);
  });
});
