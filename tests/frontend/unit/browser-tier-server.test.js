import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { describe, expect, test } from "vitest";

// Covers: WEB-068 — the browser tier serves the build the job just made.
//
// CI already builds before browsing. Locally the pretest hook now builds too,
// so both paths serve the production output. A cold `next dev` server raced
// route compilation against browser navigation and produced intermittent
// aborts and timeouts on different specs under an unchanged tree.
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
  test("local and CI runs build before serving with the same production server", () => {
    const server = CONFIG.slice(CONFIG.indexOf("webServer"));
    const command = server.match(/command:\s*"([^"]+)"/)?.[1];
    expect(command).toContain("next start -p 3100");
    expect(command).not.toContain("next dev");
    expect(PACKAGE.scripts["pretest:browser"]).toBe("next build");
    expect(server).toContain("reuseExistingServer: false");
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
