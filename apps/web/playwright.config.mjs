import { defineConfig, devices } from "@playwright/test";

// Sandboxed environments that pre-install a Chromium build (instead of the
// exact revision this @playwright/test version downloads) can point the
// suite at it; unset, browser resolution is unchanged.
const chromiumExecutable = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE;

export default defineConfig({
  testDir: "../../tests/frontend/browser",
  timeout: 30_000,
  expect: { timeout: 10_000 },
  reporter: [["line"], ["html", { open: "never" }]],
  use: {
    baseURL: "http://localhost:3100",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        ...(chromiumExecutable
          ? { launchOptions: { executablePath: chromiumExecutable } }
          : {}),
      },
    },
  ],
  webServer: {
    // In CI the tier grades the build the job just made. `next dev` compiles
    // a route the first time a browser asks for it, so the first assertion
    // against a route raced that compile against the ten-second `expect`
    // timeout -- `articles.spec.js` failed on one push with
    // `Expected: "unreadable" Received: "loading"` and passed on the next
    // with nothing changed on that route. The job already runs `npm run
    // build`, `check:bundle` and `check:csp`; testing a dev server after
    // that grades a build nobody deploys (WEB-068).
    //
    // Locally the dev server stays, so an edit is visible without a build.
    command: process.env.CI
      ? "node ./node_modules/next/dist/bin/next start -p 3100"
      : "node ./node_modules/next/dist/bin/next dev -p 3100",
    url: "http://localhost:3100/",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
