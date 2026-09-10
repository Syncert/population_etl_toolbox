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
    command: "node ./node_modules/next/dist/bin/next dev -p 3100",
    url: "http://localhost:3100/",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
