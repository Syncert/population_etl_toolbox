import { defineConfig, devices } from "@playwright/test";

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
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: "node ./node_modules/next/dist/bin/next dev -p 3100",
    url: "http://localhost:3100/",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
