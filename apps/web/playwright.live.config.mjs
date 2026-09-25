// The live map tier (WEB-118): real Chromium with software WebGL against a
// running deployment, so MapLibre fetches real tiles and paints real pixels.
//
// Separate from playwright.config.mjs because that tier runs its own Next
// server against stubbed data, and its headless Chromium has no GL context.
// This one starts no server; point it at a stack that serves /, /api/v1, and
// /tiles from one origin:
//
//   SMOKE_BASE_URL=http://localhost:3001 npm run test:maps

import { defineConfig, devices } from "@playwright/test";

const chromiumExecutable = process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE;

export default defineConfig({
  testDir: "../../tests/frontend/live",
  testMatch: "**/*.live.spec.js",
  timeout: 240_000,
  expect: { timeout: 30_000 },
  // One deployment, one warehouse: parallel pages would contend for it.
  workers: 1,
  reporter: [["line"]],
  use: {
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    viewport: { width: 1400, height: 1000 },
  },
  projects: [
    {
      name: "chromium-gl",
      use: {
        ...devices["Desktop Chrome"],
        viewport: { width: 1400, height: 1000 },
        launchOptions: {
          ...(chromiumExecutable ? { executablePath: chromiumExecutable } : {}),
          // SwiftShader gives headless Chromium a working WebGL context.
          args: ["--use-gl=angle", "--use-angle=swiftshader", "--enable-unsafe-swiftshader"],
        },
      },
    },
  ],
});
