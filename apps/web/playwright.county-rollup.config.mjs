// Opt-in review against a deployed warehouse with the captured Wisconsin FBI
// products. Separate from map CI, whose small seed does not promise this scope.
import { defineConfig } from "@playwright/test";
import liveConfig from "./playwright.live.config.mjs";

export default defineConfig({
  ...liveConfig,
  testDir: "../../tests/frontend/deployment",
  testMatch: "county-crime-rollup.live.spec.js",
  timeout: 120_000,
});
