// The live-stack smoke tier.
//
// Separate from vitest.config.mjs on two axes that matter. It runs in `node`,
// not jsdom, because the point is a real network round trip to a deployed
// origin rather than a simulated document. And it loads no setup file: the
// unit setup installs the fixture scaffolding this tier exists to do without.
//
// Run it with SMOKE_BASE_URL pointing at a deployed origin that serves both
// /api/v1 and /tiles — the composed proxy does, exactly as a browser sees it:
//
//   SMOKE_BASE_URL=http://127.0.0.1:33001 npm run test:smoke

import { defineConfig } from "vitest/config";
import { transformWithEsbuild } from "vite";

const applicationJsx = {
  name: "application-js-as-jsx",
  enforce: "pre",
  async transform(code, id) {
    if (id.includes("/apps/web/") && id.endsWith(".js")) {
      return transformWithEsbuild(code, id, { loader: "jsx", jsx: "automatic" });
    }
    return null;
  },
};

export default defineConfig({
  plugins: [applicationJsx],
  server: {
    fs: { allow: ["../.."] },
  },
  test: {
    environment: "node",
    include: ["../../tests/frontend/smoke/**/*.smoke.test.js"],
    // One stack, one set of services: parallel files would interleave reads
    // against the same seeded warehouse for no gain.
    fileParallelism: false,
    testTimeout: 60_000,
    hookTimeout: 60_000,
  },
});
