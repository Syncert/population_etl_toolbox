import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import { transformWithEsbuild } from "vite";
import path from "node:path";
import { fileURLToPath } from "node:url";

const webRoot = fileURLToPath(new URL(".", import.meta.url));

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
  plugins: [applicationJsx, react()],
  resolve: {
    alias: {
      "@testing-library/jest-dom/vitest": path.join(
        webRoot,
        "node_modules/@testing-library/jest-dom/dist/vitest.mjs",
      ),
      "@testing-library/react": path.join(
        webRoot,
        "node_modules/@testing-library/react/dist/@testing-library/react.esm.js",
      ),
    },
  },
  server: {
    fs: { allow: ["../.."] },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["../../tests/frontend/setup.js"],
    include: ["../../tests/frontend/unit/**/*.test.{js,jsx}"],
    // `lib/format` pins the locale and says nothing about the time zone, so
    // `formatDate`/`formatTime` render in the runner's. The assertions in
    // `format-and-persistence.test.js` name exact strings -- `9/16/2026`,
    // `1:05 PM` -- which are the UTC renderings, so the tier passed on CI and
    // failed on any developer machine west of UTC with an off-by-one-day
    // diff that says nothing about the code.
    //
    // Pinning it here makes the tier deterministic and makes a machine agree
    // with CI. It is deliberately not pinned in `lib/format`: its one caller
    // renders "Catalog updated", which is an instant, and an instant is
    // reasonably shown in the reader's own zone. If a calendar fact from the
    // warehouse is ever rendered through it, that is the decision to revisit,
    // and it belongs there rather than here.
    env: { TZ: "UTC" },
  },
});
