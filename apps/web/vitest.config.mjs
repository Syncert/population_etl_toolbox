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
  },
});
