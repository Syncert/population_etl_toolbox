import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { createElement, useRef } from "react";
import { render, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  MAPLIBRE_VENDOR_DIR,
  MAPLIBRE_WORKER_ENTRY,
  relativeImports,
  vendorMaplibreWorker,
} from "../../../apps/web/scripts/vendor-maplibre-worker.mjs";
import { MAPLIBRE_WORKER_URL } from "../../../apps/web/lib/mapWiring";
import { config as middlewareConfig } from "../../../apps/web/middleware";

// MapLibre 6 resolves its worker from `import.meta.url`, which the Next build
// rewrites to a `file://` path; MapLibre then starts `new Worker("")`, the page
// itself loads as the worker module, and every map draws nothing. These pin the
// three things that keep the worker alive: the files are served, the map is
// told where, and the request for them is not rewritten on the way.

// Mocked by the file the application resolves: `maplibre-gl` is installed
// under apps/web, so the bare name does not resolve from this directory and a
// mock registered under it would intercept nothing.
const mapCalls = [];
vi.mock("../../../apps/web/node_modules/maplibre-gl/dist/maplibre-gl.mjs", () => ({
  setWorkerUrl: (url) => mapCalls.push(["setWorkerUrl", url]),
  Map: class {
    constructor() {
      mapCalls.push(["Map"]);
    }
    addControl() {}
    on() {}
    remove() {}
  },
  NavigationControl: class {},
}));

/** The repository root, found by walking up from where the tier runs. */
function findRepoRoot() {
  let directory = process.cwd();
  for (;;) {
    if (existsSync(path.join(directory, "apps", "web", "package.json"))) {
      return directory;
    }
    const parent = path.dirname(directory);
    if (parent === directory) {
      throw new Error("apps/web not found from " + process.cwd());
    }
    directory = parent;
  }
}

const repoRoot = findRepoRoot();
const webRoot = path.join(repoRoot, "apps", "web");
const scratch = [];

function scratchDir() {
  const dir = mkdtempSync(path.join(tmpdir(), "maplibre-worker-"));
  scratch.push(dir);
  return dir;
}

afterEach(() => {
  while (scratch.length > 0) {
    rmSync(scratch.pop(), { recursive: true, force: true });
  }
  mapCalls.length = 0;
});

describe("vendoring the MapLibre worker", () => {
  it("copies the worker entry and every module it reaches by relative import", () => {
    const distDir = scratchDir();
    writeFileSync(path.join(distDir, MAPLIBRE_WORKER_ENTRY), 'import{a}from"./shared.mjs";');
    writeFileSync(path.join(distDir, "shared.mjs"), 'import "./deeper.mjs";export const a=1;');
    writeFileSync(path.join(distDir, "deeper.mjs"), "export {};");
    writeFileSync(path.join(distDir, "maplibre-gl.mjs"), "the main bundle, not the worker's");
    const targetDir = path.join(scratchDir(), "vendor");

    const copied = vendorMaplibreWorker({ distDir, targetDir });

    expect(copied).toEqual([MAPLIBRE_WORKER_ENTRY, "shared.mjs", "deeper.mjs"]);
    expect(readdirSync(targetDir).sort()).toEqual(
      ["deeper.mjs", MAPLIBRE_WORKER_ENTRY, "shared.mjs"].sort(),
    );
  });

  it("refuses a worker whose imported sibling is missing rather than serving half of it", () => {
    const distDir = scratchDir();
    writeFileSync(path.join(distDir, MAPLIBRE_WORKER_ENTRY), 'import "./gone.mjs";');

    expect(() => vendorMaplibreWorker({ distDir, targetDir: scratchDir() })).toThrow(
      /imports \.\/gone\.mjs/,
    );
  });

  it("refuses a missing worker entry", () => {
    expect(() => vendorMaplibreWorker({ distDir: scratchDir(), targetDir: scratchDir() })).toThrow(
      /MapLibre worker not found/,
    );
  });

  it("vendors the installed MapLibre release whole", () => {
    const distDir = path.join(webRoot, "node_modules", "maplibre-gl", "dist");
    const targetDir = scratchDir();

    const copied = vendorMaplibreWorker({ distDir, targetDir });

    expect(copied[0]).toBe(MAPLIBRE_WORKER_ENTRY);
    for (const name of copied) {
      for (const specifier of relativeImports(readFileSync(path.join(targetDir, name), "utf8"))) {
        expect(copied).toContain(path.posix.normalize(specifier));
      }
    }
  });
});

describe("where the map finds its worker", () => {
  it("is the vendored entry, at the path this origin serves it from", () => {
    const servedPath = `/${path.posix.join(
      ...MAPLIBRE_VENDOR_DIR.split(path.sep).slice(1),
      MAPLIBRE_WORKER_ENTRY,
    )}`;
    expect(MAPLIBRE_WORKER_URL).toBe(servedPath);
  });

  it("is set before the first map is constructed", async () => {
    const { useMapLibre } = await import("../../../apps/web/components/useMapLibre");
    function Host() {
      const ref = useRef(null);
      useMapLibre(ref, true);
      return createElement("div", { ref });
    }

    render(createElement(Host));

    await waitFor(() => expect(mapCalls.some(([call]) => call === "Map")).toBe(true));
    expect(mapCalls[0]).toEqual(["setWorkerUrl", MAPLIBRE_WORKER_URL]);
  });

  it("is not handed to the middleware, whose script policy would refuse the worker's imports", () => {
    const [{ source }] = middlewareConfig.matcher;
    const matcher = new RegExp(`^${source}$`);

    expect(matcher.test(MAPLIBRE_WORKER_URL)).toBe(false);
    expect(matcher.test("/explore")).toBe(true);
  });

  it("ships in the runtime image, which copies public/ beside the standalone server", () => {
    const dockerfile = readFileSync(path.join(repoRoot, "infra", "docker", "Dockerfile.web"), "utf8");
    const runner = dockerfile.slice(dockerfile.lastIndexOf("AS runner"));

    expect(runner).toMatch(/COPY --from=builder\s+--chown=\S+\s+\/app\/public\s+\.\/public/);
  });
});

describe("the vendored copy is generated, not committed", () => {
  it("is ignored by git", () => {
    const gitignore = readFileSync(path.join(repoRoot, ".gitignore"), "utf8");
    expect(gitignore.split(/\r?\n/)).toContain("apps/web/public/vendor/");
  });
});
