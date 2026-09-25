// Serve MapLibre's worker from this origin.
//
// MapLibre 6 ships its worker as a separate ES module and finds it through
// `import.meta.url`, resolving `./maplibre-gl-worker.mjs` next to its own
// file. Bundled by Next, that URL is the build machine's `file://` path, which
// MapLibre refuses, so it falls back to an empty worker URL: the browser loads
// the page itself as the worker module, the worker dies on its first line, and
// every GeoJSON and vector source waits for it forever. The map draws its
// background and nothing else, with no error on the page. MapLibre 4 inlined
// its worker as a blob, which is why nothing broke until the 4 -> 6 upgrade.
//
// The fix is MapLibre's own: `setWorkerUrl` pointed at a copy served here.
// The worker imports its siblings by relative path (`./maplibre-gl-shared.mjs`
// in 6.9), so the copy follows that import graph rather than naming files:
// a release that splits the worker differently is still served whole.
//
// `next.config.mjs` calls this, because the config is evaluated by `next dev`,
// `next build`, and `next start` alike -- including the browser tier's web
// server, which runs `next` directly and so would skip an npm lifecycle hook.

import { copyFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

/** The worker entry MapLibre looks for, and where this origin serves it. */
export const MAPLIBRE_WORKER_ENTRY = "maplibre-gl-worker.mjs";
export const MAPLIBRE_VENDOR_DIR = path.join("public", "vendor", "maplibre-gl");

const RELATIVE_IMPORT = /(?:\bfrom|\bimport)\s*["'](\.\/[^"']+)["']/g;

/** The relative module specifiers one module's source imports. */
export function relativeImports(source) {
  return [...new Set([...source.matchAll(RELATIVE_IMPORT)].map((match) => match[1]))];
}

/**
 * Copy the worker entry and every module it reaches by relative import from
 * `distDir` into `targetDir`. Returns the copied file names, entry first.
 */
export function vendorMaplibreWorker({ distDir, targetDir }) {
  const entryPath = path.join(distDir, MAPLIBRE_WORKER_ENTRY);
  if (!existsSync(entryPath)) {
    throw new Error(
      `MapLibre worker not found at ${entryPath}; install apps/web dependencies first`,
    );
  }

  mkdirSync(targetDir, { recursive: true });
  const copied = [];
  const pending = [MAPLIBRE_WORKER_ENTRY];
  while (pending.length > 0) {
    const name = pending.shift();
    if (copied.includes(name)) {
      continue;
    }
    const sourcePath = path.join(distDir, name);
    if (!existsSync(sourcePath)) {
      throw new Error(`MapLibre worker imports ./${name}, which ${distDir} does not contain`);
    }
    copyFileSync(sourcePath, path.join(targetDir, name));
    copied.push(name);
    for (const specifier of relativeImports(readFileSync(sourcePath, "utf8"))) {
      pending.push(path.posix.normalize(specifier));
    }
  }
  return copied;
}

/** Vendor the worker into this application's `public/`. */
export function vendorMaplibreWorkerForApp(webRoot) {
  return vendorMaplibreWorker({
    distDir: path.join(webRoot, "node_modules", "maplibre-gl", "dist"),
    targetDir: path.join(webRoot, MAPLIBRE_VENDOR_DIR),
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const webRoot = fileURLToPath(new URL("..", import.meta.url));
  console.log(`vendored ${vendorMaplibreWorkerForApp(webRoot).join(", ")}`);
}
