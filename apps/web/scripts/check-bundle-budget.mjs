// Route bundle budgets, measured on a controlled production build.
//
// A performance budget is only meaningful if it is measured the same way
// every time and fails loudly when crossed. This reads the production build
// manifest, sums the real byte size of every JavaScript chunk each route
// loads, and compares it to an explicit per-route budget. It measures the
// build that shipped rather than estimating from source.
//
// A route with no declared budget fails rather than passing silently: a new
// route that nobody set a threshold for is exactly the one that grows
// unnoticed.
//
// Usage: node scripts/check-bundle-budget.mjs [--update]
//   --update rewrites the budgets file from the current build, for a
//   deliberate, reviewable baseline change.

import { readFileSync, statSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const appRoot = join(here, "..");
const budgetsPath = join(here, "bundle-budgets.json");
const manifestPath = join(appRoot, ".next", "app-build-manifest.json");

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function chunkBytes(chunk) {
  try {
    return statSync(join(appRoot, ".next", chunk)).size;
  } catch {
    return 0;
  }
}

/** Total bytes of the JavaScript each route loads, deduplicated. */
export function measureRoutes(manifest) {
  const measured = {};
  for (const [route, chunks] of Object.entries(manifest.pages || {})) {
    const unique = [...new Set(chunks)];
    measured[route] = unique.reduce((total, chunk) => total + chunkBytes(chunk), 0);
  }
  return measured;
}

function main() {
  const update = process.argv.includes("--update");
  let manifest;
  try {
    manifest = readJson(manifestPath);
  } catch {
    console.error(
      "No production build found. Run `npm run build` before checking bundle budgets.",
    );
    process.exit(2);
  }

  const measured = measureRoutes(manifest);

  if (update) {
    const budgets = Object.fromEntries(
      Object.entries(measured)
        .sort(([left], [right]) => left.localeCompare(right))
        // Headroom is explicit and uniform rather than per-route judgement:
        // 15% above what shipped, rounded to a readable kilobyte.
        .map(([route, bytes]) => [route, Math.ceil((bytes * 1.15) / 1024) * 1024]),
    );
    writeFileSync(budgetsPath, `${JSON.stringify(budgets, null, 2)}\n`, "utf8");
    console.log(`Wrote ${Object.keys(budgets).length} route budgets from this build.`);
    return;
  }

  const budgets = readJson(budgetsPath);
  const failures = [];
  const rows = [];

  for (const [route, bytes] of Object.entries(measured).sort()) {
    const budget = budgets[route];
    if (budget === undefined) {
      failures.push(
        `${route}: no declared budget. Add one to scripts/bundle-budgets.json (or run --update) so this route cannot grow unnoticed.`,
      );
      continue;
    }
    rows.push(
      `${route.padEnd(24)} ${(bytes / 1024).toFixed(1).padStart(8)} kB / ${(budget / 1024).toFixed(0)} kB`,
    );
    if (bytes > budget) {
      failures.push(
        `${route}: ${(bytes / 1024).toFixed(1)} kB exceeds its ${(budget / 1024).toFixed(0)} kB budget by ${((bytes - budget) / 1024).toFixed(1)} kB.`,
      );
    }
  }

  for (const [route] of Object.entries(budgets)) {
    if (measured[route] === undefined) {
      failures.push(`${route}: has a budget but no longer appears in the build.`);
    }
  }

  console.log(rows.join("\n"));

  if (failures.length > 0) {
    console.error("\nBundle budget failures:");
    for (const failure of failures) {
      console.error(`  - ${failure}`);
    }
    process.exit(1);
  }
  console.log("\nEvery route is within its declared budget.");
}

if (process.argv[1] && process.argv[1].endsWith("check-bundle-budget.mjs")) {
  main();
}
